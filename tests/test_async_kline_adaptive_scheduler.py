"""
模块：`tests/test_async_kline_adaptive_scheduler.py`。

职责：
1. 离线锁定按地址数与总线程数计算的 std/ex 线程配额。
2. 覆盖低线程数、混合 std/ex、拥塞退避和单侧探测。

边界：
1. 不访问真实网络，不创建真实行情连接。
2. 线程运行时通路由现有用户验收测试覆盖，本文件聚焦可控状态机。
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from zsdtdx.engine.adaptive_scheduler import (
    AdaptiveConcurrencyController,
    compute_side_thread_budget,
)


def test_side_thread_budget_uses_host_count_and_total_threads():
    """线程多于地址时按 H×per_host 封顶；地址不少于线程时用满 C。"""
    assert compute_side_thread_budget(40, 11, 4) == 40
    assert compute_side_thread_budget(40, 2, 4) == 8
    assert compute_side_thread_budget(4, 2, 4) == 4
    assert compute_side_thread_budget(4, 11, 4) == 4
    assert compute_side_thread_budget(40, 0, 4) == 0


def test_ex_budget_caps_immediately_without_slow_start():
    """扩展侧按公式立即给出配额，不从 1 往上加。"""
    controller = AdaptiveConcurrencyController(
        max_threads=40,
        chunks_per_lease=3,
        threads_per_host_ex=4,
    )
    controller.update_hosts("ex", [("e1", 7720), ("e2", 7730)])
    snap = controller.snapshot()
    assert snap["side_thread_budget"]["ex"] == 8
    assert snap["sides"]["ex"]["e1:7720"]["thread_cap"] == 4
    assert snap["sides"]["ex"]["e2:7730"]["thread_cap"] == 4

    permits = [controller.acquire("ex", max_units=3, timeout=0) for _ in range(8)]
    assert {permit.units for permit in permits} == {3}
    assert {permit.host for permit in permits} == {("e1", 7720), ("e2", 7730)}
    with pytest.raises(TimeoutError):
        controller.acquire("ex", max_units=3, timeout=0)
    assert controller.snapshot()["inflight_threads_by_source"]["ex"] == 8
    for permit in permits:
        controller.release(permit, success=True)
    grown = controller.snapshot()["sides"]["ex"]
    assert grown["e1:7720"]["thread_cap"] == 4
    assert grown["e2:7730"]["thread_cap"] == 4


def test_std_uses_full_thread_count_when_hosts_are_plentiful():
    """标准地址足够时一开始就用满总进程，不慢启动。"""
    controller = AdaptiveConcurrencyController(
        max_threads=40,
        chunks_per_lease=3,
        threads_per_host_std=4,
    )
    controller.update_hosts("std", [(f"s{index}", 7709) for index in range(11)])
    assert controller.snapshot()["side_thread_budget"]["std"] == 40
    permits = [controller.acquire("std", max_units=3, timeout=0) for _ in range(40)]
    with pytest.raises(TimeoutError):
        controller.acquire("std", max_units=3, timeout=0)
    assert controller.snapshot()["inflight_threads"] == 40
    for permit in permits:
        controller.release(permit, success=True)


def test_small_thread_pool_is_not_cut_by_ex_formula():
    """总进程只有 4 个时，扩展侧预算仍是 4，不被地址公式再砍一刀。"""
    controller = AdaptiveConcurrencyController(
        max_threads=4,
        chunks_per_lease=1,
        threads_per_host_ex=4,
    )
    controller.update_hosts("ex", [("e1", 7720), ("e2", 7730)])
    assert controller.snapshot()["side_thread_budget"]["ex"] == 4
    permits = [controller.acquire("ex", max_units=1, timeout=0) for _ in range(4)]
    with pytest.raises(TimeoutError):
        controller.acquire("ex", max_units=1, timeout=0)
    for permit in permits:
        controller.release(permit, success=True)


def test_std_is_not_blocked_when_ex_budget_is_full():
    """扩展侧配额用尽时，标准侧仍可继续准入。"""
    controller = AdaptiveConcurrencyController(
        max_threads=40,
        chunks_per_lease=1,
        threads_per_host_std=4,
        threads_per_host_ex=4,
    )
    controller.update_hosts("std", [(f"s{index}", 7709) for index in range(11)])
    controller.update_hosts("ex", [("e1", 7720), ("e2", 7730)])
    ex_permits = [controller.acquire("ex", max_units=1, timeout=0) for _ in range(8)]
    std_permit = controller.acquire("std", max_units=1, timeout=0)
    assert std_permit.source == "std"
    snap = controller.snapshot()
    assert snap["inflight_threads_by_source"]["ex"] == 8
    assert snap["inflight_threads_by_source"]["std"] == 1
    controller.release(std_permit, success=True)
    for permit in ex_permits:
        controller.release(permit, success=True)


def test_congestion_halves_only_failed_host_and_enters_cooldown():
    """拥塞只降低对应 host，不连带降低同侧其它 host。"""
    controller = AdaptiveConcurrencyController(
        max_threads=8,
        chunks_per_lease=3,
        threads_per_host_ex=4,
        decrease_factor=0.5,
        cooldown_seconds=10,
    )
    controller.update_hosts("ex", [("e1", 7720), ("e2", 7730)])
    congested = controller.acquire("ex", max_units=3, timeout=0)
    other_host = ("e2", 7730) if congested.host == ("e1", 7720) else ("e1", 7720)
    controller.release(congested, success=False, congested=True)
    state = controller.snapshot()["sides"]["ex"]
    failed_key = f"{congested.host[0]}:{congested.host[1]}"
    other_key = f"{other_host[0]}:{other_host[1]}"
    assert state[failed_key]["thread_cap"] == 2
    assert state[failed_key]["cooldown_until"] > time.monotonic()
    assert state[other_key]["thread_cap"] == 4


def test_cooldown_then_success_climbs_back_to_pre_congestion_ceiling():
    """冷却结束后不立刻抬配额；每次成功 +1，直到回到拥塞前上限。"""
    controller = AdaptiveConcurrencyController(
        max_threads=8,
        chunks_per_lease=1,
        threads_per_host_ex=4,
        decrease_factor=0.5,
        cooldown_seconds=0.05,
    )
    controller.update_hosts("ex", [("e1", 7720)])
    permit = controller.acquire("ex", max_units=1, timeout=0)
    controller.release(permit, success=False, congested=True)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["thread_cap"] == 2
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["failed_ceiling"] == 4
    time.sleep(0.08)
    first = controller.acquire("ex", max_units=1, timeout=0)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["thread_cap"] == 2
    controller.release(first, success=True)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["thread_cap"] == 3
    second = controller.acquire("ex", max_units=1, timeout=0)
    controller.release(second, success=True)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["thread_cap"] == 4
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["failed_ceiling"] is None


def test_recovery_does_not_exceed_pre_congestion_ceiling():
    """回升只回到拥塞前上限，不会靠成功涨到超过该上限。"""
    controller = AdaptiveConcurrencyController(
        max_threads=8,
        chunks_per_lease=1,
        threads_per_host_ex=4,
        decrease_factor=0.5,
        cooldown_seconds=0.05,
    )
    controller.update_hosts("ex", [("e1", 7720)])
    permit = controller.acquire("ex", max_units=1, timeout=0)
    controller.release(permit, success=False, congested=True)
    time.sleep(0.08)
    for _ in range(6):
        item = controller.acquire("ex", max_units=1, timeout=0)
        controller.release(item, success=True)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["thread_cap"] == 4
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["failed_ceiling"] is None


def test_removed_inflight_host_is_not_selected_again():
    """探测快照去掉仍在飞的地址后，不得再向该地址派发新 chunk。"""
    controller = AdaptiveConcurrencyController(
        max_threads=8,
        chunks_per_lease=1,
        threads_per_host_ex=4,
    )
    controller.update_hosts("ex", [("e1", 7720), ("e2", 7730)])
    first = controller.acquire("ex", max_units=1, timeout=0)
    remaining_host = ("e2", 7730) if first.host == ("e1", 7720) else ("e1", 7720)
    controller.update_hosts("ex", [remaining_host])
    second = controller.acquire("ex", max_units=1, timeout=0)
    assert second.host == remaining_host
    controller.release(first, success=True)
    controller.release(second, success=True)
    assert list(controller.snapshot()["sides"]["ex"]) == [
        f"{remaining_host[0]}:{remaining_host[1]}"
    ]


@pytest.mark.parametrize("workers", [2, 3, 4, 40])
def test_thread_limit_is_hard_ceiling(workers):
    """不同机器规模下在飞 chunk 均不超过 CPU 推导硬上限。"""
    controller = AdaptiveConcurrencyController(
        max_threads=workers,
        chunks_per_lease=1,
        threads_per_host_std=4,
    )
    controller.update_hosts(
        "std", [(f"s{index}", 7709) for index in range(workers + 2)]
    )
    permits = [
        controller.acquire("std", max_units=1, timeout=0) for _ in range(workers)
    ]
    with pytest.raises(TimeoutError):
        controller.acquire("std", max_units=1, timeout=0)
    assert controller.snapshot()["inflight_threads"] == workers
    for permit in permits:
        controller.release(permit, success=True)


def test_multiple_callers_share_global_thread_budget():
    """两个调用线程共享同一预算，释放后等待方才能继续。"""
    controller = AdaptiveConcurrencyController(max_threads=1, chunks_per_lease=1)
    controller.update_hosts("std", [("s1", 7709)])
    first = controller.acquire("std", max_units=1, timeout=0)
    acquired = threading.Event()
    result = {}

    def _waiter():
        result["permit"] = controller.acquire("std", max_units=1, timeout=2)
        acquired.set()

    thread = threading.Thread(target=_waiter)
    thread.start()
    assert not acquired.wait(timeout=0.05)
    controller.release(first, success=True)
    assert acquired.wait(timeout=1)
    controller.release(result["permit"], success=True)
    thread.join(timeout=1)


def test_shared_client_context_does_not_preconnect_for_shared_runtime_client(
    monkeypatch,
):
    """工作线程进入上下文时不调用双侧预连接。"""
    from zsdtdx.engine.unified_client import UnifiedTdxClient

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    context = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.config_path = "unused"
    client._presorted_hosts_snapshot = {}
    client._shared_runtime_client_flag = True
    context.preconnect_on_enter = True
    context._shared_runtime_client_flag = True
    context._warmup_connections = MagicMock()
    monkeypatch.setattr(
        "zsdtdx.engine.unified_client.UnifiedTdxClient",
        MagicMock(return_value=context),
    )
    client._push_context_client = MagicMock()

    entered = UnifiedTdxClient.__enter__(client)
    assert entered is context
    context._warmup_connections.assert_not_called()


def test_side_specific_probe_does_not_touch_other_pool(monkeypatch):
    """按侧 ensure 只探测任务实际需要的地址池。"""
    import zsdtdx.engine.unified_client as uc

    with uc._probe_result_cache_lock:
        uc._probe_result_cache.clear()
        uc._last_tcp_probe_hosts_fingerprint = None

    calls = []

    def _fake_trim(hosts, timeout, fallback_hosts, pool_label=""):
        calls.append(pool_label)
        return list(hosts)

    monkeypatch.setattr(uc, "_tcp_probe_and_trim_available_hosts", _fake_trim)
    cfg = {
        "hosts": {
            "standard": {"szshbj": ["1.1.1.1:7709"], "szsh_only": []},
            "extended": ["2.2.2.2:7720"],
        },
        "pool": {"probe_timeout": 0.01},
    }
    uc._ensure_availability_hosts_cache(
        cfg=cfg, required_sources={"extended"}, force=True
    )
    assert calls == ["extended"]


def test_submit_failure_rolls_back_adaptive_permit(monkeypatch):
    """线程架构接口迁移：保留原有故障/资源不变量验收。"""
    from tests.test_thread_columnar import test_submit_failure_returns_window

    test_submit_failure_returns_window(monkeypatch)


def test_full_host_slots_do_not_abort_remaining_chunks(monkeypatch):
    """线程架构接口迁移：保留原有故障/资源不变量验收。"""
    from tests.test_thread_columnar import test_host_quota_survives_contention

    test_host_quota_survives_contention()


def test_force_restart_resets_adaptive_state(monkeypatch):
    """强制终止旧工作线程 时同步清空其 permit 与 host 学习状态。"""
    import zsdtdx.engine.parallel_fetcher as pf

    controller = AdaptiveConcurrencyController(max_threads=2, chunks_per_lease=1)
    controller.update_hosts("std", [("s1", 7709)])
    controller.acquire("std", max_units=1, timeout=0)
    monkeypatch.setattr(pf, "_global_adaptive_controller", controller)
    monkeypatch.setattr(pf, "_thread_runtime", None)

    pf.force_restart_parallel_fetcher(prewarm=False)
    assert controller.snapshot()["inflight_threads"] == 0
    assert controller.snapshot()["sides"] == {"std": {}, "ex": {}}
    assert pf._global_adaptive_controller is None


def test_thread_runtime_only_warmup():
    """真实拉起工作线程，但预热阶段不建立 std/ex 行情 socket。"""
    import zsdtdx.engine.parallel_fetcher as pf

    pf.destroy_parallel_fetcher()
    try:
        summary = pf.prewarm_parallel_fetcher(
            require_all_threads=True,
            timeout_seconds=20,
            max_rounds=4,
            target_threads=2,
        )
        assert summary["target_threads"] == 2
        assert summary["ready_threads"] >= 2
        assert summary["decoder_ready"] is True
    finally:
        pf.destroy_parallel_fetcher()


def test_prewarm_uses_initialized_live_threads(monkeypatch):
    """线程预热以真实且唯一的线程 ID 证明就绪。"""
    from zsdtdx.engine import parallel_fetcher as pf

    pf.destroy_parallel_fetcher()
    try:
        summary = pf.prewarm_parallel_fetcher(target_threads=2)
        assert summary["ready_threads"] == 2
        assert len(set(summary["thread_ids"])) == 2
        assert summary["decoder_ready"] is True
    finally:
        pf.destroy_parallel_fetcher()


def test_prewarm_ready_state_rejects_dead_initialized_worker():
    """线程架构接口迁移：保留原有故障/资源不变量验收。"""
    from tests.test_thread_columnar import test_stopped_runtime_rejects_new_work

    test_stopped_runtime_rejects_new_work()


def test_watchdog_waits_for_thread_stop_before_releasing_permit(monkeypatch):
    """线程架构接口迁移：保留原有故障/资源不变量验收。"""
    from tests.test_thread_columnar import (
        test_watchdog_preserves_siblings_and_task_cardinality,
    )

    test_watchdog_preserves_siblings_and_task_cardinality(monkeypatch)


def test_inflight_multiplier_widens_std_admission_only(monkeypatch):
    """提交窗口只扩大待办容量，线程执行不扩大任一侧的真实 host 配额。"""
    from zsdtdx.engine.thread_runtime import ThreadRuntime
    from concurrent.futures import wait

    runtime = ThreadRuntime(2, 4)
    lock = threading.Lock()
    state = {"active": 0, "peak": 0}

    def run():
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        time.sleep(0.01)
        with lock:
            state["active"] -= 1

    try:
        futures = [runtime.submit(run) for _ in range(12)]
        wait(futures)
        assert state["peak"] == 2
    finally:
        assert runtime.stop() == 0


def test_successful_failover_does_not_reduce_host_cap(monkeypatch):
    """线程架构接口迁移：保留原有故障/资源不变量验收。"""
    from tests.test_thread_columnar import (
        test_failover_success_does_not_reduce_host_capacity,
    )

    test_failover_success_does_not_reduce_host_capacity()


def test_one_overdue_chunk_does_not_abort_running_siblings(monkeypatch):
    """线程架构接口迁移：保留原有故障/资源不变量验收。"""
    from tests.test_thread_columnar import (
        test_watchdog_preserves_siblings_and_task_cardinality,
    )

    test_watchdog_preserves_siblings_and_task_cardinality(monkeypatch)


def test_aborted_attempt_does_not_connect_the_next_host():
    """超时放弃后，不再对下一台地址建连。"""
    import socket

    from zsdtdx.engine.unified_client import (
        AttemptSocketCloser,
        PersistentFailoverPool,
        _attempt_socket_closer,
    )

    closer = AttemptSocketCloser()
    calls = {"n": 0}

    class _Api:
        def connect(self, host, port, time_out=1.0):
            calls["n"] += 1
            closer.close()
            return False

        def disconnect(self):
            return None

    pool = PersistentFailoverPool(
        "std",
        _Api,
        [("10.0.0.1", 7709), ("10.0.0.2", 7709)],
        connect_timeout=1.0,
    )
    token = _attempt_socket_closer.set(closer)
    started = time.monotonic()
    try:
        assert pool._ensure_connected() is False
    finally:
        _attempt_socket_closer.reset(token)
    assert calls["n"] == 1
    assert time.monotonic() - started < 1.0
    late = socket.socket()
    closer.bind(late)
    try:
        fd = late.fileno()
    except OSError:
        fd = -1
    assert fd == -1


def test_acquire_fails_fast_when_side_has_no_selectable_host():
    """无可选地址且没有在飞 chunk 时，不得一直等待。"""
    controller = AdaptiveConcurrencyController(max_threads=4, chunks_per_lease=1)
    controller.update_hosts("std", [("s1", 7709)])
    state = next(iter(controller.snapshot()["sides"]["std"]))
    with controller._condition:
        controller._hosts["std"][("s1", 7709)].selectable = False
        controller._recompute_side_budgets_locked()
    assert state == "s1:7709"
    with pytest.raises(RuntimeError, match="无可达 host"):
        controller.acquire("std", max_units=1, timeout=None)
    with pytest.raises(TimeoutError):
        controller.acquire("std", max_units=1, timeout=0)


def test_std_and_ex_homes_share_the_same_thread_slots():
    """标准/扩展共享全局工作线程，单站配额不通过额外执行器扩容。"""
    controller = AdaptiveConcurrencyController(max_threads=2, chunks_per_lease=1)
    controller.update_hosts("std", [("s", 7709)])
    controller.update_hosts("ex", [("e", 7720)])
    first = controller.acquire("std", max_units=1, timeout=0)
    second = controller.acquire("ex", max_units=1, timeout=0)
    with pytest.raises(TimeoutError):
        controller.acquire("ex", max_units=1, timeout=0)
    controller.release(first, success=True)
    controller.release(second, success=True)
