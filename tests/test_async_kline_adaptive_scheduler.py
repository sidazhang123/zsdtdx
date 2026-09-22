"""
模块：`tests/test_async_kline_adaptive_scheduler.py`。

职责：
1. 离线锁定按地址数与总进程数计算的 std/ex 进程配额。
2. 覆盖低进程数、混合 std/ex、拥塞退避和单侧探测。

边界：
1. 不访问真实网络，不创建真实行情连接。
2. Windows spawn 的进程池通路由现有用户验收测试覆盖，本文件聚焦可控状态机。
"""

from __future__ import annotations

import threading
import time
import sys
from unittest.mock import MagicMock

import pytest

from zsdtdx.adaptive_scheduler import (
    AdaptiveConcurrencyController,
    compute_side_process_budget,
)


def test_side_process_budget_uses_host_count_and_total_processes():
    """进程多于地址时按 H×per_host 封顶；地址不少于进程时用满 C。"""
    assert compute_side_process_budget(40, 11, 4) == 40
    assert compute_side_process_budget(40, 2, 4) == 8
    assert compute_side_process_budget(4, 2, 4) == 4
    assert compute_side_process_budget(4, 11, 4) == 4
    assert compute_side_process_budget(40, 0, 4) == 0


def test_ex_budget_caps_immediately_without_slow_start():
    """扩展侧按公式立即给出配额，不从 1 往上加。"""
    controller = AdaptiveConcurrencyController(
        max_processes=40,
        inproc_limit=3,
        processes_per_host_ex=4,
    )
    controller.update_hosts("ex", [("e1", 7720), ("e2", 7730)])
    snap = controller.snapshot()
    assert snap["side_process_budget"]["ex"] == 8
    assert snap["sides"]["ex"]["e1:7720"]["process_cap"] == 4
    assert snap["sides"]["ex"]["e2:7730"]["process_cap"] == 4

    permits = [controller.acquire("ex", max_units=3, timeout=0) for _ in range(8)]
    assert {permit.units for permit in permits} == {3}
    assert {permit.host for permit in permits} == {("e1", 7720), ("e2", 7730)}
    with pytest.raises(TimeoutError):
        controller.acquire("ex", max_units=3, timeout=0)
    assert controller.snapshot()["inflight_processes_by_source"]["ex"] == 8
    for permit in permits:
        controller.release(permit, success=True)
    grown = controller.snapshot()["sides"]["ex"]
    assert grown["e1:7720"]["process_cap"] == 4
    assert grown["e2:7730"]["process_cap"] == 4


def test_std_uses_full_process_count_when_hosts_are_plentiful():
    """标准地址足够时一开始就用满总进程，不慢启动。"""
    controller = AdaptiveConcurrencyController(
        max_processes=40,
        inproc_limit=3,
        processes_per_host_std=4,
    )
    controller.update_hosts("std", [(f"s{index}", 7709) for index in range(11)])
    assert controller.snapshot()["side_process_budget"]["std"] == 40
    permits = [controller.acquire("std", max_units=3, timeout=0) for _ in range(40)]
    with pytest.raises(TimeoutError):
        controller.acquire("std", max_units=3, timeout=0)
    assert controller.snapshot()["inflight_processes"] == 40
    for permit in permits:
        controller.release(permit, success=True)


def test_small_process_pool_is_not_cut_by_ex_formula():
    """总进程只有 4 个时，扩展侧预算仍是 4，不被地址公式再砍一刀。"""
    controller = AdaptiveConcurrencyController(
        max_processes=4,
        inproc_limit=1,
        processes_per_host_ex=4,
    )
    controller.update_hosts("ex", [("e1", 7720), ("e2", 7730)])
    assert controller.snapshot()["side_process_budget"]["ex"] == 4
    permits = [controller.acquire("ex", max_units=1, timeout=0) for _ in range(4)]
    with pytest.raises(TimeoutError):
        controller.acquire("ex", max_units=1, timeout=0)
    for permit in permits:
        controller.release(permit, success=True)


def test_std_is_not_blocked_when_ex_budget_is_full():
    """扩展侧配额用尽时，标准侧仍可继续准入。"""
    controller = AdaptiveConcurrencyController(
        max_processes=40,
        inproc_limit=1,
        processes_per_host_std=4,
        processes_per_host_ex=4,
    )
    controller.update_hosts("std", [(f"s{index}", 7709) for index in range(11)])
    controller.update_hosts("ex", [("e1", 7720), ("e2", 7730)])
    ex_permits = [controller.acquire("ex", max_units=1, timeout=0) for _ in range(8)]
    std_permit = controller.acquire("std", max_units=1, timeout=0)
    assert std_permit.source == "std"
    snap = controller.snapshot()
    assert snap["inflight_processes_by_source"]["ex"] == 8
    assert snap["inflight_processes_by_source"]["std"] == 1
    controller.release(std_permit, success=True)
    for permit in ex_permits:
        controller.release(permit, success=True)


def test_congestion_halves_only_failed_host_and_enters_cooldown():
    """拥塞只降低对应 host，不连带降低同侧其它 host。"""
    controller = AdaptiveConcurrencyController(
        max_processes=8,
        inproc_limit=3,
        processes_per_host_ex=4,
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
    assert state[failed_key]["process_cap"] == 2
    assert state[failed_key]["cooldown_until"] > time.monotonic()
    assert state[other_key]["process_cap"] == 4


def test_cooldown_then_success_climbs_back_to_pre_congestion_ceiling():
    """冷却结束后不立刻抬配额；每次成功 +1，直到回到拥塞前上限。"""
    controller = AdaptiveConcurrencyController(
        max_processes=8,
        inproc_limit=1,
        processes_per_host_ex=4,
        decrease_factor=0.5,
        cooldown_seconds=0.05,
    )
    controller.update_hosts("ex", [("e1", 7720)])
    permit = controller.acquire("ex", max_units=1, timeout=0)
    controller.release(permit, success=False, congested=True)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["process_cap"] == 2
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["failed_ceiling"] == 4
    time.sleep(0.08)
    first = controller.acquire("ex", max_units=1, timeout=0)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["process_cap"] == 2
    controller.release(first, success=True)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["process_cap"] == 3
    second = controller.acquire("ex", max_units=1, timeout=0)
    controller.release(second, success=True)
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["process_cap"] == 4
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["failed_ceiling"] is None


def test_recovery_does_not_exceed_pre_congestion_ceiling():
    """回升只回到拥塞前上限，不会靠成功涨到超过该上限。"""
    controller = AdaptiveConcurrencyController(
        max_processes=8,
        inproc_limit=1,
        processes_per_host_ex=4,
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
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["process_cap"] == 4
    assert controller.snapshot()["sides"]["ex"]["e1:7720"]["failed_ceiling"] is None


def test_removed_inflight_host_is_not_selected_again():
    """探测快照去掉仍在飞的地址后，不得再向该地址派发新 bundle。"""
    controller = AdaptiveConcurrencyController(
        max_processes=8,
        inproc_limit=1,
        processes_per_host_ex=4,
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
def test_process_limit_is_hard_ceiling(workers):
    """不同机器规模下在飞 bundle 均不超过 CPU 推导硬上限。"""
    controller = AdaptiveConcurrencyController(
        max_processes=workers,
        inproc_limit=1,
        processes_per_host_std=4,
    )
    controller.update_hosts(
        "std", [(f"s{index}", 7709) for index in range(workers + 2)]
    )
    permits = [
        controller.acquire("std", max_units=1, timeout=0) for _ in range(workers)
    ]
    with pytest.raises(TimeoutError):
        controller.acquire("std", max_units=1, timeout=0)
    assert controller.snapshot()["inflight_processes"] == workers
    for permit in permits:
        controller.release(permit, success=True)


def test_multiple_callers_share_global_process_budget():
    """两个调用线程共享同一预算，释放后等待方才能继续。"""
    controller = AdaptiveConcurrencyController(max_processes=1, inproc_limit=1)
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


def test_stock_and_index_chunks_keep_route_homogeneous():
    """股票/指数父进程 chunk 均固化 route_source，bundle 不混侧。"""
    import zsdtdx.parallel_fetcher as pf

    fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
    fetcher.num_processes = 4
    stock_tasks = [
        {
            "code": "600000",
            "freq": "d",
            "start_time": "2026-01-01",
            "end_time": "2026-01-02",
        },
        {
            "code": "00700",
            "freq": "d",
            "start_time": "2026-01-01",
            "end_time": "2026-01-02",
        },
    ]
    normalized_stock = [pf._normalize_task_payload(item) for item in stock_tasks]
    chunks = fetcher._build_task_chunks(normalized_stock)
    bundles = fetcher._build_chunk_bundles(chunks, 3)
    assert {chunk.route_source for chunk in chunks} == {"std", "ex"}
    assert [bundle.route_source for bundle in bundles] == ["std", "ex"]
    assert all(
        {chunk.route_source for chunk in bundle.chunks} == {bundle.route_source}
        for bundle in bundles
    )

    index_task = pf._normalize_index_task_payload(
        {
            "index_name": "测试扩展指数",
            "freq": "d",
            "start_time": "2026-01-01",
            "end_time": "2026-01-02",
            "_index_route_source": "ex",
            "_index_route_market": 62,
            "_index_route_code": "000905",
        }
    )
    assert index_task["_route_source"] == "ex"


def test_worker_context_does_not_preconnect_for_worker_client(monkeypatch):
    """worker 进入上下文时不调用双侧预连接。"""
    from zsdtdx.unified_client import UnifiedTdxClient

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    context = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.config_path = "unused"
    client._presorted_hosts_snapshot = {}
    client._worker_client_flag = True
    context.preconnect_on_enter = True
    context._worker_client_flag = True
    context._warmup_connections = MagicMock()
    monkeypatch.setattr(
        "zsdtdx.unified_client.UnifiedTdxClient",
        MagicMock(return_value=context),
    )
    client._push_context_client = MagicMock()

    entered = UnifiedTdxClient.__enter__(client)
    assert entered is context
    context._warmup_connections.assert_not_called()


def test_stock_recovery_uses_explicit_ex_route():
    """港股 stock chunk 连接异常时恢复目标为 ex。"""
    from zsdtdx.parallel_fetcher import _infer_recover_target_from_chunk

    prep = {
        "task_kind": "stock",
        "normalized_tasks": [{"code": "00700", "freq": "d", "_route_source": "ex"}],
    }
    assert _infer_recover_target_from_chunk(prep) == "ex"


def test_side_specific_probe_does_not_touch_other_pool(monkeypatch):
    """按侧 ensure 只探测任务实际需要的地址池。"""
    import zsdtdx.unified_client as uc

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
            "standard": ["1.1.1.1:7709"],
            "extended": ["2.2.2.2:7720"],
        },
        "pool": {"probe_timeout": 0.01},
    }
    uc._ensure_availability_hosts_cache(
        cfg=cfg, required_sources={"extended"}, force=True
    )
    assert calls == ["extended"]


def test_submit_failure_rolls_back_adaptive_permit(monkeypatch):
    """进程池 submit 抛错时必须归还 permit，不能永久缩减全局容量。"""
    import zsdtdx.parallel_fetcher as pf

    controller = AdaptiveConcurrencyController(max_processes=2, inproc_limit=1)
    controller.update_hosts("std", [("s1", 7709)])

    class _BrokenExecutor:
        def submit(self, *_args, **_kwargs):
            raise RuntimeError("pool is broken")

    fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
    fetcher.num_processes = 2
    fetcher.task_chunk_inproc_coroutine_workers = 1
    fetcher.task_chunk_cache_min_tasks = 2
    fetcher.chunk_reconnect_on_unavailable = True
    fetcher.chunk_timeout_seconds = 1
    fetcher.chunk_retry_max_attempts = 0
    fetcher.bundle_watchdog_grace_seconds = 0
    fetcher.force_recycle_on_timeout = True
    fetcher.adaptive_concurrency_enabled = True
    fetcher.adaptive_processes_per_host_std = 4
    fetcher.adaptive_processes_per_host_ex = 4
    fetcher.adaptive_decrease_factor = 0.5
    fetcher.adaptive_cooldown_seconds = 5.0
    fetcher.config_path = "unused"
    fetcher.config = {}

    monkeypatch.setattr(
        "zsdtdx.unified_client._ensure_availability_hosts_cache",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        pf, "_get_global_adaptive_controller", lambda **_kwargs: controller
    )
    monkeypatch.setattr(
        pf, "_get_global_process_pool", lambda _workers: _BrokenExecutor()
    )

    task = pf._normalize_task_payload(
        {
            "code": "600000",
            "freq": "d",
            "start_time": "2026-01-01",
            "end_time": "2026-01-02",
        }
    )
    with pytest.raises(RuntimeError, match="pool is broken"):
        list(fetcher._iter_task_payloads_parallel_chunked([task]))
    assert controller.snapshot()["inflight_processes"] == 0


def test_full_host_slots_do_not_abort_remaining_bundles(monkeypatch):
    """host 槽位占满时，内置 TimeoutError 不能中断后续 bundle。"""
    import zsdtdx.parallel_fetcher as pf
    from concurrent.futures import Future

    controller = AdaptiveConcurrencyController(max_processes=4, inproc_limit=1)
    controller.update_hosts("ex", [("e1", 7720)])

    class _ImmediateExecutor:
        def submit(self, *_args, **_kwargs):
            future = Future()
            future.set_result({"chunk_reports": []})
            return future

    fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
    fetcher.num_processes = 4
    fetcher.task_chunk_inproc_coroutine_workers = 1
    fetcher.task_chunk_cache_min_tasks = 2
    fetcher.chunk_reconnect_on_unavailable = False
    fetcher.chunk_timeout_seconds = 1
    fetcher.chunk_retry_max_attempts = 0
    fetcher.bundle_watchdog_grace_seconds = 0
    fetcher.adaptive_concurrency_enabled = True
    fetcher.adaptive_processes_per_host_std = 4
    fetcher.adaptive_processes_per_host_ex = 4
    fetcher.adaptive_decrease_factor = 0.5
    fetcher.adaptive_cooldown_seconds = 0
    fetcher.config_path = "unused"
    fetcher.config = {}

    monkeypatch.setattr(
        "zsdtdx.unified_client._ensure_availability_hosts_cache",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        pf, "_get_global_adaptive_controller", lambda **_kwargs: controller
    )
    monkeypatch.setattr(
        pf, "_get_global_process_pool", lambda _workers: _ImmediateExecutor()
    )
    tasks = [
        pf._normalize_task_payload(
            {
                "code": "00700",
                "freq": freq,
                "start_time": "2026-09-17",
                "end_time": "2026-09-18",
            }
        )
        for freq in ("15", "d")
    ]
    payloads = list(fetcher._iter_task_payloads_parallel_chunked(tasks))
    assert len(payloads) == 2
    assert controller.snapshot()["inflight_processes"] == 0


def test_force_restart_resets_adaptive_state(monkeypatch):
    """强制终止旧 worker 时同步清空其 permit 与 host 学习状态。"""
    import zsdtdx.parallel_fetcher as pf

    controller = AdaptiveConcurrencyController(max_processes=2, inproc_limit=1)
    controller.update_hosts("std", [("s1", 7709)])
    controller.acquire("std", max_units=1, timeout=0)
    monkeypatch.setattr(pf, "_global_adaptive_controller", controller)
    monkeypatch.setattr(pf, "_global_process_pool", None)
    monkeypatch.setattr(pf, "_global_pool_max_workers", 2)

    pf.force_restart_parallel_fetcher(prewarm=False)
    assert controller.snapshot()["inflight_processes"] == 0
    assert controller.snapshot()["sides"] == {"std": {}, "ex": {}}
    assert pf._global_adaptive_controller is None


@pytest.mark.skipif(sys.platform != "win32", reason="仅验证 Windows spawn")
def test_windows_spawn_process_only_warmup():
    """真实 spawn 拉起 worker，但预热阶段不建立 std/ex 行情 socket。"""
    import zsdtdx.parallel_fetcher as pf

    pf.destroy_parallel_fetcher()
    try:
        summary = pf.prewarm_parallel_fetcher(
            require_all_workers=True,
            timeout_seconds=20,
            max_rounds=4,
            target_workers=2,
        )
        assert summary["target_workers"] == 2
        assert summary["warmed_workers"] >= 2
        assert all(
            item.get("std_ok") is False and item.get("ex_ok") is False
            for item in summary.get("worker_states", [])
        )
    finally:
        pf.destroy_parallel_fetcher()


def test_chunk_attempt_reports_actual_failover_host(monkeypatch):
    """worker 将线程实际活跃 host 回传，父进程可识别首选站 failover。"""
    import zsdtdx.parallel_fetcher as pf

    pool = MagicMock()
    pool.get_active_host.return_value = "s2:7709"
    context = MagicMock()
    context.std_pool = pool
    context.get_stock_kline_rows_for_chunk_tasks.return_value = {
        "results": [],
        "chunk_hit_tasks": 0,
        "chunk_network_page_calls": 1,
    }
    monkeypatch.setattr(pf, "_ensure_worker_client_context", lambda: context)
    monkeypatch.setattr(pf, "_apply_chunk_socket_read_deadline", lambda *_a, **_k: None)
    monkeypatch.setattr(
        pf, "_restore_chunk_socket_read_timeout", lambda *_a, **_k: None
    )

    result = pf._fetch_one_chunk_fetch_attempt(
        {
            "task_kind": "stock",
            "normalized_tasks": [],
            "enable_cache": False,
            "qfq": True,
            "chunk_timeout": 1.0,
            "route_source": "std",
            "preferred_host": ("s1", 7709),
        }
    )
    assert result["_active_host"] == "s2:7709"


def test_watchdog_waits_for_worker_stop_before_releasing_permit(monkeypatch):
    """无法取消的运行中 future 不得在 watchdog 返回失败时提前释放容量。"""
    import zsdtdx.parallel_fetcher as pf
    from concurrent.futures import Future

    controller = AdaptiveConcurrencyController(max_processes=1, inproc_limit=1)
    controller.update_hosts("std", [("s1", 7709)])
    held_future = Future()
    assert held_future.set_running_or_notify_cancel()

    class _Executor:
        def submit(self, *_args, **_kwargs):
            return held_future

    fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
    fetcher.num_processes = 1
    fetcher.task_chunk_inproc_coroutine_workers = 1
    fetcher.task_chunk_cache_min_tasks = 2
    fetcher.chunk_reconnect_on_unavailable = True
    fetcher.chunk_timeout_seconds = 0.05
    fetcher.chunk_retry_max_attempts = 0
    fetcher.bundle_watchdog_grace_seconds = 0
    fetcher.adaptive_concurrency_enabled = True
    fetcher.adaptive_processes_per_host_std = 4
    fetcher.adaptive_processes_per_host_ex = 4
    fetcher.adaptive_decrease_factor = 0.5
    fetcher.adaptive_cooldown_seconds = 0
    fetcher.auto_prewarm_on_async = False
    fetcher.config_path = "unused"
    fetcher.config = {}

    monkeypatch.setattr(
        "zsdtdx.unified_client._ensure_availability_hosts_cache",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        pf, "_get_global_adaptive_controller", lambda **_kwargs: controller
    )
    monkeypatch.setattr(pf, "_get_global_process_pool", lambda _workers: _Executor())

    tasks = [
        {
            "code": code,
            "freq": "d",
            "start_time": "2026-01-01",
            "end_time": "2026-01-02",
        }
        for code in ("600000", "000001")
    ]
    job = fetcher.fetch_stock_tasks_async(tasks=tasks)
    payloads = job.result(timeout=3)
    assert {payload["error"] for payload in payloads} == {
        "bundle watchdog timeout",
        "bundle dispatch aborted after watchdog timeout",
    }
    assert controller.snapshot()["inflight_processes"] == 1

    held_future.set_result({"chunk_reports": []})
    assert controller.snapshot()["inflight_processes"] == 0


def test_retry_success_does_not_mark_chunk_congested():
    """超时后重试成功时，chunk 报告不得再带拥塞标记。"""
    import asyncio
    from unittest.mock import patch

    import zsdtdx.parallel_fetcher as pf

    calls = {"n": 0}

    async def flaky(_prep):
        calls["n"] += 1
        if calls["n"] == 1:
            raise asyncio.TimeoutError("chunk attempt timeout")
        return {
            "results": [
                {
                    "task": {},
                    "rows": [{"datetime": "2026-01-02 00:00:00"}],
                    "error": None,
                }
            ],
            "chunk_hit_tasks": 0,
            "chunk_network_page_calls": 1,
            "_active_host": "s1:7709",
        }

    chunk_payload = {
        "chunk_id": "retry-ok",
        "task_kind": "stock",
        "tasks": [
            {
                "code": "600000",
                "freq": "d",
                "start_time": "2026-01-01",
                "end_time": "2026-01-02",
            }
        ],
        "chunk_timeout_seconds": 1.0,
        "chunk_retry_max_attempts": 1,
    }
    with patch.object(
        pf, "_fetch_one_chunk_attempt_with_timeout_async", side_effect=flaky
    ):
        with patch.object(
            pf,
            "_recover_worker_pools_current_thread",
            return_value={"std": {}, "ex": {}},
        ):
            report = asyncio.run(pf._fetch_one_task_chunk_async(chunk_payload))

    assert calls["n"] == 2
    assert report.get("congested") is False
    assert report.get("active_host") == "s1:7709"
    payloads = list(report.get("payloads") or [])
    assert len(payloads) == 1
    assert not str(payloads[0].get("error") or "").strip()


def test_inflight_multiplier_widens_std_admission_only(monkeypatch):
    """提交窗口为进程数×倍率；标准侧配额随倍率放大，扩展侧保持配置值。"""
    import zsdtdx.parallel_fetcher as pf
    from concurrent.futures import Future

    pf._reset_global_adaptive_controller()
    seen: dict = {}
    original = pf._get_global_adaptive_controller

    def _wrap(**kwargs):
        seen.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(pf, "_get_global_adaptive_controller", _wrap)
    monkeypatch.setattr(
        "zsdtdx.unified_client._ensure_availability_hosts_cache",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        "zsdtdx.unified_client.get_probe_result_cache",
        lambda: {
            "standard": [(f"10.0.0.{index}", 7709) for index in range(10)],
            "extended": [("114.117.72.207", 7720), ("118.31.28.30", 7730)],
        },
    )

    class _DoneExecutor:
        def submit(self, *_args, **_kwargs):
            future = Future()
            future.set_result({"chunk_reports": []})
            return future

    monkeypatch.setattr(
        pf, "_get_global_process_pool", lambda _workers: _DoneExecutor()
    )
    fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
    fetcher.num_processes = 40
    fetcher.task_chunk_max_inflight_multiplier = 2
    fetcher.task_chunk_inproc_coroutine_workers = 3
    fetcher.task_chunk_cache_min_tasks = 2
    fetcher.chunk_reconnect_on_unavailable = False
    fetcher.chunk_timeout_seconds = 1.0
    fetcher.chunk_retry_max_attempts = 0
    fetcher.bundle_watchdog_grace_seconds = 0
    fetcher.adaptive_concurrency_enabled = True
    fetcher.adaptive_processes_per_host_std = 4
    fetcher.adaptive_processes_per_host_ex = 4
    fetcher.adaptive_decrease_factor = 0.75
    fetcher.adaptive_cooldown_seconds = 1.5
    fetcher.auto_prewarm_on_async = False
    fetcher.config_path = "unused"
    fetcher.config = {}
    fetcher._ensure_async_prewarm = lambda *args, **kwargs: None

    try:
        tasks = [
            {
                "code": "600000",
                "freq": "d",
                "start_time": "2026-01-01",
                "end_time": "2026-01-02",
            }
        ]
        payloads = fetcher.fetch_stock_tasks_async(tasks=tasks).result(timeout=5)
        assert seen["max_processes"] == 80
        assert seen["processes_per_host_std"] == 8
        assert seen["processes_per_host_ex"] == 4
        controller = pf._global_adaptive_controller
        assert controller is not None
        snapshot = controller.snapshot()
        assert snapshot["max_processes"] == 80
        assert snapshot["processes_per_host"]["std"] == 8
        assert snapshot["processes_per_host"]["ex"] == 4
        assert snapshot["side_process_budget"]["std"] == 80
        assert snapshot["side_process_budget"]["ex"] == 8
        assert payloads
    finally:
        pf._reset_global_adaptive_controller()


def test_successful_failover_does_not_reduce_host_cap(monkeypatch):
    """数据已取回且只是换了站点时，父进程释放凭证不得降低原地址配额。"""
    import zsdtdx.parallel_fetcher as pf
    from concurrent.futures import Future

    pf._reset_global_adaptive_controller()
    monkeypatch.setattr(
        "zsdtdx.unified_client._ensure_availability_hosts_cache",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        "zsdtdx.unified_client.get_probe_result_cache",
        lambda: {"standard": [("10.0.0.1", 7709)], "extended": []},
    )

    class _FailoverExecutor:
        def submit(self, _fn, bundle_payload):
            preferred = list(bundle_payload.get("preferred_host") or [])
            reports = []
            for chunk in list(bundle_payload.get("chunks") or []):
                tasks = list(chunk.get("tasks") or [])
                reports.append(
                    {
                        "chunk_id": str(chunk.get("chunk_id") or ""),
                        "congested": False,
                        "active_host": "9.9.9.9:7709",
                        "payloads": [
                            {
                                "task": tasks[0] if tasks else {},
                                "rows": [{"datetime": "2026-01-02 00:00:00"}],
                                "error": "",
                                "worker_pid": 1,
                            }
                        ],
                    }
                )
            assert preferred == ["10.0.0.1", 7709]
            future = Future()
            future.set_result({"chunk_reports": reports})
            return future

    monkeypatch.setattr(
        pf, "_get_global_process_pool", lambda _workers: _FailoverExecutor()
    )
    fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
    fetcher.num_processes = 1
    fetcher.task_chunk_max_inflight_multiplier = 1
    fetcher.task_chunk_inproc_coroutine_workers = 1
    fetcher.task_chunk_cache_min_tasks = 2
    fetcher.chunk_reconnect_on_unavailable = False
    fetcher.chunk_timeout_seconds = 1.0
    fetcher.chunk_retry_max_attempts = 0
    fetcher.bundle_watchdog_grace_seconds = 0
    fetcher.adaptive_concurrency_enabled = True
    fetcher.adaptive_processes_per_host_std = 4
    fetcher.adaptive_processes_per_host_ex = 4
    fetcher.adaptive_decrease_factor = 0.5
    fetcher.adaptive_cooldown_seconds = 5.0
    fetcher.auto_prewarm_on_async = False
    fetcher.config_path = "unused"
    fetcher.config = {}
    fetcher._ensure_async_prewarm = lambda *args, **kwargs: None

    try:
        payloads = fetcher.fetch_stock_tasks_async(
            tasks=[
                {
                    "code": "600000",
                    "freq": "d",
                    "start_time": "2026-01-01",
                    "end_time": "2026-01-02",
                }
            ]
        ).result(timeout=5)
        assert payloads
        assert not str(payloads[0].get("error") or "").strip()
        controller = pf._global_adaptive_controller
        assert controller is not None
        host_state = controller.snapshot()["sides"]["std"]["10.0.0.1:7709"]
        assert host_state["process_cap"] == 4
        assert host_state["process_inflight"] == 0
        assert host_state["failed_ceiling"] is None
        assert host_state["cooldown_until"] == 0
    finally:
        pf._reset_global_adaptive_controller()


def test_one_overdue_bundle_does_not_abort_running_siblings(monkeypatch):
    """一条 bundle 超过时限时只失败这一条，仍在跑的其它 bundle 继续返回。"""
    import zsdtdx.parallel_fetcher as pf
    from concurrent.futures import Future

    controller = AdaptiveConcurrencyController(
        max_processes=2,
        inproc_limit=1,
        processes_per_host_std=4,
        processes_per_host_ex=4,
        decrease_factor=0.5,
        cooldown_seconds=0,
    )
    controller.update_hosts("std", [("s1", 7709)])
    monkeypatch.setattr(
        pf, "_get_global_adaptive_controller", lambda **_kwargs: controller
    )
    monkeypatch.setattr(
        "zsdtdx.unified_client._ensure_availability_hosts_cache",
        lambda **_kwargs: {},
    )

    hung = Future()
    assert hung.set_running_or_notify_cancel()
    calls = {"n": 0}

    class _Executor:
        def submit(self, *_args, **_kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return hung
            finished = Future()
            finished.set_result({"chunk_reports": []})
            return finished

    monkeypatch.setattr(pf, "_get_global_process_pool", lambda _workers: _Executor())
    fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
    fetcher.num_processes = 2
    fetcher.task_chunk_max_inflight_multiplier = 1
    fetcher.task_chunk_inproc_coroutine_workers = 1
    fetcher.task_chunk_cache_min_tasks = 2
    fetcher.chunk_reconnect_on_unavailable = False
    fetcher.chunk_timeout_seconds = 0.05
    fetcher.chunk_retry_max_attempts = 0
    fetcher.bundle_watchdog_grace_seconds = 0
    fetcher.adaptive_concurrency_enabled = True
    fetcher.adaptive_processes_per_host_std = 4
    fetcher.adaptive_processes_per_host_ex = 4
    fetcher.adaptive_decrease_factor = 0.5
    fetcher.adaptive_cooldown_seconds = 0
    fetcher.auto_prewarm_on_async = False
    fetcher.config_path = "unused"
    fetcher.config = {}
    fetcher._ensure_async_prewarm = lambda *args, **kwargs: None

    try:
        payloads = fetcher.fetch_stock_tasks_async(
            tasks=[
                {
                    "code": code,
                    "freq": "d",
                    "start_time": "2026-01-01",
                    "end_time": "2026-01-02",
                }
                for code in ("600000", "000001")
            ]
        ).result(timeout=3)
        errors = {str(item.get("error") or "") for item in payloads}
        assert "bundle watchdog timeout" in errors
        assert "bundle dispatch aborted after watchdog timeout" not in errors
        assert "missing_chunk_payload" in errors
        assert controller.snapshot()["inflight_processes"] == 1
        hung.set_result({"chunk_reports": []})
        assert controller.snapshot()["inflight_processes"] == 0
    finally:
        pf._reset_global_adaptive_controller()


def test_chunk_timeout_returns_while_socket_and_recover_block():
    """阻塞读和重连都卡住时，chunk 仍在单次时限内返回失败，而不是一直等。"""
    import asyncio
    import socket
    import threading
    from unittest.mock import patch

    import zsdtdx.parallel_fetcher as pf
    from zsdtdx.unified_client import _bind_attempt_socket

    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    held: list = []

    def _accept() -> None:
        while True:
            try:
                conn, _addr = server.accept()
            except OSError:
                return
            held.append(conn)

    threading.Thread(target=_accept, daemon=True).start()

    def _hang_attempt(_prep):
        sock = socket.socket()
        sock.settimeout(None)
        _bind_attempt_socket(sock)
        sock.connect(("127.0.0.1", port))
        sock.recv(8)
        return {"results": []}

    def _hang_recover(*_args, **_kwargs):
        sock = socket.socket()
        sock.settimeout(None)
        _bind_attempt_socket(sock)
        sock.connect(("127.0.0.1", port))
        sock.recv(8)
        return {"std": {}, "ex": {}}

    chunk_payload = {
        "chunk_id": "timeout-returns",
        "task_kind": "stock",
        "tasks": [
            {
                "code": "600000",
                "freq": "d",
                "start_time": "2026-01-01",
                "end_time": "2026-01-02",
            }
        ],
        "chunk_timeout_seconds": 1.0,
        "chunk_retry_max_attempts": 0,
    }
    started = time.monotonic()
    try:
        with (
            patch.object(
                pf, "_fetch_one_chunk_fetch_attempt", side_effect=_hang_attempt
            ),
            patch.object(
                pf, "_recover_worker_pools_current_thread", side_effect=_hang_recover
            ),
        ):
            report = asyncio.run(pf._fetch_one_task_chunk_async(chunk_payload))
        elapsed = time.monotonic() - started
    finally:
        server.close()
        for conn in held:
            try:
                conn.close()
            except Exception:
                pass
    # 单次尝试 1 秒，随后的重连也被同一时限打断，不能进入 30 秒的阻塞重连。
    assert elapsed < 3.5
    assert report.get("congested") is True
    payloads = list(report.get("payloads") or [])
    assert payloads
    assert str(payloads[0].get("error") or "").strip()


def test_aborted_attempt_does_not_connect_the_next_host():
    """超时放弃后，不再对下一台地址建连。"""
    import socket

    from zsdtdx.unified_client import (
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
    """无可选地址且没有在飞 bundle 时，不得一直等待。"""
    controller = AdaptiveConcurrencyController(max_processes=4, inproc_limit=1)
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


def test_allocate_host_worker_counts_caps_each_host_and_total():
    """进程在建池时按站摊开，单站不超过上限，总数不超过进程数。"""
    from zsdtdx.parallel_fetcher import allocate_host_worker_counts

    hosts = [(f"10.0.0.{index}", 7709) for index in range(11)]
    wide = allocate_host_worker_counts(hosts, 40, 4)
    assert sum(wide.values()) == 40
    assert max(wide.values()) == 4
    assert min(wide.values()) == 3
    narrow = allocate_host_worker_counts(hosts[:2], 40, 4)
    assert narrow == {("10.0.0.0", 7709): 4, ("10.0.0.1", 7709): 4}
    few = allocate_host_worker_counts(hosts, 2, 4)
    assert few == {("10.0.0.0", 7709): 1, ("10.0.0.1", 7709): 1}


def test_std_and_ex_homes_share_the_same_process_slots():
    """标准侧和扩展侧的自家站都落在同一批进程槽位上，名额仍由按站分配决定。"""
    from zsdtdx.parallel_fetcher import (
        _build_worker_host_slot_assignments,
        allocate_host_worker_counts,
    )

    std_hosts = [(f"10.0.0.{index}", 7709) for index in range(11)]
    ex_hosts = [("114.117.72.207", 7720), ("118.31.28.30", 7730)]
    std_plan = allocate_host_worker_counts(std_hosts, 40, 4)
    ex_plan = allocate_host_worker_counts(ex_hosts, 40, 4)
    slots = _build_worker_host_slot_assignments(
        {"standard": std_hosts, "extended": ex_hosts}, 40, 4, 4
    )

    assert len(slots) == 40
    std_homes: dict = {}
    ex_homes: dict = {}
    for item in slots:
        std_list = list(item["standard"])
        ex_list = list(item["extended"])
        assert set(std_list) == set(std_hosts)
        std_homes[std_list[0]] = std_homes.get(std_list[0], 0) + 1
        if ex_list:
            assert set(ex_list) == set(ex_hosts)
            ex_homes[ex_list[0]] = ex_homes.get(ex_list[0], 0) + 1
    assert std_homes == std_plan
    assert ex_homes == ex_plan
    assert sum(ex_homes.values()) == 8


def test_chunk_reports_mark_congested_ignores_payload_error_text():
    """任务错误里的 timeout 文本不能单独降配额。"""
    from zsdtdx.parallel_fetcher import (
        _chunk_reports_mark_congested,
        _is_transport_congestion_error,
    )

    assert _is_transport_congestion_error("timed out") is True
    assert _is_transport_congestion_error("读取超时") is True
    assert _is_transport_congestion_error("no_data") is False
    assert (
        _chunk_reports_mark_congested(
            {
                "chunk_reports": [
                    {"congested": False, "payloads": [{"error": "timed out"}]}
                ]
            }
        )
        is False
    )
    assert (
        _chunk_reports_mark_congested(
            {"chunk_reports": [{"congested": True, "payloads": [{"error": ""}]}]}
        )
        is True
    )


def test_socket_timed_out_marks_chunk_congested():
    """内层 socket 的 timed out 在重试耗尽后仍记为拥塞。"""
    import asyncio
    from unittest.mock import patch

    from zsdtdx import parallel_fetcher as pf

    chunk_payload = {
        "chunk_id": "c-timed-out",
        "task_kind": "stock",
        "tasks": [
            {
                "code": "600000",
                "freq": "d",
                "start_time": "2026-01-01",
                "end_time": "2026-01-02",
            }
        ],
        "chunk_timeout_seconds": 1.0,
        "chunk_retry_max_attempts": 0,
    }

    async def _raise_timed_out(_prep):
        raise OSError("timed out")

    with (
        patch.object(
            pf,
            "_fetch_one_chunk_attempt_with_timeout_async",
            side_effect=_raise_timed_out,
        ),
        patch.object(
            pf,
            "_recover_worker_pools_current_thread",
            return_value={"std": {}, "ex": {}},
        ),
    ):
        report = asyncio.run(pf._fetch_one_task_chunk_async(chunk_payload))
    assert report.get("congested") is True
    assert "timed out" in str(report["payloads"][0].get("error") or "")
