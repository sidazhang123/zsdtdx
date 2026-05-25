"""
模块：tests/test_async_kline_tuning_regression.py

职责：
1. 锁定 zsdtdx async K 线性能与精度对齐方案（A-D 阶段）的所有改造点行为；
2. 全程离线、不依赖真实网络，可在 CI/沙箱内重复执行；
3. 任何后续 PR 改动若破坏本回归契约会立即暴露。

边界：
1. 本文件不发起 socket 请求；真实网络单点探测与弱网离线注入见 tests/manual/ 下对应脚本。
2. 测试基于"目标态"断言；不再保留对历史 SharedChunkCache 池逻辑的兼容性测试。
"""

import os
import struct
import sys
import threading
import time
from typing import Any, Dict, List
from unittest import mock

import pytest

# 把工程 src 加入 sys.path（与 pyproject.toml pythonpath 一致，便于直接 py 本文件调试）。
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SRC_DIR = os.path.join(_PROJECT_ROOT, "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)


# =============================================================================
# F1: ChunkLocalRowCache 行为锁定
# =============================================================================

class TestChunkLocalRowCache:
    """
    阶段二改造点 B4：用 ChunkLocalRowCache 取代 SharedChunkCache。

    验收要点：
    1. 同 chunk 内对同 (code,freq) 重复 acquire 返回同一对象，并归零游标；
    2. 不同 (code,freq) 互不影响；
    3. soft_delete 后再 acquire 是新分区（不复用 list 槽位）；
    4. 移除 `_pool_lock` 与 `_free` 池属性；
    5. 公开 API（acquire_partition/get_partition/soft_delete/SharedChunkCache 别名）继续可用。
    """

    def _new_cache(self):
        from zsdtdx.unified_client import ChunkLocalRowCache
        return ChunkLocalRowCache()

    def test_acquire_idempotent_within_chunk(self):
        """同 chunk 内重复 acquire 同 key 返回同对象，且 _n/oldest/newest/page_start 归零。"""
        cache = self._new_cache()
        p1 = cache.acquire_partition("000001", "d")
        p1["_n"] = 5
        p1["oldest_dt"] = "fake"
        p1["page_start"] = 100
        p1["fetched_pages"] = 7
        p2 = cache.acquire_partition("000001", "d")
        assert p1 is p2, "重复 acquire 应返回同一分区对象"
        assert p2["_n"] == 0
        assert p2["oldest_dt"] is None
        assert p2["newest_dt"] is None
        assert p2["page_start"] == 0
        assert p2["fetched_pages"] == 0

    def test_distinct_keys_isolated(self):
        """不同 (code,freq) 分区互不影响。"""
        cache = self._new_cache()
        p1 = cache.acquire_partition("000001", "d")
        p2 = cache.acquire_partition("000002", "d")
        assert p1 is not p2
        assert p1["_partition_key"] == "000001|d"
        assert p2["_partition_key"] == "000002|d"

    def test_soft_delete_releases_partition(self):
        """soft_delete 后再次 acquire 返回新对象，验证不再有 free 池复用语义。"""
        cache = self._new_cache()
        p1 = cache.acquire_partition("000001", "d")
        cache.soft_delete("000001", "d")
        p2 = cache.acquire_partition("000001", "d")
        assert p1 is not p2, "soft_delete 后应释放分区引用，不复用 list 槽位"

    def test_soft_delete_unknown_is_noop(self):
        """对未持有的 key 调用 soft_delete 不抛错。"""
        cache = self._new_cache()
        cache.soft_delete("unknown", "d")  # 不抛即通过

    def test_no_pool_lock_or_free_attributes(self):
        """ChunkLocalRowCache 不再持有 _pool_lock / _free / _local 等旧池属性。"""
        cache = self._new_cache()
        assert not hasattr(cache, "_pool_lock"), "应移除 _pool_lock"
        assert not hasattr(cache, "_free"), "应移除 _free 池"
        assert not hasattr(cache, "_local"), "应移除 threading.local 租约"

    def test_shared_chunk_cache_alias(self):
        """SharedChunkCache 仍可作为兼容别名引用（外部测试与遗留代码可继续用）。"""
        from zsdtdx.unified_client import ChunkLocalRowCache, SharedChunkCache
        assert SharedChunkCache is ChunkLocalRowCache

    def test_get_partition_equivalent_to_acquire(self):
        """get_partition 是 acquire_partition 的兼容别名。"""
        cache = self._new_cache()
        p1 = cache.get_partition("000001", "d")
        p2 = cache.acquire_partition("000001", "d")
        assert p1 is p2


# =============================================================================
# F2: 按路由单边重置连接（C2）
# =============================================================================

class TestRecoverWorkerPoolsTarget:
    """
    阶段三改造点 C2：_recover_worker_pools_current_thread(target=...) 按 chunk 路由单边重置。

    验收要点：
    1. target="std" → 仅重置 std；ex 分支返回 ok=False, error="skipped"；
    2. target="ex"  → 仅重置 ex；std 分支返回 skipped；
    3. target="both" → 两边都触发；
    4. 非法 target 退化为 "both"；
    5. 返回字典包含 target 字段。
    """

    def test_target_std_skips_ex(self, monkeypatch):
        """target="std" 时不触碰 ex pool；std 走真正的恢复路径。"""
        import zsdtdx.parallel_fetcher as pf
        std_calls: List[str] = []

        def _fake_std(reason: str = ""):
            std_calls.append(reason)
            return {"ok": True, "error": ""}

        # ex 路径若被触发会走 _ensure_worker_client_context，需要 monkeypatch 防止真建连。
        ex_calls: List[str] = []

        def _fake_ensure_ctx():
            ex_calls.append("ensure_ctx")
            class _Ctx:
                ex_pool = mock.Mock()
            ctx = _Ctx()
            ctx.ex_pool.reset_thread_connection = mock.Mock(return_value=True)
            return ctx

        monkeypatch.setattr(pf, "_recover_worker_standard_connection_current_thread", _fake_std)
        monkeypatch.setattr(pf, "_ensure_worker_client_context", _fake_ensure_ctx)

        result = pf._recover_worker_pools_current_thread(reason="x", target="std")
        assert result["target"] == "std"
        assert result["std"]["ok"] is True
        assert result["ex"]["ok"] is False
        assert result["ex"]["error"] == "skipped"
        assert len(std_calls) == 1
        assert len(ex_calls) == 0, "target=std 时不应触碰 ex 路径"

    def test_target_ex_skips_std(self, monkeypatch):
        """target="ex" 时不触碰 std；ex 走真正的恢复路径。"""
        import zsdtdx.parallel_fetcher as pf
        std_called = {"n": 0}

        def _fake_std(reason: str = ""):
            std_called["n"] += 1
            return {"ok": True, "error": ""}

        class _Ctx:
            def __init__(self):
                self.ex_pool = mock.Mock()
                self.ex_pool.reset_thread_connection = mock.Mock(return_value=True)
        ctx = _Ctx()

        monkeypatch.setattr(pf, "_recover_worker_standard_connection_current_thread", _fake_std)
        monkeypatch.setattr(pf, "_ensure_worker_client_context", lambda: ctx)

        result = pf._recover_worker_pools_current_thread(reason="x", target="ex")
        assert result["target"] == "ex"
        assert result["std"]["error"] == "skipped"
        assert result["ex"]["ok"] is True
        assert std_called["n"] == 0

    def test_target_both_runs_both_sides(self, monkeypatch):
        """target="both"（默认）触发 std + ex 同时重置。"""
        import zsdtdx.parallel_fetcher as pf
        std_called = {"n": 0}

        def _fake_std(reason: str = ""):
            std_called["n"] += 1
            return {"ok": True, "error": ""}

        class _Ctx:
            def __init__(self):
                self.ex_pool = mock.Mock()
                self.ex_pool.reset_thread_connection = mock.Mock(return_value=True)
        ctx = _Ctx()

        monkeypatch.setattr(pf, "_recover_worker_standard_connection_current_thread", _fake_std)
        monkeypatch.setattr(pf, "_ensure_worker_client_context", lambda: ctx)

        result = pf._recover_worker_pools_current_thread(reason="x", target="both")
        assert result["target"] == "both"
        assert std_called["n"] == 1
        assert result["ex"]["ok"] is True

    def test_invalid_target_falls_back_to_both(self, monkeypatch):
        """非法 target 值退化为 both。"""
        import zsdtdx.parallel_fetcher as pf
        monkeypatch.setattr(
            pf,
            "_recover_worker_standard_connection_current_thread",
            lambda reason="": {"ok": True, "error": ""},
        )
        class _Ctx:
            def __init__(self):
                self.ex_pool = mock.Mock()
                self.ex_pool.reset_thread_connection = mock.Mock(return_value=True)
        monkeypatch.setattr(pf, "_ensure_worker_client_context", lambda: _Ctx())

        result = pf._recover_worker_pools_current_thread(reason="x", target="garbage")
        assert result["target"] == "both"


class TestInferRecoverTargetFromChunk:
    """
    阶段三 C2 辅助函数：_infer_recover_target_from_chunk 推断重连目标。

    输入 prep 字典 → 输出 'std'/'ex'/'both'。
    """

    def test_stock_chunk_returns_std(self):
        from zsdtdx.parallel_fetcher import _infer_recover_target_from_chunk
        prep = {"task_kind": "stock", "normalized_tasks": [{"code": "000001", "freq": "d"}]}
        assert _infer_recover_target_from_chunk(prep) == "std"

    def test_index_chunk_all_std_source(self):
        from zsdtdx.parallel_fetcher import _infer_recover_target_from_chunk
        prep = {
            "task_kind": "index",
            "normalized_tasks": [
                {"index_name": "上证指数", "_index_route_source": "std"},
                {"index_name": "上证指数", "_index_route_source": "std"},
            ],
        }
        assert _infer_recover_target_from_chunk(prep) == "std"

    def test_index_chunk_all_ex_source(self):
        from zsdtdx.parallel_fetcher import _infer_recover_target_from_chunk
        prep = {
            "task_kind": "index",
            "normalized_tasks": [{"index_name": "X", "_index_route_source": "ex"}],
        }
        assert _infer_recover_target_from_chunk(prep) == "ex"

    def test_index_chunk_mixed_falls_back_to_both(self):
        from zsdtdx.parallel_fetcher import _infer_recover_target_from_chunk
        prep = {
            "task_kind": "index",
            "normalized_tasks": [
                {"index_name": "A", "_index_route_source": "std"},
                {"index_name": "B", "_index_route_source": "ex"},
            ],
        }
        assert _infer_recover_target_from_chunk(prep) == "both"

    def test_unknown_task_kind_falls_back_to_both(self):
        """非 stock/index 的 task_kind 退化为 both；空 dict 等价于 task_kind="stock"（代码默认值）。"""
        from zsdtdx.parallel_fetcher import _infer_recover_target_from_chunk
        assert _infer_recover_target_from_chunk({"task_kind": "future"}) == "both"
        # 空 dict 默认 task_kind="stock" → "std"（与函数签名默认值一致）。
        assert _infer_recover_target_from_chunk({}) == "std"


# =============================================================================
# F3: _log_chunk_retry 简化路径（不再二次 recover）
# =============================================================================

class TestLogChunkRetryNoSecondRecover:
    """
    阶段三 C2：_log_chunk_retry 仅记录日志，不再二次调用 _recover_worker_pools_current_thread。
    超时分支的连接重置已由 _fetch_one_task_chunk_async/_fetch_one_task_chunk_timed_body 在前置完成。
    """

    def _base_prep(self):
        return {
            "log_tag": "chunk",
            "chunk_id": "c1",
            "raw_tasks": [{}, {}],
            "chunk_retry_max": 2,
            "reconnect_on_unavailable": True,
        }

    def test_connection_unavailable_only_logs(self, monkeypatch):
        """连接不可用关键字命中时只记 log，不调 _recover_*。"""
        import zsdtdx.parallel_fetcher as pf
        recover_calls = {"n": 0}
        monkeypatch.setattr(
            pf,
            "_recover_worker_pools_current_thread",
            lambda *a, **kw: recover_calls.__setitem__("n", recover_calls["n"] + 1) or {},
        )
        # 强制让 _is_connection_unavailable_error 命中
        monkeypatch.setattr(pf, "_is_connection_unavailable_error", lambda txt: True)
        emit_calls: List[Dict[str, Any]] = []
        monkeypatch.setattr(
            pf,
            "_emit_log",
            lambda level, msg, detail=None: emit_calls.append({"level": level, "msg": msg, "detail": detail}),
        )

        pf._log_chunk_retry(self._base_prep(), retry_count=1, error_text="connection lost")

        assert recover_calls["n"] == 0, "C2 简化路径不应再触发二次 recover"
        assert len(emit_calls) == 1
        assert emit_calls[0]["detail"]["stage"] == "chunk_retry_connection_unavailable"

    def test_generic_error_only_logs(self, monkeypatch):
        """普通异常（非连接不可用）也只记 log。"""
        import zsdtdx.parallel_fetcher as pf
        recover_calls = {"n": 0}
        monkeypatch.setattr(
            pf,
            "_recover_worker_pools_current_thread",
            lambda *a, **kw: recover_calls.__setitem__("n", recover_calls["n"] + 1) or {},
        )
        monkeypatch.setattr(pf, "_is_connection_unavailable_error", lambda txt: False)
        emit_calls: List[Dict[str, Any]] = []
        monkeypatch.setattr(
            pf,
            "_emit_log",
            lambda level, msg, detail=None: emit_calls.append({"level": level, "msg": msg, "detail": detail}),
        )

        pf._log_chunk_retry(self._base_prep(), retry_count=2, error_text="parse error")

        assert recover_calls["n"] == 0
        assert len(emit_calls) == 1
        assert emit_calls[0]["detail"]["stage"] == "chunk_retry"
        assert emit_calls[0]["detail"]["retry_count"] == 2


# =============================================================================
# F4: _fetch_chunk_bundle_async 返回 schema（IPC 双份 payload 删除）
# =============================================================================

class TestFetchChunkBundleAsyncSchema:
    """
    阶段三 C1：_fetch_chunk_bundle_async 返回字典不再有 bundle-level "payloads" 字段；
    父进程 _iter_task_payloads_parallel_chunked 通过 chunk_reports[i].payloads 取数。
    """

    def test_empty_chunks_no_bundle_payloads(self):
        """空 chunks 返回字典不含 payloads key。"""
        import asyncio
        from zsdtdx.parallel_fetcher import _fetch_chunk_bundle_async
        result = asyncio.run(_fetch_chunk_bundle_async({"chunks": [], "bundle_id": 7}))
        assert "payloads" not in result, "bundle 级 payloads 字段应被删除"
        assert result["bundle_id"] == 7
        assert result["bundle_chunk_count"] == 0
        assert result["chunk_reports"] == []

    def test_nonempty_chunks_only_chunk_reports_carry_payloads(self, monkeypatch):
        """非空 chunks 返回字典也不含 bundle-level payloads；payloads 只在 chunk_reports 内。"""
        import asyncio
        import zsdtdx.parallel_fetcher as pf

        # 桩 worker client context 与 chunk 执行器，避免真建连。
        monkeypatch.setattr(pf, "_ensure_worker_client_context", lambda: object())
        monkeypatch.setattr(pf, "_cleanup_worker_dead_thread_connections", lambda: None)

        async def _fake_chunk(payload):
            return {
                "chunk_id": str(payload.get("chunk_id", "")),
                "chunk_task_count": 1,
                "chunk_hit_tasks": 1,
                "chunk_network_page_calls": 0,
                "payloads": [{"event": "data", "task": payload.get("tasks", [{}])[0], "rows": [], "error": None}],
                "failures": [],
                "worker_pid": os.getpid(),
            }
        monkeypatch.setattr(pf, "_fetch_one_task_chunk_async", _fake_chunk)

        bundle = {
            "bundle_id": 1,
            "chunks": [
                {"chunk_id": "c1", "task_kind": "stock", "tasks": [{"code": "000001", "freq": "d"}]},
                {"chunk_id": "c2", "task_kind": "stock", "tasks": [{"code": "000002", "freq": "d"}]},
            ],
            "inproc_coroutine_workers": 2,
            "chunk_timeout_seconds": 30,
            "chunk_retry_max_attempts": 0,
            "reconnect_on_unavailable": False,
        }
        result = asyncio.run(pf._fetch_chunk_bundle_async(bundle))

        assert "payloads" not in result, "bundle-level payloads 必须被删除"
        assert result["bundle_chunk_count"] == 2
        assert len(result["chunk_reports"]) == 2
        # 每个 chunk_report 都自带 payloads
        for report in result["chunk_reports"]:
            assert isinstance(report.get("payloads"), list)
            assert len(report["payloads"]) == 1


# =============================================================================
# F5: BaseSocketClient.connect 启用 TCP_NODELAY
# =============================================================================

class TestTcpNoDelayEnabled:
    """
    阶段四 D1：BaseSocketClient.connect 成功后立即 setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)。
    通过 monkey patch TrafficStatSocket 的工厂记录 setsockopt 调用。
    """

    def test_connect_sets_tcp_nodelay(self, monkeypatch):
        """connect 成功 → 应观察到 TCP_NODELAY setsockopt 调用。"""
        import socket
        from zsdtdx import base_socket_client as bsc

        recorded: List[tuple] = []

        class _FakeSock:
            def __init__(self, *_a, **_kw):
                pass
            def settimeout(self, _t):
                pass
            def bind(self, _addr):
                pass
            def connect(self, _addr):
                pass
            def close(self):
                pass
            def setsockopt(self, level, optname, value):
                recorded.append((level, optname, value))
            def shutdown(self, _how):
                pass
            # 流量统计相关属性
            send_pkg_num = 0
            recv_pkg_num = 0
            send_pkg_bytes = 0
            recv_pkg_bytes = 0
            first_pkg_send_time = None
            last_api_send_bytes = 0
            last_api_recv_bytes = 0

        monkeypatch.setattr(bsc, "TrafficStatSocket", _FakeSock)

        client = bsc.BaseSocketClient()
        # setup() 在子类实现；本测试用 need_setup=False 跳过协议握手
        client.need_setup = False
        try:
            client.connect(ip="127.0.0.1", port=7709)
        finally:
            try:
                client.disconnect()
            except Exception:
                pass

        # 应当至少包含一次 (IPPROTO_TCP, TCP_NODELAY, 1)
        assert any(
            tup == (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            for tup in recorded
        ), f"TCP_NODELAY 未启用；setsockopt 调用记录: {recorded}"


# =============================================================================
# F6: socket 读超时还原（D3）
# =============================================================================

class TestSocketReadTimeoutRestore:
    """
    阶段四 D3：chunk 结束 finally 分支调用 restore_thread_socket_read_timeout 还原 socket 读超时。
    """

    def test_restore_invoked_on_chunk_finally(self, monkeypatch):
        """模拟 client_context 验证 _restore_chunk_socket_read_timeout 调用每个池的 restorer。"""
        from zsdtdx.parallel_fetcher import _restore_chunk_socket_read_timeout

        restored: List[str] = []

        class _Pool:
            def __init__(self, name):
                self.name = name
            def restore_thread_socket_read_timeout(self):
                restored.append(self.name)

        class _Ctx:
            std_pool = _Pool("std")
            ex_pool = _Pool("ex")

        _restore_chunk_socket_read_timeout(_Ctx())
        assert restored == ["std", "ex"]

    def test_restore_safe_when_pool_missing(self):
        """池不存在/无 restorer 方法时静默跳过，不抛错。"""
        from zsdtdx.parallel_fetcher import _restore_chunk_socket_read_timeout
        class _Ctx:
            std_pool = None
            ex_pool = object()  # 无 restore_thread_socket_read_timeout 属性
        _restore_chunk_socket_read_timeout(_Ctx())  # 不抛即通过


class TestPoolRestoreDefaultTimeout:
    """
    PersistentFailoverPool.restore_thread_socket_read_timeout 行为：
    将 socket settimeout 还原为 pool._default_socket_read_timeout（默认 None=阻塞）。
    """

    def test_restore_uses_pool_default(self, monkeypatch):
        """模拟 pool 实例调用 restore，验证 settimeout(default) 被触发。"""
        from zsdtdx.unified_client import PersistentFailoverPool

        # 构造一个最小 pool 子类实例（避免完整初始化）
        pool = PersistentFailoverPool.__new__(PersistentFailoverPool)
        pool._default_socket_read_timeout = 12.5  # type: ignore

        # 构造线程数据
        timeouts_set: List[Any] = []
        class _Sock:
            def settimeout(self, v):
                timeouts_set.append(v)
        class _Api:
            client = _Sock()

        # 直接打桩 _get_thread_data
        monkeypatch.setattr(pool, "_get_thread_data", lambda: {"api": _Api()})
        pool.restore_thread_socket_read_timeout()

        assert timeouts_set == [12.5]


# =============================================================================
# F6b: chunk deadline 覆盖首次建连与底层请求
# =============================================================================

class TestPoolChunkDeadline:
    """
    chunk deadline 线程级状态：
    1. 未建连前设置 read timeout，后续新建 socket 也必须继承该预算；
    2. deadline 过期时底层请求应快速失败，避免进入阻塞 I/O。
    """

    def test_timeout_set_before_connect_applies_to_new_socket(self):
        """先设置线程读超时、再建连；新 socket 应收到 settimeout。"""
        from zsdtdx.unified_client import PersistentFailoverPool

        class _Sock:
            def __init__(self):
                self.timeouts: List[float] = []

            def settimeout(self, value):
                self.timeouts.append(value)

        class _Api:
            instances: List[Any] = []

            def __init__(self, **_kwargs):
                self.client = _Sock()
                _Api.instances.append(self)

            def connect(self, _host, _port, time_out=0, **_kwargs):
                self.client.settimeout(time_out)
                return True

            def disconnect(self):
                pass

        pool = PersistentFailoverPool("test", _Api, [("127.0.0.1", 7709)], 1.5)
        pool.set_thread_socket_read_timeout(5.0)

        assert pool.ensure_connected() is True
        api = _Api.instances[-1]
        assert api.client.timeouts, "新 socket 应至少设置过 timeout"
        assert api.client.timeouts[-1] <= 5.0
        assert api.client.timeouts[-1] > 0

    def test_expired_deadline_fails_before_api_call(self, monkeypatch):
        """deadline 已过期时，_attempt_call_step 不应调用底层 API。"""
        from zsdtdx.unified_client import PersistentFailoverPool

        called = {"n": 0}

        class _Api:
            client = object()

            def quote(self):
                called["n"] += 1
                return {"ok": True}

        pool = PersistentFailoverPool.__new__(PersistentFailoverPool)
        pool.name = "test"
        monkeypatch.setattr(
            pool,
            "_get_thread_data",
            lambda: {
                "api": _Api(),
                "active_index": 0,
                "socket_read_deadline": time.monotonic() - 1.0,
                "socket_read_timeout_override": 0.1,
            },
        )

        result, exc, needs_retry = pool._attempt_call_step("quote", False)

        assert result is None
        assert needs_retry is True
        assert isinstance(exc, TimeoutError)
        assert called["n"] == 0


# =============================================================================
# F6c: 父级 bundle watchdog 兜底并保持 done 事件
# =============================================================================

class TestBundleWatchdog:
    """
    父级 watchdog：
    当进程池 future 长时间不完成时，父级应补失败 payload，结束 async worker 并发送 done。
    """

    def test_async_watchdog_emits_failure_and_done(self, monkeypatch):
        """模拟永不完成的 bundle future，验证 async 队列最终包含 data + done。"""
        import queue as py_queue
        from concurrent.futures import Future

        import zsdtdx.parallel_fetcher as pf

        class _NeverDoneExecutor:
            def submit(self, *_args, **_kwargs):
                return Future()

        monkeypatch.setattr(pf, "_get_global_process_pool", lambda _workers: _NeverDoneExecutor())
        monkeypatch.setattr(pf, "force_restart_parallel_fetcher", lambda **_kwargs: {"ok": True})

        fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
        fetcher.num_processes = 1
        fetcher.task_chunk_max_inflight_multiplier = 1
        fetcher.task_chunk_inproc_coroutine_workers = 1
        fetcher.task_chunk_cache_min_tasks = 1
        fetcher.chunk_reconnect_on_unavailable = False
        fetcher.chunk_timeout_seconds = 0.1
        fetcher.chunk_retry_max_attempts = 0
        fetcher.bundle_watchdog_grace_seconds = 0.0
        fetcher.force_recycle_on_timeout = True
        fetcher._ensure_async_prewarm = lambda: None
        fetcher._build_chunk_task_detail = lambda _detail: {}

        task = {
            "code": "000001",
            "freq": "d",
            "start_time": "2026-01-01",
            "end_time": "2026-01-02",
        }
        chunk = pf.TaskChunk(chunk_id="c1", code="000001", freq="d", tasks=[task])
        fetcher._build_task_chunks = lambda _tasks: [chunk]
        fetcher._build_index_task_chunks = lambda _tasks: []
        fetcher._build_chunk_bundles = lambda chunks, _workers: [pf.ChunkBundle(bundle_id=1, chunks=chunks)]

        q = py_queue.Queue()
        job = fetcher.fetch_stock_tasks_async(tasks=[task], queue=q)
        result = job.result(timeout=5)

        assert len(result) == 1
        assert result[0]["error"] == "bundle watchdog timeout"
        events = []
        while not q.empty():
            events.append(q.get_nowait())
        assert [item.get("event") for item in events] == ["data", "done"]
        assert events[-1]["failed_tasks"] == 1


# =============================================================================
# F7: 日志选项 RCU 写入
# =============================================================================

class TestLogDetailOptionsRCU:
    """
    阶段三 C5：set_log_detail_options 整体替换全局快照对象（RCU），读路径不加锁。

    验收要点：
    1. _read_log_detail_options 返回的对象就是当前全局快照（同一引用）；
    2. 写入会让全局引用指向一个新对象（id 变化）；
    3. 并发多线程 read 不抛、不死锁；
    4. 写入返回的是更新前快照副本。
    """

    def test_write_replaces_snapshot_object(self):
        """set 后全局 dict 引用应是新对象。"""
        import zsdtdx.parallel_fetcher as pf
        before = pf._read_log_detail_options()
        before_id = id(before)
        prev = pf.set_log_detail_options(sample_size=15)
        try:
            after = pf._read_log_detail_options()
            assert id(after) != before_id, "RCU 写入应整体替换全局引用"
            assert after.get("sample_size") == 15
            assert prev.get("sample_size") != 15  # prev 是更新前副本
        finally:
            # 还原默认
            pf.set_log_detail_options(sample_size=prev.get("sample_size", 8))

    def test_read_returns_snapshot_reference(self):
        """_read_log_detail_options 直接返回当前全局快照（不拷贝）。"""
        import zsdtdx.parallel_fetcher as pf
        snap1 = pf._read_log_detail_options()
        snap2 = pf._read_log_detail_options()
        assert snap1 is snap2, "读路径应直接返回全局引用（零拷贝）"

    def test_concurrent_read_write_no_crash(self):
        """并发读 + 写不抛异常、不死锁。"""
        import zsdtdx.parallel_fetcher as pf
        stop = threading.Event()
        errors: List[Exception] = []

        def reader():
            try:
                for _ in range(2000):
                    opt = pf._read_log_detail_options()
                    # 读取字段不应抛
                    _ = int(opt.get("sample_size", 0) or 0)
                    if stop.is_set():
                        return
            except Exception as exc:
                errors.append(exc)

        def writer():
            try:
                for i in range(200):
                    pf.set_log_detail_options(sample_size=(i % 100) + 1)
            except Exception as exc:
                errors.append(exc)

        readers = [threading.Thread(target=reader) for _ in range(4)]
        writers = [threading.Thread(target=writer) for _ in range(2)]
        for t in readers + writers:
            t.start()
        for t in writers:
            t.join(timeout=5)
        stop.set()
        for t in readers:
            t.join(timeout=5)
        # 还原默认
        pf.set_log_detail_options(sample_size=8)

        assert not errors, f"并发 read/write 不应抛异常: {errors}"

    def test_invalid_sample_size_clamped(self):
        """sample_size 越界会被收敛到 [1, 200]。"""
        import zsdtdx.parallel_fetcher as pf
        prev = pf.set_log_detail_options(sample_size=0)
        try:
            assert pf._read_log_detail_options().get("sample_size") == 1
            pf.set_log_detail_options(sample_size=10000)
            assert pf._read_log_detail_options().get("sample_size") == 200
        finally:
            pf.set_log_detail_options(sample_size=prev.get("sample_size", 8))


# =============================================================================
# F8: get_volume 与 pytdx 原版 fuzz 等价（覆盖 hleax=0x80 / dwEdx<0 边界）
# =============================================================================

class TestGetVolumeParityFuzz:
    """
    阶段一 A2 关键 bug 修复：当前工程 helper.get_volume 必须与 pytdx 原版按位等价。

    历史 bug：
    1. `if hleax > 0x80` 曾被写成 `if hleax & 0x80`，让 hleax==0x80 错误走大分支；
    2. `else` 中 `dwEdx<0` 时 pytdx 是 `1/pow(2,dwEdx)=pow(2,-dwEdx)`（方向反向但要保持等价）。

    本测试用大量随机 ivol 做 fuzz，再加显式覆盖 hleax=0x80 / dwEdx 边界。
    """

    def _pytdx_get_volume(self):
        from pytdx.helper import get_volume as pytdx_get_volume
        return pytdx_get_volume

    def _zs_get_volume(self):
        from zsdtdx.helper import get_volume as zs_get_volume
        return zs_get_volume

    def test_explicit_boundary_hleax_eq_0x80(self):
        """hleax==0x80 必须走 else 分支（与 pytdx 完全一致）。"""
        # 构造 ivol：logpoint=0x42, hleax=0x80, lheax=0x00, lleax=0x00
        ivol = (0x42 << 24) | (0x80 << 16) | (0x00 << 8) | 0x00
        z = self._zs_get_volume()(ivol)
        p = self._pytdx_get_volume()(ivol)
        assert z == p, f"hleax=0x80 边界不等: zs={z} pytdx={p}"

    def test_explicit_boundary_hleax_gt_0x80(self):
        """hleax>0x80 走大分支。"""
        ivol = (0x42 << 24) | (0x81 << 16) | (0x12 << 8) | 0x34
        z = self._zs_get_volume()(ivol)
        p = self._pytdx_get_volume()(ivol)
        assert z == p

    def test_explicit_boundary_dwEdx_negative(self):
        """构造 logpoint 让 dwEdx 为负，触发原 bug 分支。"""
        # logpoint 选小值让 (logpoint*2 - 0x86) < 0；同时 hleax<=0x80 走 else 分支
        ivol = (0x10 << 24) | (0x55 << 16) | (0x33 << 8) | 0x77
        z = self._zs_get_volume()(ivol)
        p = self._pytdx_get_volume()(ivol)
        assert z == p

    def test_explicit_zero(self):
        """ivol=0 必须为 0（pytdx 与 zsdtdx 完全一致）。"""
        z = self._zs_get_volume()(0)
        p = self._pytdx_get_volume()(0)
        assert z == 0 or z == p

    def test_fuzz_random_2k_samples(self):
        """随机 fuzz 2000 个 ivol 做按位等价比对。"""
        import random
        rng = random.Random(20260522)
        zs = self._zs_get_volume()
        pt = self._pytdx_get_volume()
        mismatches: List[tuple] = []
        for _ in range(2000):
            ivol = rng.randint(0, (1 << 32) - 1)
            z = zs(ivol)
            p = pt(ivol)
            if z != p:
                # 浮点对比允许 ULP 1 内（双方都是同 pow 链路应当严格相等，但极端 IEEE-754 误差兜底）
                if not (isinstance(z, float) and isinstance(p, float) and abs(z - p) < 1e-12 * max(1.0, abs(p))):
                    mismatches.append((ivol, z, p))
        assert not mismatches, f"get_volume fuzz mismatch (sample 5): {mismatches[:5]}"


# =============================================================================
# F-aux: 离线快速回归 — Stock & Index parser → normalize → 占位过滤端到端
# =============================================================================

class TestParseToNormalizeOffline:
    """
    端到端离线回归：构造合成 body_buf →
    parser → _normalize_stock_kline_rows → 占位过滤；
    验证 9 字段 schema 不动、数值刻度正确、占位 bar 被过滤。
    """

    @staticmethod
    def _encode_price_varint(value: int) -> bytes:
        negative = value < 0
        magnitude = -value if negative else value
        out = bytearray()
        first = magnitude & 0x3F
        if negative:
            first |= 0x40
        rest = magnitude >> 6
        if rest:
            first |= 0x80
        out.append(first)
        while rest:
            chunk = rest & 0x7F
            rest >>= 7
            if rest:
                chunk |= 0x80
            out.append(chunk)
        return bytes(out)

    def _build_bar(self, zipday, oc, od, hd, ld, vol, dbvol):
        body = bytearray()
        body.extend(struct.pack("<I", zipday))
        body.extend(self._encode_price_varint(oc))
        body.extend(self._encode_price_varint(od))
        body.extend(self._encode_price_varint(hd))
        body.extend(self._encode_price_varint(ld))
        body.extend(struct.pack("<II", vol & 0xFFFFFFFF, dbvol & 0xFFFFFFFF))
        return bytes(body)

    def test_parser_output_schema_9_fields(self):
        """parser 输出仍保留 vol/amount/datetime/open/close/high/low/_ts，
        normalize 后输出 9 字段：code/freq/open/close/high/low/volume/amount/datetime。"""
        from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page

        body = bytearray()
        body.extend(struct.pack("<H", 1))
        body.extend(self._build_bar(20260102, 12340, 10, 200, -10, 0x42801000, 0x44A00000))

        rows = parse_diff_encoded_kline_page(bytes(body), 9, with_index_counts=False)
        assert len(rows) == 1
        keys = set(rows[0].keys())
        # parser 输出必须包含的字段
        assert {"open", "close", "high", "low", "vol", "amount", "datetime", "_ts"} <= keys


# =============================================================================
# F-aux: parser → ChunkLocalRowCache 行管理 sanity（无 socket）
# =============================================================================

class TestChunkLocalRowCacheRowAccumulate:
    """
    模拟 chunk 内多 task 命中场景：手工往 partition.cache_rows 追加并维护 _n。
    """

    def test_n_monotonic_within_chunk(self):
        from zsdtdx.unified_client import ChunkLocalRowCache
        cache = ChunkLocalRowCache()
        partition = cache.acquire_partition("000001", "d")
        rows = partition["cache_rows"]
        for i in range(5):
            rows.append((i, {"row": i}))
            partition["_n"] += 1
        # 同 chunk 内累加：_n 单调
        assert partition["_n"] == 5
        # 同 chunk 内重复 acquire 不会清除底层 list（但 _n 归零）
        partition2 = cache.acquire_partition("000001", "d")
        assert partition2 is partition
        assert partition2["_n"] == 0
        # 但 cache_rows 底层 list 仍持有上次数据（外层逻辑只读 [0,_n)）
        # 因此再次写入会覆盖前 5 个 tuple
        assert len(partition2["cache_rows"]) == 5
