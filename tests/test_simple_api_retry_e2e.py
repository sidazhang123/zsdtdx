# -*- coding: utf-8 -*-
"""
simple_api 端到端：同连接 None 确认重试 + chunk 重建连接重试。

通过 get_stock_kline(sync/async) 走完整 simple_api → parallel_fetcher → worker 链路；
网络 I/O 用 worker 内 mock API 替代，统计 pool 与 recover 触发次数。
"""

from __future__ import annotations

import contextlib
import os
import sys
import unittest
from concurrent.futures import Future
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock, patch

# 保证优先加载工作区 src，而非 site-packages 旧包
_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from zsdtdx import get_stock_kline, set_config_path  # noqa: E402
from zsdtdx.parallel_fetcher import (  # noqa: E402
    _close_worker_client_context,
    destroy_parallel_fetcher,
)
from zsdtdx.unified_client import UnifiedTdxClient  # noqa: E402

_CONFIG_PATH = str(_SRC / "zsdtdx" / "config.yaml")
_SAMPLE_TASK = {
    "code": "600000",
    "freq": "d",
    "start_time": "2026-01-01 00:00:00",
    "end_time": "2026-01-02 00:00:00",
}
_SAMPLE_BAR = {
    "datetime": "2026-01-01 15:00:00",
    "open": 10.0,
    "close": 10.5,
    "high": 11.0,
    "low": 9.5,
    "vol": 100,
    "amount": 1000.0,
}


class _InlineProcessPool:
    """将进程池 submit 内联到当前进程，便于 Windows 下 async E2E。"""

    def submit(self, fn, *args, **kwargs):
        fut: Future = Future()
        try:
            fut.set_result(fn(*args, **kwargs))
        except Exception as exc:
            fut.set_exception(exc)
        return fut

    def shutdown(self, wait=True, cancel_futures=False):
        return None


class _InlineThreadPoolExecutor:
    """在当前线程立即执行 submit，保证 pool thread-local 与 mock API 一致。"""

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def submit(self, fn, *args, **kwargs):
        fut: Future = Future()
        try:
            fut.set_result(fn(*args, **kwargs))
        except Exception as exc:
            fut.set_exception(exc)
        return fut

    def shutdown(self, wait=True, cancel_futures=False):
        return None


@contextlib.contextmanager
def _inline_thread_pools() -> Iterator[None]:
    """chunk 调度线程池内联执行，避免子线程重建真实连接。"""
    inline = _InlineThreadPoolExecutor()
    with patch(
        "zsdtdx.parallel_fetcher.ThreadPoolExecutor",
        _InlineThreadPoolExecutor,
    ), patch(
        "zsdtdx.parallel_fetcher._get_worker_chunk_executor",
        return_value=inline,
    ):
        yield


def _reset_parallel_state() -> None:
    destroy_parallel_fetcher()
    _close_worker_client_context()


def _install_mock_worker(
    *,
    bars_side_effect=None,
    chunk_rows_impl=None,
) -> Tuple[UnifiedTdxClient, Any]:
    """
    安装带 mock std API 的 worker 常驻客户端（不发起真实行情请求）。

    输入：
    1. bars_side_effect: 传给 get_security_bars 的 side_effect。
    2. chunk_rows_impl: 若提供则替换 get_stock_kline_rows_for_chunk_tasks。
    输出：
    1. (worker 上下文 UnifiedTdxClient, ensure patch 对象)；调用方须在 tearDown 中 stop patch。
    """
    import zsdtdx.parallel_fetcher as pf

    _reset_parallel_state()
    pf._restore_ensure_worker_client_context_binding()
    pf._worker_client_holder = None
    pf._worker_client_context = None
    pf._worker_client_pid = None
    set_config_path(_CONFIG_PATH, async_background_probe=False)

    ctx = UnifiedTdxClient(config_path=_CONFIG_PATH, worker_client=True)
    ctx.preconnect_on_enter = False
    ctx.std_pool.ensure_connected = lambda: True  # type: ignore[method-assign]
    ctx.ex_pool.ensure_connected = lambda: True  # type: ignore[method-assign]

    thread_data = ctx.std_pool._get_thread_data()
    api = MagicMock()
    if bars_side_effect is not None:
        api.get_security_bars = MagicMock(side_effect=bars_side_effect)
    else:
        api.get_security_bars = MagicMock(return_value=[])
    thread_data["api"] = api
    thread_data["active_index"] = 0

    ctx._stock_route = {
        "600000": {
            "code": "600000",
            "market": 1,
            "market_name": "上海",
            "source": "std",
            "asset_type": "stock",
        }
    }

    def _resolve_stock_route_no_df(code: str, freq: str):
        return "600000", dict(ctx._stock_route["600000"])

    ctx._resolve_stock_route_no_df = _resolve_stock_route_no_df  # type: ignore[method-assign]

    if chunk_rows_impl is not None:
        ctx.get_stock_kline_rows_for_chunk_tasks = chunk_rows_impl  # type: ignore[method-assign]

    class _Holder:
        def __exit__(self, *_args):
            return False

    pf._worker_client_holder = _Holder()
    pf._worker_client_context = ctx
    pf._worker_client_pid = os.getpid()

    ensure_patch = patch.object(
        pf,
        "_ensure_worker_client_context",
        side_effect=lambda: ctx,
    )
    ensure_patch.start()
    return ctx, ensure_patch


class TestSimpleApiSameConnectionRetryE2E(unittest.TestCase):
    _worker_ensure_patch: Any = None

    def tearDown(self) -> None:
        import zsdtdx.parallel_fetcher as pf

        if self._worker_ensure_patch is not None:
            self._worker_ensure_patch.stop()
            self._worker_ensure_patch = None
        pf._restore_ensure_worker_client_context_binding()
        _reset_parallel_state()

    def test_sync_transient_none_confirm_then_success(self):
        """simple_api sync：首跳 None → 同连接确认 → 返回 K 线。"""
        calls = {"n": 0}

        def _bars(*_args, **_kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return None
            return [dict(_SAMPLE_BAR)]

        ctx, self._worker_ensure_patch = _install_mock_worker(bars_side_effect=_bars)

        with _inline_thread_pools():
            payloads = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="sync")

        self.assertEqual(len(payloads), 1)
        self.assertIsNone(payloads[0].get("error"))
        self.assertGreater(len(payloads[0].get("rows") or []), 0)
        self.assertGreaterEqual(ctx.std_pool.stats["same_conn_retries"], 1)
        self.assertEqual(calls["n"], 2)

    def test_sync_persistent_none_no_chunk_recover(self):
        """simple_api sync：真 None 仍 no_data，不触发 chunk 重建连接。"""
        ctx, self._worker_ensure_patch = _install_mock_worker(bars_side_effect=[None, None])

        with patch(
            "zsdtdx.parallel_fetcher._recover_worker_standard_connection_current_thread"
        ) as recover_mock, _inline_thread_pools():
            payloads = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="sync")

        recover_mock.assert_not_called()
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0].get("error"), "no_data")
        self.assertEqual(ctx.std_pool.stats["same_conn_retries"], 1)

    def test_async_transient_none_confirm_then_success(self):
        """simple_api async：进程池内联后，同连接确认重试仍生效。"""
        calls = {"n": 0}

        def _bars(*_args, **_kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return None
            return [dict(_SAMPLE_BAR)]

        ctx, self._worker_ensure_patch = _install_mock_worker(bars_side_effect=_bars)
        inline_pool = _InlineProcessPool()

        with patch(
            "zsdtdx.parallel_fetcher._get_global_process_pool",
            return_value=inline_pool,
        ), patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher._ensure_async_prewarm",
            return_value=None,
        ), _inline_thread_pools():
            job = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="async")
            payloads = job.result(timeout=60)

        self.assertEqual(len(payloads), 1)
        self.assertIsNone(payloads[0].get("error"))
        self.assertGreaterEqual(ctx.std_pool.stats["same_conn_retries"], 1)
        self.assertEqual(calls["n"], 2)


class TestSimpleApiChunkReconnectRetryE2E(unittest.TestCase):
    _worker_ensure_patch: Any = None

    def tearDown(self) -> None:
        import zsdtdx.parallel_fetcher as pf

        if self._worker_ensure_patch is not None:
            self._worker_ensure_patch.stop()
            self._worker_ensure_patch = None
        pf._restore_ensure_worker_client_context_binding()
        _reset_parallel_state()

    def _make_flaky_chunk_fetch(self, state: Dict[str, int]):
        def _impl(*, tasks: List[Dict[str, Any]], enable_cache: bool):
            state["chunk_attempts"] = int(state.get("chunk_attempts", 0)) + 1
            if state["chunk_attempts"] == 1:
                raise RuntimeError("standard 无可用连接")
            task = dict(tasks[0])
            return {
                "results": [
                    {
                        "task": task,
                        "rows": [dict(_SAMPLE_BAR)],
                        "error": None,
                    }
                ],
                "chunk_hit_tasks": 0,
                "chunk_network_page_calls": 0,
            }

        return _impl

    def test_sync_chunk_reconnect_and_retry(self):
        """simple_api sync：chunk 异常 → 重建标准连接 → 重试成功。"""
        state: Dict[str, int] = {}
        _, self._worker_ensure_patch = _install_mock_worker(
            chunk_rows_impl=self._make_flaky_chunk_fetch(state)
        )
        recover_calls: List[str] = []

        def _fake_recover(reason: str = "") -> Dict[str, Any]:
            recover_calls.append(str(reason))
            return {
                "ok": True,
                "reason": str(reason),
                "active_host_before": "1.2.3.4:7709",
                "active_host_after": "1.2.3.4:7709",
            }

        with patch(
            "zsdtdx.parallel_fetcher._recover_worker_standard_connection_current_thread",
            side_effect=_fake_recover,
        ), _inline_thread_pools():
            payloads = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="sync")

        self.assertGreaterEqual(len(recover_calls), 1)
        self.assertGreaterEqual(state.get("chunk_attempts", 0), 2)
        self.assertEqual(len(payloads), 1)
        self.assertIsNone(payloads[0].get("error"))
        self.assertGreater(len(payloads[0].get("rows") or []), 0)

    def test_async_chunk_reconnect_and_retry(self):
        """simple_api async：chunk 重建连接重试（内联进程池）。"""
        state: Dict[str, int] = {}
        _, self._worker_ensure_patch = _install_mock_worker(
            chunk_rows_impl=self._make_flaky_chunk_fetch(state)
        )
        recover_calls: List[str] = []

        def _fake_recover(reason: str = "") -> Dict[str, Any]:
            recover_calls.append(str(reason))
            return {"ok": True, "reason": str(reason), "active_host_before": "", "active_host_after": ""}

        inline_pool = _InlineProcessPool()
        with patch(
            "zsdtdx.parallel_fetcher._recover_worker_standard_connection_current_thread",
            side_effect=_fake_recover,
        ), patch(
            "zsdtdx.parallel_fetcher._get_global_process_pool",
            return_value=inline_pool,
        ), patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher._ensure_async_prewarm",
            return_value=None,
        ), _inline_thread_pools():
            job = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="async")
            payloads = job.result(timeout=60)

        self.assertGreaterEqual(len(recover_calls), 1)
        self.assertGreaterEqual(state.get("chunk_attempts", 0), 2)
        self.assertEqual(len(payloads), 1)
        self.assertIsNone(payloads[0].get("error"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
