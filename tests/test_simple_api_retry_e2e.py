# -*- coding: utf-8 -*-
"""
simple_api 端到端：同连接 None 确认重试 + chunk 重建连接重试。

通过 get_stock_kline(sync/async) 走完整 simple_api → parallel_fetcher → 线程链路；
网络 I/O 用共享客户端内的 mock API 替代，统计连接重试次数。
"""

from __future__ import annotations

import queue
import sys
import unittest
from zsdtdx.engine.thread_runtime import ThreadRuntime
from zsdtdx.parser.kline_columns import KlineColumns
import datetime as dt
import contextlib
import numpy as np
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock, patch

# 保证优先加载工作区 src，而非 site-packages 旧包
_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from zsdtdx import get_stock_kline, set_config_path  # noqa: E402
from zsdtdx.engine.parallel_fetcher import (  # noqa: E402
    _close_shared_client_context,
    destroy_parallel_fetcher,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient  # noqa: E402

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


def _data_events(job: Any) -> List[Dict[str, Any]]:
    """
    输入已完成的异步句柄，输出其中的 data 事件。

    用途：后台完成后从队列取出行情，供断言使用。
    边界：忽略 done 与其它事件；队列已空时返回当前已取出的 data。
    """
    payloads: List[Dict[str, Any]] = []
    while True:
        try:
            event = job.queue.get_nowait()
        except queue.Empty:
            return payloads
        if isinstance(event, dict) and event.get("event") == "data":
            payloads.append(event)


class _InlineThreadRuntime(ThreadRuntime):
    """测试接口适配：实际使用单工作线程运行时。"""

    def __init__(self):
        super().__init__(1)
        import zsdtdx.engine.parallel_fetcher as pf

        pf._thread_runtime = self


@contextlib.contextmanager
def _thread_test_scope():
    """输入无，输出测试上下文；保留调用结构以便组合其他补丁。"""
    yield


def _reset_parallel_state() -> None:
    destroy_parallel_fetcher()
    _close_shared_client_context()


def _as_columns(rows):
    """把旧测试夹具适配到列式页，生产链路不包含此转换。"""
    if rows is None:
        return None
    data = {
        name: np.array([float(row[name]) for row in rows])
        for name in ("open", "close", "high", "low", "amount")
    }
    data["vol"] = np.array(
        [float(row.get("vol", row.get("volume", 0))) for row in rows]
    )
    times = [dt.datetime.fromisoformat(row["datetime"]) for row in rows]
    data["_ts"] = np.array([int(value.timestamp()) for value in times], dtype=np.int64)
    data["_clock"] = np.array(
        [int(value.strftime("%Y%m%d%H%M")) for value in times], dtype=np.int64
    )
    return KlineColumns(data)


def _install_mock_shared_client(
    *,
    bars_side_effect=None,
    chunk_rows_impl=None,
) -> Tuple[UnifiedTdxClient, Any]:
    """
    安装带 mock std API 的 线程运行时共享客户端（不发起真实行情请求）。

    输入：
    1. bars_side_effect: 传给 get_security_bars 的 side_effect。
    2. chunk_rows_impl: 若提供则替换 get_stock_kline_rows_for_chunk_tasks。
    输出：
    1. (线程运行时上下文 UnifiedTdxClient, ensure patch 对象)；调用方须在 tearDown 中 stop patch。
    """
    import zsdtdx.engine.parallel_fetcher as pf

    _reset_parallel_state()
    pf._shared_client_holder = None
    pf._shared_client_context = None
    with patch("zsdtdx.engine.unified_client._cache_usable_for_cfg", return_value=True):
        set_config_path(_CONFIG_PATH, async_background_probe=False)

    cache_dir = _ROOT / ".pytest_cache" / "simple_api_retry_catalog"
    cache_dir.mkdir(parents=True, exist_ok=True)
    with patch(
        "zsdtdx.engine.unified_client.resolve_catalog_cache_dir",
        return_value=cache_dir,
    ):
        ctx = UnifiedTdxClient(
            config_path=_CONFIG_PATH,
            presorted_hosts={
                "standard": [("127.0.0.1", 7709)],
                "extended": [("127.0.0.1", 7720)],
            },
            shared_runtime_client=True,
        )
    ctx.preconnect_on_enter = False
    ctx.std_pool.ensure_connected = lambda: True  # type: ignore[method-assign]
    ctx.ex_pool.ensure_connected = lambda: True  # type: ignore[method-assign]

    thread_data = ctx.std_pool._get_thread_data()
    api = MagicMock()
    if bars_side_effect is not None:
        if callable(bars_side_effect):
            api.get_security_bars = MagicMock(
                side_effect=lambda *args, **kwargs: _as_columns(
                    bars_side_effect(*args, **kwargs)
                )
            )
        else:
            samples = iter(bars_side_effect)
            api.get_security_bars = MagicMock(
                side_effect=lambda *args, **kwargs: _as_columns(next(samples))
            )
    else:
        api.get_security_bars = MagicMock(return_value=[])
    thread_data["api"] = api
    thread_data["active_index"] = 0
    ctx.std_pool._get_thread_data = lambda: thread_data
    ctx.std_pool.source, ctx.ex_pool.source = "std", "ex"
    ctx.std_pool.begin_attempt = ctx.ex_pool.begin_attempt = lambda: None
    ctx.std_pool._reconnect_active_host = lambda: False
    ctx.std_pool._rotate = lambda: False
    ctx.std_pool.release_thread = lambda **kwargs: None
    ctx.ex_pool.release_thread = lambda **kwargs: None

    ctx._stock_route = {
        "600000": {
            "code": "600000",
            "market": 1,
            "market_name": "上海",
            "source": "std",
            "asset_type": "stock",
        }
    }

    def _resolve_stock_route_no_df(
        code: str, freq: str, route_source: str | None = None
    ):
        return "600000", dict(ctx._stock_route["600000"])

    ctx._resolve_stock_route_no_df = _resolve_stock_route_no_df  # type: ignore[method-assign]

    if chunk_rows_impl is not None:

        def fetch_columns(client, tasks, kind, qfq):
            result = chunk_rows_impl(tasks=tasks, enable_cache=True, qfq=qfq)
            for item in result["results"]:
                item.update(
                    columns=_as_columns(item.pop("rows")),
                    route={"source": "std", "market": 1},
                    kind=kind,
                    code="600000",
                )
            return result

        ensure_patch = patch(
            "zsdtdx.engine.columnar_kline.fetch_chunk_columns",
            side_effect=fetch_columns,
        )
    else:
        ensure_patch = patch.object(
            pf, "_ensure_shared_client_context", return_value=ctx
        )

    class _Holder:
        def close(self):
            return None

    pf._shared_client_holder = _Holder()
    pf._shared_client_context = ctx

    ensure_patch.start()
    return ctx, ensure_patch


class TestSimpleApiSameConnectionRetryE2E(unittest.TestCase):
    _shared_ensure_patch: Any = None

    def tearDown(self) -> None:
        if self._shared_ensure_patch is not None:
            self._shared_ensure_patch.stop()
            self._shared_ensure_patch = None
        _reset_parallel_state()

    def test_sync_transient_none_confirm_then_success(self):
        """simple_api sync：首跳 None → 同连接确认 → 返回 K 线。"""
        calls = {"n": 0}

        def _bars(*_args, **_kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return None
            return [dict(_SAMPLE_BAR)]

        ctx, self._shared_ensure_patch = _install_mock_shared_client(
            bars_side_effect=_bars
        )

        with _thread_test_scope():
            payloads = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="sync")

        self.assertEqual(len(payloads), 1)
        self.assertIsNone(payloads[0].get("error"))
        self.assertGreater(len(payloads[0].get("rows") or []), 0)
        self.assertGreaterEqual(ctx.std_pool.stats["same_conn_retries"], 1)
        self.assertEqual(calls["n"], 2)

    def test_sync_persistent_none_no_chunk_recover(self):
        """simple_api sync：持续无合法回包明确失败，不冒充合法空页。"""
        ctx, self._shared_ensure_patch = _install_mock_shared_client(
            bars_side_effect=lambda *args, **kwargs: None
        )

        with _thread_test_scope():
            payloads = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="sync")

        self.assertEqual(len(payloads), 1)
        self.assertTrue(payloads[0].get("error"))
        self.assertNotEqual(payloads[0].get("error"), "no_data")
        self.assertGreaterEqual(ctx.std_pool.stats["same_conn_retries"], 1)

    def test_async_transient_none_confirm_then_success(self):
        """simple_api async：线程运行时内联后，同连接确认重试仍生效。"""
        calls = {"n": 0}

        def _bars(*_args, **_kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return None
            return [dict(_SAMPLE_BAR)]

        ctx, self._shared_ensure_patch = _install_mock_shared_client(
            bars_side_effect=_bars
        )
        inline_pool = _InlineThreadRuntime()

        with (
            patch(
                "zsdtdx.engine.parallel_fetcher._get_global_thread_runtime",
                return_value=inline_pool,
            ),
            patch(
                "zsdtdx.engine.parallel_fetcher.ParallelKlineFetcher._ensure_async_prewarm",
                return_value=None,
            ),
            _thread_test_scope(),
        ):
            job = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="async")
            summary = job.result(timeout=60)
            payloads = _data_events(job)

        self.assertEqual(
            summary, {"total_tasks": 1, "success_tasks": 1, "failed_tasks": 0}
        )
        self.assertEqual(len(payloads), 1)
        self.assertIsNone(payloads[0].get("error"))
        self.assertGreaterEqual(ctx.std_pool.stats["same_conn_retries"], 1)
        self.assertEqual(calls["n"], 2)


class TestSimpleApiChunkReconnectRetryE2E(unittest.TestCase):
    _shared_ensure_patch: Any = None

    def tearDown(self) -> None:
        if self._shared_ensure_patch is not None:
            self._shared_ensure_patch.stop()
            self._shared_ensure_patch = None
        _reset_parallel_state()

    def _make_flaky_chunk_fetch(self, state: Dict[str, int]):
        def _impl(*, tasks: List[Dict[str, Any]], enable_cache: bool, qfq: bool = True):
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
        _, self._shared_ensure_patch = _install_mock_shared_client(
            chunk_rows_impl=self._make_flaky_chunk_fetch(state)
        )
        with _thread_test_scope():
            payloads = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="sync")

        self.assertGreaterEqual(state.get("chunk_attempts", 0), 2)
        self.assertEqual(len(payloads), 1)
        self.assertIsNone(payloads[0].get("error"))
        self.assertGreater(len(payloads[0].get("rows") or []), 0)

    def test_async_chunk_reconnect_and_retry(self):
        """simple_api async：chunk 重建连接重试（内联线程运行时）。"""
        state: Dict[str, int] = {}
        _, self._shared_ensure_patch = _install_mock_shared_client(
            chunk_rows_impl=self._make_flaky_chunk_fetch(state)
        )
        inline_pool = _InlineThreadRuntime()
        with (
            patch(
                "zsdtdx.engine.parallel_fetcher._get_global_thread_runtime",
                return_value=inline_pool,
            ),
            patch(
                "zsdtdx.engine.parallel_fetcher.ParallelKlineFetcher._ensure_async_prewarm",
                return_value=None,
            ),
            _thread_test_scope(),
        ):
            job = get_stock_kline(task=[dict(_SAMPLE_TASK)], mode="async")
            summary = job.result(timeout=60)
            payloads = _data_events(job)

        self.assertGreaterEqual(state.get("chunk_attempts", 0), 2)
        self.assertEqual(
            summary, {"total_tasks": 1, "success_tasks": 1, "failed_tasks": 0}
        )
        self.assertEqual(len(payloads), 1)
        self.assertIsNone(payloads[0].get("error"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
