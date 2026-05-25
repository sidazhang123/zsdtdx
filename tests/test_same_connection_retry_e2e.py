# -*- coding: utf-8 -*-
"""pool.call 固定四步失败恢复流水线测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from zsdtdx.unified_client import PersistentFailoverPool, UnifiedTdxClient


def _pool_with_connected_api(
    api: MagicMock,
    hosts: list[tuple[str, int]] | None = None,
) -> PersistentFailoverPool:
    """构造已注入 mock API 的连接池，跳过真实建连。"""
    host_list = hosts or [("127.0.0.1", 7709)]
    pool = PersistentFailoverPool(
        "test",
        MagicMock,
        host_list,
        connect_timeout=1.0,
    )
    thread_data = pool._get_thread_data()
    thread_data["api"] = api
    thread_data["active_index"] = 0
    return pool


class TestPoolHostCallLadder(unittest.TestCase):
    def test_transient_none_then_empty_page(self):
        api = MagicMock()
        api.get_security_bars = MagicMock(side_effect=[None, []])
        pool = _pool_with_connected_api(api)

        result = pool.call(
            "get_security_bars",
            4,
            1,
            "600000",
            0,
            800,
            allow_none=True,
        )

        self.assertEqual(result, [])
        self.assertEqual(api.get_security_bars.call_count, 2)
        self.assertEqual(pool.stats["same_conn_retries"], 1)
        self.assertEqual(pool.stats["none_as_end"], 0)

    def test_persistent_none_single_host_three_attempts(self):
        api = MagicMock()
        api.get_security_bars = MagicMock(return_value=None)
        pool = _pool_with_connected_api(api)

        with patch.object(pool, "_reconnect_active_host", return_value=True) as reconnect_mock:
            result = pool.call(
                "get_security_bars",
                4,
                1,
                "600000",
                0,
                800,
                allow_none=True,
            )

        self.assertIsNone(result)
        self.assertEqual(api.get_security_bars.call_count, 3)
        reconnect_mock.assert_called_once()
        self.assertEqual(pool.stats["same_conn_retries"], 1)
        self.assertEqual(pool.stats["none_as_end"], 1)

    def test_persistent_none_multi_host_six_attempts(self):
        api = MagicMock()
        api.get_security_bars = MagicMock(return_value=None)
        hosts = [("127.0.0.1", 7709), ("127.0.0.2", 7709)]
        pool = _pool_with_connected_api(api, hosts=hosts)

        with patch.object(pool, "_reconnect_active_host", return_value=True), patch.object(
            pool, "_rotate", return_value=True
        ) as rotate_mock:
            result = pool.call(
                "get_security_bars",
                4,
                1,
                "600000",
                0,
                800,
                allow_none=True,
            )

        self.assertIsNone(result)
        self.assertEqual(api.get_security_bars.call_count, 6)
        rotate_mock.assert_called_once()
        self.assertEqual(pool.stats["same_conn_retries"], 2)
        self.assertEqual(pool.stats["none_as_end"], 1)

    def test_empty_page_no_retry(self):
        api = MagicMock()
        api.get_security_bars = MagicMock(return_value=[])
        pool = _pool_with_connected_api(api)

        result = pool.call(
            "get_security_bars",
            4,
            1,
            "600000",
            0,
            800,
            allow_none=True,
        )

        self.assertEqual(result, [])
        self.assertEqual(api.get_security_bars.call_count, 1)
        self.assertEqual(pool.stats["same_conn_retries"], 0)

    def test_no_sleep_between_steps(self):
        api = MagicMock()
        api.get_security_bars = MagicMock(side_effect=[None, []])
        pool = _pool_with_connected_api(api)

        with patch("zsdtdx.unified_client.time.sleep") as sleep_mock:
            pool.call(
                "get_security_bars",
                4,
                1,
                "600000",
                0,
                800,
                allow_none=True,
            )

        sleep_mock.assert_not_called()

    def test_exception_single_host_three_attempts(self):
        api = MagicMock()
        api.get_security_bars = MagicMock(side_effect=RuntimeError("socket boom"))
        pool = _pool_with_connected_api(api)

        with patch.object(pool, "_reconnect_active_host", return_value=True):
            with self.assertRaises(RuntimeError):
                pool.call(
                    "get_security_bars",
                    4,
                    1,
                    "600000",
                    0,
                    800,
                    allow_none=True,
                )

        self.assertEqual(api.get_security_bars.call_count, 3)
        self.assertEqual(pool.stats["same_conn_retries"], 1)

    def test_single_host_skips_rotate(self):
        api = MagicMock()
        api.get_security_bars = MagicMock(return_value=None)
        pool = _pool_with_connected_api(api)

        with patch.object(pool, "_reconnect_active_host", return_value=True), patch.object(
            pool, "_rotate"
        ) as rotate_mock:
            pool.call(
                "get_security_bars",
                4,
                1,
                "600000",
                0,
                800,
                allow_none=True,
            )

        rotate_mock.assert_not_called()


class TestUnifiedClientKlineWrapper(unittest.TestCase):
    def test_fetch_kline_page_uses_pool_call_allow_none(self):
        client = UnifiedTdxClient.__new__(UnifiedTdxClient)
        mock_pool = MagicMock()
        mock_pool.call.return_value = []
        client.std_pool = mock_pool

        rows = client._fetch_kline_page_rows_no_df(
            source="std",
            market=1,
            code="600000",
            category=4,
            start=0,
            page_size=800,
        )

        self.assertEqual(rows, [])
        mock_pool.call.assert_called_once()
        _args, kwargs = mock_pool.call.call_args
        self.assertIs(kwargs.get("allow_none"), True)


class TestChunkBehaviorUnchangedOnNone(unittest.TestCase):
    def test_persistent_none_does_not_trigger_chunk_recover(self):
        import asyncio

        import zsdtdx.parallel_fetcher as pf

        chunk_payload = {
            "chunk_id": "c1",
            "task_kind": "stock",
            "tasks": [
                {
                    "code": "sh.600000",
                    "freq": "d",
                    "start_time": "2026-01-01 00:00:00",
                    "end_time": "2026-01-02 00:00:00",
                }
            ],
            "enable_cache": False,
            "chunk_retry_max_attempts": 2,
            "reconnect_on_unavailable": True,
        }

        stub_report = {
            "chunk_id": "c1",
            "chunk_task_count": 1,
            "chunk_hit_tasks": 0,
            "chunk_network_page_calls": 0,
            "task_detail": [],
            "payloads": [
                {
                    "task": chunk_payload["tasks"][0],
                    "rows": [],
                    "error": "no_data",
                    "worker_pid": 0,
                }
            ],
            "failures": [("sh.600000", "d", "no_data")],
            "worker_pid": 0,
        }

        with patch.object(
            pf,
            "_fetch_one_task_chunk_async",
            new_callable=AsyncMock,
            return_value=stub_report,
        ) as fetch_mock, patch(
            "zsdtdx.parallel_fetcher._recover_worker_standard_connection_current_thread"
        ) as recover_mock:
            result = asyncio.run(pf._fetch_one_task_chunk_async(chunk_payload))

        fetch_mock.assert_called_once()
        recover_mock.assert_not_called()
        self.assertEqual(len(result.get("payloads") or []), 1)
        self.assertEqual(result["payloads"][0].get("error"), "no_data")


if __name__ == "__main__":
    unittest.main()
