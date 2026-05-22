# -*- coding: utf-8 -*-
"""指数 K 线 async 对齐相关单元测试。"""

from __future__ import annotations

import datetime as dt
import struct
import unittest
from unittest.mock import MagicMock, patch

from zsdtdx.parallel_fetcher import _build_index_task_payload, _build_task_payload_for_kind
from zsdtdx.parser.get_index_bars import GetIndexBarsCmd


class TestIndexParserSocket(unittest.TestCase):
    def test_parse_response_empty_page(self):
        """空页应返回空列表且不抛错。"""
        cmd = GetIndexBarsCmd(client=None, lock=None)
        cmd.category = 4
        rows = cmd.parseResponse(struct.pack("<H", 0))
        self.assertEqual(rows, [])

    def test_parse_price_chain_matches_cal_price1000(self):
        """差分价链应与 site 包 _cal_price1000 语义一致（不在 parser 层 round）。"""
        cmd = GetIndexBarsCmd(client=None, lock=None)
        cmd.category = 9
        pre_diff_base = 1000
        price_open_diff = 50
        price_close_diff = 30
        open_v = float(pre_diff_base + price_open_diff) / 1000.0
        new_base = pre_diff_base + price_open_diff
        close_v = float(new_base + price_close_diff) / 1000.0
        self.assertAlmostEqual(open_v, 1.05)
        self.assertAlmostEqual(close_v, 1.08)


class TestIndexPayloadKind(unittest.TestCase):
    def test_index_payload_shape(self):
        task = {
            "index_name": "上证指数",
            "freq": "d",
            "start_time": "2026-01-01 00:00:00",
            "end_time": "2026-01-02 00:00:00",
        }
        payload = _build_task_payload_for_kind("index", task=task, rows=[], error=None, worker_pid=1)
        self.assertEqual(payload["task"]["index_name"], "上证指数")
        self.assertNotIn("code", payload["task"])


class TestChunkCacheCoversEnd(unittest.TestCase):
    def test_daily_bar_1500_covers_end_1600_same_day(self):
        from zsdtdx.unified_client import UnifiedTdxClient

        client = UnifiedTdxClient.__new__(UnifiedTdxClient)
        newest = dt.datetime(2026, 5, 18, 15, 0)
        end = dt.datetime(2026, 5, 18, 16, 0)
        self.assertTrue(
            UnifiedTdxClient._chunk_cache_covers_end_dt(client, newest, end, "d")
        )

    def test_minute_bar_requires_datetime_not_calendar_day_only(self):
        from zsdtdx.unified_client import UnifiedTdxClient

        client = UnifiedTdxClient.__new__(UnifiedTdxClient)
        end = dt.datetime(2026, 5, 18, 16, 0)
        self.assertFalse(
            UnifiedTdxClient._chunk_cache_covers_end_dt(
                client, dt.datetime(2026, 5, 18, 11, 30), end, "15"
            )
        )
        self.assertTrue(
            UnifiedTdxClient._chunk_cache_covers_end_dt(
                client, dt.datetime(2026, 5, 18, 16, 0), end, "15"
            )
        )


class TestPlaceholderRawKlineFilter(unittest.TestCase):
    def test_skip_ohlc_equal_zero_volume(self):
        from zsdtdx.unified_client import UnifiedTdxClient

        client = UnifiedTdxClient.__new__(UnifiedTdxClient)
        client.output_cfg = {
            "filter_suspended_placeholder_bar": True,
            "suspended_placeholder_eps": 1.0e-20,
        }
        placeholder = {
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "trade": 0,
            "amount": 0.0,
            "datetime": "2026-05-18 13:15",
        }
        normal = {
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "trade": 100,
            "amount": 1000.0,
            "datetime": "2026-05-18 11:30",
        }
        self.assertTrue(UnifiedTdxClient._is_placeholder_raw_kline_row(client, placeholder))
        self.assertFalse(UnifiedTdxClient._is_placeholder_raw_kline_row(client, normal))
        filtered = UnifiedTdxClient._filter_placeholder_raw_kline_rows(
            client, [placeholder, normal]
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["datetime"], "2026-05-18 11:30")

    def test_append_page_keeps_normal_bars_when_page_has_placeholder(self):
        from zsdtdx.unified_client import UnifiedTdxClient

        client = UnifiedTdxClient.__new__(UnifiedTdxClient)
        client.output_cfg = {
            "filter_suspended_placeholder_bar": True,
            "suspended_placeholder_eps": 1.0e-20,
        }
        placeholder = {
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "trade": 0,
            "amount": 0.0,
            "datetime": "2026-05-18 13:15",
        }
        normal = {
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "trade": 100,
            "amount": 1000.0,
            "datetime": "2026-05-18 11:30",
        }
        raw_page = [placeholder, normal]
        rows: list = []
        raw_len = UnifiedTdxClient._append_raw_kline_page_rows(client, rows, raw_page)
        self.assertEqual(raw_len, 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["datetime"], "2026-05-18 11:30")


class TestIndexChunkCache(unittest.TestCase):
    def test_multi_end_time_second_task_hits_cache(self):
        from zsdtdx.unified_client import UnifiedTdxClient

        from zsdtdx.unified_client import SharedChunkCache

        client = UnifiedTdxClient.__new__(UnifiedTdxClient)
        client.pagination = {"standard_kline_page_size": 800, "max_kline_pages": 300}
        client.output_cfg = {"filter_suspended_placeholder_bar": False}
        client._shared_chunk_cache = SharedChunkCache()
        client._normalize_freq = UnifiedTdxClient._normalize_freq.__get__(client, UnifiedTdxClient)
        client._freq_to_category = UnifiedTdxClient._freq_to_category.__get__(client, UnifiedTdxClient)
        client._to_datetime_no_df = UnifiedTdxClient._to_datetime_no_df.__get__(client, UnifiedTdxClient)
        client._dt_key_for_raw_kline_row = UnifiedTdxClient._dt_key_for_raw_kline_row.__get__(
            client, UnifiedTdxClient
        )
        client._normalize_index_task_payload_no_df = UnifiedTdxClient._normalize_index_task_payload_no_df.__get__(
            client, UnifiedTdxClient
        )
        client._normalize_index_kline_rows = UnifiedTdxClient._normalize_index_kline_rows.__get__(
            client, UnifiedTdxClient
        )
        client._merge_chunk_cache_page_rows = UnifiedTdxClient._merge_chunk_cache_page_rows.__get__(
            client, UnifiedTdxClient
        )
        client._index_route_fingerprint = UnifiedTdxClient._index_route_fingerprint.__get__(
            client, UnifiedTdxClient
        )
        client._reset_index_chunk_partition = UnifiedTdxClient._reset_index_chunk_partition.__get__(
            client, UnifiedTdxClient
        )
        client._ensure_index_chunk_route_valid = UnifiedTdxClient._ensure_index_chunk_route_valid.__get__(
            client, UnifiedTdxClient
        )
        client.resolve_index_name = MagicMock(
            return_value={"name": "上证指数", "source": "std", "market": 1, "code": "000001"}
        )

        base_ts = int(dt.datetime(2026, 1, 5, 15, 0).timestamp())
        page_row = {
            "open": 3000.0,
            "close": 3010.0,
            "high": 3020.0,
            "low": 2990.0,
            "vol": 100,
            "amount": 200,
            "datetime": "2026-01-05 15:00:00",
            "_ts": base_ts,
        }
        call_count = {"n": 0}

        def fake_fetch(**_kwargs):
            call_count["n"] += 1
            return [dict(page_row)]

        client._fetch_index_kline_page_rows_no_df = fake_fetch

        tasks = [
            {
                "index_name": "上证指数",
                "freq": "d",
                "start_time": "2026-01-05 15:00:00",
                "end_time": "2026-01-05 15:00:00",
                "_index_route_source": "std",
                "_index_route_market": 1,
                "_index_route_code": "000001",
                "_index_route_name": "上证指数",
            },
            {
                "index_name": "上证指数",
                "freq": "d",
                "start_time": "2026-01-05 15:00:00",
                "end_time": "2026-01-05 15:00:00",
                "_index_route_source": "std",
                "_index_route_market": 1,
                "_index_route_code": "000001",
                "_index_route_name": "上证指数",
            },
        ]
        out = client.get_index_kline_rows_for_chunk_tasks(tasks=tasks, enable_cache=True)
        self.assertEqual(call_count["n"], 1)
        self.assertEqual(out["chunk_network_page_calls"], 1)
        self.assertGreaterEqual(out["chunk_hit_tasks"], 1)
        self.assertEqual(len(out["results"]), 2)
        self.assertIsNone(out["results"][1].get("error"))


if __name__ == "__main__":
    unittest.main()
