"""代码质量重构回归：max_kline_pages、import 无 refresh、路由构建等价性、worker __enter__ 传参。"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from zsdtdx.errors import TdxFunctionCallError
from zsdtdx.unified_client import UnifiedTdxClient, _DEFAULT_MAX_KLINE_PAGES


def test_init_module_no_import_time_tcp_refresh():
    init_path = Path(__file__).resolve().parents[1] / "src" / "zsdtdx" / "__init__.py"
    text = init_path.read_text(encoding="utf-8")
    assert "refresh_tcp_probe_cache_from_config_path" not in text


def test_default_max_kline_pages_constant():
    assert _DEFAULT_MAX_KLINE_PAGES == 400
    uc_path = Path(__file__).resolve().parents[1] / "src" / "zsdtdx" / "unified_client.py"
    text = uc_path.read_text(encoding="utf-8")
    assert 'max_kline_pages", 200)' not in text
    assert 'max_kline_pages", 300)' not in text
    assert "def _paginate_kline_pages" in text
    assert text.count("for _ in range(max_pages)") == 0


def test_stock_route_build_equivalent_to_iterrows():
    df = pd.DataFrame(
        [
            {"code": "600000", "source": "std", "market": 1, "name": "浦发银行"},
            {"code": "000001", "source": "std", "market": 0, "name": "平安银行"},
        ]
    )
    legacy = {row["code"]: row.to_dict() for _, row in df.iterrows()}
    current = {str(rec["code"]): rec for rec in df.to_dict(orient="records")}
    assert set(legacy.keys()) == set(current.keys())
    for code in legacy:
        assert legacy[code]["name"] == current[code]["name"]
        assert int(legacy[code]["market"]) == int(current[code]["market"])


def test_ex_market_map_equivalent_to_iterrows():
    ex_df = pd.DataFrame(
        [
            {"market": 31, "name": "上海期货"},
            {"market": None, "name": "无效"},
            {"market": 44, "name": "北京"},
        ]
    )
    legacy = {
        int(row["market"]): row.get("name", "")
        for _, row in ex_df.iterrows()
        if pd.notna(row.get("market"))
    }
    current: dict[int, str] = {}
    for rec in ex_df.to_dict(orient="records"):
        market_val = rec.get("market")
        if market_val is None or (isinstance(market_val, float) and pd.isna(market_val)):
            continue
        name_val = rec.get("name", "")
        current[int(market_val)] = (
            "" if (name_val is None or (isinstance(name_val, float) and pd.isna(name_val))) else str(name_val)
        )
    assert legacy == current


def test_filter_stock_df_by_scopes_equivalent_to_apply():
    df = pd.DataFrame(
        [
            {"code": "600000", "source": "std", "market": 1, "name": "A"},
            {"code": "920001", "source": "ex", "market": 44, "name": "B"},
            {"code": "999999", "source": "other", "market": 9, "name": "C"},
        ]
    )
    scopes = {"szsh", "bj"}

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.market_rules = {"include_hk_market_names": ["港股通"]}
    client._ex_market_name_map = {}

    legacy_mask = df.apply(
        lambda row: client._route_in_scopes(
            {
                "source": row.get("source", ""),
                "market": row.get("market", -1),
                "code": row.get("code", ""),
            },
            scopes,
        ),
        axis=1,
    )
    records = df.to_dict(orient="records")
    current_mask = [
        client._route_in_scopes(
            {
                "source": rec.get("source", ""),
                "market": rec.get("market", -1),
                "code": rec.get("code", ""),
            },
            scopes,
        )
        for rec in records
    ]
    assert legacy_mask.tolist() == current_mask
    assert df.loc[legacy_mask, "code"].tolist() == df.loc[current_mask, "code"].tolist()


def test_enter_passes_presorted_hosts_and_worker_client_to_inner():
    init_calls: list[dict] = []

    def recording_init(self, config_path=None, presorted_hosts=None, worker_client=False):
        init_calls.append(
            {
                "config_path": config_path,
                "presorted_hosts": presorted_hosts,
                "worker_client": worker_client,
            }
        )
        self.config_path = str(config_path or "")
        self.preconnect_on_enter = False
        self.client_cfg = {}
        self.pagination = {}
        self.output_cfg = {}
        self.market_rules = {}
        self.stock_scope_cfg = {}
        self.stock_scope_defaults = {}
        self.index_kline_cfg = {}
        self.index_kline_aliases_cfg = {}
        self.index_kline_lookup_cfg = {}
        self._index_kline_fingerprint = ""
        self._index_catalog_records = []
        self._index_name_route_map = {}
        self.index_kline_route_cache_cfg = {}
        self._index_route_cache_enabled = False
        self._index_route_cache_refresh_granularity = "day"
        self._index_route_cache_date = ""
        self._index_route_cache_path = None
        self._markets_df = None
        self._ex_market_name_map = {}
        self._stock_df = None
        self._future_df = None
        self._stock_route = {}
        self._future_route = {}
        self._instrument_cache = None
        self._runtime_failures = []
        self._entered_client = None
        self._worker_client_flag = bool(worker_client)
        self._presorted_hosts_snapshot = dict(presorted_hosts or {})
        self._shared_chunk_cache = object()

    presorted = {
        "standard": [("120.76.1.198", 7709)],
        "extended": [("47.112.95.207", 7720)],
    }

    with patch.object(UnifiedTdxClient, "__init__", recording_init):
        with patch.object(UnifiedTdxClient, "_warmup_connections", lambda self: None):
            with patch.object(UnifiedTdxClient, "_push_context_client", classmethod(lambda cls, c: None)):
                with patch.object(UnifiedTdxClient, "_pop_context_client", classmethod(lambda cls, c: None)):
                    outer = UnifiedTdxClient(
                        config_path="dummy.yaml",
                        presorted_hosts=presorted,
                        worker_client=True,
                    )
                    init_calls.clear()
                    inner = outer.__enter__()
                    assert inner is not None
                    assert len(init_calls) == 1
                    call = init_calls[0]
                    assert call["worker_client"] is True
                    assert call["presorted_hosts"] == presorted
                    with patch.object(UnifiedTdxClient, "close", lambda self: None):
                        outer.__exit__(None, None, None)


def test_index_kline_at_most_one_refresh_per_request():
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"standard_kline_page_size": 800, "max_kline_pages": 10}
    refresh_true_count = 0
    route = {"name": "测试指数", "source": "std", "market": 1, "code": "000001"}

    def counting_resolve(self, index_name, refresh=False):
        nonlocal refresh_true_count
        if refresh:
            refresh_true_count += 1
        return dict(route)

    client.resolve_index_name = counting_resolve.__get__(client, UnifiedTdxClient)
    client.std_pool = MagicMock()
    client.ex_pool = MagicMock()
    client._pool_call_allow_none = lambda *args, **kwargs: []
    client._append_raw_kline_page_rows = lambda rows, page: None
    client._normalize_index_kline_rows = lambda **kwargs: []
    client._to_datetime_no_df = UnifiedTdxClient._to_datetime_no_df.__get__(
        client, UnifiedTdxClient
    )
    client._freq_to_category = UnifiedTdxClient._freq_to_category.__get__(
        client, UnifiedTdxClient
    )

    task = {
        "index_name": "测试指数",
        "freq": "d",
        "start_time": "2020-01-01",
        "end_time": "2020-12-31",
    }
    client._fetch_index_rows_for_task_with_route_no_df(task, route)
    assert refresh_true_count <= 1


def test_discover_index_route_records_reuses_instrument_cache():
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._index_catalog_records = []
    client.pagination = {
        "standard_security_list_page_size": 800,
        "extended_instrument_info_page_size": 800,
    }
    client.index_kline_cfg = {"prefer_ex_markets": [62]}
    client.index_kline_lookup_cfg = {}
    client._index_route_cache_enabled = False
    client._index_route_cache_path = None
    client._index_kline_fingerprint = ""
    client.std_pool = MagicMock()
    client.std_pool.call.return_value = []
    client.ex_pool = MagicMock()
    client._get_ex_market_name = lambda market: "指数市场"

    instrument_rows = [
        {"name": "中证500", "code": "IC9999", "market": 62},
    ]
    fetch_calls: list[bool] = []

    def fake_fetch_all(self, refresh=False):
        fetch_calls.append(bool(refresh))
        return list(instrument_rows)

    client._fetch_all_instrument_info = fake_fetch_all.__get__(client, UnifiedTdxClient)

    records = client._discover_index_route_records(refresh=False)
    assert len(records) == 1
    assert records[0]["source"] == "ex"
    client.ex_pool.call.assert_not_called()
    assert fetch_calls == [False]

    client._discover_index_route_records(refresh=False)
    assert fetch_calls == [False]


def test_parallel_fetcher_load_config_uses_resolve_and_active_path(tmp_path):
    custom_cfg = tmp_path / "custom_parallel.yaml"
    custom_cfg.write_text(
        "parallel:\n  auto_prewarm_timeout_seconds: 42.5\n  auto_prewarm_max_rounds: 7\n",
        encoding="utf-8",
    )
    from zsdtdx.parallel_fetcher import ParallelKlineFetcher, set_active_config_path

    set_active_config_path(str(custom_cfg.resolve()))
    fetcher = ParallelKlineFetcher(config_path=None)
    assert fetcher.config_path == str(custom_cfg.resolve())
    assert float(fetcher.auto_prewarm_timeout_seconds) == 42.5
    assert int(fetcher.auto_prewarm_max_rounds) == 7


def test_restart_parallel_fetcher_reads_config_defaults(monkeypatch):
    from zsdtdx import simple_api

    captured: dict = {}

    def fake_force_restart(*, prewarm, prewarm_timeout_seconds, max_rounds):
        captured["prewarm"] = prewarm
        captured["prewarm_timeout_seconds"] = prewarm_timeout_seconds
        captured["max_rounds"] = max_rounds
        return {"ok": True}

    class FakeFetcher:
        auto_prewarm_timeout_seconds = 88.0
        auto_prewarm_max_rounds = 5

    monkeypatch.setattr(simple_api, "_force_restart_parallel_fetcher", fake_force_restart)
    monkeypatch.setattr(simple_api, "_ensure_active_config_ready", lambda caller_name: "cfg.yaml")
    monkeypatch.setattr(simple_api, "get_fetcher", lambda: FakeFetcher())

    simple_api.restart_parallel_fetcher()
    assert captured["prewarm"] is True
    assert captured["prewarm_timeout_seconds"] == 88.0
    assert captured["max_rounds"] == 5

    simple_api.restart_parallel_fetcher(prewarm=False, prewarm_timeout_seconds=12.0, max_rounds=2)
    assert captured["prewarm"] is False
    assert captured["prewarm_timeout_seconds"] == 12.0
    assert captured["max_rounds"] == 2


def test_paginate_kline_pages_no_df_and_pandas_boundaries():
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"max_kline_pages": 10}

    pages = [
        [{"datetime": "2020-06-01 15:00:00"}, {"datetime": "2020-06-02 15:00:00"}],
        [{"datetime": "2020-01-01 15:00:00"}],
    ]
    calls: list[int] = []

    def fetch_page(start: int):
        calls.append(start)
        if start == 0:
            return pages[0]
        if start == 2:
            return pages[1]
        return None

    client._append_raw_kline_page_rows = UnifiedTdxClient._append_raw_kline_page_rows.__get__(
        client, UnifiedTdxClient
    )
    client._to_datetime_no_df = UnifiedTdxClient._to_datetime_no_df.__get__(client, UnifiedTdxClient)
    client._is_placeholder_raw_kline_row = lambda row: False

    rows = client._paginate_kline_pages(
        page_size=2,
        start_boundary=client._to_datetime_no_df("2020-03-01"),
        boundary_mode="no_df",
        fetch_page=fetch_page,
    )
    assert len(rows) == 3
    assert calls == [0, 2]

    calls.clear()
    rows_pd = client._paginate_kline_pages(
        page_size=2,
        start_boundary=pd.Timestamp("2020-03-01"),
        boundary_mode="pandas",
        fetch_page=fetch_page,
    )
    assert len(rows_pd) == 3
    assert calls == [0, 2]


def test_parallel_fetcher_no_chunk_timeout_executor_shutdown():
    pf_path = Path(__file__).resolve().parents[1] / "src" / "zsdtdx" / "parallel_fetcher.py"
    text = pf_path.read_text(encoding="utf-8")
    assert "_shutdown_chunk_timeout_executor" not in text
    assert "_get_chunk_timeout_executor" not in text
    assert "task_chunk_inproc_coroutine_workers" in text


def test_parallel_fetcher_loads_coroutine_workers_from_config(tmp_path):
    custom_cfg = tmp_path / "coroutine_workers.yaml"
    custom_cfg.write_text(
        "parallel:\n  task_chunk_inproc_coroutine_workers: 9\n",
        encoding="utf-8",
    )
    from zsdtdx.parallel_fetcher import ParallelKlineFetcher, set_active_config_path

    set_active_config_path(str(custom_cfg.resolve()))
    fetcher = ParallelKlineFetcher(config_path=None)
    assert int(fetcher.task_chunk_inproc_coroutine_workers) == 9


def test_fetch_one_task_chunk_async_timeout_marks_all_tasks_failed():
    import asyncio

    from zsdtdx import parallel_fetcher as pf

    chunk_payload = {
        "chunk_id": "c1",
        "task_kind": "stock",
        "tasks": [
            {"code": "600000", "freq": "d", "start_time": "2026-01-01", "end_time": "2026-01-02"},
            {"code": "600001", "freq": "d", "start_time": "2026-01-01", "end_time": "2026-01-02"},
        ],
        "chunk_timeout_seconds": 0.01,
        "chunk_retry_max_attempts": 0,
    }

    async def instant_timeout(_prep):
        raise asyncio.TimeoutError("chunk attempt timeout")

    with patch.object(pf, "_fetch_one_chunk_attempt_with_timeout_async", side_effect=instant_timeout):
        with patch.object(pf, "_recover_worker_pools_current_thread", return_value={"std": {}, "ex": {}}):
            report = asyncio.run(pf._fetch_one_task_chunk_async(chunk_payload))

    assert int(report.get("chunk_hit_tasks", 0)) == 0
    payloads = list(report.get("payloads") or [])
    assert len(payloads) == 2
    assert all(str(item.get("error") or "").strip() for item in payloads)
    assert all("attempt timeout" in str(item.get("error") or "").lower() for item in payloads)


def test_index_chunk_partition_refresh_at_most_once():
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {
        "standard_kline_page_size": 800,
        "max_kline_pages": 10,
    }
    refresh_true_count = 0
    route = {"name": "测试指数", "source": "std", "market": 1, "code": "000001"}

    def counting_resolve(self, index_name, refresh=False):
        nonlocal refresh_true_count
        if refresh:
            refresh_true_count += 1
        return dict(route)

    client.resolve_index_name = counting_resolve.__get__(client, UnifiedTdxClient)
    client.std_pool = MagicMock()
    client.ex_pool = MagicMock()
    client._to_datetime_no_df = UnifiedTdxClient._to_datetime_no_df.__get__(
        client, UnifiedTdxClient
    )
    client._freq_to_category = UnifiedTdxClient._freq_to_category.__get__(
        client, UnifiedTdxClient
    )
    client._chunk_cache_covers_end_dt = lambda newest, end_dt, freq: False
    client._ensure_index_chunk_route_valid = lambda partition, route_obj: None
    client._merge_index_chunk_page_rows = lambda **kwargs: None
    client._normalize_index_kline_rows = lambda **kwargs: []
    client._slice_index_rows_for_task = lambda **kwargs: []

    page_calls = {"count": 0}

    def flaky_page(**kwargs):
        page_calls["count"] += 1
        if page_calls["count"] == 1:
            raise RuntimeError("page failed")
        return []

    client._fetch_index_kline_page_rows_no_df = flaky_page

    class FakePartitionCache:
        def acquire_partition(self, index_name, freq):
            return {
                "cache_rows": [None] * 8,
                "_n": 0,
                "oldest_dt": None,
                "newest_dt": None,
                "page_start": 0,
                "fetched_pages": 0,
            }

        def soft_delete(self, index_name, freq):
            return None

    client._shared_chunk_cache = FakePartitionCache()

    def fake_merge(page_rows, cache_rows, _n_ref):
        _n_ref[0] = 1
        dt_key = client._to_datetime_no_df("2020-06-01")
        cache_rows[0] = (dt_key, {"datetime": "2020-06-01 15:00:00"})
        return dt_key, dt_key

    client._merge_chunk_cache_page_rows = fake_merge
    client._dt_key_for_raw_kline_row = UnifiedTdxClient._dt_key_for_raw_kline_row.__get__(
        client, UnifiedTdxClient
    )

    tasks = [
        {
            "index_name": "测试指数",
            "freq": "d",
            "start_time": "2020-01-01",
            "end_time": "2020-03-01",
        },
        {
            "index_name": "测试指数",
            "freq": "d",
            "start_time": "2020-04-01",
            "end_time": "2020-06-01",
        },
    ]
    client.get_index_kline_rows_for_chunk_tasks(tasks=tasks, enable_cache=True)
    assert refresh_true_count <= 1


def test_tdx_function_call_error_includes_method_and_endpoint():
    from zsdtdx.base_socket_client import update_last_ack_time

    class DummyClient:
        raise_exception = True
        ip = "127.0.0.1"
        port = 7709
        last_ack_time = 0.0
        last_transaction_failed = False

        @update_last_ack_time
        def get_security_bars(self):
            raise RuntimeError("socket reset")

    client = DummyClient()
    try:
        client.get_security_bars()
    except TdxFunctionCallError as err:
        assert "get_security_bars" in str(err)
        assert "127.0.0.1:7709" in str(err)
        assert err.original_exception is not None
    else:
        raise AssertionError("expected TdxFunctionCallError")
