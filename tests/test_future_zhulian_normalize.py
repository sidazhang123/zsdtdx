"""离线验收期货品种/连续合约/合约月份解析，以及主连补全。"""

import pandas as pd
import pytest
from concurrent.futures import Future

from zsdtdx.engine import parallel_fetcher as pf
from zsdtdx.util.helper import parse_future_symbol
from zsdtdx.engine.unified_client import (
    UnifiedTdxClient,
    _future_variety_prefix_from_main_code,
)


def _client_with_future_catalog(rows: list[dict]) -> UnifiedTdxClient:
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._future_df = pd.DataFrame(rows)
    client._future_route = {}
    client._future_zhulian_by_variety = {}
    client._rebuild_future_zhulian_index()
    return client


def test_variety_prefix_from_main_code():
    assert _future_variety_prefix_from_main_code("CUL8") == "CU"
    assert _future_variety_prefix_from_main_code("al8") == "A"
    assert _future_variety_prefix_from_main_code("ALL8") == "AL"
    assert _future_variety_prefix_from_main_code("CUL9") == "CU"
    assert _future_variety_prefix_from_main_code("L-FL8") == "L-F"
    assert _future_variety_prefix_from_main_code("CU2603") == ""
    assert _future_variety_prefix_from_main_code("EHR00W") == ""


def test_parse_future_symbol_kinds():
    assert parse_future_symbol("CU") == ("variety", "CU")
    assert parse_future_symbol("CUL8") == ("continuous", "CU")
    assert parse_future_symbol("CUL9") == ("continuous", "CU")
    assert parse_future_symbol("ALL8") == ("continuous", "AL")
    assert parse_future_symbol("L-FL8") == ("continuous", "L-F")
    assert parse_future_symbol("CU2603") == ("month", "CU")
    assert parse_future_symbol("cu2603") == ("month", "CU")
    assert parse_future_symbol("TA605") == ("month", "TA")
    assert parse_future_symbol("600000") == ("other", "")
    assert parse_future_symbol("") == ("other", "")


def test_future_batch_does_not_depend_on_variety_whitelist(monkeypatch):
    """新上市品种未进入旧白名单时，期货批处理仍必须走扩展行情。"""

    class _Client:
        def __init__(self):
            self.future_calls = []

        def get_future_kline(self, **kwargs):
            self.future_calls.append(kwargs)
            return iter(
                [pd.DataFrame([{"code": kwargs["codes"], "freq": kwargs["freq"]}])]
            )

        def get_stock_kline(self, **kwargs):
            raise AssertionError(f"期货代码不应进入股票路由: {kwargs}")

    client = _Client()
    monkeypatch.setattr(pf, "_ensure_worker_client_context", lambda: client)
    result = pf._fetch_future_batch([(0, "AD2612", "d", "2026-09-14", "2026-09-18")])

    assert len(result["data"]) == 1
    assert result["failures"] == []
    assert client.future_calls[0]["codes"] == "AD2612"


def test_future_worker_failure_is_recorded_without_serial_fallback(monkeypatch):
    """worker 级异常应逐任务记入运行态失败，但不触发串行补拉。"""

    class _Executor:
        def submit(self, *_args, **_kwargs):
            future = Future()
            future.set_exception(RuntimeError("worker crashed"))
            return future

    class _Context:
        def __init__(self):
            self.failures = []

        def _record_failure(self, *args):
            self.failures.append(args)

    context = _Context()
    monkeypatch.setattr(pf, "_get_global_process_pool", lambda _workers: _Executor())
    monkeypatch.setattr(
        pf.UnifiedTdxClient,
        "get_active_context_client",
        classmethod(lambda cls: context),
    )

    fetcher = pf.ParallelKlineFetcher.__new__(pf.ParallelKlineFetcher)
    fetcher.num_processes = 2
    fetcher.parallel_total_timeout_seconds = 300.0
    fetcher.force_recycle_on_timeout = False
    fetcher.timeout_fallback_to_serial = False
    fetcher._fetch_future_serial = lambda *_args, **_kwargs: pytest.fail(
        "worker 级异常不应串行补拉"
    )
    fetcher._build_chunk_task_detail = lambda _tasks: {}

    result = fetcher._fetch_future_parallel(
        [("AD2612", "d")], "2026-09-14", "2026-09-18"
    )

    assert result.empty
    assert context.failures == [
        ("future_kline", "AD2612", "fetch_error", "worker crashed", "d")
    ]


def test_no_digit_code_maps_to_catalog_zhulian_not_l9():
    client = _client_with_future_catalog(
        [
            {"code": "CUL8", "name": "沪铜主连"},
            {"code": "CUL9", "name": "沪铜加权"},
            {"code": "CU2603", "name": "沪铜2603"},
            {"code": "ALL8", "name": "沪铝主连"},
            {"code": "AL8", "name": "豆一主连"},
            {"code": "L-FL8", "name": "聚乙烯月均价主连"},
            {"code": "LL8", "name": "聚乙烯主连"},
        ]
    )
    assert client._normalize_future_query_code("CU") == "CUL8"
    assert client._normalize_future_query_code("AL") == "ALL8"
    assert client._normalize_future_query_code("A") == "AL8"
    assert client._normalize_future_query_code("L") == "LL8"
    assert client._normalize_future_query_code("L-F") == "L-FL8"
    assert client._normalize_future_query_code("cu2603") == "CU2603"
    assert client._normalize_future_query_code("CUL8") == "CUL8"
    assert client._normalize_future_query_code("CUL9") == "CUL9"


def test_variety_without_zhulian_raises():
    client = _client_with_future_catalog(
        [
            {"code": "ZC2610", "name": "动煤2610"},
            {"code": "ZCL9", "name": "动煤加权"},
        ]
    )
    with pytest.raises(ValueError, match="没有主连合约"):
        client._normalize_future_query_code("ZC")


def test_empty_code_raises():
    client = _client_with_future_catalog([])
    with pytest.raises(ValueError, match="不能为空"):
        client._normalize_future_query_code("  ")
