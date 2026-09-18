"""离线验收期货品种/连续合约/合约月份解析，以及主连补全。"""

import pandas as pd
import pytest

from zsdtdx.helper import parse_future_symbol
from zsdtdx.parallel_fetcher import is_future_code
from zsdtdx.unified_client import (
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


def test_is_future_code_continuous_not_merged_into_variety():
    assert is_future_code("CUL8") is True
    assert is_future_code("ALL8") is True
    assert is_future_code("L-FL8") is True
    assert is_future_code("CU") is True
    assert is_future_code("CU2603") is True
    assert is_future_code("L-F") is True
    assert is_future_code("600000") is False
    assert is_future_code("00700") is False
    assert is_future_code("") is False


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
