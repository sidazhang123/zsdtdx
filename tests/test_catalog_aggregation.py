"""离线验收码表聚合层：按侧缓存、使用时过滤、指数同时确保两边。"""

from unittest.mock import MagicMock

from zsdtdx.unified_client import UnifiedTdxClient


def _catalog_client() -> UnifiedTdxClient:
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {
        "standard_security_list_page_size": 1600,
        "extended_instrument_info_page_size": 800,
    }
    client.output_cfg = {"return_df_default": False}
    client.market_rules = {
        "include_beijing_prefixes": ["92"],
        "stock_prefix_sz": ["000", "001", "002", "003", "300"],
        "stock_prefix_sh": ["600", "601", "603", "605"],
        "future_market_names": ["上海期货", "大连商品"],
    }
    client.index_kline_cfg = {}
    client.index_kline_lookup_cfg = {"normalize_whitespace": True}
    client._ex_market_name_map = {
        30: "上海期货",
        31: "香港主板",
        47: "中金所期货",
        62: "中证指数",
        71: "港股通",
    }
    client._catalog_cache_enabled = False
    client._catalog_cache_dir = None
    client._std_catalog_records = None
    client._ex_catalog_records = None
    client._std_catalog_date = ""
    client._ex_catalog_date = ""
    client._index_catalog_records = []
    client._index_name_route_map = {}
    client._stock_df = None
    client._stock_route = {}
    client._future_df = None
    client._future_route = {}
    client._future_zhulian_by_variety = {}
    client.stock_scope_defaults = {}
    client.std_pool = MagicMock()
    client.ex_pool = MagicMock()
    return client


def test_ensure_code_catalog_only_downloads_needed_side():
    client = _catalog_client()
    std_calls = []
    ex_calls = []

    def fake_std():
        std_calls.append(1)
        return [{"market": 1, "code": "600000", "name": "浦发银行"}]

    def fake_ex():
        ex_calls.append(1)
        return [{"market": 30, "code": "CUL8", "name": "沪铜主连"}]

    client._download_std_security_catalog = fake_std
    client._download_ex_instrument_catalog = fake_ex

    client.ensure_code_catalog(need_ex=True)
    assert std_calls == []
    assert ex_calls == [1]

    client.ensure_code_catalog(need_std=True)
    assert std_calls == [1]
    assert ex_calls == [1]

    client.ensure_code_catalog(need_std=True, need_ex=True)
    assert std_calls == [1]
    assert ex_calls == [1]


def test_stock_and_future_filter_at_use_not_download():
    client = _catalog_client()
    client._download_std_security_catalog = lambda: [
        {"market": 0, "code": "000001", "name": "平安银行"},
        {"market": 0, "code": "399001", "name": "深证成指"},
        {"market": 1, "code": "000001", "name": "上证指数"},
        {"market": 2, "code": "899050", "name": "北证50"},
        {"market": 2, "code": "920002", "name": "万达轴承"},
    ]
    client._download_ex_instrument_catalog = lambda: [
        {"market": 30, "code": "CUL8", "name": "沪铜主连"},
        {"market": 47, "code": "IFL8", "name": "沪深主连"},
        {"market": 31, "code": "00700", "name": "腾讯控股"},
        {"market": 71, "code": "00700", "name": "腾讯控股"},
        {"market": 71, "code": "SHGGT", "name": "沪港通"},
        {"market": 62, "code": "000905", "name": "中证500"},
    ]

    stocks = client.get_all_stock_list(return_df=False, refresh=True)
    stock_codes = {row["code"] for row in stocks}
    assert stock_codes == {"000001", "920002", "00700"}
    assert "399001" not in stock_codes
    assert {row["code"] for row in client._std_catalog_records} >= {
        "399001",
        "899050",
        "920002",
    }

    futures = client.get_all_future_list(return_df=False, use_cache=False)
    future_codes = {row["code"] for row in futures}
    assert future_codes == {"CUL8"}
    assert "IFL8" not in future_codes
    assert any(row["code"] == "IFL8" for row in client._ex_catalog_records)


def test_index_discover_requires_both_catalog_sides():
    client = _catalog_client()
    sides = []

    def fake_std():
        sides.append("std")
        return [
            {"market": 1, "code": "000001", "name": "上证指数"},
            {"market": 1, "code": "600000", "name": "浦发银行"},
        ]

    def fake_ex():
        sides.append("ex")
        return [
            {"market": 62, "code": "000905", "name": "中证500"},
            {"market": 30, "code": "CUL8", "name": "沪铜主连"},
        ]

    client._download_std_security_catalog = fake_std
    client._download_ex_instrument_catalog = fake_ex
    records = client._discover_index_route_records(refresh=True)
    assert sides == ["std", "ex"]
    names = {row["name"] for row in records}
    assert "上证指数" in names
    assert "中证500" in names
    assert "浦发银行" not in names
    assert "沪铜主连" not in names


def test_catalog_disk_hit_skips_download(tmp_path):
    client = _catalog_client()
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = tmp_path
    client._download_std_security_catalog = lambda: [
        {"market": 1, "code": "600000", "name": "浦发银行"}
    ]
    client._download_ex_instrument_catalog = lambda: [
        {"market": 30, "code": "CUL8", "name": "沪铜主连"}
    ]
    client.ensure_code_catalog(need_std=True, need_ex=True)

    other = _catalog_client()
    other._catalog_cache_enabled = True
    other._catalog_cache_dir = tmp_path
    other._download_std_security_catalog = lambda: (_ for _ in ()).throw(
        AssertionError("std 不应再下载")
    )
    other._download_ex_instrument_catalog = lambda: (_ for _ in ()).throw(
        AssertionError("ex 不应再下载")
    )
    other.ensure_code_catalog(need_std=True, need_ex=True)
    assert other._std_catalog_records[0]["code"] == "600000"
    assert other._ex_catalog_records[0]["code"] == "CUL8"


def test_stock_list_keeps_hk_connect_not_main_board():
    """输入：香港主板与港股通同码、港股通占位码；输出：仅港股通五位代码入股票清单。"""
    client = _catalog_client()
    client._download_std_security_catalog = lambda: [
        {"market": 1, "code": "600000", "name": "浦发银行"},
    ]
    client._download_ex_instrument_catalog = lambda: [
        {"market": 31, "code": "00700", "name": "腾讯控股"},
        {"market": 71, "code": "00700", "name": "腾讯控股"},
        {"market": 71, "code": "02800", "name": "盈富基金"},
        {"market": 71, "code": "SHGGT", "name": "沪港通"},
        {"market": 48, "code": "08328", "name": "信义储电"},
    ]
    client.stock_scope_defaults = {"get_stock_code_name": ["szsh", "hk"]}
    stocks = client.get_all_stock_list(return_df=False, refresh=True)
    by_code = {row["code"]: row for row in stocks}
    assert set(by_code) == {"600000", "00700", "02800"}
    assert by_code["00700"]["market"] == 71
    assert by_code["00700"]["market_name"] == "港股通"
    assert client._stock_code_with_prefix("ex", 71, "00700") == "hk.00700"
    assert client._is_hk_stock_ex(31, "00700") is False
    assert client._is_hk_stock_ex(71, "SHGGT") is False
    assert client._is_hk_stock_ex(71, "0700") is False
    assert client._is_hk_stock_ex(71, "000700") is False
    assert client._route_scope({"source": "ex", "market": 71, "code": "00700"}) == "hk"
    assert client._route_scope({"source": "ex", "market": 31, "code": "00700"}) is None
    names = client.get_stock_code_name_map(use_cache=True)
    assert "hk.00700" in names
    assert names["hk.00700"] == "腾讯控股"
    assert "hk.02800" in names
    assert "hk.SHGGT" not in names
