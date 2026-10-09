"""离线验收码表聚合层：按侧缓存、使用时过滤、指数同时确保两边。"""

import threading
import time
from unittest.mock import MagicMock

import pandas as pd
import pytest

from zsdtdx.engine.unified_client import UnifiedTdxClient


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

    futures = client.get_all_future_list(return_df=False)
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
            {"market": 33, "code": "510500", "name": "中证500ETF"},
            {"market": 33, "code": "017526", "name": "华夏北证50指数C"},
            {"market": 57, "code": "9099DY", "name": "中证1000指数1号"},
        ]

    client._download_std_security_catalog = fake_std
    client._download_ex_instrument_catalog = fake_ex
    client._get_ex_market_name = lambda market: {
        33: "开放式基金",
        57: "券商集合理财",
        62: "中证指数",
    }.get(int(market), "")
    records = client._discover_index_route_records(refresh=True)
    assert sides == ["std", "ex"]
    names = {row["name"] for row in records}
    assert "上证指数" in names
    assert "中证500" in names
    assert "浦发银行" not in names
    assert "沪铜主连" not in names
    assert "中证500ETF" not in names
    assert "华夏北证50指数C" not in names
    assert "中证1000指数1号" not in names


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


def test_std_catalog_page_error_never_caches_partial_rows():
    """输入首分页有数据、下一页返回 None。输出整次失败且旧/半截数据均不可复用。"""
    client = _catalog_client()
    client._std_catalog_records = [
        {"market": 1, "code": "600001", "name": "昨日旧数据"}
    ]
    client._std_catalog_date = "2000-01-01"
    client._std_catalog_markets = lambda: [1]
    calls = 0

    def page(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [
                {"code": f"{600000 + index:06d}", "name": f"股票{index}"}
                for index in range(1600)
            ]
        return None

    client.std_pool.call = page
    with pytest.raises(RuntimeError, match="标准行情码表分页失败"):
        client._ensure_std_catalog()
    assert client._std_catalog_records is None
    assert client._std_catalog_date == ""
    assert client._stock_df is None


def test_std_catalog_short_page_is_terminal():
    """输入标准码表短页。输出直接结束，不额外请求短页后的空页。"""
    client = _catalog_client()
    client._std_catalog_markets = lambda: [2]
    offsets: list[int] = []

    def page(method, market, start, size, **kwargs):
        offsets.append(start)
        if start != 0:
            raise AssertionError("标准码表短页后不应再请求")
        return [{"code": "920000", "name": "北交样本"}]

    client.std_pool.call = page
    rows = client._download_std_security_catalog()
    assert rows == [{"code": "920000", "name": "北交样本", "market": 2}]
    assert offsets == [0]


def test_ex_catalog_empty_page_is_normal_end(monkeypatch):
    """输入一整页数据后空页。输出空页只表示没有更早数据，完整结果可缓存。"""
    client = _catalog_client()
    page_size = 5
    monkeypatch.setattr(
        "zsdtdx.engine.unified_client.TDXParams.EXTENDED_INSTRUMENT_INFO_PAGE_SIZE",
        page_size,
    )
    pages = {
        0: [
            {"market": 30, "code": f"CU{index:04d}", "name": f"沪铜{index}"}
            for index in range(page_size)
        ],
        page_size: [],
    }
    client.ex_pool.call = lambda method, start, size, **kwargs: pages[start]
    client._ensure_ex_catalog()
    assert len(client._ex_catalog_records) == page_size
    assert client._catalog_memory_fresh("ex") is True


def test_catalog_short_page_does_not_end_before_empty_page(monkeypatch):
    """输入短页后仍有一页数据。输出必须继续翻页，直到服务端明确返回空页。"""
    client = _catalog_client()
    monkeypatch.setattr(
        "zsdtdx.engine.unified_client.TDXParams.EXTENDED_INSTRUMENT_INFO_PAGE_SIZE",
        5,
    )
    offsets: list[int] = []
    pages = {
        0: [
            {"market": 30, "code": "CU01", "name": "沪铜一"},
            {"market": 30, "code": "CU02", "name": "沪铜二"},
        ],
        2: [{"market": 30, "code": "CU03", "name": "沪铜三"}],
        3: [],
    }

    def page(method, start, size, **kwargs):
        offsets.append(start)
        return pages[start]

    client.ex_pool.call = page
    rows = client._download_ex_instrument_catalog()
    assert [row["code"] for row in rows] == ["CU01", "CU02", "CU03"]
    assert offsets == [0, 2, 3]


def test_expired_derived_future_view_is_rebuilt():
    """输入昨日派生期货表和今日新码表下载。输出不得直接复用昨日派生表。"""
    client = _catalog_client()
    client._future_df = pd.DataFrame(
        [{"code": "OLDL8", "name": "旧主连", "market": 30}]
    )
    client._ex_catalog_records = [{"market": 30, "code": "OLDL8", "name": "旧主连"}]
    client._ex_catalog_date = "2000-01-01"
    client._download_ex_instrument_catalog = lambda: [
        {"market": 30, "code": "CUL8", "name": "沪铜主连"}
    ]
    rows = client.get_all_future_list(return_df=False)
    assert [row["code"] for row in rows] == ["CUL8"]


def test_one_catalog_write_failure_does_not_disable_other_cache(monkeypatch, tmp_path):
    """输入 std 写盘失败。输出内存结果仍可用，且 ex 缓存能力不会被连带关闭。"""
    import zsdtdx.engine.unified_client as unified_module

    client = _catalog_client()
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = tmp_path
    client._download_std_security_catalog = lambda: [
        {"market": 1, "code": "600000", "name": "浦发银行"}
    ]
    client._download_ex_instrument_catalog = lambda: [
        {"market": 30, "code": "CUL8", "name": "沪铜主连"}
    ]
    failures: list[str] = []
    client._record_failure = lambda *args: failures.append(str(args))
    real_save = unified_module.save_catalog_cache

    def selective_save(path, **kwargs):
        if kwargs.get("kind") == "std":
            raise OSError("磁盘只读")
        return real_save(path, **kwargs)

    monkeypatch.setattr(unified_module, "save_catalog_cache", selective_save)
    client.ensure_code_catalog(need_std=True, need_ex=True)
    assert client._catalog_cache_enabled is True
    assert client._std_catalog_records[0]["code"] == "600000"
    assert (tmp_path / "ex_instrument_info.pkl").is_file()
    assert any("write_failed" in item for item in failures)


def test_catalog_cold_start_singleflight_downloads_once(tmp_path):
    """输入两个客户端同时冷启动同一缓存。输出仅一个下载，另一个在锁后读盘。"""
    clients = [_catalog_client(), _catalog_client()]
    for client in clients:
        client._catalog_cache_enabled = True
        client._catalog_cache_dir = tmp_path
    downloads: list[int] = []
    downloads_lock = threading.Lock()

    def download():
        with downloads_lock:
            downloads.append(1)
        time.sleep(0.1)
        return [{"market": 1, "code": "600000", "name": "浦发银行"}]

    for client in clients:
        client._download_std_security_catalog = download
    threads = [
        threading.Thread(
            target=client.ensure_code_catalog,
            kwargs={"need_std": True},
        )
        for client in clients
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)
    assert all(not thread.is_alive() for thread in threads)
    assert downloads == [1]
    assert all(client._std_catalog_records[0]["code"] == "600000" for client in clients)


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
    names = client.get_stock_code_name_map()
    assert "hk.00700" in names
    assert names["hk.00700"] == "腾讯控股"
    assert "hk.02800" in names
    assert "hk.SHGGT" not in names
