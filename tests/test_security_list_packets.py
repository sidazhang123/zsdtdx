"""离线验收标准行情码表 0x044D 组包与 37 字节记录解析。"""

import struct
from unittest.mock import MagicMock

from zsdtdx.params import TDXParams
from zsdtdx.parser.get_security_list import (
    GetSecurityList,
    pack_security_list_request,
)
from zsdtdx.unified_client import UnifiedTdxClient

# 银河 0x044D 抓包：market=2 首页第一条 899050 北证50。
_BJ_FIRST_RECORD = bytes.fromhex(
    "3839393035306400b1b1d6a43530000000000000000000003b43dd46023f09824400000000"
)
# 银河 0x044D 深圳页：000001 / 159915 / 159105。
_SZ_000001_RECORD = bytes.fromhex(
    "3030303030316400c6bdb0b2d2f8d0d0000000000000000085de4f450233333b4147321c26"
)
_SZ_159915_RECORD = bytes.fromhex(
    "3135393931356400b4b4d2b5b0e5455446d2d7b7bdb4ef00d9eb814703250659408d32e312"
)
_SZ_159105_RECORD = bytes.fromhex(
    "3135393130356400bae3c9fac9faceefbfc6bcbc455446d21c98824503d7a3703f8d320000"
)
# 银河 0x044D 上海页：510050 / 510300（16 字节名称到「柏」为止）。
_SH_510050_RECORD = bytes.fromhex(
    "3531303035306400c9cfd6a43530455446bbaacfc40000003279ac460366663e408d320325"
)
_SH_510300_RECORD = bytes.fromhex(
    "3531303330306400bba6c9ee333030455446bbaacca9b0d81a79df4603be9f92408d322425"
)


def test_pack_security_list_request_matches_official_layout():
    """输入：market=2、start=0；输出：26 字节、命令 0x044D、count=1600。"""
    pkg = pack_security_list_request(2, 0)
    assert len(pkg) == 26
    assert struct.unpack_from("<H", pkg, 10)[0] == 0x044D
    market, start, count = struct.unpack_from("<HIH", pkg, 12)
    assert (market, start, count) == (2, 0, TDXParams.MAX_SECURITY_LIST_COUNT)
    assert bytes(pkg[20:]) == bytes(6)


def test_pack_security_list_request_paginates_start():
    """输入：start=1600；输出：32 位 start 字段为 1600。"""
    pkg = pack_security_list_request(0, 1600, 1600)
    assert struct.unpack_from("<I", pkg, 14)[0] == 1600


def test_get_security_list_cmd_uses_pack_helper():
    """输入：GetSecurityList.setParams；输出：与 pack 函数一致。"""
    cmd = GetSecurityList(None)
    cmd.setParams(1, 27200, 1600)
    assert bytes(cmd.send_pkg) == bytes(pack_security_list_request(1, 27200, 1600))


def test_parse_security_list_37_byte_beijing_index():
    """输入：抓包 37 字节记录；输出：代码 899050、名称北证50、小数位 2。"""
    assert len(_BJ_FIRST_RECORD) == TDXParams.SECURITY_LIST_RECORD_SIZE
    body = struct.pack("<H", 1) + _BJ_FIRST_RECORD
    rows = GetSecurityList(None).parseResponse(body)
    assert len(rows) == 1
    assert rows[0]["code"] == "899050"
    assert rows[0]["name"] == "北证50"
    assert rows[0]["volunit"] == 100
    assert rows[0]["decimal_point"] == 2


def test_parse_security_list_16_byte_gbk_name_from_galaxy_capture():
    """输入：银河 0x044D 37 字节记录；输出：16 字节 GBK 名称与小数位。"""
    body = struct.pack("<H", 5) + (
        _SZ_000001_RECORD
        + _SZ_159915_RECORD
        + _SZ_159105_RECORD
        + _SH_510050_RECORD
        + _SH_510300_RECORD
    )
    rows = GetSecurityList(None).parseResponse(body)
    by_code = {row["code"]: row for row in rows}
    assert by_code["000001"]["name"] == "平安银行"
    assert by_code["000001"]["decimal_point"] == 2
    assert by_code["159915"]["name"] == "创业板ETF易方达"
    assert by_code["159915"]["decimal_point"] == 3
    assert by_code["159105"]["name"] == "恒生生物科技ETF"
    assert by_code["159105"]["decimal_point"] == 3
    assert by_code["510050"]["name"] == "上证50ETF华夏"
    assert by_code["510050"]["decimal_point"] == 3
    assert by_code["510050"]["volunit"] == 100
    assert by_code["510300"]["name"] == "沪深300ETF华泰柏"


def test_parse_security_list_empty_and_truncated():
    """输入：空包与半截记录；输出：空列表或仅完整记录。"""
    cmd = GetSecurityList(None)
    assert cmd.parseResponse(b"") == []
    assert cmd.parseResponse(b"\x01") == []
    body = struct.pack("<H", 2) + _BJ_FIRST_RECORD + b"\x00\x01"
    rows = cmd.parseResponse(body)
    assert [row["code"] for row in rows] == ["899050"]


def test_is_std_index_item_covers_sz_sh_bj():
    """输入：深沪京码表条目；输出：指数保留、个股排除。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    assert client._is_std_index_item(0, "399001", "深证成指") is True
    assert client._is_std_index_item(0, "000001", "平安银行") is False
    assert client._is_std_index_item(1, "000001", "上证指数") is True
    assert client._is_std_index_item(1, "600000", "浦发银行") is False
    assert client._is_std_index_item(2, "899050", "北证50") is True
    assert client._is_std_index_item(2, "920002", "万达轴承") is False


def test_get_all_stock_list_takes_beijing_from_hq_market_2():
    """输入：HQ 0/1/2 码表 + ExHQ 44；输出：北交所仅来自 market=2，44 不混入。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._stock_df = None
    client._stock_route = {}
    client.output_cfg = {"return_df_default": False}
    client.pagination = {
        "standard_security_list_page_size": TDXParams.MAX_SECURITY_LIST_COUNT
    }
    client.market_rules = {
        "include_beijing_prefixes": ["92"],
        "stock_prefix_sz": ["000", "001", "002", "003", "300"],
        "stock_prefix_sh": ["600", "601", "603", "605"],
    }
    client._ex_market_name_map = {44: "股转系统", 71: "港股通"}
    client._catalog_cache_enabled = False
    client._catalog_cache_dir = None
    client._std_catalog_records = None
    client._ex_catalog_records = None
    client._std_catalog_date = ""
    client._ex_catalog_date = ""
    client._index_catalog_records = []
    client._index_name_route_map = {}
    client._future_df = None
    client._future_route = {}
    client._future_zhulian_by_variety = {}

    def fake_call(method_name, market, start, count=1600, allow_none=True):
        if int(start) != 0:
            return []
        if int(market) == 0:
            return [{"code": "000001", "name": "平安银行"}]
        if int(market) == 1:
            return [{"code": "600000", "name": "浦发银行"}]
        if int(market) == 2:
            return [
                {"code": "920002", "name": "万达轴承"},
                {"code": "920025", "name": "凯达重工"},
                {"code": "899050", "name": "北证50"},
            ]
        return []

    client.std_pool = MagicMock()
    client.std_pool.call.side_effect = fake_call
    client._download_ex_instrument_catalog = lambda: [
        {"market": 44, "code": "920229", "name": "世纪数码"},
        {"market": 71, "code": "00700", "name": "腾讯控股"},
    ]

    rows = client.get_all_stock_list(return_df=False, refresh=True)
    by_code = {row["code"]: row for row in rows}
    assert by_code["000001"]["market"] == 0
    assert by_code["600000"]["market"] == 1
    assert by_code["920002"]["source"] == "std"
    assert by_code["920002"]["market"] == 2
    assert by_code["920025"]["name"] == "凯达重工"
    assert "899050" not in by_code
    assert "920229" not in by_code
    assert by_code["00700"]["source"] == "ex"
    std_codes = {row["code"] for row in client._std_catalog_records}
    assert "899050" in std_codes


def test_discover_index_route_includes_beijing_from_std_list():
    """输入：HQ 三市场码表；输出：指数目录含上证指数与北证50。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._index_catalog_records = []
    client._index_name_route_map = {}
    client._catalog_cache_enabled = False
    client._catalog_cache_dir = None
    client._std_catalog_records = None
    client._ex_catalog_records = None
    client._std_catalog_date = ""
    client._ex_catalog_date = ""
    client._stock_df = None
    client._stock_route = {}
    client._future_df = None
    client._future_route = {}
    client._future_zhulian_by_variety = {}
    client._ex_market_name_map = {}
    client.pagination = {
        "standard_security_list_page_size": TDXParams.MAX_SECURITY_LIST_COUNT
    }
    client.index_kline_cfg = {"prefer_ex_markets": [62]}
    client.index_kline_lookup_cfg = {"normalize_whitespace": True}

    def fake_call(method_name, market, start, count=1600, allow_none=True):
        if int(start) != 0:
            return []
        if int(market) == 0:
            return [
                {"code": "000001", "name": "平安银行"},
                {"code": "399001", "name": "深证成指"},
            ]
        if int(market) == 1:
            return [{"code": "000001", "name": "上证指数"}]
        if int(market) == 2:
            return [
                {"code": "899050", "name": "北证50"},
                {"code": "920002", "name": "万达轴承"},
            ]
        return []

    client.std_pool = MagicMock()
    client.std_pool.call.side_effect = fake_call
    client._download_ex_instrument_catalog = lambda: [
        {"name": "中证500", "code": "000905", "market": 62},
    ]
    client._get_ex_market_name = lambda market: "中证指数" if int(market) == 62 else ""

    records = client._discover_index_route_records(refresh=True)
    by_name = {row["name"]: row for row in records}
    assert by_name["深证成指"]["source"] == "std"
    assert by_name["深证成指"]["market"] == 0
    assert by_name["上证指数"]["market"] == 1
    assert by_name["北证50"]["source"] == "std"
    assert by_name["北证50"]["market"] == 2
    assert "万达轴承" not in by_name
    assert "平安银行" not in by_name
    assert by_name["中证500"]["source"] == "ex"
