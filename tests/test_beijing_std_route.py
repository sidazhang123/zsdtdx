"""离线验收北交所走标准行情 market=2，扩展行情 market=44 不再作为北交所路由。"""

from zsdtdx.hq import TdxHq_API
from zsdtdx.params import TDXParams
from zsdtdx.unified_client import UnifiedTdxClient


def _client() -> UnifiedTdxClient:
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.market_rules = {
        "include_beijing_prefixes": ["92"],
        "stock_prefix_sz": ["000", "001", "002", "003", "300"],
        "stock_prefix_sh": ["600", "601", "603", "605"],
    }
    client._ex_market_name_map = {44: "股转系统", 71: "港股通"}
    client._stock_route = {}
    return client


def test_hq_get_markets_includes_beijing_market_2():
    api = TdxHq_API.__new__(TdxHq_API)
    markets = api.get_markets()
    by_market = {int(row["market"]): row["name"] for row in markets}
    assert by_market[TDXParams.MARKET_BJ] == "北京"
    assert TDXParams.MARKET_BJ == 2


def test_beijing_code_routes_to_std_market_2():
    client = _client()
    route = client._lookup_stock_route("920002")
    assert route["source"] == "std"
    assert route["market"] == 2
    assert route["market_name"] == "北京"
    assert client._route_scope(route) == "bj"
    assert client._stock_code_with_prefix("std", 2, "920002") == "bj.920002"


def test_ex_market_44_is_not_beijing_stock_scope():
    client = _client()
    assert client._route_scope({"source": "ex", "market": 44, "code": "920002"}) is None


def test_listed_std_stock_accepts_beijing_prefix_on_market_2():
    client = _client()
    assert client._is_listed_std_stock(2, "920001") is True
    assert client._is_listed_std_stock(2, "600000") is False
    assert client._is_listed_std_stock(0, "000001") is True
