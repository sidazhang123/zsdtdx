"""期货最新价清洗与失败记录回归测试。"""

from zsdtdx.engine.unified_client import UnifiedTdxClient


class _QuotePool:
    def __init__(self, rows_by_code):
        self.rows_by_code = rows_by_code

    def call(self, method_name, market, code, **kwargs):
        assert method_name == "get_instrument_quote"
        assert kwargs.get("allow_none") is True
        return self.rows_by_code.get(code)


def _client(rows_by_code):
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._future_df = None
    client._future_route = {code: {"market": 30, "code": code} for code in rows_by_code}
    client.ex_pool = _QuotePool(rows_by_code)
    client._runtime_failures = []
    client._runtime_failures_lock = None
    client.get_all_future_list = lambda return_df=True: None
    client._normalize_code_list = lambda codes: list(codes)
    client._normalize_future_query_code = lambda code: str(code)
    return client


def test_future_latest_price_uses_positive_price_then_pre_close():
    client = _client(
        {
            "CU2610": [{"price": 80000.0, "pre_close": 79000.0}],
            "AL2610": [{"price": 0.0, "pre_close": 23000.0}],
        }
    )
    assert client.get_future_latest_price(["CU2610", "AL2610"]) == {
        "CU2610": 80000.0,
        "AL2610": 23000.0,
    }
    assert client._runtime_failures == []


def test_future_latest_price_maps_all_zero_quote_to_none():
    client = _client({"JML8": [{"price": 0.0, "pre_close": 0.0}]})
    assert client.get_future_latest_price(["JML8"]) == {"JML8": None}
    assert client._runtime_failures[-1]["reason"] == "no_valid_quote"
