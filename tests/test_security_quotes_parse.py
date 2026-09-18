"""离线验收标准行情五档解析：未上市占位码不得毒死同批其它代码，也不得触发拆单重试。"""

from zsdtdx.parser.get_security_quotes import GetSecurityQuotesCmd
from zsdtdx.unified_client import UnifiedTdxClient

# 2026-09-18 现场回包：sz.301569 联亚药业未上市占位，61 字节；与 600000 同批 141 字节。
_BODY_301569_UNLISTED = bytes.fromhex(
    "016001000033303135363900000000000000a401000000000000000000000000"
    "0000000000000000000000000000000000000000800000000000000000"
)
_BODY_301569_AND_600000 = bytes.fromhex(
    "011402000033303135363900000000000000a401000000000000000000000000"
    "0000000000000000000000000000000000000000800000000000000000013630"
    "3030303035078c0e42430748b5f1de0acc0ea2a12301a32b7a4db8ba10abe612"
    "0181bf024100bba10194304201b36bac1f4302847c9d124403992c852f4504b3"
    "5ab93b4c0f00000000f6ff3507"
)


def _parse(body: bytes):
    """输入回包 body，输出解析行列表。"""
    return GetSecurityQuotesCmd(None).parseResponse(body)


def test_format_time_zero_does_not_raise():
    cmd = GetSecurityQuotesCmd(None)
    assert cmd._format_time("0") == ""
    assert cmd._format_time("") == ""
    assert cmd._format_time(None) == ""
    formatted = cmd._format_time("112507758")
    assert formatted
    assert ":" in formatted


def test_unlisted_quote_parses_without_raising():
    rows = _parse(_BODY_301569_UNLISTED)
    assert len(rows) == 1
    assert rows[0]["code"] == "301569"
    assert rows[0]["market"] == 0
    assert rows[0]["servertime"] == ""
    assert float(rows[0]["price"]) == 0.0


def test_unlisted_stock_does_not_drop_batch_peer():
    rows = _parse(_BODY_301569_AND_600000)
    by_code = {row["code"]: row for row in rows}
    assert set(by_code) == {"301569", "600000"}
    assert float(by_code["600000"]["price"]) > 0
    assert float(by_code["301569"]["price"]) == 0.0


def test_quote_last_price_fallback_to_last_close():
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    assert client._quote_last_price({"price": 9.08, "last_close": 9.06}) == 9.08
    assert client._quote_last_price({"price": 0, "last_close": 9.06}) == 9.06
    assert client._quote_last_price({"price": 0.0, "last_close": 0.0}) is None
    assert client._quote_last_price({"price": None, "last_close": None}) is None
    assert client._quote_last_price(None) is None


def test_latest_price_unlisted_uses_one_batch_no_split_retry():
    """未上市占位与正常票同批：只发一次五档请求，占位记 None，正常票取现价。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.output_cfg = {"latest_quote_batch_size": 80}
    client.std_pool = object()
    client.ex_pool = object()
    client._runtime_failures = []
    routes = {
        "301569": {"market": 0, "source": "std", "code": "301569"},
        "600000": {"market": 1, "source": "std", "code": "600000"},
    }
    client._lookup_stock_route = lambda code: routes.get(code)
    calls = []

    def _fake_pool_call(pool, method, req):
        calls.append((method, list(req)))
        return [
            {"market": 0, "code": "301569", "price": 0.0, "last_close": 0.0},
            {"market": 1, "code": "600000", "price": 9.08, "last_close": 9.06},
        ]

    client._pool_call_allow_none = _fake_pool_call
    got = client.get_stock_latest_price(["301569", "600000"])
    assert got == {"301569": None, "600000": 9.08}
    assert len(calls) == 1
    assert calls[0][0] == "get_security_quotes"
    assert calls[0][1] == [(0, "301569"), (1, "600000")]
