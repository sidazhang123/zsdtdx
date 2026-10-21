"""离线验收标准行情五档解析：未上市占位码不得毒死同批其它代码。"""

import pytest

from zsdtdx.parser.get_security_quotes import (
    QUOTE_PRICE_DECIMAL_POINTS,
    GetSecurityQuotesCmd,
    resolve_quote_decimal_point,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient

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


def test_quote_product_decimal_constants_are_stable():
    """品种价格刻度来自全市场实盘验收，不依赖代码前缀或盘中价格。"""

    assert QUOTE_PRICE_DECIMAL_POINTS == {
        "stock": 2,
        "index": 2,
        "fund": 3,
        "convertible_bond": 4,
    }
    assert resolve_quote_decimal_point("fund") == 3
    assert resolve_quote_decimal_point("convertible_bond") == 4
    assert resolve_quote_decimal_point(decimal_point=4) == 4


def test_quote_explicit_decimal_point_scales_every_price_field():
    """同一原始报价按显式品种精度缩放，默认两位兼容旧股票调用。"""

    default_rows = _parse(_BODY_301569_AND_600000)
    default_row = {row["code"]: row for row in default_rows}["600000"]

    cmd = GetSecurityQuotesCmd(None)
    cmd.setParams([(1, "600000", 3)])
    fund_row = {row["code"]: row for row in cmd.parseResponse(_BODY_301569_AND_600000)}[
        "600000"
    ]

    for field in (
        "price",
        "last_close",
        "open",
        "high",
        "low",
        "bid1",
        "ask1",
        "bid5",
        "ask5",
    ):
        assert fund_row[field] == pytest.approx(default_row[field] / 10)


def test_quote_homogeneous_security_type_sets_batch_decimal_point():
    """同品种入口可直接传固定品种名，不需要加载 0x044D 目录。"""

    cmd = GetSecurityQuotesCmd(None)
    cmd.setParams([(1, "600000")], security_type="convertible_bond")
    bond_row = {row["code"]: row for row in cmd.parseResponse(_BODY_301569_AND_600000)}[
        "600000"
    ]
    default_row = {row["code"]: row for row in _parse(_BODY_301569_AND_600000)}[
        "600000"
    ]
    assert bond_row["price"] == pytest.approx(default_row["price"] / 100)


def test_quote_last_price_fallback_to_last_close():
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    assert client._quote_last_price({"price": 9.08, "last_close": 9.06}) == 9.08
    assert client._quote_last_price({"price": 0, "last_close": 9.06}) == 9.06
    assert client._quote_last_price({"price": 0.0, "last_close": 0.0}) is None
    assert client._quote_last_price({"price": None, "last_close": None}) is None
    assert client._quote_last_price(None) is None
