"""离线验收扩展行情码表 0x2422 组包与文本记录解析。"""

from zsdtdx.parser.ex_get_instrument_info import (
    GetInstrumentInfo,
    pack_instrument_info_request,
    parse_instrument_info_body,
)

_CAPTURED_START0 = bytes.fromhex(
    "010000000000800080002224000000000000000000781f0e6a37447b502b7c0d01404c0a"
    "000000000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000100000000000000000000000000000000"
)
_CAPTURED_START2023 = bytes.fromhex(
    "010000000000800080002224e70700000000000000781f0e6a37447b502b7c0d01404c0a"
    "000000000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000100000000000000000000000000000000"
)


def test_pack_instrument_info_request_matches_tdxw():
    """输入：start=0 / 2023；输出：138 字节，与 TdxW 0x2422 逐字节一致。"""
    assert len(_CAPTURED_START0) == 138
    assert bytes(pack_instrument_info_request(0)) == _CAPTURED_START0
    assert bytes(pack_instrument_info_request(2023)) == _CAPTURED_START2023
    cmd = GetInstrumentInfo(None)
    cmd.setParams(0)
    assert bytes(cmd.send_pkg) == _CAPTURED_START0


def test_parse_instrument_info_named_records():
    """输入：带中文名的文本页；输出：港股/期货/中证 code+name+market。"""
    body = (
        b"\x00" * 20
        + "31#00700|腾讯控股,30#CUL8|沪铜主连,62#000905|中证500,".encode("gbk")
    )
    rows = parse_instrument_info_body(body)
    by_code = {row["code"]: row for row in rows}
    assert by_code["00700"]["market"] == 31
    assert by_code["00700"]["name"] == "腾讯控股"
    assert by_code["CUL8"]["market"] == 30
    assert by_code["CUL8"]["name"] == "沪铜主连"
    assert by_code["000905"]["market"] == 62
    assert by_code["000905"]["name"] == "中证500"


def test_parse_instrument_info_empty_end_page():
    """输入：结束页无文本记录；输出：空列表。"""
    assert parse_instrument_info_body(b"") == []
    assert parse_instrument_info_body(bytes(170)) == []
    cmd = GetInstrumentInfo(None)
    assert cmd.parseResponse(bytes(64)) == []
