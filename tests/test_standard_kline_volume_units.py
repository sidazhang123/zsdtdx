"""标准 0x052D K 线成交量单位归一化回归。"""

import struct

import pytest

from zsdtdx.parser.get_index_bars import GetIndexBarsCmd
from zsdtdx.parser.get_block_bars import GetBlockBarsCmd
from zsdtdx.parser.get_security_bars import GetSecurityBarsCmd


def _encode_price(value: int) -> bytes:
    negative, value = value < 0, abs(value)
    first, value = value & 0x3F, value >> 6
    output = bytearray([first | (0x40 if negative else 0) | (0x80 if value else 0)])
    while value:
        part, value = value & 0x7F, value >> 7
        output.append(part | (0x80 if value else 0))
    return bytes(output)


def _one_bar_body(category: int, *, counts: bool = False) -> bytes:
    """构造原始成交量 100、成交额 1000 的单根标准 K 线。"""

    body = bytearray(struct.pack("<H", 1))
    if category < 4:
        zipday = ((2026 - 2004) << 11) + 10 * 100 + 20
        body.extend(struct.pack("<HH", zipday, 9 * 60 + 45))
    else:
        body.extend(struct.pack("<I", 20261020))
    for value in (10000, 0, 0, 0):
        body.extend(_encode_price(value))
    body.extend(struct.pack("<II", 0x42C80000, 0x447A0000))
    if counts:
        body.extend(struct.pack("<HH", 10, 5))
    return bytes(body)


@pytest.mark.parametrize("category", [5, 6])
def test_security_weekly_and_monthly_volume_return_minimum_unit(category):
    """股票/基金/转债周月线服务端量为手，公开低层 API 统一乘 100。"""

    cmd = GetSecurityBarsCmd(client=None)
    cmd.category = category
    row = cmd.parseResponse(_one_bar_body(category))[0]
    assert row["vol"] == 10000
    assert row["amount"] == 1000


@pytest.mark.parametrize("category", [0, 1, 2, 3])
def test_index_minute_volume_removes_server_hundredfold_scale(category):
    """标准指数分钟线服务端量放大 100 倍，公开低层 API 统一除 100。"""

    cmd = GetIndexBarsCmd(client=None)
    cmd.category = category
    row = cmd.parseResponse(_one_bar_body(category, counts=True))[0]
    assert row["vol"] == 1
    assert row["amount"] == 1000


@pytest.mark.parametrize("category", [5, 6])
def test_index_weekly_and_monthly_volume_return_minimum_unit(category):
    """指数周月线与普通证券相同，服务端手数统一还原为最小单位。"""

    cmd = GetIndexBarsCmd(client=None)
    cmd.category = category
    row = cmd.parseResponse(_one_bar_body(category, counts=True))[0]
    assert row["vol"] == 10000
    assert row["amount"] == 1000


@pytest.mark.parametrize("category", [0, 1, 2, 3, 4])
def test_security_intraday_and_daily_volume_keep_existing_unit(category):
    """普通证券分钟/日线已经是最小单位，不重复缩放。"""

    cmd = GetSecurityBarsCmd(client=None)
    cmd.category = category
    row = cmd.parseResponse(_one_bar_body(category))[0]
    assert row["vol"] == 100
    assert row["amount"] == 1000


def test_unverified_block_protocol_keeps_legacy_volume_decoder():
    """0x0523 未完成独立取证前不得被 0x052D 的 float32 结论连带修改。"""

    body = bytearray(_one_bar_body(4, counts=True))
    volume_pos = len(body) - 12
    body[volume_pos : volume_pos + 4] = bytes.fromhex("00004041")
    cmd = GetBlockBarsCmd(client=None)
    cmd.category = 4
    row = cmd.parseResponse(bytes(body))[0]
    assert row["vol"] == 1032
