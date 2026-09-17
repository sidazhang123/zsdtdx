"""离线验收标准行情 K 线请求组包：官方 54 字节 0x052D、前复权开关、翻页 inner。"""

import struct

from zsdtdx.parser.get_index_bars import GetIndexBarsCmd
from zsdtdx.parser.get_security_bars import (
    GetSecurityBarsCmd,
    pack_standard_kline_request,
)


def _unpack_head(pkg: bytes):
    """输入 54 字节包；输出前 38 字节结构化字段。"""
    assert len(pkg) == 54
    return struct.unpack("<HIHHHH6sHHHHIIH", pkg[:38])


def test_pack_standard_kline_request_first_page_qfq():
    """输入：start=0、qfq=True、count=420；输出：54 字节、首包 inner、reserved0=1。"""
    pkg = pack_standard_kline_request(4, 1, "601088", 0, 420, qfq=True)
    assert bytes(pkg[38:]) == bytes(16)
    (
        magic,
        inner,
        zipsize,
        unzipsize,
        cmd,
        market,
        code,
        category,
        flag,
        start,
        count,
        reserved0,
        reserved1,
        reserved2,
    ) = _unpack_head(bytes(pkg))
    assert magic == 0x040C
    assert inner == 0x0101D208
    assert zipsize == 0x2C
    assert unzipsize == 0x2C
    assert cmd == 0x052D
    assert market == 1
    assert code == b"601088"
    assert category == 4
    assert flag == 1
    assert start == 0
    assert count == 420
    assert reserved0 == 1
    assert reserved1 == 0
    assert reserved2 == 0


def test_pack_standard_kline_request_next_page_raw():
    """输入：start>0、qfq=False；输出：翻页 inner、reserved0=0。"""
    pkg = pack_standard_kline_request(7, 1, "601088", 420, 420, qfq=False)
    fields = _unpack_head(bytes(pkg))
    inner = fields[1]
    category = fields[7]
    start = fields[9]
    count = fields[10]
    reserved0 = fields[11]
    assert inner == 0x0101D308
    assert category == 7
    assert start == 420
    assert count == 420
    assert reserved0 == 0


def test_security_and_index_cmds_share_official_pack():
    """输入：相同参数分别组个股/指数包；输出：字节完全一致。"""
    stock = GetSecurityBarsCmd(None)
    stock.setParams(4, 1, "601088", 0, 420, qfq=True)
    index = GetIndexBarsCmd(None)
    index.setParams(4, 1, "601088", 0, 420, qfq=True)
    assert bytes(stock.send_pkg) == bytes(index.send_pkg)
    assert len(stock.send_pkg) == 54


def test_unified_daily_category_is_official_four():
    """输入：高层频率 d；输出：官方日线 category=4。"""
    from zsdtdx.unified_client import UnifiedTdxClient

    assert UnifiedTdxClient.PERIOD_MAP["d"] == 4
