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


def test_security_and_index_cmds_share_official_layout():
    """输入：个股 qfq=True、指数无 qfq；输出：同为 54 字节 0x052D，指数 reserved0=0。"""
    stock = GetSecurityBarsCmd(None)
    stock.setParams(4, 1, "601088", 0, 420, qfq=True)
    index = GetIndexBarsCmd(None)
    index.setParams(4, 1, "000001", 0, 420)
    assert len(stock.send_pkg) == 54
    assert len(index.send_pkg) == 54
    assert bytes(stock.send_pkg) != bytes(index.send_pkg)
    index_fields = _unpack_head(bytes(index.send_pkg))
    assert index_fields[10] == 420
    assert index_fields[11] == 0


def test_unified_daily_category_is_official_four():
    """输入：高层频率 d；输出：官方日线 category=4。"""
    from zsdtdx.unified_client import UnifiedTdxClient

    assert UnifiedTdxClient.PERIOD_MAP["d"] == 4


def test_pack_standard_kline_request_count_800_first_page():
    """输入：start=0、count=800；输出：54 字节、首包 inner、count 字段为 800。"""
    pkg = pack_standard_kline_request(4, 1, "601088", 0, 800, qfq=True)
    fields = _unpack_head(bytes(pkg))
    inner = fields[1]
    start = fields[9]
    count = fields[10]
    assert len(pkg) == 54
    assert inner == 0x0101D208
    assert start == 0
    assert count == 800


def test_default_standard_kline_page_size_is_800():
    """输入：包内 config 与客户端缺省；输出：标准 K 线单页默认 800。"""
    from pathlib import Path

    import yaml

    from zsdtdx.params import TDXParams
    from zsdtdx.unified_client import UnifiedTdxClient

    cfg_path = Path(__file__).resolve().parents[1] / "src" / "zsdtdx" / "config.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    assert cfg["pagination"]["standard_kline_page_size"] == 800
    assert TDXParams.MAX_KLINE_COUNT == 800
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {}
    assert client._standard_kline_page_size() == 800
