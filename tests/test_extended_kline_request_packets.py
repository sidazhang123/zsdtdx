"""离线验收扩展行情 K 线请求组包：官方 64 字节 0xD808/0xD908、翻页命令号。"""

import struct

from zsdtdx.parser.ex_get_instrument_bars import (
    GetInstrumentBars,
    pack_extended_kline_request,
)
from zsdtdx.parser.ex_setup_commands import ExSetupCmd1

# 00700 15 分钟首包参考字节（market=31, start=0, count=420）。
_CAPTURED_00700_15MIN_FIRST = bytes.fromhex(
    "010b08d801013600360089241f3030373030000000000000000000000000000000000000"
    "0100010000000000a401000000000000000000000000000000000000"
)


def _unpack_ext(pkg: bytes):
    """输入 64 字节包；输出关键字段。"""
    assert len(pkg) == 64
    zipflag, seq, cmd, unk, ln, ln2, inner, market, code = struct.unpack_from(
        "<BBHHHHHB9s", pkg, 0
    )
    category, flag, start, count, extra = struct.unpack_from("<HHIHH", pkg, 36)
    return {
        "zipflag": zipflag,
        "seq": seq,
        "cmd": cmd,
        "unk": unk,
        "ln": ln,
        "ln2": ln2,
        "inner": inner,
        "market": market,
        "code": code.split(b"\x00", 1)[0],
        "category": category,
        "flag": flag,
        "start": start,
        "count": count,
        "extra": extra,
    }


def test_ex_setup_matches_tdxw_92_bytes():
    """输入：扩展握手；输出：92 字节、命令号 0x6548。"""
    cmd = ExSetupCmd1(None)
    cmd.setup()
    raw = bytes(cmd.send_pkg)
    assert len(raw) == 92
    zipflag, seq, cmd_id, unk, ln, ln2 = struct.unpack_from("<BBHHHH", raw, 0)
    assert zipflag == 1
    assert seq == 1
    assert cmd_id == 0x6548
    assert ln == 0x52
    assert ln2 == 0x52
    assert raw[10:12] == bytes.fromhex("5424")


def test_pack_extended_kline_first_page_matches_tdxw():
    """输入：00700 15 分钟 start=0 count=420；输出：与 TdxW 首包逐字节一致。"""
    pkg = pack_extended_kline_request(1, 31, "00700", 0, 420)
    assert bytes(pkg) == _CAPTURED_00700_15MIN_FIRST


def test_pack_extended_kline_qfq_sets_extra():
    """输入：港股 qfq=True/False；输出：extra 分别为 1 与 0。"""
    qfq_pkg = pack_extended_kline_request(4, 31, "00700", 0, 700, qfq=True)
    raw_pkg = pack_extended_kline_request(4, 31, "00700", 0, 700, qfq=False)
    assert _unpack_ext(bytes(qfq_pkg))["extra"] == 1
    assert _unpack_ext(bytes(raw_pkg))["extra"] == 0
    cmd = GetInstrumentBars(None)
    cmd.setParams(4, 31, "00700", 0, 700, qfq=True)
    assert _unpack_ext(bytes(cmd.send_pkg))["extra"] == 1


def test_pack_extended_kline_next_page_uses_d908():
    """输入：start>0；输出：64 字节、翻页命令 0xD908、start/count 写入对应字段。"""
    pkg = pack_extended_kline_request(7, 30, "CUL8", 420, 420)
    fields = _unpack_ext(bytes(pkg))
    assert fields["cmd"] == 0xD908
    assert fields["inner"] == 0x2489
    assert fields["market"] == 30
    assert fields["code"] == b"CUL8"
    assert fields["category"] == 7
    assert fields["flag"] == 1
    assert fields["start"] == 420
    assert fields["count"] == 420
    assert fields["extra"] == 0


def test_get_instrument_bars_parses_prefix_and_one_bar():
    """输入：42 字节前缀 count=1 + 一条日线；输出：datetime 与 OHLC。"""
    cmd = GetInstrumentBars(None)
    cmd.category = 4
    zipday = 20260917
    prefix = bytearray(42)
    prefix[0] = 31
    prefix[1:6] = b"00700"
    struct.pack_into("<H", prefix, 24, 4)
    struct.pack_into("<H", prefix, 26, 1)
    struct.pack_into("<H", prefix, 40, 1)
    bar = struct.pack("<IffffIII", zipday, 10.0, 11.0, 9.0, 10.5, 1, 100, 0)
    rows = cmd.parseResponse(bytes(prefix) + bar)
    assert len(rows) == 1
    assert rows[0]["datetime"] == "2026-09-17 15:00:00"
    assert rows[0]["open"] == 10.0
    assert rows[0]["high"] == 11.0
    assert rows[0]["low"] == 9.0
    assert rows[0]["close"] == 10.5


def test_pack_extended_kline_request_count_700_first_page():
    """输入：start=0、count=700；输出：64 字节、首包命令、count 字段为 700。"""
    pkg = pack_extended_kline_request(4, 31, "00700", 0, 700)
    fields = _unpack_ext(bytes(pkg))
    assert len(pkg) == 64
    assert fields["cmd"] == 0xD808
    assert fields["start"] == 0
    assert fields["count"] == 700


def test_default_extended_kline_page_size_is_700():
    """输入：包内 config 与协议常量；输出：扩展 K 线单页默认 700（服务端硬上限）。"""
    from pathlib import Path

    import yaml

    from zsdtdx.params import TDXParams

    cfg = yaml.safe_load(
        (
            Path(__file__).resolve().parents[1] / "src" / "zsdtdx" / "config.yaml"
        ).read_text(encoding="utf-8")
    )
    assert cfg["pagination"]["extended_kline_page_size"] == 700
    assert TDXParams.MAX_EXTENDED_KLINE_COUNT == 700
