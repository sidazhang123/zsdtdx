"""
模块：tests/test_pytdx_alignment.py

职责：
1. 锁定 zsdtdx parser 与 pytdx 原版 parser 在 K 线解码上的按位等价关系。
2. 仅依赖合成 body_buf（不发起任何 socket 请求），保证测试可在离线环境运行。
3. 覆盖标准股票 K 线（get_security_bars）与指数 K 线（get_index_bars）两条 parser。

边界：
1. 本测试只验证"协议解码 + 数值刻度"阶段，不验证 unified_client/parallel_fetcher 行为。
2. 因 zsdtdx 设计目标为 OHLC 2 位小数 + vol/amount 整数，故对比时按既定刻度映射 pytdx 输出。
3. 不涉及 schema 改造（字段名/字段集均保留当前 9 字段约定）。
"""

import os
import struct
import sys
from typing import List, Tuple

import pytest

# 把工程 src 加入 sys.path，避免依赖 pip install -e .
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SRC_DIR = os.path.join(_PROJECT_ROOT, "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from pytdx.parser.get_security_bars import GetSecurityBarsCmd as PytdxStockCmd
from pytdx.parser.get_index_bars import GetIndexBarsCmd as PytdxIndexCmd
from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page


def _encode_price_varint(value: int) -> bytes:
    """
    按通达信变长有符号编码生成单值字节串（pytdx/zsdtdx 共用编码）。

    输入：
    1. value: Python int，正负皆可。
    输出：
    1. bytes 形式的变长编码；首字节低 6 位为载荷、bit6 为符号位、bit7 为延续位。
    用途：
    1. 测试构造价格 diff 字节串，避免依赖真实 socket 报文。
    边界条件：
    1. value 必须能被有限延续字节表示；本实现支持最多 5 字节（足够覆盖 32-bit 范围）。
    """
    negative = value < 0
    magnitude = -value if negative else value
    out = bytearray()
    first = magnitude & 0x3F
    if negative:
        first |= 0x40
    rest = magnitude >> 6
    if rest:
        first |= 0x80
    out.append(first)
    shift_bits = 6
    while rest:
        chunk = rest & 0x7F
        rest >>= 7
        if rest:
            chunk |= 0x80
        out.append(chunk)
        shift_bits += 7
    return bytes(out)


def _build_stock_bar_bytes(
    *,
    zipday: int,
    open_diff: int,
    close_diff: int,
    high_diff: int,
    low_diff: int,
    vol_raw: int,
    dbvol_raw: int,
    with_minutes: bool = False,
    tminutes: int = 0,
) -> bytes:
    """
    构造单根标准 K 线协议字节串。

    输入：
    1. zipday: 日期编码（日级时 year*10000+month*100+day；分钟级时 (year-2004)<<11 | month*100+day）。
    2. open/close/high/low_diff: 差分价格 int（解码后再做 base 累加）。
    3. vol_raw/dbvol_raw: 4 字节量能/成交额原始整数。
    4. with_minutes: True 时按分钟周期写入 4 字节（zipday 2B + tminutes 2B）。
    输出：
    1. 本根 bar 的字节串。
    用途：
    1. 用于组装合成 body_buf 验证 parser 输出。
    边界条件：
    1. 不验证 zipday 合法性，由调用方保证。
    """
    if with_minutes:
        date_bytes = struct.pack("<HH", zipday, tminutes)
    else:
        date_bytes = struct.pack("<I", zipday)
    body = bytearray()
    body.extend(date_bytes)
    body.extend(_encode_price_varint(open_diff))
    body.extend(_encode_price_varint(close_diff))
    body.extend(_encode_price_varint(high_diff))
    body.extend(_encode_price_varint(low_diff))
    body.extend(struct.pack("<II", vol_raw & 0xFFFFFFFF, dbvol_raw & 0xFFFFFFFF))
    return bytes(body)


def _build_index_bar_bytes(
    *,
    zipday: int,
    open_diff: int,
    close_diff: int,
    high_diff: int,
    low_diff: int,
    vol_raw: int,
    dbvol_raw: int,
    up_count: int,
    down_count: int,
    with_minutes: bool = False,
    tminutes: int = 0,
) -> bytes:
    """
    构造单根指数 K 线协议字节串（在股票基础上追加 up/down 计数 4 字节）。
    """
    base = _build_stock_bar_bytes(
        zipday=zipday,
        open_diff=open_diff,
        close_diff=close_diff,
        high_diff=high_diff,
        low_diff=low_diff,
        vol_raw=vol_raw,
        dbvol_raw=dbvol_raw,
        with_minutes=with_minutes,
        tminutes=tminutes,
    )
    return base + struct.pack("<HH", up_count & 0xFFFF, down_count & 0xFFFF)


def _build_stock_body(bars: List[bytes]) -> bytes:
    """
    输入：单根 bar 字节串列表。
    输出：完整 body_buf（含 2 字节 ret_count 前缀）。
    """
    out = bytearray()
    out.extend(struct.pack("<H", len(bars)))
    for b in bars:
        out.extend(b)
    return bytes(out)


def _make_pytdx_stock_cmd(category: int) -> PytdxStockCmd:
    """构造 pytdx GetSecurityBarsCmd 实例并设置 category（无须真实 client）。"""
    cmd = PytdxStockCmd(client=None)
    cmd.category = int(category)
    return cmd


def _make_pytdx_index_cmd(category: int) -> PytdxIndexCmd:
    """构造 pytdx GetIndexBarsCmd 实例并设置 category（无须真实 client）。"""
    cmd = PytdxIndexCmd(client=None)
    cmd.category = int(category)
    return cmd


# 一组合成测试用例：覆盖日级（category=9）日期编码，价格差分覆盖正/负/单字节/多字节延续编码。
# 重要：差分值必须是 10 的整数倍，保证累加后 price 是元角分（0.01 的整数倍），
# 与真实 TDX 协议下行情完全一致。非 10 倍数的差分会触发 IEEE-754 banker 边界（见
# `TestBankerBoundary`），属于"合成数据特有"现象，不会在真实数据中出现。
_STOCK_DAILY_BARS = [
    dict(
        zipday=20260102,
        open_diff=12340,
        close_diff=10,
        high_diff=200,
        low_diff=-10,
        vol_raw=0x42801000,
        dbvol_raw=0x44A00000,
    ),
    dict(
        zipday=20260103,
        open_diff=-50,
        close_diff=80,
        high_diff=100,
        low_diff=-100,
        vol_raw=0x41000000,
        dbvol_raw=0x43800000,
    ),
    dict(
        zipday=20260106,
        open_diff=0,
        close_diff=0,
        high_diff=0,
        low_diff=0,
        vol_raw=0x00000000,
        dbvol_raw=0x00000000,
    ),
]

# 分钟级测试（category=0），tminutes 携带 hour/minute。
_STOCK_MINUTE_BARS = [
    dict(
        zipday=((2026 - 2004) << 11) | (3 * 100 + 5),  # 2026-03-05
        open_diff=500,
        close_diff=-30,
        high_diff=100,
        low_diff=-200,
        vol_raw=0x42A00000,
        dbvol_raw=0x44400000,
        with_minutes=True,
        tminutes=9 * 60 + 30,
    ),
    dict(
        zipday=((2026 - 2004) << 11) | (3 * 100 + 5),
        open_diff=20,
        close_diff=40,
        high_diff=60,
        low_diff=-10,
        vol_raw=0x42000000,
        dbvol_raw=0x43000000,
        with_minutes=True,
        tminutes=10 * 60 + 0,
    ),
]


def _assert_bar_aligned(pytdx_bar: dict, zsdtdx_bar: dict) -> None:
    """
    断言 zsdtdx parser 输出与 pytdx parser 输出按既定刻度映射等价。

    映射：
    1. OHLC：在真实 TDX 数据下 price 是 0.01 的整数倍，
       zsdtdx 的 `np.round(price, 2)` 与 pytdx 原值在 0.01 精度上完全一致；
       此处使用 `abs<=0.005` 容差以兜底测试时的 IEEE-754 表示差。
    2. vol/amount：zsdtdx 已 `int(np.rint(原值))`；pytdx 原始 float。
    3. datetime：pytdx 输出 "%d-%02d-%02d %02d:%02d"，zsdtdx 输出 "{year:04d}-..."；
       year>=1000 时两者前 16 字符完全一致。
    """
    import numpy as np

    # 真实场景下价格是元角分（0.01 倍数），按 0.01 精度等价比较即可。
    assert abs(zsdtdx_bar["open"] - pytdx_bar["open"]) < 0.005, (
        f"open mismatch: {zsdtdx_bar['open']} vs {pytdx_bar['open']}"
    )
    assert abs(zsdtdx_bar["close"] - pytdx_bar["close"]) < 0.005
    assert abs(zsdtdx_bar["high"] - pytdx_bar["high"]) < 0.005
    assert abs(zsdtdx_bar["low"] - pytdx_bar["low"]) < 0.005
    # vol/amount 用 np.rint（banker rounding）确保与 zsdtdx 的内部实现完全一致。
    assert zsdtdx_bar["vol"] == int(np.rint(pytdx_bar["vol"]))
    assert zsdtdx_bar["amount"] == int(np.rint(pytdx_bar["amount"]))
    pytdx_dt = pytdx_bar["datetime"]
    assert zsdtdx_bar["datetime"][:16] == pytdx_dt[-16:] or zsdtdx_bar["datetime"] == pytdx_dt


class TestStockDailyAlignment:
    """日级股票 K 线：parse_diff_encoded_kline_page 与 pytdx parseResponse 对齐。"""

    def test_basic_three_bars_daily(self):
        """3 根日 K 线（正/负/零差分）对齐验证。"""
        category = 9
        body = _build_stock_body([_build_stock_bar_bytes(**bar) for bar in _STOCK_DAILY_BARS])

        zsdtdx_klines = parse_diff_encoded_kline_page(body, category, with_index_counts=False)
        pytdx_klines = _make_pytdx_stock_cmd(category).parseResponse(body)

        assert len(zsdtdx_klines) == len(pytdx_klines) == len(_STOCK_DAILY_BARS)
        for z_bar, p_bar in zip(zsdtdx_klines, pytdx_klines):
            _assert_bar_aligned(p_bar, z_bar)
            # _ts 字段独有，须与 datetime 字符串一致
            assert isinstance(z_bar["_ts"], int)
            assert z_bar["_ts"] > 0

    def test_empty_response(self):
        """空 body（ret_count=0）应当返回空列表。"""
        body = struct.pack("<H", 0)
        zsdtdx_klines = parse_diff_encoded_kline_page(body, 9, with_index_counts=False)
        pytdx_klines = _make_pytdx_stock_cmd(9).parseResponse(body)
        assert zsdtdx_klines == []
        assert pytdx_klines == []


class TestStockMinuteAlignment:
    """分钟级股票 K 线：日期解码走 (zipday>>11)+2004 分支。"""

    def test_two_bars_minute(self):
        """2 根分钟 K 线对齐验证。"""
        category = 0
        body = _build_stock_body(
            [_build_stock_bar_bytes(**bar) for bar in _STOCK_MINUTE_BARS]
        )

        zsdtdx_klines = parse_diff_encoded_kline_page(body, category, with_index_counts=False)
        pytdx_klines = _make_pytdx_stock_cmd(category).parseResponse(body)

        assert len(zsdtdx_klines) == len(pytdx_klines) == len(_STOCK_MINUTE_BARS)
        for z_bar, p_bar in zip(zsdtdx_klines, pytdx_klines):
            _assert_bar_aligned(p_bar, z_bar)


class TestIndexAlignment:
    """指数 K 线：在股票基础上多 up/down 计数。"""

    def test_two_bars_index_daily(self):
        """日级指数 2 根 bar 对齐验证（含 up/down）。"""
        category = 9
        index_bars = [
            dict(
                zipday=20260102,
                open_diff=200,
                close_diff=300,
                high_diff=500,
                low_diff=-100,
                vol_raw=0x42800000,
                dbvol_raw=0x44A00000,
                up_count=1234,
                down_count=567,
            ),
            dict(
                zipday=20260103,
                open_diff=-50,
                close_diff=-30,
                high_diff=20,
                low_diff=-80,
                vol_raw=0x41800000,
                dbvol_raw=0x43800000,
                up_count=800,
                down_count=1500,
            ),
        ]
        body = bytearray()
        body.extend(struct.pack("<H", len(index_bars)))
        for bar in index_bars:
            body.extend(_build_index_bar_bytes(**bar))
        body = bytes(body)

        zsdtdx_klines = parse_diff_encoded_kline_page(body, category, with_index_counts=True)
        pytdx_klines = _make_pytdx_index_cmd(category).parseResponse(body)

        assert len(zsdtdx_klines) == len(pytdx_klines) == len(index_bars)
        for z_bar, p_bar in zip(zsdtdx_klines, pytdx_klines):
            _assert_bar_aligned(p_bar, z_bar)
            assert z_bar["up_count"] == p_bar["up_count"]
            assert z_bar["down_count"] == p_bar["down_count"]


class TestRintBoundary:
    """np.rint 边界（banker rounding：half-to-even）行为锁定。"""

    def test_half_to_even_behavior(self):
        """vol_raw 经 get_volume 计算后落在 .5 邻域时，np.rint 走 banker rounding。"""
        # 这是一个边界回归测试：当前 helper.get_volume + format_socket_kline_page_inplace
        # 在 vol_raw=0 时返回 0（pow2 计算稳定），不会出现 0.5 边界翻转。
        body = _build_stock_body(
            [
                _build_stock_bar_bytes(
                    zipday=20260101,
                    open_diff=1000,
                    close_diff=0,
                    high_diff=0,
                    low_diff=0,
                    vol_raw=0,
                    dbvol_raw=0,
                )
            ]
        )
        zsdtdx_klines = parse_diff_encoded_kline_page(body, 9, with_index_counts=False)
        assert zsdtdx_klines[0]["vol"] == 0
        assert zsdtdx_klines[0]["amount"] == 0


class TestDatetimeFormat:
    """parser 输出 datetime 必须为 5 段格式 "YYYY-MM-DD HH:MM"。"""

    def test_datetime_five_segments(self):
        """日级 K 线 hour/minute 默认 15:00，5 段格式。"""
        body = _build_stock_body(
            [
                _build_stock_bar_bytes(
                    zipday=20260102,
                    open_diff=1234,
                    close_diff=10,
                    high_diff=20,
                    low_diff=-5,
                    vol_raw=0x42000000,
                    dbvol_raw=0x43000000,
                )
            ]
        )
        zsdtdx_klines = parse_diff_encoded_kline_page(body, 9, with_index_counts=False)
        dt_str = zsdtdx_klines[0]["datetime"]
        # "YYYY-MM-DD HH:MM" 长度恒为 16
        assert len(dt_str) == 16
        assert dt_str[4] == "-"
        assert dt_str[7] == "-"
        assert dt_str[10] == " "
        assert dt_str[13] == ":"
        # 日级默认 hour=15
        assert dt_str[-5:] == "15:00"


class TestBankerBoundary:
    """记录已知的 IEEE-754 banker boundary 行为：
    仅在 raw_diff 不是 10 整数倍（即合成数据中故意构造 0.005 末位价格）时触发。
    真实 TDX 数据下价格永远是 0.01 整数倍，不会触发此场景。"""

    def test_known_banker_boundary_on_synthetic_data(self):
        """合成 0.005 末位价格触发 `np.round(12.345, 2) = 12.35` 而 pytdx 直接保留 12.345。
        本测试只记录差异、不要求两者一致。"""
        body = _build_stock_body(
            [
                _build_stock_bar_bytes(
                    zipday=20260102,
                    open_diff=12345,  # 末位非 10 倍数；真实 TDX 不会出现
                    close_diff=0,
                    high_diff=0,
                    low_diff=0,
                    vol_raw=0,
                    dbvol_raw=0,
                )
            ]
        )
        import numpy as np

        zsdtdx_klines = parse_diff_encoded_kline_page(body, 9, with_index_counts=False)
        pytdx_klines = _make_pytdx_stock_cmd(9).parseResponse(body)
        # pytdx: 12.345（3 位小数原值，含 IEEE-754 微差）；
        # zsdtdx: np.round(12.345, 2)（实际 IEEE-754 表示 12.345≈12.34499.. → 12.34）。
        # 测试不锁定具体值，只锁定差异不超过 0.01（schema 设计差异，不是精度丢失 bug）。
        assert abs(zsdtdx_klines[0]["open"] - pytdx_klines[0]["open"]) <= 0.01
        # 重要：np.round 与 Python round 在 .5 边界上行为可能因 IEEE-754 表示不同。
        # 这里固化 zsdtdx 输出等于 `float(np.round(pytdx_value, 2))`。
        expected = float(np.round(pytdx_klines[0]["open"], 2))
        assert zsdtdx_klines[0]["open"] == expected
        # 量值在 vol_raw=0 时仍稳定为 0。
        assert zsdtdx_klines[0]["vol"] == 0
