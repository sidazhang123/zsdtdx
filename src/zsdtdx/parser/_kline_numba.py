"""模块：`parser/_kline_numba.py`。

职责：释放 GIL 解码标准差分/绝对价及扩展固定记录。
边界：只处理有界二进制与数值列，不处理连接、时区和 Python 行对象。
"""

import math

import numpy as np
from numba import njit


@njit(nogil=True, cache=True, fastmath=False)
def read_price(body, pos):
    """输入字节和游标，输出有符号价差及新游标；截断或溢出抛错。"""
    if pos >= len(body):
        raise ValueError("K线价差截断")
    b = int(body[pos])
    value = b & 63
    negative = b & 64
    shift = 6
    while b & 128:
        pos += 1
        if pos >= len(body) or shift > 55:
            raise ValueError("K线价差截断或溢出")
        b = int(body[pos])
        value += (b & 127) << shift
        shift += 7
    return (-value if negative else value), pos + 1


@njit(nogil=True, cache=True, fastmath=False)
def read_u32(body, pos):
    """输入字节和游标，输出小端整数；长度不足抛错。"""
    if pos + 4 > len(body):
        raise ValueError("K线四字节字段截断")
    return (
        int(body[pos])
        | int(body[pos + 1]) << 8
        | int(body[pos + 2]) << 16
        | int(body[pos + 3]) << 24
    )


@njit(nogil=True, cache=True, fastmath=False)
def volume_value(value):
    """输入协议量能整数，输出浮点数；0x80 边界按真实量能值进入指数分支。"""
    point = value >> 24
    hi, mid, low = (value >> 16) & 255, (value >> 8) & 255, value & 255
    ecx, edx, esi, eax = (
        2 * point - 127,
        2 * point - 134,
        2 * point - 142,
        2 * point - 150,
    )
    base = math.ldexp(1.0, abs(ecx))
    if ecx < 0:
        base = 1.0 / base
    if hi >= 128:
        first = math.ldexp(1.0, edx) * 128.0 + (hi & 127) * math.ldexp(1.0, edx + 1)
    else:
        first = math.ldexp(1.0, abs(edx)) * hi
    second, third = math.ldexp(1.0, esi) * mid, math.ldexp(1.0, eax) * low
    if hi & 128:
        second *= 2.0
        third *= 2.0
    return base + first + second + third


@njit(nogil=True, cache=True, fastmath=False)
def wall_clock(value, category):
    """输入协议时间和周期，输出年月日时分整数；日期合法性由外层验证。"""
    if category < 4 or category == 7 or category == 8:
        day, minute = value & 65535, value >> 16
        year = (day >> 11) + 2004
        month, date = (day % 2048) // 100, (day % 2048) % 100
        return (
            (year * 10000 + month * 100 + date) * 10000
            + (minute // 60) * 100
            + minute % 60
        )
    return value * 10000 + 1500


@njit(nogil=True, cache=True, fastmath=False)
def decode_standard(body, category, counts, absolute):
    """输入标准页及格式标记，输出数值矩阵、时间列、家数列；坏页抛错。"""
    if len(body) < 2:
        raise ValueError("K线记录数截断")
    n = int(body[0]) | int(body[1]) << 8
    if n > 800:
        raise ValueError("标准K线记录数越界")
    values, clocks, breadth = (
        np.empty((6, n), np.float64),
        np.empty(n, np.int64),
        np.empty((2, n), np.int32),
    )
    pos, previous = 2, 0
    for i in range(n):
        clocks[i] = wall_clock(read_u32(body, pos), category)
        pos += 4
        po, pos = read_price(body, pos)
        pc, pos = read_price(body, pos)
        ph, pos = read_price(body, pos)
        pl, pos = read_price(body, pos)
        if (
            abs(po) > 4503599627370496
            or abs(pc) > 4503599627370496
            or abs(ph) > 4503599627370496
            or abs(pl) > 4503599627370496
            or abs(previous) > 4503599627370496
        ):
            raise ValueError("K线差分价链溢出")
        values[4, i], values[5, i] = (
            volume_value(read_u32(body, pos)),
            volume_value(read_u32(body, pos + 4)),
        )
        pos += 8
        if absolute:
            values[0, i], values[1, i], values[2, i], values[3, i] = (
                po / 1000.0,
                pc / 1000.0,
                ph / 1000.0,
                pl / 1000.0,
            )
        else:
            base = previous + po
            values[0, i], values[1, i], values[2, i], values[3, i] = (
                base / 1000.0,
                (base + pc) / 1000.0,
                (base + ph) / 1000.0,
                (base + pl) / 1000.0,
            )
            previous = base + pc
        if counts:
            packed = read_u32(body, pos)
            breadth[0, i], breadth[1, i] = packed & 65535, packed >> 16
            pos += 4
    return values, clocks, breadth


@njit(nogil=True, cache=True, fastmath=False)
def decode_extended(times, floats, integers, category):
    """输入扩展页的有界零拷贝视图，输出数值矩阵与时间列；不解释市场。"""
    n = len(times)
    values, clocks = np.empty((8, n), np.float64), np.empty(n, np.int64)
    for i in range(n):
        clocks[i] = wall_clock(int(times[i]), category)
        values[0, i], values[1, i], values[2, i], values[3, i] = (
            floats[i, 0],
            floats[i, 3],
            floats[i, 1],
            floats[i, 2],
        )
        values[4, i], values[5, i] = float(integers[i, 5]), floats[i, 4]
        values[6, i], values[7, i] = float(integers[i, 4]), floats[i, 6]
    return values, clocks
