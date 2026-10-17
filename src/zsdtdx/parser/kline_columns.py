"""模块：`parser/kline_columns.py`。

职责：定义只读列式页、调用 numba 内核，并在交付边缘生成行对象。
边界：不负责连接、分页和市场路由；时间戳严格遵循本机 datetime.timestamp。
"""

from __future__ import annotations

import contextvars
import datetime as dt
import struct
import time
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache

import numpy as np

columnar_decode = contextvars.ContextVar("zsdtdx_columnar_decode", default=False)


@lru_cache(maxsize=32768)
def _date_offset(day: int, timezone_signature):
    """输入日期和本机时区签名，输出稳定偏移或 None；时区变化自动隔离缓存。"""
    year, month, date = day // 10000, day // 100 % 100, day % 100
    epoch = dt.datetime(1970, 1, 1)
    offsets = []
    for hour in (0, 12, 23):
        value = dt.datetime(year, month, date, hour)
        offsets.append(int((value - epoch).total_seconds()) - int(value.timestamp()))
    return offsets[0] if offsets[0] == offsets[1] == offsets[2] else None


def timestamps_for(clocks):
    """输入年月日时分列，输出本地 epoch 秒列；夏令时切换日逐值精确转换。"""
    days, hm = clocks // 10000, clocks % 10000
    hours, minutes = hm // 100, hm % 100
    if np.any(hours > 23) or np.any(minutes > 59):
        raise ValueError("K线时分越界")
    year, month, day = days // 10000, days // 100 % 100, days % 100
    a = (14 - month) // 12
    yy, mm = year + 4800 - a, month + 12 * a - 3
    jdn = day + (153 * mm + 2) // 5 + 365 * yy + yy // 4 - yy // 100 + yy // 400 - 32045
    result = (jdn - 2440588) * 86400 + hours * 3600 + minutes * 60
    dates, inverse = np.unique(days, return_inverse=True)
    zone = (time.tzname, time.timezone, time.altzone, time.daylight)
    offsets = [_date_offset(int(date), zone) for date in dates]
    stable_offsets = np.array([offset or 0 for offset in offsets], dtype=np.int64)
    result -= stable_offsets[inverse]
    changed = [index for index, offset in enumerate(offsets) if offset is None]
    if changed:
        for i in np.flatnonzero(np.isin(inverse, changed)):
            result[i] = int(
                dt.datetime(
                    int(year[i]),
                    int(month[i]),
                    int(day[i]),
                    int(hours[i]),
                    int(minutes[i]),
                ).timestamp()
            )
    return result


@lru_cache(maxsize=65536)
def clock_text(clock: int) -> str:
    """输入年月日时分整数，输出现有 datetime 字符串；秒固定为零。"""
    day, hm = divmod(int(clock), 10000)
    return f"{day // 10000:04d}-{day // 100 % 100:02d}-{day % 100:02d} {hm // 100:02d}:{hm % 100:02d}:00"


@dataclass(frozen=True)
class KlineColumns:
    """只读页；列长度一致，切片持有底层数组引用，缓存释放不使结果失效。"""

    columns: Mapping[str, np.ndarray]

    def __post_init__(self):
        """输入列映射，输出不可变列快照；不等长拒绝。"""
        from types import MappingProxyType

        data = dict(self.columns)
        if len({len(v) for v in data.values()}) > 1:
            raise ValueError("K线列长度不一致")
        for value in data.values():
            value.flags.writeable = False
        object.__setattr__(self, "columns", MappingProxyType(data))

    def __len__(self):
        """输入无，输出有效行数；空映射返回零。"""
        return len(next(iter(self.columns.values()))) if self.columns else 0

    def take(self, index):
        """输入切片或索引，输出持有有效引用的只读列页；不修改原页。"""
        return KlineColumns({k: v[index] for k, v in self.columns.items()})

    def raw_rows(self):
        """输入无，输出底层 API 原有行格式；仅在非列式调用边缘构造对象。"""
        data = self.columns
        integers = {
            "vol",
            "amount",
            "trade",
            "position",
            "up_count",
            "down_count",
            "_ts",
        }
        result = []
        for i in range(len(self)):
            row = {
                k: int(v[i]) if k in integers else float(v[i])
                for k, v in data.items()
                if k != "_clock"
            }
            row["datetime"] = clock_text(data["_clock"][i])
            result.append(row)
        return result


def _finish(values, clocks, extra=None, extended=False):
    """输入解码数组，输出刻度后的列页；价格和量能按既有 numpy 规则处理。"""
    np.round(values[:4], 2, out=values[:4])
    np.rint(
        values[4:7] if extended else values[4:6],
        out=values[4:7] if extended else values[4:6],
    )
    names = ["open", "close", "high", "low", "trade" if extended else "vol", "amount"]
    if extended:
        np.round(values[7], 2, out=values[7])
        names += ["position", "price"]
    data = dict(zip(names, values))
    data.update(_clock=clocks, _ts=timestamps_for(clocks))
    if extra:
        data.update(extra)
    return KlineColumns(data)


def parse_standard_columns(
    body, category, *, with_index_counts=False, absolute_ohlc=False
):
    """输入标准回包和周期，输出只读列页；格式错误抛 ValueError。"""
    from zsdtdx.parser._kline_numba import decode_standard

    values, clocks, breadth = decode_standard(
        np.frombuffer(body, np.uint8),
        int(category),
        bool(with_index_counts),
        bool(absolute_ohlc),
    )
    extra = (
        {"up_count": breadth[0], "down_count": breadth[1]}
        if with_index_counts
        else None
    )
    return _finish(values, clocks, extra)


def parse_extended_columns(body, category):
    """输入扩展回包和周期，输出列页；短前缀为空页，截断记录明确报错。"""
    from zsdtdx.parser._kline_numba import decode_extended

    if body is None or len(body) < 42:
        count = 0
        body = bytes(42)
    else:
        count = struct.unpack_from("<H", body, 40)[0]
    if count > 700 or len(body) < 42 + count * 32:
        raise ValueError("扩展K线记录数或包长错误")
    times = np.ndarray((count,), dtype="<u4", buffer=body, offset=42, strides=(32,))
    floats = np.ndarray(
        (count, 7),
        dtype="<f4",
        buffer=body,
        offset=46 if count else 42,
        strides=(32, 4),
    )
    integers = np.ndarray(
        (count, 7),
        dtype="<u4",
        buffer=body,
        offset=46 if count else 42,
        strides=(32, 4),
    )
    values, clocks = decode_extended(times, floats, integers, int(category))
    return _finish(values, clocks, extended=True)


def merge_pages(pages):
    """输入按抓取先后排列的页，输出时间升序去重列；重复时间保留最新页。"""
    if not pages:
        return None
    columns = {
        k: np.concatenate([p.columns[k] for p in pages]) for k in pages[0].columns
    }
    _, index = np.unique(columns["_ts"], return_index=True)
    return KlineColumns({k: v[index] for k, v in columns.items()})


def warmup_kline_decoders():
    """输入无，输出无；编译全部格式签名，不建立行情连接。"""
    for counts, absolute in ((False, False), (True, False), (True, True)):
        parse_standard_columns(
            b"\x00\x00", 4, with_index_counts=counts, absolute_ohlc=absolute
        )
    parse_extended_columns(bytes(42), 4)
