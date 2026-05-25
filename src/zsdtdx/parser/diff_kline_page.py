"""
模块：`parser/diff_kline_page.py`。

职责：
1. 标准行情差分编码 K 线单页解码（个股/指数共用布局）。
2. 解码后调用 helper 向量化格式化，作为 socket 层唯一数值刻度出口。

边界：
1. 仅处理 get_security_bars / get_index_bars 同构回包，不承担分页。
2. 时间：本层从二进制解析年月日时分，写入 datetime 为 `YYYY-MM-DD HH:MM:SS`（秒位固定 `:00`）与 `_ts`；不向 dict 返回分列字段。
3. socket 层以上（unified_client 等）只消费 datetime/_ts，不再解析 year/month/day/hour/minute。
"""

# coding=utf-8

from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, List

import numpy as np

from zsdtdx.helper import format_socket_kline_page_inplace, get_price, get_volume


def parse_diff_encoded_kline_page(
    body_buf,
    category: int,
    *,
    with_index_counts: bool = False,
) -> List[Dict[str, Any]]:
    """
    解析差分编码 K 线单页。

    输入：body_buf 回包体；category K 线周期；with_index_counts 为 True 时解析涨跌家数。
    输出：已格式化的 K 线 dict 列表（OHLC 2dp，vol/amount 整数，datetime，_ts）。
    """
    ret_count = body_buf[0] | (body_buf[1] << 8)
    if ret_count <= 0:
        return []

    buf = memoryview(body_buf) if not isinstance(body_buf, memoryview) else body_buf
    cat = int(category)

    opens = np.empty(ret_count, dtype=np.float64)
    closes = np.empty(ret_count, dtype=np.float64)
    highs = np.empty(ret_count, dtype=np.float64)
    lows = np.empty(ret_count, dtype=np.float64)
    volumes = np.empty(ret_count, dtype=np.float64)
    amounts = np.empty(ret_count, dtype=np.float64)
    timestamps = np.empty(ret_count, dtype=np.int64)
    datetimes: List[str] = [""] * ret_count
    up_counts = np.empty(ret_count, dtype=np.int32) if with_index_counts else None
    down_counts = np.empty(ret_count, dtype=np.int32) if with_index_counts else None

    pos = 2
    pre_diff_base = 0
    for i in range(ret_count):
        if cat < 4 or cat == 7 or cat == 8:
            zipday = buf[pos] | (buf[pos + 1] << 8)
            tminutes = buf[pos + 2] | (buf[pos + 3] << 8)
            year = (zipday >> 11) + 2004
            month = int((zipday % 2048) / 100)
            day = (zipday % 2048) % 100
            hour = int(tminutes / 60)
            minute = tminutes % 60
        else:
            zipday = (
                buf[pos]
                | (buf[pos + 1] << 8)
                | (buf[pos + 2] << 16)
                | (buf[pos + 3] << 24)
            )
            year = int(zipday / 10000)
            month = int((zipday % 10000) / 100)
            day = zipday % 100
            hour = 15
            minute = 0
        pos += 4

        price_open_diff, pos = get_price(buf, pos)
        price_close_diff, pos = get_price(buf, pos)
        price_high_diff, pos = get_price(buf, pos)
        price_low_diff, pos = get_price(buf, pos)

        vol_raw = (
            buf[pos] | (buf[pos + 1] << 8) | (buf[pos + 2] << 16) | (buf[pos + 3] << 24)
        )
        vol = get_volume(vol_raw)
        pos += 4
        dbvol_raw = (
            buf[pos] | (buf[pos + 1] << 8) | (buf[pos + 2] << 16) | (buf[pos + 3] << 24)
        )
        dbvol = get_volume(dbvol_raw)
        pos += 4

        if with_index_counts:
            up_counts[i] = buf[pos] | (buf[pos + 1] << 8)
            down_counts[i] = buf[pos + 2] | (buf[pos + 3] << 8)
            pos += 4

        open_v = (pre_diff_base + price_open_diff) / 1000.0
        new_base = pre_diff_base + price_open_diff
        close_v = (new_base + price_close_diff) / 1000.0
        high_v = (new_base + price_high_diff) / 1000.0
        low_v = (new_base + price_low_diff) / 1000.0
        pre_diff_base = new_base + price_close_diff

        opens[i] = open_v
        closes[i] = close_v
        highs[i] = high_v
        lows[i] = low_v
        volumes[i] = vol
        amounts[i] = dbvol
        datetimes[i] = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:00"
        timestamps[i] = int(_dt.datetime(year, month, day, hour, minute).timestamp())

    format_socket_kline_page_inplace(
        opens, closes, highs, lows, volumes, amounts, ret_count
    )

    klines: List[Dict[str, Any]] = [None] * ret_count  # type: ignore[list-item]
    for i in range(ret_count):
        item: Dict[str, Any] = {
            "open": float(opens[i]),
            "close": float(closes[i]),
            "high": float(highs[i]),
            "low": float(lows[i]),
            "vol": int(volumes[i]),
            "amount": int(amounts[i]),
            "datetime": datetimes[i],
            "_ts": int(timestamps[i]),
        }
        if with_index_counts:
            item["up_count"] = int(up_counts[i])
            item["down_count"] = int(down_counts[i])
        klines[i] = item
    return klines
