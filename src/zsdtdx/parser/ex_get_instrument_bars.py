"""
模块：`parser/ex_get_instrument_bars.py`。

职责：
1. 扩展行情（期货/指数等）K 线协议封装与 socket 回包解析。
2. 记录为固定 28 字节布局：OHLC(float×4)、持仓(uint32)、成交量(uint32)、结算价；含 `_ts`。
3. `amount` 与 pytdx 一致：对 `+16` 处 4 字节按 float 解读（与 `position` 同偏移），向量化 rint 后输出整数。

边界：
1. 仅负责单页解析；指数 ex 路由与期货共用本解析器。
2. 时间：本层解析年月日时分仅填充 datetime/_ts，不返回分列字段；上层只读 datetime/_ts。
"""

# coding=utf-8

import struct

import numpy as np
import six

from zsdtdx.helper import format_socket_kline_page_inplace, get_datetime
from zsdtdx.parser.base import BaseParser


class GetInstrumentBars(BaseParser):

    def setup(self):
        """输入无；输出无；扩展行情 K 线无需额外 setup。"""
        pass

    def setParams(self, category, market, code, start, count):
        """
        输入：category/market/code/start/count。
        输出：无；构造 send_pkg。
        用途：组装扩展行情 K 线请求包。
        边界条件：code 为 str 时转 bytes。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")
        pkg = bytearray.fromhex('01 01 08 6a 01 01 16 00 16 00')
        pkg.extend(bytearray.fromhex("ff 23"))

        self.category = category

        pkg.extend(struct.pack('<B9sHHIH', market, code, category, 1, start, count))
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """
        输入：body_buf 为 socket 回包体。
        输出：K 线 dict 列表（含 `_ts`）。
        用途：P1+P2 优化解析。
        边界条件：空页返回 []。
        """
        import datetime as _dt

        pos = 18
        ret_count = body_buf[pos] | (body_buf[pos + 1] << 8)
        pos += 2
        if ret_count <= 0:
            return []

        buf = memoryview(body_buf) if not isinstance(body_buf, memoryview) else body_buf
        cat = self.category

        opens = np.empty(ret_count, dtype=np.float64)
        highs = np.empty(ret_count, dtype=np.float64)
        lows = np.empty(ret_count, dtype=np.float64)
        closes = np.empty(ret_count, dtype=np.float64)
        settlements = np.empty(ret_count, dtype=np.float64)
        trades = np.empty(ret_count, dtype=np.float64)
        positions = np.empty(ret_count, dtype=np.float64)
        amounts = np.empty(ret_count, dtype=np.float64)
        timestamps = np.empty(ret_count, dtype=np.int64)
        datetimes = [""] * ret_count

        for i in range(ret_count):
            year, month, day, hour, minute, pos = get_datetime(cat, buf, pos)

            opens[i] = struct.unpack_from("<f", buf, pos)[0]
            highs[i] = struct.unpack_from("<f", buf, pos + 4)[0]
            lows[i] = struct.unpack_from("<f", buf, pos + 8)[0]
            closes[i] = struct.unpack_from("<f", buf, pos + 12)[0]
            positions[i] = float(
                buf[pos + 16] | (buf[pos + 17] << 8) | (buf[pos + 18] << 16) | (buf[pos + 19] << 24)
            )
            amounts[i] = struct.unpack_from("<f", buf, pos + 16)[0]
            trades[i] = float(
                buf[pos + 20] | (buf[pos + 21] << 8) | (buf[pos + 22] << 16) | (buf[pos + 23] << 24)
            )
            settlements[i] = struct.unpack_from("<f", buf, pos + 24)[0]
            pos += 28

            datetimes[i] = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}"
            timestamps[i] = int(_dt.datetime(year, month, day, hour, minute).timestamp())

        format_socket_kline_page_inplace(
            opens,
            closes,
            highs,
            lows,
            trades,
            amounts,
            ret_count,
            settlement_prices=settlements,
            positions=positions,
        )

        klines = [None] * ret_count
        for i in range(ret_count):
            klines[i] = {
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
                "position": int(positions[i]),
                "trade": int(trades[i]),
                "amount": int(amounts[i]),
                "price": float(settlements[i]),
                "datetime": datetimes[i],
                "_ts": int(timestamps[i]),
            }
        return klines
