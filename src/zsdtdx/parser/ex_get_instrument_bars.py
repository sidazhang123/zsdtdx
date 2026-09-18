"""
模块：`parser/ex_get_instrument_bars.py`。

职责：
1. 扩展行情（港股/期货等）K 线协议封装与 socket 回包解析。
2. 请求为 64 字节 0xD808（首包）/ 0xD908（翻页）。
3. 回包为 42 字节前缀 + 固定 32 字节记录：4 字节时间 + OHLC(float×4) + 持仓/成交量/结算价。
4. 港股前复权走 extra（1=前复权，0=不复权）；期货忽略该字段，调用方应保持 0。

边界：
1. 仅负责单页解析；指数 ex 路由与期货共用本解析器。
2. 时间：本层解析年月日时分，写入 datetime 为 `YYYY-MM-DD HH:MM:SS`（秒位固定 `:00`）与 `_ts`；上层只读 datetime/_ts。
3. 默认页长为服务端硬上限 700，由 `extended_kline_page_size` 控制。
"""

# coding=utf-8

import datetime as _dt
import struct

import numpy as np
import six

from zsdtdx.helper import format_socket_kline_page_inplace, get_datetime
from zsdtdx.parser.base import BaseParser

# 首包 cmd=0xD808，翻页 cmd=0xD908；inner 固定 0x2489。
_KLINE_CMD_FIRST = 0xD808
_KLINE_CMD_PAGE = 0xD908
_KLINE_INNER = 0x2489
_KLINE_SEQ = 0x0B
_KLINE_UNK = 0x0101
_KLINE_BODY_LEN = 0x36
_KLINE_PREFIX_LEN = 42
_KLINE_RECORD_LEN = 32


def pack_extended_kline_request(
    category, market, code, start, count, qfq: bool = False
) -> bytearray:
    """
    组装扩展行情 0xD808/0xD908 K 线请求。

    输入：
    1. category: K 线周期（与标准行情相同：1 分钟=7，日线=4）。
    2. market/code/start/count: 市场、代码、分页偏移、本页条数。
    3. qfq: 港股前复权开关；True 写 extra=1，False 写 extra=0。
    输出：
    1. 64 字节 send_pkg。
    用途：
    1. GetInstrumentBars 组包，避免与解析器漂移。
    边界：
    1. start==0 用 0xD808，翻页用 0xD908；count 按调用方传入，服务端单页最多返回 700 条。
    2. 期货忽略 extra；指数/期货调用保持 qfq=False。
    """
    if type(code) is six.text_type:
        code = code.encode("utf-8")
    cmd = _KLINE_CMD_FIRST if int(start) == 0 else _KLINE_CMD_PAGE
    pkg = bytearray(
        struct.pack(
            "<BBHHHHHB9s",
            0x01,
            _KLINE_SEQ,
            cmd,
            _KLINE_UNK,
            _KLINE_BODY_LEN,
            _KLINE_BODY_LEN,
            _KLINE_INNER,
            int(market),
            code,
        )
    )
    pkg.extend(bytes(14))
    extra = 1 if qfq else 0
    pkg.extend(
        struct.pack(
            "<HHIHH",
            int(category),
            1,
            int(start),
            int(count),
            extra,
        )
    )
    pkg.extend(bytes(16))
    return pkg


class GetInstrumentBars(BaseParser):
    def setup(self):
        """输入无；输出无；扩展行情 K 线无需额外 setup。"""
        pass

    def setParams(self, category, market, code, start, count, qfq=False):
        """
        输入：category/market/code/start/count/qfq。
        输出：无；构造 64 字节 send_pkg。
        用途：组装扩展行情 K 线请求包。
        边界条件：code 为 str 时转 bytes；不在此处分页；qfq 仅港股有效。
        """
        self.category = category
        self.send_pkg = pack_extended_kline_request(
            category, market, code, start, count, qfq=qfq
        )

    def parseResponse(self, body_buf):
        """
        输入：body_buf 为 socket 回包体。
        输出：K 线 dict 列表（含 `_ts`）。
        用途：按 42 字节前缀 + 32 字节记录解码。
        边界条件：空页或前缀不足返回 []。
        """
        if body_buf is None or len(body_buf) < _KLINE_PREFIX_LEN:
            return []

        ret_count = struct.unpack_from("<H", body_buf, 40)[0]
        max_n = (len(body_buf) - _KLINE_PREFIX_LEN) // _KLINE_RECORD_LEN
        if ret_count > max_n:
            ret_count = max_n
        if ret_count <= 0:
            return []

        buf = memoryview(body_buf) if not isinstance(body_buf, memoryview) else body_buf
        cat = self.category
        pos = _KLINE_PREFIX_LEN

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
                buf[pos + 16]
                | (buf[pos + 17] << 8)
                | (buf[pos + 18] << 16)
                | (buf[pos + 19] << 24)
            )
            amounts[i] = struct.unpack_from("<f", buf, pos + 16)[0]
            trades[i] = float(
                buf[pos + 20]
                | (buf[pos + 21] << 8)
                | (buf[pos + 22] << 16)
                | (buf[pos + 23] << 24)
            )
            settlements[i] = struct.unpack_from("<f", buf, pos + 24)[0]
            pos += 28

            datetimes[i] = (
                f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:00"
            )
            timestamps[i] = int(
                _dt.datetime(year, month, day, hour, minute).timestamp()
            )

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
