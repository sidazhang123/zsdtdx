"""
模块：`parser/ex_get_history_instrument_bars_range.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 5 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
3. 时间：socket 层解析 d1/d2 仅拼 datetime 字符串，不返回 year/month/day/hour/minute 分列字段。
"""

# coding=utf-8


import datetime as _dt
import struct
from collections import OrderedDict
from typing import Any, Dict, List

import numpy as np

from zsdtdx.helper import format_socket_kline_page_inplace
from zsdtdx.parser.base import BaseParser


class GetHistoryInstrumentBarsRange(BaseParser):
    def __init__(self, *args, **kvargs):
        """
        输入：
        1. *args: 输入参数，约束以协议定义与函数实现为准。
        2. **kvargs: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__init__` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        self.seqid = 1
        BaseParser.__init__(self, *args, **kvargs)
        
        
    def setParams(self, market, code, date,date2):
        
        
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. date: 输入参数，约束以协议定义与函数实现为准。
        4. date2: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pkg = bytearray.fromhex('01')
        pkg.extend(struct.pack("<B", self.seqid))
        self.seqid = self.seqid+1
        pkg.extend(bytearray.fromhex('38 92 00 01 16 00 16 00 0D 24'))
        code = code.encode("utf-8")
        #x =struct.pack("<B9s",  market, code)
        pkg.extend(struct.pack("<B9s",  market, code))
        pkg.extend(bytearray.fromhex('07 00'))
        pkg.extend(struct.pack("<LL", date,date2))
        #print(hexdump.hexdump(pkg))
        self.send_pkg = pkg
#      

    def parseResponse(self, body_buf):
#        print('测试', body_buf)
#        fileobj = open("a.bin", 'wb')  # make partfile
#        fileobj.write(body_buf)  # write data into partfile
#        fileobj.close()
        #print(hexdump.hexdump(body_buf[0:1024]))
#        import zlib
#        d=zlib.decompress(body_buf[16:])        
#        print(hexdump.hexdump(d))
        """
        输入：
        1. body_buf: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `parseResponse` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pos = 12
        (ret_count,) = struct.unpack("H", body_buf[pos: pos + 2])
        pos += 2
        if ret_count <= 0:
            return []

        opens = np.empty(ret_count, dtype=np.float64)
        closes = np.empty(ret_count, dtype=np.float64)
        highs = np.empty(ret_count, dtype=np.float64)
        lows = np.empty(ret_count, dtype=np.float64)
        positions = np.empty(ret_count, dtype=np.float64)
        trades = np.empty(ret_count, dtype=np.float64)
        settlements = np.empty(ret_count, dtype=np.float64)
        d1_arr = np.empty(ret_count, dtype=np.uint16)
        d2_arr = np.empty(ret_count, dtype=np.uint16)

        for i in range(ret_count):
            d1, d2, open_price, high, low, close, position, trade, settlementprice = struct.unpack(
                "<HHffffIIf", body_buf[pos:pos + 32]
            )
            pos += 32
            d1_arr[i] = d1
            d2_arr[i] = d2
            opens[i] = open_price
            highs[i] = high
            lows[i] = low
            closes[i] = close
            positions[i] = float(position)
            trades[i] = float(trade)
            settlements[i] = settlementprice

        format_socket_kline_page_inplace(
            opens,
            closes,
            highs,
            lows,
            trades,
            None,
            ret_count,
            settlement_prices=settlements,
            positions=positions,
        )

        d1 = d1_arr.astype(np.int64)
        d2 = d2_arr.astype(np.int64)
        years = d1 // 2048 + 2004
        months = (d1 % 2048) // 100
        days = (d1 % 2048) % 100
        hours = d2 // 60
        minutes = d2 % 60
        datetimes: List[str] = [
            f"{int(y):04d}-{int(m):02d}-{int(d):02d} {int(h):02d}:{int(mi):02d}"
            for y, m, d, h, mi in zip(years, months, days, hours, minutes)
        ]
        timestamps = np.empty(ret_count, dtype=np.int64)
        for i in range(ret_count):
            timestamps[i] = int(
                _dt.datetime(int(years[i]), int(months[i]), int(days[i]), int(hours[i]), int(minutes[i])).timestamp()
            )

        klines: List[Dict[str, Any]] = [None] * ret_count  # type: ignore[list-item]
        for i in range(ret_count):
            klines[i] = OrderedDict(
                [
                    ("datetime", datetimes[i]),
                    ("_ts", int(timestamps[i])),
                    ("open", float(opens[i])),
                    ("high", float(highs[i])),
                    ("low", float(lows[i])),
                    ("close", float(closes[i])),
                    ("position", int(positions[i])),
                    ("trade", int(trades[i])),
                    ("settlementprice", float(settlements[i])),
                ]
            )
        return klines
        
    
#00000000  01 01 08 6A 01 01 16 00  16 00 FF 23 2F 49 46 4C   ...j.... ...#/IFL 
#00000010  30 00 F0 F4 94 13 07 00  01 00 00 00 00 00 F0 00   0....... ........ 
    
#00000000: 01 01 08 6A 01 01 16 00  16 00 FF 23 4A 4E 56 44  ...j.......#JNVD
#00000010: 41 00 C0 EC A3 13 07 00  01 00 00 00 00 00 C0 03  A...............    

#00000000  01 01 08 6A 01 01 16 00  16 00 FF 23 2F 49 46 31   ...j.... ...#/IF1
#00000010  37 30 39 00 94 13 07 00  01 00 00 00 00 00 F0 00   709..... ........
