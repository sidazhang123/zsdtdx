"""
模块：`parser/ex_get_instrument_bars.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 3 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import struct
from collections import OrderedDict

import six

from zsdtdx.helper import get_datetime
from zsdtdx.parser.base import BaseParser


class GetInstrumentBars(BaseParser):

    # ﻿ff232f49464c30007401a9130400010000000000f000
    """

    first：

    ﻿0000   01 01 08 6a 01 01 16 00 16 00                    ...j......


    second：
    ﻿0000   ff 23 2f 49 46 4c 30 00 74 01 a9 13 04 00 01 00  .#/IFL0.t.......
    0010   00 00 00 00 f0 00                                ......

    ﻿0000   ff 23 28 42 41 42 41 00 00 00 a9 13 04 00 01 00  .#(BABA.........
    0010   00 00 00 00 f0 00                                ......

    ﻿0000   ff 23 28 42 41 42 41 00 00 00 a9 13 03 00 01 00  .#(BABA.........
    0010   00 00 00 00 f0 00                                ......

    ﻿0000   ff 23 08 31 30 30 30 30 38 34 33 13 04 00 01 00  .#.10000843.....
    0010   00 00 00 00 f0 00                                ......
    """

    def setup(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setup` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pass
        #self.client.send(bytearray.fromhex('01 01 08 6a 01 01 16 00 16 00'))

    def setParams(self, category, market, code, start, count):
        """
        输入：
        1. category: 输入参数，约束以协议定义与函数实现为准。
        2. market: 输入参数，约束以协议定义与函数实现为准。
        3. code: 输入参数，约束以协议定义与函数实现为准。
        4. start: 输入参数，约束以协议定义与函数实现为准。
        5. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")
        pkg = bytearray.fromhex('01 01 08 6a 01 01 16 00 16 00')
        pkg.extend(bytearray.fromhex("ff 23"))

        self.category = category

        #pkg = bytearray.fromhex("ff 23")

        #count
        last_value = 0x00f00000
        pkg.extend(struct.pack('<B9sHHIH', market, code, category, 1, start, count))
                                                                # 这个1还不确定是什么作用，疑似和是否复权有关
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
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
        pos = 0

        # 算了，前面不解析了，没太大用
        # (market, code) = struct.unpack("<B9s", body_buf[0: 10])
        pos += 18
        (ret_count, ) = struct.unpack('<H', body_buf[pos: pos+2])
        pos += 2

        klines = []

        for i in range(ret_count):
            year, month, day, hour, minute, pos = get_datetime(self.category, body_buf, pos)
            (open_price, high, low, close, position, trade, price) = struct.unpack("<ffffIIf", body_buf[pos: pos+28])
            (amount, ) = struct.unpack("f", body_buf[pos+16: pos+16+4])

            pos += 28
            kline = OrderedDict([
                ("open", open_price),
                ("high", high),
                ("low", low),
                ("close", close),
                ("position", position),
                ("trade", trade),
                ("price", price),
                ("year", year),
                ("month", month),
                ("day", day),
                ("hour", hour),
                ("minute", minute),
                ("datetime", "%d-%02d-%02d %02d:%02d" % (year, month, day, hour, minute)),
                ("amount", amount),
            ])

            klines.append(kline)

        return klines
