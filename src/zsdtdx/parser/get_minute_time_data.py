"""
模块：`parser/get_minute_time_data.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 2 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import struct
from collections import OrderedDict

import six

from zsdtdx.helper import get_price
from zsdtdx.parser.base import BaseParser


class GetMinuteTimeData(BaseParser):

    def setParams(self, market, code):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")
        pkg = bytearray.fromhex(u'0c 1b 08 00 01 01 0e 00 0e 00 1d 05')
        pkg.extend(struct.pack("<H6sI", market, code, 0))
        self.send_pkg = pkg

    """
    b1cb74000c1b080001b61d05be03be03f0000000a208ce038d2c028302972f4124b11a00219821011183180014891c0009be0b4207b11000429c2041....

    In [26]: get_price(b, 0)
Out[26]: (0, 1)

In [27]: get_price(b, 1)
Out[27]: (0, 2)

In [28]: get_price(b, 2)
Out[28]: (546, 4)

In [29]: get_price(b, 4)
Out[29]: (-206, 6)

In [30]: get_price(b, 6)
Out[30]: (2829, 8)

In [31]: get_price(b, 8)
Out[31]: (2, 9)

In [32]: get_price(b, 9)
Out[32]: (131, 11)

In [36]: get_price(b, 11)
Out[36]: (3031, 13)

In [37]: get_price(b, 13)
Out[37]: (-1, 14)

In [38]: get_price(b, 14)
Out[38]: (36, 15)

In [39]: get_price(b, 15)
Out[39]: (1713, 17)

In [40]: get_price(b, 17)
Out[40]: (0, 18)
    """
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
        (num, ) = struct.unpack("<H", body_buf[:2])
        last_price = 0
        pos += 4
        prices = []
        for i in range(num):
            price_raw, pos = get_price(body_buf, pos)
            reversed1, pos = get_price(body_buf, pos)
            vol, pos = get_price(body_buf, pos)
            last_price = last_price + price_raw
            price = OrderedDict(
                [
                    ("price", float(last_price)/100),
                    ("vol", vol)
                ]
            )
            prices.append(price)
        return prices
