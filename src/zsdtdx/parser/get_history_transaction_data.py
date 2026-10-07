"""
模块：`parser/get_history_transaction_data.py`。

职责：
1. 构造并解析标准行情历史逐笔成交请求。
2. 将协议二进制字段转换为上层可消费的数据结构。

边界：
1. 只负责单次请求组包与回包解析，不管理连接池、重试或 host 切换。
2. 网络错误由上层客户端处理；本模块只定义协议数据边界。
"""

# coding=utf-8

import struct
from collections import OrderedDict

import six

from zsdtdx.util.helper import get_price, get_time
from zsdtdx.parser.base import BaseParser


class GetHistoryTransactionData(BaseParser):
    def setParams(self, market, code, start, count, date):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. start: 输入参数，约束以协议定义与函数实现为准。
        4. count: 输入参数，约束以协议定义与函数实现为准。
        5. date: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")

        if type(date) is (type(date) is six.text_type) or (
            type(date) is six.binary_type
        ):
            date = int(date)

        pkg = bytearray.fromhex("0c 01 30 01 00 01 12 00 12 00 b5 0f")
        pkg.extend(struct.pack("<IH6sHH", date, market, code, start, count))
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
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        pos = 0
        (num,) = struct.unpack("<H", body_buf[:2])
        pos += 2
        ticks = []

        # skip 4 bytes
        pos += 4

        last_price = 0
        for i in range(num):
            ### ?? get_time
            # \x80\x03 = 14:56

            hour, minute, pos = get_time(body_buf, pos)

            price_raw, pos = get_price(body_buf, pos)
            vol, pos = get_price(body_buf, pos)
            buyorsell, pos = get_price(body_buf, pos)
            _, pos = get_price(body_buf, pos)

            last_price = last_price + price_raw

            tick = OrderedDict(
                [
                    ("time", "%02d:%02d" % (hour, minute)),
                    ("price", float(last_price) / 100),
                    ("vol", vol),
                    ("buyorsell", buyorsell),
                ]
            )

            ticks.append(tick)

        return ticks
