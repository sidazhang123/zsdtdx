"""
模块：`parser/get_history_transaction_data.py`。

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

from zsdtdx.helper import get_price, get_time
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
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")

        if type(date) is (type(date) is six.text_type) or (type(date) is six.binary_type):
            date = int(date)

        pkg = bytearray.fromhex(u'0c 01 30 01 00 01 12 00 12 00 b5 0f')
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
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pos = 0
        (num, ) = struct.unpack("<H", body_buf[:2])
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
                    ("price", float(last_price)/100),
                    ("vol", vol),
                    ("buyorsell", buyorsell),
                ]
            )

            ticks.append(tick)

        return ticks
