"""
模块：`parser/get_history_minute_time_data.py`。

职责：
1. 构造并解析标准行情历史分时请求。
2. 将协议二进制字段转换为上层可消费的数据结构。

边界：
1. 只负责单次请求组包与回包解析，不管理连接池、重试或 host 切换。
2. 网络错误由上层客户端处理；本模块只定义协议数据边界。
"""

# coding=utf-8

import struct
from collections import OrderedDict

import six

from zsdtdx.util.helper import get_price
from zsdtdx.parser.base import BaseParser


class GetHistoryMinuteTimeData(BaseParser):
    def setParams(self, market, code, date):
        """
        :param market: 0/1
        :param code: '000001'
        :param date: 20161201  类似这样的整型
        :return:
        """

        if (type(date) is six.text_type) or (type(date) is six.binary_type):
            date = int(date)

        if type(code) is six.text_type:
            code = code.encode("utf-8")

        pkg = bytearray.fromhex("0c 01 30 00 01 01 0d 00 0d 00 b4 0f")
        pkg.extend(struct.pack("<IB6s", date, market, code))
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
        last_price = 0
        # 跳过了4个字节，实在不知道是什么意思
        pos += 6
        prices = []
        for i in range(num):
            price_raw, pos = get_price(body_buf, pos)
            _reserved, pos = get_price(body_buf, pos)
            vol, pos = get_price(body_buf, pos)
            last_price = last_price + price_raw
            price = OrderedDict([("price", float(last_price) / 100), ("vol", vol)])
            prices.append(price)
        return prices
