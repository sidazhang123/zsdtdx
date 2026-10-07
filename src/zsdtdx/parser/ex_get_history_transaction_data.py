"""
模块：`parser/ex_get_history_transaction_data.py`。

职责：
1. 构造并解析扩展行情历史逐笔成交请求。
2. 将协议二进制字段转换为上层可消费的数据结构。

边界：
1. 只负责单次请求组包与回包解析，不管理连接池、重试或 host 切换。
2. 网络错误由上层客户端处理；本模块只定义协议数据边界。
"""

# coding=utf-8

import datetime
import struct
from collections import OrderedDict

from zsdtdx.parser.base import BaseParser


class GetHistoryTransactionData(BaseParser):
    def setParams(self, market, code, date, start, count):
        # if type(code) is six.text_type:
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. date: 输入参数，约束以协议定义与函数实现为准。
        4. start: 输入参数，约束以协议定义与函数实现为准。
        5. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        code = code.encode("utf-8")

        # if type(date) is (type(date) is six.text_type) or (type(date) is six.binary_type):
        #     date = int(date)

        # pkg1 = bytearray.fromhex('01 01 30 00 02 01 16 00 16 00 06 24 3b c8 33 01 1f 30 30 30 32 30 00 00 00 01 00 00 00 00 f0 00')
        pkg = bytearray.fromhex("01 01 30 00 02 01 16 00 16 00 06 24")
        pkg.extend(struct.pack("<IB9siH", date, market, code, start, count))
        self.send_pkg = pkg
        self.date = date

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
        market, code, _, num = struct.unpack("<B9s4sH", body_buf[pos : pos + 16])
        pos += 16
        result = []
        for i in range(num):
            (raw_time, price, volume, zengcang, direction) = struct.unpack(
                "<HIIiH", body_buf[pos : pos + 16]
            )

            pos += 16
            year = self.date // 10000
            month = self.date % 10000 // 100
            day = self.date % 100
            hour = raw_time // 60
            minute = raw_time % 60
            second = direction % 10000
            # 保留 direction 原始值，供 nature 字段直接复用。
            nature = direction
            value = direction // 10000
            # 对于大于59秒的值，属于无效数值
            if second > 59:
                second = 0
            date = datetime.datetime(year, month, day, hour, minute, second)

            if value == 0:
                direction = 1
                if zengcang > 0:
                    if volume > zengcang:
                        nature_name = "多开"
                    elif volume == zengcang:
                        nature_name = "双开"
                elif zengcang == 0:
                    nature_name = "多换"
                else:
                    if volume == -zengcang:
                        nature_name = "双平"
                    else:
                        nature_name = "空平"
            elif value == 1:
                direction = -1
                if zengcang > 0:
                    if volume > zengcang:
                        nature_name = "空开"
                    elif volume == zengcang:
                        nature_name = "双开"
                elif zengcang == 0:
                    nature_name = "空换"
                else:
                    if volume == -zengcang:
                        nature_name = "双平"
                    else:
                        nature_name = "多平"
            else:
                direction = 0
                if zengcang > 0:
                    if volume > zengcang:
                        nature_name = "开仓"
                    elif volume == zengcang:
                        nature_name = "双开"
                elif zengcang < 0:
                    if volume > -zengcang:
                        nature_name = "平仓"
                    elif volume == -zengcang:
                        nature_name = "双平"
                else:
                    nature_name = "换手"

            if market in [31, 48]:
                if nature == 0:
                    direction = 1
                    nature_name = "B"
                elif nature == 256:
                    direction = -1
                    nature_name = "S"
                else:  # 512
                    direction = 0
                    nature_name = ""

            result.append(
                OrderedDict(
                    [
                        ("date", date),
                        ("hour", hour),
                        ("minute", minute),
                        ("price", price),
                        ("volume", volume),
                        ("zengcang", zengcang),
                        ("natrue_name", nature_name),
                        # 同时输出两个键名，便于不同调用方按需读取。
                        ("nature_name", nature_name),
                        ("direction", direction),
                        ("nature", nature),
                    ]
                )
            )

        return result
