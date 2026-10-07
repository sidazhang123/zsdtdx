"""
模块：`parser/ex_get_minute_time_data.py`。

职责：
1. 构造并解析扩展行情当日分时请求。
2. 将协议二进制字段转换为上层可消费的数据结构。

边界：
1. 只负责单次请求组包与回包解析，不管理连接池、重试或 host 切换。
2. 网络错误由上层客户端处理；本模块只定义协议数据边界。
"""

# coding=utf-8

import struct
from collections import OrderedDict

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
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        pkg = bytearray.fromhex("01 07 08 00 01 01 0c 00 0c 00 0b 24")
        code = code.encode("utf-8")
        pkg.extend(struct.pack("<B9s", market, code))
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
        market, code, num = struct.unpack("<B9sH", body_buf[pos : pos + 12])
        pos += 12

        result = []
        for i in range(num):
            (raw_time, price, avg_price, volume, amount) = struct.unpack(
                "<HffII", body_buf[pos : pos + 18]
            )
            pos += 18
            hour = raw_time // 60
            minute = raw_time % 60

            result.append(
                OrderedDict(
                    [
                        ("hour", hour),
                        ("minute", minute),
                        ("price", price),
                        ("avg_price", avg_price),
                        ("volume", volume),
                        ("open_interest", amount),
                    ]
                )
            )

        return result
