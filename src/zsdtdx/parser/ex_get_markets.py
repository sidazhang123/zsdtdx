"""
模块：`parser/ex_get_markets.py`。

职责：
1. 构造并解析扩展行情市场目录请求。
2. 将协议二进制字段转换为上层可消费的数据结构。

边界：
1. 只负责单次请求组包与回包解析，不管理连接池、重试或 host 切换。
2. 网络错误由上层客户端处理；本模块只定义协议数据边界。
"""

# coding=utf-8

import struct
from collections import OrderedDict

from zsdtdx.parser.base import BaseParser


class GetMarkets(BaseParser):
    def setup(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setup` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        self.send_pkg = bytearray.fromhex("01 02 48 69 00 01 02 00 02 00 f4 23")

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
        (cnt,) = struct.unpack("<H", body_buf[pos : pos + 2])
        pos += 2

        result = []
        for i in range(cnt):
            # 64byte for one
            (category, raw_name, market, raw_short_name, _, unknown_bytes) = (
                struct.unpack("<B32sB2s26s2s", body_buf[pos : pos + 64])
            )
            pos += 64

            if category == 0 and market == 0:
                continue

            name = raw_name.decode("gbk")
            short_name = raw_short_name.decode("gbk")

            result.append(
                OrderedDict(
                    [
                        ("market", market),
                        ("category", category),
                        ("name", name.rstrip("\x00")),
                        ("short_name", short_name.rstrip("\x00")),
                        # ('unknown_bytes', unknown_bytes)
                    ]
                )
            )

        return result
