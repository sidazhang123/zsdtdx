"""
模块：`parser/get_security_count.py`。

职责：
1. 构造并解析标准行情证券数量请求。
2. 将协议二进制字段转换为上层可消费的数据结构。

边界：
1. 只负责单次请求组包与回包解析，不管理连接池、重试或 host 切换。
2. 网络错误由上层客户端处理；本模块只定义协议数据边界。
"""

# coding=utf-8

import struct

from zsdtdx.parser.base import BaseParser


class GetSecurityCountCmd(BaseParser):
    def setParams(self, market):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        pkg = bytearray.fromhex("0c 0c 18 6c 00 01 08 00 08 00 4e 04")
        market_pkg = struct.pack("<H", market)
        pkg.extend(market_pkg)
        pkg.extend(b"\x75\xc7\x33\x01")
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
        (num,) = struct.unpack("<H", body_buf[:2])
        return num
