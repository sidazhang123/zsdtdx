"""
模块：`parser/ex_get_instrument_info.py`。

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

from zsdtdx.parser.base import BaseParser


class GetInstrumentInfo(BaseParser):

    """
    01 08 04 0b 00 01 0b 00 0b 00

    00 24
    08 类别
    00 00 00 00
    26 00  数量  38 个
    01 00 未知

    In [8]: 11402/38
    Out[8]: 300.05263157894734

    In [9]: 11402%38
    Out[9]: 2

    """
    def setParams(self, start, count=100):
        """
        输入：
        1. start: 输入参数，约束以协议定义与函数实现为准。
        2. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pkg = bytearray.fromhex("01 04 48 67 00 01 08 00 08 00 f5 23")
        pkg.extend(struct.pack('<IH', start, count))
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
        start, count = struct.unpack("<IH", body_buf[:6])
        pos += 6
        result = []
        for i in range(count):
            (category, market, unused_bytes, code_raw, name_raw, desc_raw) = \
                struct.unpack("<BB3s9s17s9s", body_buf[pos: pos+40])

            code = code_raw.decode("gbk", 'ignore')
            name = name_raw.decode("gbk", 'ignore')
            desc = desc_raw.decode("gbk", 'ignore')

            one = OrderedDict(
                [
                    ("category", category),
                    ("market", market),
                    ("code", code.rstrip("\x00")),
                    ("name", name.rstrip("\x00")),
                    ("desc", desc.rstrip("\x00")),
                ]
            )

            pos += 64
            result.append(one)

        return result
