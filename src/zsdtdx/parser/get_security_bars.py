"""
模块：`parser/get_security_bars.py`。

职责：
1. 标准行情个股 K 线协议封装与 socket 回包解析。
2. 解码委托 `diff_kline_page`，数值格式化在 helper 向量化完成。

边界：
1. 仅负责单页解析，不承担分页。
"""

# coding=utf-8

import struct

import six

from zsdtdx.parser.base import BaseParser
from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page


class GetSecurityBarsCmd(BaseParser):
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

        self.category = category

        values = (
            0x10C,
            0x01016408,
            0x1C,
            0x1C,
            0x052D,
            market,
            code,
            category,
            1,
            start,
            count,
            0,
            0,
            0,  # I + I +  H total 10 zero
        )

        pkg = struct.pack("<HIHHHH6sHHHHIIH", *values)
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """输入 body_buf；输出已格式化的 K 线 dict 列表。"""
        return parse_diff_encoded_kline_page(
            body_buf, self.category, with_index_counts=False
        )
