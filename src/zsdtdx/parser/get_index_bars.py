"""
模块：`parser/get_index_bars.py`。

职责：
1. 标准行情指数 K 线协议封装与 socket 回包解析。
2. 解码委托 `diff_kline_page`，数值格式化在 helper 向量化完成。

边界：
1. 仅负责指数 K 线单页解析，不承担分页与路由。
"""

# coding=utf-8

import struct

import six

from zsdtdx.parser.base import BaseParser
from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page


class GetIndexBarsCmd(BaseParser):

    def setParams(self, category, market, code, start, count):
        """
        输入：category/market/code/start/count。
        输出：无；构造 send_pkg。
        用途：组装指数 K 线请求包。
        边界条件：code 为 str 时转 bytes。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")

        self.category = category

        values = (
            0x10c,
            0x01016408,
            0x1c,
            0x1c,
            0x052d,
            market,
            code,
            category,
            1,
            start,
            count,
            0, 0, 0,
        )

        pkg = struct.pack("<HIHHHH6sHHHHIIH", *values)
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """
        输入：body_buf 为 socket 回包体。
        输出：K 线 dict 列表（含 `_ts`、`up_count`、`down_count`）。
        用途：差分编码页解析，刻度由 helper 批量完成。
        边界条件：空页返回 []。
        """
        return parse_diff_encoded_kline_page(body_buf, self.category, with_index_counts=True)
