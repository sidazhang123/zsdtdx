"""
模块：`parser/get_index_bars.py`。

职责：
1. 标准行情指数 K 线协议封装与 socket 回包解析。
2. 解码委托 `diff_kline_page`，数值格式化在 helper 向量化完成。

边界：
1. 仅负责指数 K 线单页解析，不承担分页与路由。
2. 请求包与个股 K 线相同（官方 0x052D，54 字节）。
"""

# coding=utf-8

from zsdtdx.parser.base import BaseParser
from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page
from zsdtdx.parser.get_security_bars import pack_standard_kline_request


class GetIndexBarsCmd(BaseParser):
    def setParams(self, category, market, code, start, count, qfq=True):
        """
        输入：category/market/code/start/count，以及 qfq 前复权开关。
        输出：无；构造 54 字节 send_pkg。
        用途：组装指数 K 线请求包（与个股 0x052D 同布局）。
        边界条件：code 为 str 时由组包函数转 bytes。
        """
        self.category = category
        self.send_pkg = pack_standard_kline_request(
            category, market, code, start, count, qfq=qfq
        )

    def parseResponse(self, body_buf):
        """
        输入：body_buf 为 socket 回包体。
        输出：K 线 dict 列表（含 `_ts`、`up_count`、`down_count`）。
        用途：差分编码页解析，刻度由 helper 批量完成。
        边界条件：空页返回 []。
        """
        return parse_diff_encoded_kline_page(
            body_buf, self.category, with_index_counts=True
        )
