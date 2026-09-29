"""
模块：`parser/get_block_bars.py`。

职责：
1. 标准行情板块指数 K 线协议封装与 socket 回包解析。
2. 组包复用个股/指数的 54 字节布局，命令号为 0x0523。

边界：
1. 仅负责单页解析，不承担分页与板块名称解析。
2. 回包含涨跌家数；OHLC 为绝对价（`absolute_ohlc=True`），与 0x052D 差分不同。
3. 无复权语义；reserved0 固定为 0。
"""

# coding=utf-8

from zsdtdx.parser.base import BaseParser
from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page
from zsdtdx.parser.get_security_bars import pack_standard_kline_request

# 板块指数 K 线命令号（个股/普通指数为 0x052D）。
BLOCK_KLINE_COMMAND = 0x0523


class GetBlockBarsCmd(BaseParser):
    def setParams(self, category, market, code, start, count):
        """
        输入：category/market/code/start/count。
        输出：无；构造 54 字节 send_pkg。
        用途：组装板块指数 K 线请求（0x0523，布局与 0x052D 相同）。
        边界条件：reserved0 固定为 0；code 为 str 时由组包函数转 bytes。
        """
        self.category = category
        self.send_pkg = pack_standard_kline_request(
            category,
            market,
            code,
            start,
            count,
            qfq=False,
            command=BLOCK_KLINE_COMMAND,
        )

    def parseResponse(self, body_buf):
        """
        输入：body_buf 为 socket 回包体。
        输出：K 线 dict 列表（含 `_ts`、`up_count`、`down_count`）。
        用途：解析板块指数 K 线（含涨跌家数；OHLC 绝对价）。
        边界条件：空页返回 []；若按差分解会 open 落在 [low,high] 外并价格发散。
        """
        return parse_diff_encoded_kline_page(
            body_buf,
            self.category,
            with_index_counts=True,
            absolute_ohlc=True,
        )
