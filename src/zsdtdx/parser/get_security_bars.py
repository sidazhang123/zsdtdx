"""
模块：`parser/get_security_bars.py`。

职责：
1. 标准行情个股 K 线协议封装与 socket 回包解析。
2. 解码委托 `diff_kline_page`，数值格式化在 helper 向量化完成。

边界：
1. 仅负责单页解析，不承担分页。
2. 请求包对齐通达信官方客户端（TdxW）0x052D：54 字节，含前复权开关。
"""

# coding=utf-8

import struct

import six

from zsdtdx.parser.base import BaseParser
from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page

# 官方 TdxW 抓包：首包 inner=0x0101D208，翻页 inner=0x0101D308。
_KLINE_INNER_FIRST = 0x0101D208
_KLINE_INNER_PAGE = 0x0101D308
# 命令号 0x052D 后固定 16 字节填充。
_KLINE_TAIL_PAD = bytes(16)


def pack_standard_kline_request(
    category, market, code, start, count, qfq=True
) -> bytearray:
    """
    组装标准行情 0x052D K 线请求（个股与指数共用）。

    输入：
    1. category: K 线周期（官方 1 分钟=7，日线=4）。
    2. market/code/start/count: 市场、代码、分页偏移、本页条数。
    3. qfq: True 为前复权（reserved0=1），False 为不复权（reserved0=0）。
    输出：
    1. 54 字节 send_pkg。
    用途：
    1. GetSecurityBarsCmd / GetIndexBarsCmd 共用组包，避免两处漂移。
    边界：
    1. 单页 count 官方常用 420；start==0 与翻页使用不同 inner 字段。
    """
    if type(code) is six.text_type:
        code = code.encode("utf-8")

    reserved0 = 1 if qfq else 0
    inner = _KLINE_INNER_FIRST if int(start) == 0 else _KLINE_INNER_PAGE
    values = (
        0x040C,
        inner,
        0x2C,
        0x2C,
        0x052D,
        int(market),
        code,
        int(category),
        1,
        int(start),
        int(count),
        int(reserved0),
        0,
        0,
    )
    return bytearray(struct.pack("<HIHHHH6sHHHHIIH", *values) + _KLINE_TAIL_PAD)


class GetSecurityBarsCmd(BaseParser):
    def setParams(self, category, market, code, start, count, qfq=True):
        """
        输入：
        1. category/market/code/start/count: 与官方 0x052D 字段一致。
        2. qfq: 前复权开关，默认 True。
        输出：
        1. 写入 54 字节 send_pkg。
        用途：
        1. 组包后由 call_api 发送。
        边界条件：
        1. code 为 str 时转 utf-8；不在此处分页。
        """
        self.category = category
        self.send_pkg = pack_standard_kline_request(
            category, market, code, start, count, qfq=qfq
        )

    def parseResponse(self, body_buf):
        """输入 body_buf；输出已格式化的 K 线 dict 列表。"""
        return parse_diff_encoded_kline_page(
            body_buf, self.category, with_index_counts=False
        )
