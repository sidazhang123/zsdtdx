"""
模块：`parser/get_company_info_content.py`。

职责：
1. 组包并解析标准行情公司信息正文单页（命令 0x02D0）。
2. 请求带分类下标、文件内 start 与剩余 length；服务端单页上限 30720 字节。

边界：
1. 本解析器只处理单次请求/单页回包，翻页由统一客户端按剩余 length 继续发起。
2. 回包正文为 10 字节前缀 + uint16 本页字节数 + GBK 文本。
"""

# coding=utf-8

import struct

import six

from zsdtdx.parser.base import BaseParser

# 服务端公司信息正文单页上限（字节）。
COMPANY_INFO_CONTENT_PAGE_SIZE = 30720


class GetCompanyInfoContent(BaseParser):
    def setParams(self, market, code, filename, start, length, category_index):
        """
        输入：
        1. market: 标准市场编号。
        2. code: 股票代码。
        3. filename: 目录返回的文件名（如 300063.V14）。
        4. start: 本页在文件内的字节偏移。
        5. length: 从 start 起的剩余总字节，不是本页上限。
        6. category_index: 目录记录下标，写入请求中 code 后的 uint16。
        输出：
        1. 无；写入 114 字节 send_pkg。
        用途：
        1. 按官方客户端组一页正文请求。
        边界条件：
        1. filename 不足 80 字节时右侧补 0；category_index 以 uint16 打包。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")

        if type(filename) is six.text_type:
            filename = filename.encode("utf-8")

        if len(filename) != 80:
            filename = filename.ljust(80, b"\x00")

        pkg = bytearray.fromhex("0c 07 10 9c 00 01 68 00 68 00 d0 02")
        pkg.extend(
            struct.pack(
                "<H6sH80sIII",
                int(market),
                code,
                int(category_index),
                filename,
                int(start),
                int(length),
                0,
            )
        )
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """
        解析公司信息内容并执行容错解码。

        输入:
            body_buf(bytes): 服务端返回的数据包正文。
        输出:
            str: 使用 GBK 解码后的公司信息文本。
        用途:
            让上层调用方直接获得可读文本，避免重复解码。
        边界条件:
            遇到非法字节时忽略该字节，不抛出异常中断流程。
        """
        pos = 0
        _, length = struct.unpack("<10sH", body_buf[:12])
        pos += 12
        content = body_buf[pos : pos + length]
        return content.decode("gbk", "ignore")
