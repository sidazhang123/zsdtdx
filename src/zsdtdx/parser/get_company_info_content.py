"""
模块：`parser/get_company_info_content.py`。

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

import six

from zsdtdx.parser.base import BaseParser


class GetCompanyInfoContent(BaseParser):

    def setParams(self, market, code, filename, start, length):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. filename: 输入参数，约束以协议定义与函数实现为准。
        4. start: 输入参数，约束以协议定义与函数实现为准。
        5. length: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")

        if type(filename) is six.text_type:
            filename = filename.encode("utf-8")

        if len(filename) != 80:
            filename = filename.ljust(80, b'\x00')


        pkg = bytearray.fromhex(u'0c 07 10 9c 00 01 68 00 68 00 d0 02')
        pkg.extend(struct.pack(u"<H6sH80sIII", market, code, 0, filename, start, length, 0))
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
        _, length = struct.unpack(u'<10sH', body_buf[:12])
        pos += 12
        content = body_buf[pos: pos+length]
        return content.decode('gbk', 'ignore')
