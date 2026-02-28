"""
模块：`parser/get_company_info_category.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 3 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import struct
from collections import OrderedDict

import six

from zsdtdx.parser.base import BaseParser


class GetCompanyInfoCategory(BaseParser):

    def setParams(self, market, code):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")

        pkg = bytearray.fromhex(u'0c 0f 10 9b 00 01 0e 00 0e 00 cf 02')
        pkg.extend(struct.pack(u"<H6sI", market, code, 0))
        self.send_pkg = pkg
    """

    10 00 d7 ee d0 c2 cc e1 ca be 00 00 ..... 36 30 30 33 30 30 2e 74 78 74 .... e8 e3 07 00 92 1f 00 00 .....

    10.... name
    36.... filename

    e8 e3 07 00 --- start
    92 1f 00 00 --- length

    """
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
        (num, ) = struct.unpack("<H", body_buf[:2])
        pos += 2

        category = []



        def get_str(b):
            """
            输入：
            1. b: 输入参数，约束以协议定义与函数实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `get_str` 对应的协议处理、数据解析或调用适配逻辑。
            边界条件：
            1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
            """
            p = b.find(b'\x00')
            if p != -1:
                b = b[0: p]
            try:
                n = b.decode("gbk")
            except Exception:
                n = "unkown_str"
            return n

        for i in range(num):
            (name, filename, start, length) = struct.unpack(u"<64s80sII", body_buf[pos: pos+ 152])
            pos += 152
            entry = OrderedDict(
                [
                    ('name', get_str(name)),
                    ('filename', get_str(filename)),
                    ('start', start),
                    ('length', length),
                ]
            )
            category.append(entry)

        return category
