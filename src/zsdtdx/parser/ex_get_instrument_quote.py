"""
模块：`parser/ex_get_instrument_quote.py`。

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
class GetInstrumentQuote(BaseParser):

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
        pkg = bytearray.fromhex("01 01 08 02 02 01 0c 00 0c 00 fa 23")
        code = code.encode("utf-8")
        pkg.extend(struct.pack('<B9s', market, code))
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
        if (len(body_buf) < 20):
            return []

        pos = 0
        market, code = struct.unpack('<B9s', body_buf[pos: pos+10])
        pos += 10

        # jump 4
        pos += 4

        ## 持仓 ((13340,), 66),

        (pre_close, open_price, high, low, price, kaicang, _,
         zongliang, xianliang, _ , neipan, waipai,
         _, chicang,
         b1, b2, b3, b4, b5,
         bv1, bv2, bv3, bv4, bv5,
         a1, a2, a3, a4, a5,
         av1, av2, av3, av4, av5
         ) = struct.unpack('<fffffIIIIIIIIIfffffIIIIIfffffIIIII', body_buf[pos: pos+136])


        return [
            OrderedDict(
                [
                    ('market', market),
                    ('code', code.decode("utf-8").rstrip('\x00')),
                    ('pre_close', pre_close),
                    ('open', open_price),
                    ('high', high),
                    ('low', low),
                    ('price', price),
                    ('kaicang', kaicang),
                    ('zongliang', zongliang),
                    ('xianliang', xianliang),
                    ('neipan', neipan),
                    ('waipan', waipai),
                    ('chicang', chicang),
                    ('bid1', b1),
                    ('bid2', b2),
                    ('bid3', b3),
                    ('bid4', b4),
                    ('bid5', b5),
                    ('bid_vol1', bv1),
                    ('bid_vol2', bv2),
                    ('bid_vol3', bv3),
                    ('bid_vol4', bv4),
                    ('bid_vol5', bv5),
                    ('ask1', a1),
                    ('ask2', a2),
                    ('ask3', a3),
                    ('ask4', a4),
                    ('ask5', a5),
                    ('ask_vol1', av1),
                    ('ask_vol2', av2),
                    ('ask_vol3', av3),
                    ('ask_vol4', av4),
                    ('ask_vol5', av5),
                ]
            )
        ]
