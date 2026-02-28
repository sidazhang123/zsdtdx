"""
模块：`parser/get_security_list.py`。

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

from zsdtdx.helper import get_volume
from zsdtdx.parser.base import BaseParser


class GetSecurityList(BaseParser):

    def setParams(self, market, start):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. start: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pkg = bytearray.fromhex(u'0c 01 18 64 01 01 06 00 06 00 50 04')
        pkg_param = struct.pack("<HH", market, start)
        pkg.extend(pkg_param)
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """
        解析证券列表，并对证券名称执行容错解码。

        输入:
            body_buf(bytes): 服务端返回的证券列表数据。
        输出:
            list[OrderedDict]: 证券基础信息列表。
        用途:
            支持大分页场景下稳定获取全量证券清单。
        边界条件:
            名称字段包含非法 GBK 字节时忽略异常字节，避免整页失败。
        """

        pos = 0
        (num, ) = struct.unpack("<H", body_buf[:2])
        pos += 2
        stocks = []
        for i in range(num):

            # b'880023d\x00\xd6\xd0\xd0\xa1\xc6\xbd\xbe\xf9.9\x04\x00\x02\x9a\x99\x8cA\x00\x00\x00\x00'
            # 880023 100 中小平均 276782 2 17.575001 0 80846648

            one_bytes = body_buf[pos: pos + 29]

            (code, volunit,
             name_bytes, reversed_bytes1, decimal_point,
             pre_close_raw, reversed_bytes2) = struct.unpack("<6sH8s4sBI4s", one_bytes)

            code = code.decode("utf-8")
            name = name_bytes.decode("gbk", "ignore").rstrip("\x00")
            pre_close = get_volume(pre_close_raw)
            pos += 29

            one = OrderedDict(
                [
                    ('code', code),
                    ('volunit', volunit),
                    ('decimal_point', decimal_point),
                    ('name', name),
                    ('pre_close', pre_close),
                ]
            )

            stocks.append(one)


        return stocks
