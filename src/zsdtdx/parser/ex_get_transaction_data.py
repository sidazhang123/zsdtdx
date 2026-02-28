"""
模块：`parser/ex_get_transaction_data.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 2 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import datetime
import struct
from collections import OrderedDict

import six

from zsdtdx.parser.base import BaseParser


class GetTransactionData(BaseParser):

    def setParams(self, market, code, start, count):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. start: 输入参数，约束以协议定义与函数实现为准。
        4. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")
        pkg = bytearray.fromhex('01 01 08 00 03 01 12 00 12 00 fc 23')
        pkg.extend(struct.pack("<B9siH", market, code, start, count))
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
        pos = 0
        market, code, _, num = struct.unpack('<B9s4sH', body_buf[pos: pos + 16])
        pos += 16
        result = []
        for i in range(num):

            (raw_time, price, volume, zengcang, direction) = struct.unpack("<HIIiH", body_buf[pos: pos + 16])

            pos += 16
            hour = raw_time // 60
            minute = raw_time % 60
            second = direction % 10000
            # 保留 direction 原始值，供 nature 字段直接复用。
            nature = direction

            if second > 59:
                second = 0

            date = datetime.datetime.combine(datetime.date.today(), datetime.time(hour,minute,second))

            value = direction // 10000

            if value == 0:
                direction = 1
                if zengcang > 0:
                    if volume > zengcang:
                        nature_name = "多开"
                    elif volume == zengcang:
                        nature_name = "双开"
                elif zengcang == 0:
                    nature_name = "多换"
                else:
                    if volume == -zengcang:
                        nature_name = "双平"
                    else:
                        nature_name = "空平"
            elif value == 1:
                direction = -1
                if zengcang > 0:
                    if volume > zengcang:
                        nature_name = "空开"
                    elif volume == zengcang:
                        nature_name = "双开"
                elif zengcang == 0:
                    nature_name = "空换"
                else:
                    if volume == -zengcang:
                        nature_name = "双平"
                    else:
                        nature_name = "多平"
            else:
                direction = 0
                if zengcang > 0:
                    if volume > zengcang:
                        nature_name = "开仓"
                    elif volume == zengcang:
                        nature_name = "双开"
                elif zengcang < 0:
                    if volume > -zengcang:
                        nature_name = "平仓"
                    elif volume == -zengcang:
                        nature_name = "双平"
                else:
                    nature_name = "换手"

            if market in [31,48]:
                if nature == 0:
                    direction = 1
                    nature_name = 'B'
                elif nature == 256:
                    direction = -1
                    nature_name = 'S'
                else: #512
                    direction = 0
                    nature_name = ''


            result.append(OrderedDict([
                ("date", date),
                ("hour", hour),
                ("minute", minute),
                ("second", second),
                ("price", price),
                ("volume", volume),
                ("zengcang", zengcang),
                ("nature", nature),
                ("nature_mark", nature // 10000),
                ("nature_value", nature % 10000),
                ("nature_name", nature_name),
                ("direction", direction),
            ]))

        return result
