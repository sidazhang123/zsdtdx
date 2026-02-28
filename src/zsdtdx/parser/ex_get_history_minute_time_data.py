"""
模块：`parser/ex_get_history_minute_time_data.py`。

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


class GetHistoryMinuteTimeData(BaseParser):

    def setParams(self, market, code, date):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. date: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pkg = bytearray.fromhex("01 01 30 00 01 01 10 00 10 00 0c 24")
        code = code.encode("utf-8")
        pkg.extend(struct.pack("<IB9s", date, market, code))
        #pkg = bytearray.fromhex('01 01 30 00 01 01 10 00 10 00 0c 24 3b c8 33 01 2f 49 46 4c 30 00 38 ec 2d 00')
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        #        print('测试', body_buf)
        #        fileobj = open("//Users//wy//data//a.bin", 'wb')  # make partfile
        #        fileobj.write(body_buf)  # write data into partfile
        #        fileobj.close()
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
        market, code, _, num = struct.unpack('<B9s8sH', body_buf[pos: pos + 20])
        pos += 20
#        print(market, code.decode(), num)
        result = []
        for i in range(num):

            (raw_time, price, avg_price, volume, amount) = struct.unpack("<HffII", body_buf[pos: pos + 18])

            pos += 18
            hour = raw_time // 60
            minute = raw_time % 60

            result.append(OrderedDict([
                ("hour", hour),
                ("minute", minute),
                ("price", price),
                ("avg_price", avg_price),
                ("volume", volume),
                ("open_interest", amount),
            ]))

        return result
