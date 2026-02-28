"""
模块：`parser/get_index_bars.py`。

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

from zsdtdx.helper import get_datetime, get_price, get_volume
from zsdtdx.parser.base import BaseParser

class GetIndexBarsCmd(BaseParser):

    def setParams(self, category, market, code, start, count):
        """
        输入：
        1. category: 输入参数，约束以协议定义与函数实现为准。
        2. market: 输入参数，约束以协议定义与函数实现为准。
        3. code: 输入参数，约束以协议定义与函数实现为准。
        4. start: 输入参数，约束以协议定义与函数实现为准。
        5. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")

        self.category = category

        values = (
            0x10c,
            0x01016408,
            0x1c,
            0x1c,
            0x052d,
            market,
            code,
            category,
            1,
            start,
            count,
            0, 0, 0  # I + I +  H total 10 zero
        )

        pkg = struct.pack("<HIHHHH6sHHHHIIH", *values)
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

        (ret_count,) = struct.unpack("<H", body_buf[0: 2])
        pos += 2

        klines = []

        pre_diff_base = 0
        for i in range(ret_count):
            year, month, day, hour, minute, pos = get_datetime(self.category, body_buf, pos)

            price_open_diff, pos = get_price(body_buf, pos)
            price_close_diff, pos = get_price(body_buf, pos)

            price_high_diff, pos = get_price(body_buf, pos)
            price_low_diff, pos = get_price(body_buf, pos)

            (vol_raw,) = struct.unpack("<I", body_buf[pos: pos + 4])
            vol = get_volume(vol_raw)

            pos += 4
            (dbvol_raw,) = struct.unpack("<I", body_buf[pos: pos + 4])
            dbvol = get_volume(dbvol_raw)
            pos += 4

            (up_count, down_count) = struct.unpack("<HH", body_buf[pos: pos + 4])
            pos += 4

            open = self._cal_price1000(price_open_diff, pre_diff_base)

            price_open_diff = price_open_diff + pre_diff_base

            close = self._cal_price1000(price_open_diff, price_close_diff)
            high = self._cal_price1000(price_open_diff, price_high_diff)
            low = self._cal_price1000(price_open_diff, price_low_diff)

            pre_diff_base = price_open_diff + price_close_diff

            #### 为了避免python处理浮点数的时候，浮点数运算不精确问题，这里引入了多余的代码

            kline = OrderedDict([
                ("open", open),
                ("close", close),
                ("high", high),
                ("low", low),
                ("vol", vol),
                ("amount", dbvol),
                ("year", year),
                ("month", month),
                ("day", day),
                ("hour", hour),
                ("minute", minute),
                ("datetime", "%d-%02d-%02d %02d:%02d" % (year, month, day, hour, minute)),
                ("up_count", up_count),
                ("down_count", down_count)
            ])
            klines.append(kline)
        return klines

    def _cal_price1000(self, base_p, diff):
        """
        输入：
        1. base_p: 输入参数，约束以协议定义与函数实现为准。
        2. diff: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_cal_price1000` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        return float(base_p + diff)/1000
