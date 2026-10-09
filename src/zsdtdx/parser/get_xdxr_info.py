"""
模块：`parser/get_xdxr_info.py`。

职责：
1. 构造并解析标准行情除权除息记录请求。
2. 将协议二进制字段转换为上层可消费的数据结构。

边界：
1. 只负责单次请求组包与回包解析，不管理连接池、重试或 host 切换。
2. 网络错误由上层客户端处理；本模块只定义协议数据边界。
"""

# coding=utf-8

import struct
from collections import OrderedDict

import six

from zsdtdx.util.helper import get_datetime, get_volume
from zsdtdx.parser.base import BaseParser

XDXR_CATEGORY_MAPPING = {
    1: "除权除息",
    2: "送配股上市",
    3: "非流通股上市",
    4: "未知股本变动",
    5: "股本变化",
    6: "增发新股",
    7: "股份回购",
    8: "增发新股上市",
    9: "转配股上市",
    10: "可转债上市",
    11: "扩缩股",
    12: "非流通股缩股",
    13: "送认购权证",
    14: "送认沽权证",
}


class GetXdXrInfo(BaseParser):
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
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")
        pkg = bytearray.fromhex("0c 1f 18 76 00 01 0b 00 0b 00 0f 00 01 00")
        pkg.extend(struct.pack("<B6s", market, code))
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
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        pos = 0

        if len(body_buf) < 11:
            return []

        pos += 9  # skip 9
        (num,) = struct.unpack("<H", body_buf[pos : pos + 2])
        pos += 2

        rows = []

        def _get_v(v):
            """
            输入：
            1. v: 输入参数，约束以协议定义与函数实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_get_v` 对应的协议处理、数据解析或调用适配逻辑。
            边界条件：
            1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
            """
            if v == 0:
                return 0
            else:
                return get_volume(v)

        market, code = struct.unpack("<B6s", body_buf[:7])
        for i in range(num):
            pos += 7
            # noused = struct.unpack(u"<B", body_buf[pos: pos+1])
            pos += 1  # skip a byte
            year, month, day, hour, _minute, pos = get_datetime(9, body_buf, pos)
            (category,) = struct.unpack("<B", body_buf[pos : pos + 1])
            pos += 1

            # b'\x00\xe8\x00G' => 33000.00000
            # b'\x00\xc0\x0fF' => 9200.00000
            # b'\x00@\x83E' => 4200.0000

            suogu = None
            panqianliutong, panhouliutong, qianzongguben, houzongguben = (
                None,
                None,
                None,
                None,
            )
            songzhuangu, fenhong, peigu, peigujia = None, None, None, None
            fenshu, xingquanjia = None, None
            if category == 1:
                fenhong, peigujia, songzhuangu, peigu = struct.unpack(
                    "<ffff", body_buf[pos : pos + 16]
                )
            elif category in [11, 12]:
                (_, _, suogu, _) = struct.unpack("<IIfI", body_buf[pos : pos + 16])
            elif category in [13, 14]:
                xingquanjia, _, fenshu, _ = struct.unpack(
                    "<fIfI", body_buf[pos : pos + 16]
                )
            else:
                (
                    panqianliutong_raw,
                    qianzongguben_raw,
                    panhouliutong_raw,
                    houzongguben_raw,
                ) = struct.unpack("<IIII", body_buf[pos : pos + 16])
                panqianliutong = _get_v(panqianliutong_raw)
                panhouliutong = _get_v(panhouliutong_raw)
                qianzongguben = _get_v(qianzongguben_raw)
                houzongguben = _get_v(houzongguben_raw)

            pos += 16

            row = OrderedDict(
                [
                    ("year", year),
                    ("month", month),
                    ("day", day),
                    ("category", category),
                    ("name", self.get_category_name(category)),
                    ("fenhong", fenhong),
                    ("peigujia", peigujia),
                    ("songzhuangu", songzhuangu),
                    ("peigu", peigu),
                    ("suogu", suogu),
                    ("panqianliutong", panqianliutong),
                    ("panhouliutong", panhouliutong),
                    ("qianzongguben", qianzongguben),
                    ("houzongguben", houzongguben),
                    ("fenshu", fenshu),
                    ("xingquanjia", xingquanjia),
                ]
            )
            rows.append(row)

        return rows

    def get_category_name(self, category_id):
        """
        输入：
        1. category_id: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_category_name` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        if category_id in XDXR_CATEGORY_MAPPING:
            return XDXR_CATEGORY_MAPPING[category_id]
        else:
            return str(category_id)
