"""
模块：`parser/get_report_file.py`。

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

from zsdtdx.parser.base import BaseParser


class GetReportFile(BaseParser):
    def setParams(self, filename, offset=0):
        """
        输入：
        1. filename: 输入参数，约束以协议定义与函数实现为准。
        2. offset: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pkg = bytearray.fromhex(u'0C 12 34 00 00 00')
        # Fom DTGear request.py file
        node_size = 0x7530
        raw_data = struct.pack(r"<H2I100s", 0x06B9,
                               offset, node_size, filename.encode("utf-8"))
        raw_data_len = struct.calcsize(r"<H2I100s")
        pkg.extend(struct.pack(u"<HH{}s".format(raw_data_len),
                               raw_data_len, raw_data_len, raw_data))
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
        (chunksize, ) = struct.unpack("<I", body_buf[:4])

        if chunksize > 0:
            return {
                "chunksize": chunksize,
                "chunkdata":  body_buf[4:]
            }
        else:
            return {
                "chunksize": 0
            }
