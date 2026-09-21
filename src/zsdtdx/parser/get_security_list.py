"""
模块：`parser/get_security_list.py`。

职责：
1. 组装标准行情证券码表请求（命令 0x044D）。
2. 解析单页回包中的 37 字节记录，输出代码与 16 字节 GBK 名称等字段。

边界：
1. 仅负责单页组包与解析，不承担跨市场分页。
2. 记录固定 37 字节（6 代码 + 2 手数单位 + 16 GBK 名称 + 4 保留 + 1 小数位 + 4 昨收 + 4 尾部）；单页 count 为 1600。
"""

# coding=utf-8

import struct
from collections import OrderedDict

from zsdtdx.helper import get_volume
from zsdtdx.params import TDXParams
from zsdtdx.parser.base import BaseParser

# 12 字节头：cmd=0x044D，声明后续 16 字节体（含 2 字节命令号）。
_SECURITY_LIST_HEADER = bytes.fromhex("0c 01 18 6e 00 01 10 00 10 00 4d 04")
_SECURITY_LIST_TAIL_PAD = bytes(6)
# 银河 0x044D：名称 16 字节 GBK，其后 4 字节保留，decimal_point 在偏移 28。
_RECORD_STRUCT = struct.Struct("<6sH16s4sBI4s")


def pack_security_list_request(market, start, count=None) -> bytearray:
    """
    组装标准行情 0x044D 码表请求。

    输入：
    1. market: 标准市场号（0 深圳 / 1 上海 / 2 北京）。
    2. start: 本页起始下标。
    3. count: 本页条数，默认 `TDXParams.MAX_SECURITY_LIST_COUNT`。
    输出：
    1. 26 字节 send_pkg。
    用途：
    1. GetSecurityList 与离线单测共用组包，避免两处漂移。
    边界条件：
    1. start 为 32 位；count 由调用方裁剪，本函数不向服务端声明硬上限以外的语义。
    """
    if count is None:
        count = TDXParams.MAX_SECURITY_LIST_COUNT
    return bytearray(
        _SECURITY_LIST_HEADER
        + struct.pack("<HIH", int(market), int(start), int(count))
        + _SECURITY_LIST_TAIL_PAD
    )


class GetSecurityList(BaseParser):
    def setParams(self, market, start, count=None):
        """
        输入：
        1. market: 标准市场号。
        2. start: 本页起始下标。
        3. count: 本页条数，默认 1600。
        输出：
        1. 写入 26 字节 send_pkg。
        用途：
        1. 组包后由 call_api 发送。
        边界条件：
        1. 不在此处分页；调用方按返回条数累加 start。
        """
        self.send_pkg = pack_security_list_request(market, start, count)

    def parseResponse(self, body_buf):
        """
        解析证券列表，并对证券名称执行容错解码。

        输入：
        1. body_buf: 解压后的码表页，前 2 字节为条数。
        输出：
        1. list[OrderedDict]：code/volunit/decimal_point/name（16 字节 GBK 去空）/pre_close。
        用途：
        1. 供标准行情深沪京码表与指数目录扫描使用。
        边界条件：
        1. 空包或不足 2 字节返回空列表；名称含非法 GBK 字节时忽略异常字节。
        2. 记录不足 37 字节时停止，避免半截记录污染结果。
        """
        if not body_buf or len(body_buf) < 2:
            return []

        (num,) = struct.unpack_from("<H", body_buf, 0)
        pos = 2
        rec_size = TDXParams.SECURITY_LIST_RECORD_SIZE
        stocks = []
        for _ in range(num):
            one_bytes = body_buf[pos : pos + rec_size]
            if len(one_bytes) < rec_size:
                break
            (
                code,
                volunit,
                name_bytes,
                _reversed_bytes1,
                decimal_point,
                pre_close_raw,
                _reversed_bytes2,
            ) = _RECORD_STRUCT.unpack(one_bytes)
            stocks.append(
                OrderedDict(
                    [
                        ("code", code.decode("utf-8", "ignore")),
                        ("volunit", volunit),
                        ("decimal_point", decimal_point),
                        ("name", name_bytes.decode("gbk", "ignore").rstrip("\x00")),
                        ("pre_close", get_volume(pre_close_raw)),
                    ]
                )
            )
            pos += rec_size
        return stocks
