"""
模块：`parser/ex_get_instrument_info.py`。

职责：
1. 组装扩展行情合约码表请求（命令 0x2422）。
2. 解析单页回包中的 `市场#代码|名称` 文本记录。

边界：
1. 仅负责单页组包与解析，不承担跨页汇总。
2. 本页条数由服务端决定；请求体 138 字节，仅 start 随页变化。
"""

# coding=utf-8

import re
import struct
from collections import OrderedDict

from zsdtdx.parser.base import BaseParser

# 138 字节 0x2422：仅偏移 12 的 start（uint32）随页变化。
_INSTRUMENT_INFO_REQUEST = bytes.fromhex(
    "010000000000800080002224000000000000000000781f0e6a37447b502b7c0d01404c0a"
    "000000000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000000000000000000"
    "000000000000000000000000000100000000000000000000000000000000"
)
_RECORD_RE = re.compile(rb"(\d+)#([^|,]+)\|([^,]*)")


def pack_instrument_info_request(start, count=None) -> bytearray:
    """
    组装扩展行情 0x2422 码表请求。

    输入：
    1. start: 本页起始下标。
    2. count: 保留参数，服务端按自身页长返回，不写入请求。
    输出：
    1. 138 字节 send_pkg。
    用途：
    1. GetInstrumentInfo 与离线单测共用组包。
    边界条件：
    1. 仅修改 start；其余字节与官方客户端一致。
    """
    pkg = bytearray(_INSTRUMENT_INFO_REQUEST)
    struct.pack_into("<I", pkg, 12, int(start))
    return pkg


def parse_instrument_info_body(body_buf) -> list:
    """
    解析 0x2422 解压后的码表页。

    输入：
    1. body_buf: 解压后的字节，正文为逗号分隔的 `市场#代码|名称`。
    输出：
    1. list[OrderedDict]：category/market/code/name/desc。
    用途：
    1. 供港股、期货、扩展指数目录使用。
    边界条件：
    1. 空包或无 `市场#` 记录返回空列表；名称按 GBK 容错解码。
    """
    if not body_buf:
        return []
    raw = bytes(body_buf)
    rows = []
    for match in _RECORD_RE.finditer(raw):
        code = match.group(2).decode("gbk", "ignore").rstrip("\x00").strip()
        if code == "":
            continue
        rows.append(
            OrderedDict(
                [
                    ("category", 0),
                    ("market", int(match.group(1))),
                    ("code", code),
                    (
                        "name",
                        match.group(3).decode("gbk", "ignore").rstrip("\x00").strip(),
                    ),
                    ("desc", ""),
                ]
            )
        )
    return rows


class GetInstrumentInfo(BaseParser):
    def setParams(self, start, count=None):
        """
        输入：
        1. start: 本页起始下标。
        2. count: 保留参数，不写入请求。
        输出：
        1. 写入 138 字节 send_pkg。
        用途：
        1. 组包后由 call_api 发送。
        边界条件：
        1. 不在此处分页；调用方按返回条数累加 start。
        """
        self.send_pkg = pack_instrument_info_request(start, count)

    def parseResponse(self, body_buf):
        """
        输入：
        1. body_buf: 解压后的码表页。
        输出：
        1. 本页合约记录列表。
        用途：
        1. 解析扩展行情码表单页。
        边界条件：
        1. 结束页无文本记录时返回空列表。
        """
        return parse_instrument_info_body(body_buf)
