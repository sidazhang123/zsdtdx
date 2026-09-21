"""
模块：`parser/get_report_file.py`。

职责：
1. 组装标准行情命名文件元数据请求（命令 0x02C5）。
2. 组装命名文件分块请求（命令 0x06B9），按固定页长翻页。
3. 解析元数据回包中的文件字节数，以及分块回包中的本页原文。

边界：
1. 组包对齐银河海王星 7709 抓包；解压由 `BaseParser.call_api` 在内存完成。
2. 本模块只处理单次请求/单页回包，跨页拼接由调用方完成。
"""

# coding=utf-8

import struct

from zsdtdx.parser.base import BaseParser

# 0x02C5：12 字节头 + 40 字节文件名。
_NAMED_FILE_META_HEADER = bytes.fromhex("0c 04 18 69 00 01 2a 00 2a 00 c5 02")
_NAMED_FILE_META_NAME_WIDTH = 40

# 0x06B9：12 字节头 + offset/页长 + 300 字节文件名。
_NAMED_FILE_CHUNK_HEADER = bytes.fromhex("0c 05 18 6a 00 01 36 01 36 01 b9 06")
_NAMED_FILE_CHUNK_NAME_WIDTH = 300
NAMED_FILE_CHUNK_SIZE = 0x7530


def _encode_filename(filename: str, width: int) -> bytes:
    """
    输入文件名与字段宽度，输出定长字节。

    输入：
    1. filename: 远程文件名（如 `infoharbor_ex.name`）。
    2. width: 字段宽度（元数据 40 / 分块 300）。
    输出：
    1. 右侧补 0 的定长 bytes。
    用途：
    1. 0x02C5 / 0x06B9 共用文件名编码。
    边界条件：
    1. 超长时截断到 width；空名得到全 0。
    """
    raw = str(filename or "").encode("utf-8")
    if len(raw) > width:
        raw = raw[:width]
    return raw.ljust(width, b"\x00")


def pack_named_file_meta_request(filename: str) -> bytearray:
    """
    组装 0x02C5 命名文件元数据请求。

    输入：
    1. filename: 远程文件名。
    输出：
    1. 52 字节 send_pkg。
    用途：
    1. 先取文件总字节，再按页拉取。
    边界条件：
    1. 文件名字段固定 40 字节。
    """
    return bytearray(
        _NAMED_FILE_META_HEADER
        + _encode_filename(filename, _NAMED_FILE_META_NAME_WIDTH)
    )


def pack_named_file_chunk_request(
    filename: str, offset: int, chunk_size: int = NAMED_FILE_CHUNK_SIZE
) -> bytearray:
    """
    组装 0x06B9 命名文件分块请求。

    输入：
    1. filename: 远程文件名。
    2. offset: 本页起始字节偏移。
    3. chunk_size: 请求页长，默认 30000。
    输出：
    1. 320 字节 send_pkg。
    用途：
    1. 按偏移拉取一页文件原文。
    边界条件：
    1. 文件名字段固定 300 字节；页长写入请求，末页由回包 chunksize 裁剪。
    """
    size = int(chunk_size) if int(chunk_size) > 0 else NAMED_FILE_CHUNK_SIZE
    return bytearray(
        _NAMED_FILE_CHUNK_HEADER
        + struct.pack("<II", int(offset), size)
        + _encode_filename(filename, _NAMED_FILE_CHUNK_NAME_WIDTH)
    )


def parse_named_file_meta_body(body_buf) -> dict:
    """
    解析 0x02C5 解压后的元数据正文。

    输入：
    1. body_buf: 解压后的字节。
    输出：
    1. `{"filesize": int, "checksum": str}`。
    用途：
    1. 供翻页循环得知总长度。
    边界条件：
    1. 不足 4 字节时 filesize=0；checksum 为 filesize 后到首个 0 的 ASCII。
    """
    raw = bytes(body_buf or b"")
    if len(raw) < 4:
        return {"filesize": 0, "checksum": ""}
    (filesize,) = struct.unpack_from("<I", raw, 0)
    checksum = raw[4:].split(b"\x00", 1)[0].decode("ascii", "ignore")
    return {"filesize": int(filesize), "checksum": checksum}


def parse_named_file_chunk_body(body_buf) -> dict:
    """
    解析 0x06B9 解压后的分块正文。

    输入：
    1. body_buf: 解压后的字节。
    输出：
    1. `{"chunksize": int, "chunkdata": bytes}`。
    用途：
    1. 调用方按 chunksize 累加偏移并拼接 chunkdata。
    边界条件：
    1. 不足 4 字节或 chunksize<=0 时 chunkdata 为空。
    """
    raw = bytes(body_buf or b"")
    if len(raw) < 4:
        return {"chunksize": 0, "chunkdata": b""}
    (chunksize,) = struct.unpack_from("<I", raw, 0)
    if chunksize <= 0:
        return {"chunksize": 0, "chunkdata": b""}
    return {"chunksize": int(chunksize), "chunkdata": raw[4 : 4 + int(chunksize)]}


class GetReportFileMeta(BaseParser):
    def setParams(self, filename):
        """
        输入：
        1. filename: 远程文件名。
        输出：
        1. 写入 52 字节 send_pkg。
        用途：
        1. 查询命名文件总字节。
        边界条件：
        1. 不在此处分页。
        """
        self.send_pkg = pack_named_file_meta_request(filename)

    def parseResponse(self, body_buf):
        """
        输入：
        1. body_buf: 解压后的元数据正文。
        输出：
        1. filesize 与 checksum。
        用途：
        1. 解析 0x02C5 回包。
        边界条件：
        1. 空包 filesize=0。
        """
        return parse_named_file_meta_body(body_buf)


class GetReportFile(BaseParser):
    def setParams(self, filename, offset=0, chunk_size=None):
        """
        输入：
        1. filename: 远程文件名。
        2. offset: 本页起始偏移。
        3. chunk_size: 请求页长，默认 30000。
        输出：
        1. 写入 320 字节 send_pkg。
        用途：
        1. 拉取命名文件一页原文。
        边界条件：
        1. 不在此处分页；调用方按 chunksize 累加 offset。
        """
        size = NAMED_FILE_CHUNK_SIZE if chunk_size is None else int(chunk_size)
        self.send_pkg = pack_named_file_chunk_request(filename, offset, size)

    def parseResponse(self, body_buf):
        """
        输入：
        1. body_buf: 解压后的分块正文。
        输出：
        1. chunksize 与 chunkdata。
        用途：
        1. 解析 0x06B9 回包。
        边界条件：
        1. 空页 chunksize=0。
        """
        return parse_named_file_chunk_body(body_buf)
