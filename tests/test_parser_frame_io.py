"""协议帧按长度收发：短读拼包、未压缩与 zlib 包体。"""

import struct
import zlib

from zsdtdx.parser.base import (
    BaseParser,
    ResponseHeaderRecvFails,
    ResponseRecvFails,
)


class _FragSocket:
    """每次最多交出 1 字节，用来模拟 TCP 短读。"""

    def __init__(self, incoming: bytes):
        self._incoming = incoming
        self._pos = 0
        self.sent = b""
        self.send_pkg_num = 0
        self.send_pkg_bytes = 0
        self.recv_pkg_num = 0
        self.recv_pkg_bytes = 0
        self.last_api_send_bytes = 0
        self.last_api_recv_bytes = 0
        self.first_pkg_send_time = None

    def sendall(self, data: bytes) -> None:
        self.sent += bytes(data)

    def recv(self, size: int) -> bytes:
        if self._pos >= len(self._incoming) or size <= 0:
            return b""
        take = min(1, int(size), len(self._incoming) - self._pos)
        chunk = self._incoming[self._pos : self._pos + take]
        self._pos += take
        return chunk


class _EchoParser(BaseParser):
    def parseResponse(self, body_buf):
        return bytes(body_buf)


def _frame(body: bytes, *, unzipsize: int | None = None) -> bytes:
    unzip = len(body) if unzipsize is None else int(unzipsize)
    head = struct.pack("<IIIHH", 1, 2, 3, len(body), unzip)
    return head + body


def test_fragmented_plain_body_roundtrip():
    payload = b"kline-page-plain"
    sock = _FragSocket(_frame(payload))
    parser = _EchoParser(sock)
    parser.send_pkg = b"\x01\x02req"
    assert parser.call_api() == payload
    assert sock.sent == b"\x01\x02req"
    assert sock.send_pkg_bytes == len(parser.send_pkg)
    assert sock.recv_pkg_bytes == 16 + len(payload)


def test_fragmented_zlib_body_roundtrip():
    plain = b"kline-page-zip" * 20
    packed = zlib.compress(plain)
    sock = _FragSocket(_frame(packed, unzipsize=len(plain)))
    parser = _EchoParser(sock)
    parser.send_pkg = b"req"
    assert parser.call_api() == plain


def test_short_header_raises_header_error():
    sock = _FragSocket(b"\x00" * 4)
    parser = _EchoParser(sock)
    parser.send_pkg = b"req"
    try:
        parser.call_api()
    except ResponseHeaderRecvFails:
        return
    raise AssertionError("短包头应抛 ResponseHeaderRecvFails")


def test_short_body_raises_recv_error():
    head = struct.pack("<IIIHH", 1, 2, 3, 10, 10)
    sock = _FragSocket(head + b"abc")
    parser = _EchoParser(sock)
    parser.send_pkg = b"req"
    try:
        parser.call_api()
    except ResponseRecvFails:
        return
    raise AssertionError("包体未读满应抛 ResponseRecvFails")
