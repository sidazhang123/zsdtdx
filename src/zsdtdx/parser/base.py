"""
模块：`parser/base.py`。

职责：
1. 提供协议 parser 的请求发送、定长接收、解压与解析基类。
2. 维护每次请求的收发字节统计，并定义底层协议异常类型。

边界：
1. 只处理单次协议调用，不负责连接重试与 host 切换。
2. 业务字段由各具体 parser 的 `parseResponse` 实现。
"""

# coding=utf-8

import datetime
import logging
import struct
import sys
import zlib

from zsdtdx.util.log import log

try:
    import cython

    if cython.compiled:

        def buffer(x):
            """
            输入：
            1. x: 输入参数，约束以协议定义与函数实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `buffer` 对应的协议处理、数据解析或调用适配逻辑。
            边界条件：
            1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
            """
            return x
except ImportError:
    pass


class SocketClientNotReady(Exception):
    pass


class SendPkgNotReady(Exception):
    pass


class SendRequestPkgFails(Exception):
    pass


class ResponseHeaderRecvFails(Exception):
    pass


class ResponseRecvFails(Exception):
    pass


RSP_HEADER_LEN = 0x10


def _note_recv(sock, size: int) -> None:
    """输入套接字和本次读到的字节数，输出无。边界：没有计数字段时跳过。"""
    if not hasattr(sock, "recv_pkg_num"):
        return
    sock.recv_pkg_num += 1
    sock.recv_pkg_bytes += int(size)


def _recv_exact(sock, size: int, *, header: bool = False) -> bytes:
    """
    输入套接字和要读的字节数。
    输出恰好 size 字节。
    用途：响应头和包体都按长度读满，避免短读把后半段留给下一次请求。
    边界：size 为 0 返回空字节；对端关闭时包头抛 ResponseHeaderRecvFails，包体抛 ResponseRecvFails；超时原样抛出。
    """
    if size < 0:
        raise ResponseRecvFails("接收数据体失败服务器断开连接")
    if size == 0:
        return b""
    parts = []
    got = 0
    while got < size:
        buf = sock.recv(size - got)
        if not buf:
            if header:
                partial = b"".join(parts)
                raise ResponseHeaderRecvFails("head_buf is not 0x10 : " + str(partial))
            raise ResponseRecvFails("接收数据体失败服务器断开连接")
        _note_recv(sock, len(buf))
        parts.append(buf)
        got += len(buf)
    if len(parts) == 1:
        return parts[0]
    return b"".join(parts)


class BaseParser(object):
    def __init__(self, client, lock=None):
        """
        输入：
        1. client: 输入参数，约束以协议定义与函数实现为准。
        2. lock: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__init__` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        self.client = client
        self.data = None
        self.send_pkg = None

        self.rsp_header = None
        self.rsp_body = None
        self.rsp_header_len = RSP_HEADER_LEN

        if lock:
            self.lock = lock
        else:
            self.lock = None

    def setParams(self, *args, **xargs):
        """
        构建请求
        :return:
        """
        pass

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
        pass

    def setup(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setup` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        pass

    def call_api(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `call_api` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        if self.lock:
            with self.lock:
                log.debug("sending thread lock api call")
                result = self._call_api()
        else:
            result = self._call_api()
        return result

    def _call_api(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_call_api` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        self.setup()

        if not (self.client):
            raise SocketClientNotReady("socket client not ready")

        if not (self.send_pkg):
            raise SendPkgNotReady("send pkg not ready")

        self.client.sendall(self.send_pkg)
        sent = len(self.send_pkg)
        self.client.send_pkg_num += 1
        self.client.send_pkg_bytes += sent
        self.client.last_api_send_bytes = sent

        if self.client.first_pkg_send_time is None:
            self.client.first_pkg_send_time = datetime.datetime.now()

        if log.isEnabledFor(logging.DEBUG):
            log.debug("send package:" + str(self.send_pkg))

        head_buf = _recv_exact(self.client, self.rsp_header_len, header=True)
        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                "recv head_buf:" + str(head_buf) + " |len is :" + str(len(head_buf))
            )
        _, _, _, zipsize, unzipsize = struct.unpack("<IIIHH", head_buf)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("zip size is: " + str(zipsize))
        if zipsize <= 0:
            log.debug("接收数据体失败服务器断开连接")
            raise ResponseRecvFails("接收数据体失败服务器断开连接")
        body_buf = _recv_exact(self.client, int(zipsize))
        self.client.last_api_recv_bytes = int(self.rsp_header_len) + int(zipsize)
        if zipsize == unzipsize:
            log.debug("不需要解压")
        else:
            log.debug("需要解压")
            if sys.version_info[0] == 2:
                body_buf = zlib.decompress(buffer(body_buf))
            else:
                body_buf = zlib.decompress(body_buf)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("recv body: ")
            log.debug(body_buf)
        return self.parseResponse(body_buf)
