"""
模块：`parser/base.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 6 个，函数 7 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import datetime
import struct
import sys
import zlib

from zsdtdx.log import DEBUG, log

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
            1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
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
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
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
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
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
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
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
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
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
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        self.setup()

        if not(self.client):
            raise SocketClientNotReady("socket client not ready")

        if not(self.send_pkg):
            raise SendPkgNotReady("send pkg not ready")

        nsended = self.client.send(self.send_pkg)

        self.client.send_pkg_num += 1
        self.client.send_pkg_bytes += nsended
        self.client.last_api_send_bytes = nsended

        if self.client.first_pkg_send_time is None:
            self.client.first_pkg_send_time = datetime.datetime.now()

        if DEBUG:
            log.debug("send package:" + str(self.send_pkg))
        if nsended != len(self.send_pkg):
            log.debug("send bytes error")
            raise SendRequestPkgFails("send fails")
        else:
            head_buf = self.client.recv(self.rsp_header_len)
            if DEBUG:
                log.debug("recv head_buf:" + str(head_buf)  + " |len is :" + str(len(head_buf)))
            if len(head_buf) == self.rsp_header_len:
                self.client.recv_pkg_num += 1
                self.client.recv_pkg_bytes += self.rsp_header_len
                _, _, _, zipsize, unzipsize = struct.unpack("<IIIHH", head_buf)
                if DEBUG:
                    log.debug("zip size is: " + str(zipsize))
                body_buf = bytearray()

                last_api_recv_bytes = self.rsp_header_len
                while True:
                    buf = self.client.recv(zipsize)
                    len_buf = len(buf)
                    self.client.recv_pkg_num += 1
                    self.client.recv_pkg_bytes += len_buf
                    last_api_recv_bytes += len_buf
                    body_buf.extend(buf)
                    if not(buf) or len_buf == 0 or len(body_buf) == zipsize:
                        break

                self.client.last_api_recv_bytes = last_api_recv_bytes

                if len(buf) == 0:
                    log.debug("接收数据体失败服务器断开连接")
                    raise ResponseRecvFails("接收数据体失败服务器断开连接")
                if zipsize == unzipsize:
                    log.debug("不需要解压")
                else:
                    log.debug("需要解压")
                    if sys.version_info[0] == 2:
                        unziped_data = zlib.decompress(buffer(body_buf))
                    else:
                        unziped_data = zlib.decompress(body_buf)
                    body_buf = unziped_data
                    ## 解压
                if DEBUG:
                    log.debug("recv body: ")
                    log.debug(body_buf)

                return self.parseResponse(body_buf)

            else:
                log.debug("head_buf is not 0x10")
                raise ResponseHeaderRecvFails("head_buf is not 0x10 : " + str(head_buf))

