"""
模块：`parser/raw_parser.py`。

职责：
1. 透传已组好的原始请求包，并将回包体原样返回。
2. 供 `base_socket_client` 在调试或自定义协议时使用。

边界：
1. 不做字段解析；调用方自行处理 `body_buf`。
"""

# coding=utf-8

from zsdtdx.parser.base import BaseParser


class RawParser(BaseParser):

    def setParams(self, pkg):
        """
        输入：
        1. pkg: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
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
        return body_buf
