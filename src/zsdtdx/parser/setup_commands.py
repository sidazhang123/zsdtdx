"""
模块：`parser/setup_commands.py`。

职责：
1. 构造标准行情连接后的三条握手请求。
2. 握手对齐银河海王星客户端：292 字节首包、13 字节第二包、42 字节「银河证券」身份包。

边界：
1. 仅负责组包与丢弃握手回包，不解析业务字段。
2. 第一条 292 字节包的末 8 字节按连接随机生成；前 284 字节为银河抓包常量。
"""

# coding=utf-8

import os

from zsdtdx.parser.base import BaseParser

# 标准行情握手首包前 284 字节常量（来源：银河海王星 F10 抓包 7709 首连）。
_SETUP1_PREFIX = bytes.fromhex(
    "0c01187b00011a011a010b0047d7b5ff89a775cb6f9a279b25c23750749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae277003571ca34012266752b310928f2dbc306c776b5bc87e"
    "b3376a05588b8447c7bbb2528608c62da95a2b69749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357f7e5f2e0"
    "de0975886f9a279b25c23750749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357"
)


class SetupCmd1(BaseParser):
    def setup(self):
        """
        输入：无。
        输出：无；写入 292 字节 send_pkg。
        用途：发送标准行情握手首包（会话常量块）。
        边界：末 8 字节每次连接随机，服务器不把它当校验和。
        """
        self.send_pkg = bytearray(_SETUP1_PREFIX + os.urandom(8))

    def parseResponse(self, body_buf):
        """
        输入：握手回包体。
        输出：原样 body_buf。
        用途：握手回包无业务字段，仅确认收包。
        边界：不解析、不校验内容。
        """
        return body_buf


class SetupCmd2(BaseParser):
    def setup(self):
        """
        输入：无。
        输出：无；写入 13 字节 send_pkg。
        用途：发送第二条握手（13 字节，末载荷为 02）。
        边界：必须紧接第一条 292 字节包之后发送。
        """
        self.send_pkg = bytearray.fromhex("0c 02 18 94 00 01 03 00 03 00 0d 00 02")

    def parseResponse(self, body_buf):
        """
        输入：握手回包体。
        输出：原样 body_buf。
        用途：握手回包无业务字段，仅确认收包。
        边界：不解析、不校验内容。
        """
        return body_buf


class SetupCmd3(BaseParser):
    def setup(self):
        """
        输入：无。
        输出：无；写入 42 字节 send_pkg。
        用途：以 GBK「银河证券」声明客户端身份（命令号 0x0fdb）。
        边界：尾字段固定为 float 11.63、u32=14、末字节 05。
        """
        self.send_pkg = bytearray.fromhex(
            "0c 03 18 99 00 01 20 00 20 00 db 0f d2 f8 ba d3 d6 a4 c8 af"
            "00 00 00 7b 14 3a 41 0e 00 00 00 00 00 00 00 00 00 00 00 00"
            "00 05"
        )

    def parseResponse(self, body_buf):
        """
        输入：握手回包体。
        输出：原样 body_buf。
        用途：握手回包无业务字段，仅确认收包。
        边界：不解析、不校验内容。
        """
        return body_buf
