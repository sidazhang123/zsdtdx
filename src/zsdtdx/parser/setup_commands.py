"""
模块：`parser/setup_commands.py`。

职责：
1. 构造标准行情连接后的三条握手请求。
2. 握手顺序为 292 字节首包、13 字节第二包、42 字节 `tdxlevel` 身份包。

边界：
1. 仅负责组包与丢弃握手回包，不解析业务字段。
2. 第一条 292 字节包的末 8 字节按连接随机生成；前 284 字节为固定常量。
"""

# coding=utf-8

import os

from zsdtdx.parser.base import BaseParser

# 标准行情握手首包前 284 字节常量。
_SETUP1_PREFIX = bytes.fromhex(
    "0c01187b00011a011a010b00e53878ee8bd8dbb8749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357b979179e"
    "dcfc5a4c6810db2bdf3e50a19e93269128ddf91f749933ae27700357749933ae"
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
        用途：发送第二条握手（13 字节，末载荷为 01）。
        边界：必须紧接第一条 292 字节包之后发送。
        """
        self.send_pkg = bytearray.fromhex("0c 02 18 94 00 01 03 00 03 00 0d 00 01")

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
        用途：以 ASCII `tdxlevel` 声明客户端身份（命令号 0x0fdb）。
        边界：尾字段固定为 7.73 / 05。
        """
        self.send_pkg = bytearray.fromhex(
            "0c 03 18 99 00 01 20 00 20 00 db 0f 74 64 78 6c 65 76 65 6c"
            "00 00 00 29 5c f7 40 11 00 00 00 00 00 00 00 00 00 00 00 00"
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
