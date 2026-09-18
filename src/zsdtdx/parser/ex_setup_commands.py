"""
模块：`parser/ex_setup_commands.py`。

职责：
1. 构造扩展行情连接后的握手请求。
2. 握手为 92 字节，命令号 0x6548，inner=0x2454。

边界：
1. 仅负责组包与丢弃握手回包，不解析业务字段。
2. 正文为固定身份块；回包无业务字段。
"""

# coding=utf-8

from zsdtdx.parser.base import BaseParser

# 扩展行情握手：92 字节，cmd=0x6548，inner=0x2454。
_EX_SETUP1 = bytes.fromhex(
    "010148650001520052005424e5bb1c2fafe525941f32c6e5d53dfb415b734cc9"
    "cdbf0ac92021bfdd1eb06d22e158c0abaff8069b6af7dccba67484f71f32c6e5"
    "d53dfb411f32c6e5d53dfb41a9325ac935dc0837335a16e4ce17c1bb"
)


class ExSetupCmd1(BaseParser):
    def setup(self):
        """
        输入：无。
        输出：无；写入 92 字节 send_pkg。
        用途：发送扩展行情握手。
        边界：正文为固定 92 字节身份块；回包无业务字段。
        """
        self.send_pkg = bytearray(_EX_SETUP1)

    def parseResponse(self, body_buf):
        """
        输入：握手回包体。
        输出：原样 body_buf。
        用途：握手回包无业务字段，仅确认收包。
        边界：不解析、不校验内容。
        """
        return body_buf
