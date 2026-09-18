"""
模块：`parser/ex_setup_commands.py`。

职责：
1. 构造扩展行情连接后的握手请求。
2. 握手对齐银河海王星客户端：92 字节，命令号 0x6548，inner=0x2454。

边界：
1. 仅负责组包与丢弃握手回包，不解析业务字段。
2. 正文为固定身份块（来源：银河 7720 抓包）；回包无业务字段。
"""

# coding=utf-8

from zsdtdx.parser.base import BaseParser

# 扩展行情握手：92 字节，cmd=0x6548，inner=0x2454（银河海王星 7720 首连）。
_EX_SETUP1 = bytes.fromhex(
    "010148650001520052005424c0630e3a8287fd4d05e7e7e75d50f95844aa94fe"
    "41d587e66d4518e53f08a034e158c0abaff8069b6af7dccba67484f7a5027104"
    "bb02ef4535a52eae2243d8e061ca5bfebce9ee9b118394211d4e3ab5"
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
