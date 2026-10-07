"""
模块：`parser/setup_base.py`。

职责：
1. 提供标准行情与扩展行情握手 parser 的公共回包透传实现。

边界：
1. 握手包的具体字节内容仍由各命令模块负责。
2. 握手回包不包含业务字段，因此不做解析与校验。
"""

from zsdtdx.parser.base import BaseParser


class SetupResponsePassthroughParser(BaseParser):
    """握手 parser 公共基类：原样返回握手回包体。"""

    def parseResponse(self, body_buf):
        """输入握手回包体，原样返回；不解析业务字段。"""
        return body_buf
