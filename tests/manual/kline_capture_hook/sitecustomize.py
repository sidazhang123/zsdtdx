"""模块：`manual/kline_capture_hook/sitecustomize.py`。

职责：验收子进程启动时安装 K 线回包固定夹具，供旧进程 worker 与新线程共同对拍。
边界：只有内部验收清单存在时启用；请求仍真实发送，只固定解压后的 K 线正文。
"""

import hashlib
import json
import os
from pathlib import Path
import time


def install():
    """输入环境内验收清单，输出无；首个完整回包成为夹具，重放缺页明确失败。"""
    raw = os.environ.get("ZSDTDX_CAPTURE_SPEC")
    if not raw:
        return
    spec = json.loads(raw)
    folder = Path(spec["directory"])
    folder.mkdir(parents=True, exist_ok=True)
    from zsdtdx.parser.base import BaseParser

    original = BaseParser._call_api

    def call(parser):
        """输入 parser，输出原协议解析结果；仅在解析前替换正文，收发逻辑不变。"""
        if parser.__class__.__name__ not in {
            "GetSecurityBarsCmd",
            "GetIndexBarsCmd",
            "GetInstrumentBars",
            "GetBlockBarsCmd",
        }:
            return original(parser)
        parse = parser.parseResponse

        def fixed(body):
            """输入真实解压回包，输出固定夹具解析；键包含完整请求与解析器类型。"""
            key = hashlib.sha256(
                parser.__class__.__name__.encode() + bytes(parser.send_pkg)
            ).hexdigest()
            packet = folder / key
            payload = packet / "body.bin"
            if spec["mode"] == "record":
                try:
                    packet.mkdir()
                except FileExistsError:
                    pass
                else:
                    temporary = packet / "body.tmp"
                    temporary.write_bytes(bytes(body))
                    temporary.replace(payload)
                    (packet / "request.json").write_text(
                        json.dumps(
                            {
                                "parser": parser.__class__.__name__,
                                "request_hex": bytes(parser.send_pkg).hex(),
                                "body_bytes": len(body),
                            },
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
            deadline = time.monotonic() + 5
            while not payload.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            if not payload.exists():
                raise RuntimeError(f"固定回包缺失: {key}")
            return parse(payload.read_bytes())

        parser.parseResponse = fixed
        try:
            return original(parser)
        finally:
            parser.parseResponse = parse

    BaseParser._call_api = call


install()
