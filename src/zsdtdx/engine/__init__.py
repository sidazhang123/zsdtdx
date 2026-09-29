"""
模块：`engine` 子包。

职责：
1. 统一客户端、并行抓取与自适应并发调度。

边界：
1. 对外 get_* 仍经 simple_api；协议组包仍在 parser/net。
"""

from __future__ import annotations

__all__: list[str] = []
