"""
模块：`biz/_client_context.py`。

职责：
1. 为业务门面借用主进程统一客户端。
2. 在没有活跃 `with get_client()` 上下文时临时创建并关闭客户端。

边界：
1. 本模块只管理主进程客户端借用，不参与并行 worker 调度。
2. 不依赖 `simple_api`，避免业务层反向导入公开 API 层。
"""

from __future__ import annotations

from typing import Callable, TypeVar

from zsdtdx.engine.unified_client import UnifiedTdxClient
from zsdtdx.util.helper import _ensure_active_config_ready, call_with_client

_T = TypeVar("_T")


def call_with_main_client(
    func: Callable[[UnifiedTdxClient], _T],
    *,
    caller_name: str,
) -> _T:
    """
    输入客户端回调与调用方名称，输出回调结果。

    用途：复用活跃主进程客户端，或在无上下文时安全创建临时客户端。
    边界：临时客户端总在 finally 中关闭；回调异常原样上抛。
    """

    def _build_client() -> UnifiedTdxClient:
        """
        输入无，输出按当前活跃配置创建的统一客户端。

        用途：作为 `call_with_client` 的惰性工厂。
        边界：仅在没有活跃上下文客户端时调用。
        """
        config_path = _ensure_active_config_ready(caller_name=caller_name)
        return UnifiedTdxClient(config_path=config_path)

    return call_with_client(
        func,
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=_build_client,
    )
