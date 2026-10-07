"""
模块：`biz/_kline_dispatch.py`。

职责：
1. 统一股票、指数、板块 K 线业务门面的公共参数校验。
2. 统一 sync/async 抓取器分发与缺省事件队列创建。

边界：
1. 不负责任务归一化、名称路由或行情协议处理。
2. 具体抓取器方法名由调用方传入，便于三个业务门面保持清晰契约。
"""

from __future__ import annotations

import queue as std_queue
from typing import Any, Callable, Dict, Optional


def validate_kline_dispatch_args(
    *,
    queue: Optional[Any],
    preprocessor_operator: Optional[
        Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ],
    mode: str,
) -> str:
    """校验公共输入并返回规范化后的 sync/async mode。"""
    if queue is not None and not hasattr(queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")
    if preprocessor_operator is not None and not callable(preprocessor_operator):
        raise ValueError("preprocessor_operator 必须是可调用对象")
    mode_key = str(mode or "async").strip().lower()
    if mode_key not in {"sync", "async"}:
        raise ValueError("mode 仅支持 'sync' 或 'async'")
    return mode_key


def dispatch_kline_tasks(
    *,
    mode: str,
    tasks: Any,
    queue: Optional[Any],
    preprocessor_operator: Optional[
        Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ],
    sync_method: str,
    async_method: str,
    extra_kwargs: Optional[Dict[str, Any]] = None,
) -> Any:
    """按 mode 调用抓取器方法；async 未提供队列时创建进程内事件队列。"""
    from zsdtdx.engine.parallel_fetcher import get_fetcher

    fetcher = get_fetcher()
    kwargs = {
        "tasks": tasks,
        "queue": queue,
        "preprocessor_operator": preprocessor_operator,
        **dict(extra_kwargs or {}),
    }
    if mode == "sync":
        return getattr(fetcher, sync_method)(**kwargs)
    kwargs["queue"] = queue if queue is not None else std_queue.Queue()
    return getattr(fetcher, async_method)(**kwargs)
