"""
模块：`biz/block_kline.py`。

职责：
1. 把板块名称换成代码路由后，按板块任务交给抓取器。

边界：
1. 任务类定义在包根 `kline_task.py`。
2. 不组 0x0523 包，不解析板块 K 线回包。
3. 不在本模块下载板块文件。
4. 不从 `simple_api` 做模块级导入。
"""

from __future__ import annotations

import queue as std_queue
from typing import Any, Callable, Dict, List, Optional

from zsdtdx.util.helper import (
    _ensure_active_config_ready,
    call_with_client,
    normalize_task_input,
)
from zsdtdx.kline_task import BlockKlineTask
from zsdtdx.engine.unified_client import UnifiedTdxClient


def fetch_block_kline(
    task: List[Any],
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[
        Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ] = None,
    mode: str = "async",
) -> Any:
    """
    输入板块任务，输出 K 线结果。

    输入：task 为 dict 或 BlockKlineTask 列表；其余参数与 get_block_kline 相同。
    输出：sync 为 payload 列表；async 为 StockKlineJob。
    用途：名称解析后按板块任务抓取，分页仍复用指数实现。
    边界条件：task 为空或 mode 非法时抛 ValueError；未知板块名称由客户端抛错。
    """
    if queue is not None and not hasattr(queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")
    if preprocessor_operator is not None and not callable(preprocessor_operator):
        raise ValueError("preprocessor_operator 必须是可调用对象")

    _ensure_active_config_ready(caller_name="get_block_kline")
    mode_key = str(mode or "async").strip().lower()
    if mode_key not in {"sync", "async"}:
        raise ValueError("mode 仅支持 'sync' 或 'async'")
    normalized_tasks = normalize_task_input(task=task, task_cls=BlockKlineTask)

    from zsdtdx.engine.parallel_fetcher import get_fetcher
    from zsdtdx.simple_api import get_client

    routed_tasks = call_with_client(
        lambda client: client.prepare_block_kline_tasks(normalized_tasks),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=get_client,
    )
    fetcher = get_fetcher()
    if mode_key == "sync":
        return fetcher.fetch_block_tasks_sync(
            tasks=routed_tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
        )
    async_queue = queue if queue is not None else std_queue.Queue()
    return fetcher.fetch_block_tasks_async(
        tasks=routed_tasks,
        queue=async_queue,
        preprocessor_operator=preprocessor_operator,
    )
