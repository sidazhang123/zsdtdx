"""
模块：`biz/index_kline.py`。

职责：
1. 缺省任务时展开指数目录，解析名称路由后交给指数抓取器。

边界：
1. 任务类定义在包根 `kline_task.py`。
2. 不解析行情包，不管理连接池。
3. 不从 `simple_api` 做模块级导入。
"""

from __future__ import annotations

import queue as std_queue
from typing import Any, Callable, Dict, List, Optional

from zsdtdx.util.helper import (
    _ensure_active_config_ready,
    call_with_client,
    normalize_task_input,
)
from zsdtdx.kline_task import IndexKlineTask
from zsdtdx.engine.unified_client import UnifiedTdxClient


def fetch_index_kline(
    task: Optional[List[Any]] = None,
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[
        Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ] = None,
    mode: str = "async",
) -> Any:
    """
    输入指数任务，输出 K 线结果。

    输入：task/queue/preprocessor_operator/mode 与 get_index_kline 相同。
    输出：sync 为 payload 列表；async 为 StockKlineJob。
    用途：缺省任务展开目录，解析路由后交给指数抓取器。
    边界条件：
    1. sync 必须显式传入非空 task。
    2. async 传 None 或空列表时按近 7 日日线展开全部指数。
    3. mode 非法时抛 ValueError，且不会先去拉目录。
    """
    if queue is not None and not hasattr(queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")
    if preprocessor_operator is not None and not callable(preprocessor_operator):
        raise ValueError("preprocessor_operator 必须是可调用对象")

    _ensure_active_config_ready(caller_name="get_index_kline")
    mode_key = str(mode or "async").strip().lower()
    if mode_key not in {"sync", "async"}:
        raise ValueError("mode 仅支持 'sync' 或 'async'")

    from zsdtdx.engine.parallel_fetcher import get_fetcher
    from zsdtdx.simple_api import get_client

    raw_tasks: List[Any]
    if task is None or (isinstance(task, (list, tuple)) and len(task) == 0):
        if mode_key == "sync":
            raw_tasks = []
        else:
            raw_tasks = call_with_client(
                lambda client: client.build_default_index_kline_tasks(),
                get_active_context_client=UnifiedTdxClient.get_active_context_client,
                build_client=get_client,
            )
    else:
        raw_tasks = list(task)

    normalized_tasks = normalize_task_input(task=raw_tasks, task_cls=IndexKlineTask)
    routed_tasks = call_with_client(
        lambda client: client.prepare_index_kline_tasks(normalized_tasks),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=get_client,
    )
    fetcher = get_fetcher()
    if mode_key == "sync":
        return fetcher.fetch_index_tasks_sync(
            tasks=routed_tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
        )
    async_queue = queue if queue is not None else std_queue.Queue()
    return fetcher.fetch_index_tasks_async(
        tasks=routed_tasks,
        queue=async_queue,
        preprocessor_operator=preprocessor_operator,
    )
