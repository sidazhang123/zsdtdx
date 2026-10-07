"""
模块：`biz/index_kline.py`。

职责：
1. 缺省任务时展开指数目录，解析名称路由后交给指数抓取器。

边界：
1. 任务类定义在包根 `kline_task.py`。
2. 不解析行情包，不管理连接池。
3. 不依赖 `simple_api`。
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from zsdtdx.biz._client_context import call_with_main_client
from zsdtdx.biz._kline_dispatch import (
    dispatch_kline_tasks,
    validate_kline_dispatch_args,
)
from zsdtdx.kline_task import IndexKlineTask
from zsdtdx.util.helper import (
    _ensure_active_config_ready,
    normalize_task_input,
)


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
    mode_key = validate_kline_dispatch_args(
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        mode=mode,
    )
    _ensure_active_config_ready(caller_name="get_index_kline")

    raw_tasks: List[Any]
    if task is None or (isinstance(task, (list, tuple)) and len(task) == 0):
        if mode_key == "sync":
            raw_tasks = []
        else:
            raw_tasks = call_with_main_client(
                lambda client: client.build_default_index_kline_tasks(),
                caller_name="get_index_kline",
            )
    else:
        raw_tasks = list(task)

    normalized_tasks = normalize_task_input(task=raw_tasks, task_cls=IndexKlineTask)
    routed_tasks = call_with_main_client(
        lambda client: client.prepare_index_kline_tasks(normalized_tasks),
        caller_name="get_index_kline",
    )
    return dispatch_kline_tasks(
        mode=mode_key,
        tasks=routed_tasks,
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        sync_method="fetch_index_tasks_sync",
        async_method="fetch_index_tasks_async",
    )
