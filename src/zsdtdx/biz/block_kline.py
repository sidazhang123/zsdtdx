"""
模块：`biz/block_kline.py`。

职责：
1. 把板块名称换成代码路由后，按板块任务交给抓取器。

边界：
1. 任务类定义在包根 `kline_task.py`。
2. 不组 0x0523 包，不解析板块 K 线回包。
3. 不在本模块下载板块文件。
4. 不依赖 `simple_api`。
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from zsdtdx.biz._client_context import call_with_main_client
from zsdtdx.biz._kline_dispatch import (
    dispatch_kline_tasks,
    validate_kline_dispatch_args,
)
from zsdtdx.kline_task import BlockKlineTask
from zsdtdx.util.helper import (
    _ensure_active_config_ready,
    normalize_task_input,
)


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
    mode_key = validate_kline_dispatch_args(
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        mode=mode,
    )
    _ensure_active_config_ready(caller_name="get_block_kline")
    normalized_tasks = normalize_task_input(task=task, task_cls=BlockKlineTask)

    routed_tasks = call_with_main_client(
        lambda client: client.prepare_block_kline_tasks(normalized_tasks),
        caller_name="get_block_kline",
    )
    return dispatch_kline_tasks(
        mode=mode_key,
        tasks=routed_tasks,
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        sync_method="fetch_block_tasks_sync",
        async_method="fetch_block_tasks_async",
    )
