"""
模块：`biz/stock_kline.py`。

职责：
1. 把股票 K 线任务交给并行抓取器的 sync/async 入口。

边界：
1. 任务类定义在包根 `kline_task.py`。
2. 不解析行情包，不管理连接池。
3. 不依赖 `simple_api`。
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from zsdtdx.biz._kline_dispatch import (
    dispatch_kline_tasks,
    validate_kline_dispatch_args,
)
from zsdtdx.kline_task import StockKlineTask
from zsdtdx.util.helper import _ensure_active_config_ready, normalize_task_input


def fetch_stock_kline(
    task: List[Any],
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[
        Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ] = None,
    mode: str = "async",
    qfq: bool = True,
) -> Any:
    """
    输入股票任务，输出 K 线结果。

    输入：task/queue/preprocessor_operator/mode/qfq 与 get_stock_kline 相同。
    输出：sync 为 payload 列表；async 为 StockKlineJob。
    用途：校验任务后交给股票抓取器。
    边界条件：queue 无法 put、钩子不可调用或 mode 非法时抛 ValueError。
    """
    mode_key = validate_kline_dispatch_args(
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        mode=mode,
    )
    _ensure_active_config_ready(caller_name="get_stock_kline")
    normalized_tasks = normalize_task_input(task=task, task_cls=StockKlineTask)
    return dispatch_kline_tasks(
        mode=mode_key,
        tasks=normalized_tasks,
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        sync_method="fetch_stock_tasks_sync",
        async_method="fetch_stock_tasks_async",
        extra_kwargs={"qfq": bool(qfq)},
    )
