"""
模块：`biz/stock_kline.py`。

职责：
1. 把股票 K 线任务交给并行抓取器的 sync/async 入口。

边界：
1. 任务类定义在包根 `kline_task.py`。
2. 不解析行情包，不管理连接池。
3. 不从 `simple_api` 做模块级导入。
"""

from __future__ import annotations

import queue as std_queue
from typing import Any, Callable, Dict, List, Optional

from zsdtdx.util.helper import _ensure_active_config_ready, normalize_task_input
from zsdtdx.kline_task import StockKlineTask


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
    if queue is not None and not hasattr(queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")
    if preprocessor_operator is not None and not callable(preprocessor_operator):
        raise ValueError("preprocessor_operator 必须是可调用对象")

    _ensure_active_config_ready(caller_name="get_stock_kline")
    normalized_tasks = normalize_task_input(task=task, task_cls=StockKlineTask)
    mode_key = str(mode or "async").strip().lower()

    from zsdtdx.engine.parallel_fetcher import get_fetcher

    fetcher = get_fetcher()
    if mode_key == "sync":
        return fetcher.fetch_stock_tasks_sync(
            tasks=normalized_tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
            qfq=bool(qfq),
        )
    if mode_key == "async":
        async_queue = queue if queue is not None else std_queue.Queue()
        return fetcher.fetch_stock_tasks_async(
            tasks=normalized_tasks,
            queue=async_queue,
            preprocessor_operator=preprocessor_operator,
            qfq=bool(qfq),
        )
    raise ValueError("mode 仅支持 'sync' 或 'async'")
