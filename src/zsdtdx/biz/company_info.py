"""
模块：`biz/company_info.py`。

职责：
1. 校验公司信息入参。
2. 按 sync/async 把多只股票的公司信息取回。

边界：
1. 单票正文解析在统一客户端。
2. async 并行在 parallel_fetcher。
3. 不依赖 `simple_api`。
"""

from __future__ import annotations

import queue as std_queue
import time
from concurrent.futures import CancelledError
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

from zsdtdx.biz._client_context import call_with_main_client
from zsdtdx.util.company_info_state import (
    company_info_outcome,
    normalize_company_info_codes,
    publish_company_info_outcome,
    record_company_info_outcome,
)
from zsdtdx.util.helper import _ensure_active_config_ready

if TYPE_CHECKING:
    from zsdtdx.engine.unified_client import UnifiedTdxClient


def _sync_company_info_attempt(client, code, category, timeout):
    """输入主上下文客户端和预算，输出正文；异常清当前连接，始终还原调用前预算。"""
    from zsdtdx.engine.thread_runtime import attempt_deadline, check_deadline

    previous_deadline = attempt_deadline.get()
    deadline = time.monotonic() + timeout
    if previous_deadline is not None:
        deadline = min(deadline, previous_deadline)
    pool = getattr(client, "std_pool", None)
    get_data = getattr(pool, "_get_thread_data", None)
    prior = {}
    if callable(get_data):
        data = get_data()
        prior = {
            key: data.get(key)
            for key in ("socket_read_deadline", "socket_read_timeout_override")
        }
    set_timeout = getattr(pool, "set_thread_socket_read_timeout", None)
    restore_timeout = getattr(pool, "restore_thread_socket_read_timeout", None)
    token = attempt_deadline.set(deadline)
    try:
        check_deadline()
        if callable(set_timeout):
            set_timeout(max(0.001, deadline - time.monotonic()))
        rows = client.get_company_info_content(
            code=code, category=category, return_df=False
        )
        check_deadline()
        return rows
    except Exception:
        reset = getattr(pool, "reset_thread_connection", None)
        if callable(reset):
            reset(reconnect=False)
        raise
    finally:
        attempt_deadline.reset(token)
        if callable(restore_timeout):
            restore_timeout()
        if prior:
            data = get_data()
            data.update(prior)
            apply_timeout = getattr(pool, "_apply_thread_socket_read_timeout", None)
            prior_deadline = prior["socket_read_deadline"]
            if callable(apply_timeout) and (
                prior_deadline is None or prior_deadline > time.monotonic()
            ):
                apply_timeout(data)


def _fetch_company_info_sync_stock(client, code, category):
    """输入单股和主客户端，输出最终状态；复用配置有限重试，不创建工作线程。"""
    config = getattr(client, "config", {}).get("parallel", {})
    retries = max(0, int(config.get("chunk_retry_max_attempts", 2)))
    timeout = max(0.001, float(config.get("chunk_timeout_seconds", 15.0)))
    for attempt in range(retries + 1):
        try:
            rows = _sync_company_info_attempt(client, code, category, timeout)
            return company_info_outcome(code, rows=rows)
        except Exception as exc:
            cancelled = (
                isinstance(exc, CancelledError)
                or getattr(exc, "reason", None) == "cancelled"
                or str(exc) == "fetcher restarted or destroyed"
            )
            retryable = getattr(exc, "retryable", not isinstance(exc, ValueError))
            if cancelled or not retryable or attempt == retries:
                return company_info_outcome(code, error=exc)


def fetch_company_info(
    codes: List[str],
    category: Optional[List[str]] = None,
    mode: str = "async",
    queue: Optional[Any] = None,
    return_df: Optional[bool] = None,
):
    """
    输入股票代码与分类，输出公司信息。

    输入：codes/category/mode/queue/return_df 与 get_company_info 相同。
    输出：sync 为 list[dict] 或 DataFrame；async 为 StockKlineJob。
    用途：多票顺序或并行拉取公司信息。
    边界条件：
    1. codes 必须是非空 list。
    2. mode 只接受 sync/async。
    3. return_df 只影响 sync 的最终返回。
    4. async 总会带队列。正文只出现在 data 事件的 rows 里；job.result() 只返回代码计数，等待、超时和任务异常仍由 result() 表现。
    """
    if isinstance(codes, (str, bytes)):
        raise TypeError('get_company_info 的 codes 须为 list，单票请传 ["600000"]')
    if codes is None:
        raise ValueError("get_company_info 需要提供非空 codes 列表")
    code_list = normalize_company_info_codes(codes)
    if not code_list:
        raise ValueError("get_company_info 需要提供非空 codes 列表")
    if queue is not None and not hasattr(queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")

    mode_norm = str(mode or "async").strip().lower()
    if mode_norm not in {"sync", "async"}:
        raise ValueError(f"不支持的 mode: {mode}")

    if mode_norm == "async":
        _ensure_active_config_ready(caller_name="get_company_info")
        from zsdtdx.engine.parallel_fetcher import fetch_company_info_async

        async_queue = queue if queue is not None else std_queue.Queue()
        return fetch_company_info_async(
            codes=code_list,
            category=category,
            queue=async_queue,
        )

    def _sync_many(client: UnifiedTdxClient):
        """
        输入客户端，按代码顺序拉取公司信息。

        输入：client 为统一客户端。
        输出：list[dict] 或 DataFrame。
        用途：sync 模式逐票取正文并可选写入队列。
        边界条件：逐股有限重试，空结果不重试；每个去重代码恰有一个最终事件。
        """
        all_rows: List[Dict[str, Any]] = []
        success_codes = 0
        failed_codes = 0
        for item in code_list:
            outcome = _fetch_company_info_sync_stock(client, item, category)
            all_rows.extend(outcome["rows"])
            if outcome["status"] == "success":
                success_codes += 1
            else:
                failed_codes += 1
            record_company_info_outcome(client, outcome)
            publish_company_info_outcome(outcome, queue)
        if queue is not None:
            queue.put(
                {
                    "event": "done",
                    "total_codes": len(code_list),
                    "success_codes": int(success_codes),
                    "failed_codes": int(failed_codes),
                }
            )
        if client._default_return_df(return_df):  # noqa: SLF001
            return pd.DataFrame(all_rows, columns=["code", "category", "content"])
        return all_rows

    return call_with_main_client(
        _sync_many,
        caller_name="get_company_info",
    )
