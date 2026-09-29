"""
模块：`biz/company_info.py`。

职责：
1. 校验公司信息入参。
2. 按 sync/async 把多只股票的公司信息取回。

边界：
1. 单票正文解析在统一客户端。
2. async 并行在 parallel_fetcher。
3. 不从 `simple_api` 做模块级导入。
"""

from __future__ import annotations

import queue as std_queue
from typing import Any, Dict, List, Optional

import pandas as pd

from zsdtdx.util.helper import _ensure_active_config_ready, call_with_client
from zsdtdx.engine.unified_client import UnifiedTdxClient


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
    """
    if isinstance(codes, (str, bytes)):
        raise TypeError('get_company_info 的 codes 须为 list，单票请传 ["600000"]')
    if codes is None:
        raise ValueError("get_company_info 需要提供非空 codes 列表")
    code_list = [str(item).strip() for item in codes if str(item).strip()]
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

    from zsdtdx.simple_api import get_client

    def _sync_many(client: UnifiedTdxClient):
        """
        输入客户端，按代码顺序拉取公司信息。

        输入：client 为统一客户端。
        输出：list[dict] 或 DataFrame。
        用途：sync 模式逐票取正文并可选写入队列。
        边界条件：单票失败记入 error 事件，不中断其余代码。
        """
        all_rows: List[Dict[str, Any]] = []
        success_codes = 0
        failed_codes = 0
        for item in code_list:
            try:
                part = client.get_company_info_content(
                    code=item, category=category, return_df=False
                )
                part_rows = list(part or [])
                all_rows.extend(part_rows)
                success_codes += 1
                if queue is not None:
                    queue.put(
                        {
                            "event": "data",
                            "code": str(item),
                            "rows": part_rows,
                            "error": None,
                        }
                    )
            except Exception as exc:
                failed_codes += 1
                if queue is not None:
                    queue.put(
                        {
                            "event": "data",
                            "code": str(item),
                            "rows": [],
                            "error": str(exc),
                        }
                    )
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

    return call_with_client(
        _sync_many,
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=get_client,
    )
