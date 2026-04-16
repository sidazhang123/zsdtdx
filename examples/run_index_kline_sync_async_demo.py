"""指数 K 线 sync/async 对比示例。

用途：
1. 演示 simple_api 新增的 get_index_kline 接口。
2. 同时跑 sync 和 async 两种模式，输出总耗时与任务级摘要。

运行方式：
1. 在项目根目录执行：`py examples/run_index_kline_sync_async_demo.py`
"""

from __future__ import annotations

import json
import queue
import time
from typing import Any, Dict, List

from zsdtdx import IndexKlineTask, get_index_kline


def build_tasks() -> List[IndexKlineTask]:
    """构造示例任务列表。"""
    index_names = ["中证1000", "中证2000", "中证500", "创业板指", "科创50", "深证成指", "上证指数"]
    freqs = ["15", "30", "60", "d", "w"]
    return [
        IndexKlineTask(
            index_name=index_name,
            freq=freq,
            start_time="2026-03-16",
            end_time="2026-04-16",
        )
        for index_name in index_names
        for freq in freqs
    ]


def summarize_payloads(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """将 payload 列表压缩为任务级摘要。"""
    summary: List[Dict[str, Any]] = []
    for payload in payloads:
        if str(payload.get("event")) != "data":
            continue
        task = payload.get("task", {}) or {}
        summary.append(
            {
                "index_name": task.get("index_name"),
                "freq": task.get("freq"),
                "rows": len(payload.get("rows") or []),
                "error": payload.get("error"),
            }
        )
    return summary


def run_sync(tasks: List[IndexKlineTask]) -> Dict[str, Any]:
    """执行 sync 模式并返回耗时与摘要。"""
    started = time.perf_counter()
    payloads = get_index_kline(task=tasks, mode="sync")
    elapsed = time.perf_counter() - started
    return {
        "mode": "sync",
        "elapsed_seconds": round(elapsed, 3),
        "task_count": len(tasks),
        "summary": summarize_payloads(payloads),
    }


def run_async(tasks: List[IndexKlineTask]) -> Dict[str, Any]:
    """执行 async 模式并返回耗时与摘要。"""
    out_queue: queue.Queue = queue.Queue()
    started = time.perf_counter()
    job = get_index_kline(task=tasks, mode="async", queue=out_queue)
    events: List[Dict[str, Any]] = []
    while True:
        event = out_queue.get(timeout=120)
        events.append(event)
        if str(event.get("event")) == "done":
            break
    job.result()
    elapsed = time.perf_counter() - started
    return {
        "mode": "async",
        "elapsed_seconds": round(elapsed, 3),
        "task_count": len(tasks),
        "done_event": events[-1] if events else {},
        "summary": summarize_payloads(events),
    }


def main() -> None:
    """示例入口。"""
    tasks = build_tasks()
    sync_result = run_sync(tasks)
    async_result = run_async(tasks)
    print(
        json.dumps(
            {
                "sync": sync_result,
                "async": async_result,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
