# -*- coding: utf-8 -*-
"""
模块：run_stock_kline_async_full_compare.py

用途：
1. 在单一 Python 环境（workspace 或 site_packages）下执行
   `get_stock_kline(task, mode=async)` 全量基准测试。
2. 采集每个 task 的解析 rows 与错误信息，并写入 artifacts。
3. 以队列 `event=done` 作为任务完成信号，收到后立即 `destroy_parallel_fetcher()` 释放进程池。

边界：
1. 必须通过环境变量 ZSDTDX_BENCH_TARGET 指定运行目标（禁止一次命令跑双环境）。
2. 主 Agent 编排时不得用「主进程是否退出」判断完成，应读取 lifecycle 状态文件。
3. 禁止被其它脚本批量串联双环境。
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

_MANUAL_DIR = Path(__file__).resolve().parent
_ROOT = _MANUAL_DIR.parents[1]
_CFG_PATH = _MANUAL_DIR / "stock_kline_async_full_compare_config.yaml"

# lifecycle 状态（供主 Agent 轮询，勿用主进程 PID 判断完成）
LIFECYCLE_RUNNING = "running"
LIFECYCLE_DONE_RECEIVED = "done_received"
LIFECYCLE_POOL_DESTROYED = "pool_destroyed"
LIFECYCLE_COMPLETED = "completed"
LIFECYCLE_FAILED = "failed"


def _load_bench_cfg() -> Dict[str, Any]:
    """读取 bench 配置块。"""
    with _CFG_PATH.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh) or {}
    bench = dict(doc.get("bench") or {})
    if not bench:
        raise ValueError("stock_kline_async_full_compare_config.yaml 缺少 bench 段")
    return bench


def _resolve_target() -> str:
    """解析 ZSDTDX_BENCH_TARGET。"""
    target = str(os.environ.get("ZSDTDX_BENCH_TARGET", "")).strip().lower()
    if target not in {"workspace", "site_packages"}:
        raise ValueError("请设置环境变量 ZSDTDX_BENCH_TARGET=workspace 或 site_packages")
    return target


def _bootstrap_import(target: str) -> str:
    """
    按目标配置 sys.path，返回 zsdtdx.__file__ 路径。

    输入：
    1. target: workspace 或 site_packages。
    输出：
    1. 实际加载的 zsdtdx 包路径字符串。
    """
    src = str((_ROOT / "src").resolve())
    sys.path[:] = [p for p in sys.path if p != src]
    if target == "workspace":
        sys.path.insert(0, src)
    import zsdtdx  # noqa: WPS433

    return str(zsdtdx.__file__)


def _build_tasks(codes: List[str], freqs: List[str], start_time: str, end_time: str) -> List[Dict[str, str]]:
    """生成全量 task 列表（所有 code × 所有 freq 一次放入 task 参数）。"""
    tasks: List[Dict[str, str]] = []
    for code in codes:
        for freq in freqs:
            tasks.append(
                {
                    "code": str(code).strip(),
                    "freq": str(freq).strip(),
                    "start_time": start_time,
                    "end_time": end_time,
                }
            )
    return tasks


def _task_key(task: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(task.get("code", "")).strip(),
            str(task.get("freq", "")).strip(),
            str(task.get("start_time", "")).strip(),
            str(task.get("end_time", "")).strip(),
        ]
    )


def _serialize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """将 rows 规范为可 JSON 序列化的数值记录。"""
    out: List[Dict[str, Any]] = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "datetime": str(row.get("datetime", "")),
                "open": float(row.get("open", 0.0)),
                "high": float(row.get("high", 0.0)),
                "low": float(row.get("low", 0.0)),
                "close": float(row.get("close", 0.0)),
                "volume": int(row.get("volume", 0)),
                "amount": int(row.get("amount", 0)),
            }
        )
    return out


def _write_lifecycle(
    path: Path,
    *,
    status: str,
    target: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    写入 lifecycle 状态，供主 Agent 判断测试阶段（勿依赖主进程退出）。

    输入：
    1. path: lifecycle.json 路径。
    2. status: 当前阶段标识。
    3. target: workspace / site_packages。
    4. extra: 附加字段。
    输出：无。
    """
    payload: Dict[str, Any] = {
        "lifecycle_status": status,
        "target": target,
        "updated_at_unix": time.time(),
    }
    if extra:
        payload.update(extra)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_record(records_path: Path, rec: Dict[str, Any]) -> None:
    """追加单条 task 记录到 jsonl。"""
    with records_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _consume_async_job(
    job: Any,
    *,
    records_path: Path,
    lifecycle_path: Path,
    target: str,
    queue_timeout_seconds: float,
    flush_every: int,
) -> Dict[str, Any]:
    """
    消费 async job 队列直到 done，并在 done 后立即销毁进程池。

    输入：
    1. job: StockKlineJob。
    2. records_path/lifecycle_path/target: 输出与状态路径。
    3. queue_timeout_seconds: 队列阻塞超时（秒）。
    4. flush_every: 进度写入 lifecycle 的间隔（data 事件数）。
    输出：
    1. 汇总字典：done_payload、destroy_summary、record_count、data_event_count。
    边界：
    1. 必须在收到 event=done 后立刻 destroy_parallel_fetcher()，再 job.result()。
    2. 未完成 done+destroy 前不得写入 lifecycle_status=completed。
    """
    from zsdtdx import destroy_parallel_fetcher  # noqa: WPS433

    q = job.queue
    done_payload: Dict[str, Any] = {}
    destroy_summary: Dict[str, Any] = {}
    record_count = 0
    data_event_count = 0

    _write_lifecycle(
        lifecycle_path,
        status=LIFECYCLE_RUNNING,
        target=target,
        extra={"phase": "consuming_queue"},
    )

    while True:
        try:
            event = q.get(timeout=float(queue_timeout_seconds))
        except Exception as exc:
            _write_lifecycle(
                lifecycle_path,
                status=LIFECYCLE_FAILED,
                target=target,
                extra={"error": f"queue.get 超时或异常: {exc}"},
            )
            raise

        event_name = str(event.get("event", "")).strip().lower()
        if event_name == "done":
            done_payload = dict(event)
            _write_lifecycle(
                lifecycle_path,
                status=LIFECYCLE_DONE_RECEIVED,
                target=target,
                extra={"done": done_payload},
            )
            break

        if event_name != "data":
            continue

        data_event_count += 1
        task = dict(event.get("task") or {})
        key = _task_key(task)
        rec = {
            "task_key": key,
            "task": task,
            "rows": _serialize_rows(list(event.get("rows") or [])),
            "error": event.get("error"),
            "worker_pid": event.get("worker_pid"),
        }
        _append_record(records_path, rec)
        record_count += 1

        if flush_every > 0 and data_event_count % flush_every == 0:
            _write_lifecycle(
                lifecycle_path,
                status=LIFECYCLE_RUNNING,
                target=target,
                extra={
                    "phase": "consuming_queue",
                    "data_event_count": data_event_count,
                    "record_count": record_count,
                },
            )

    # 关键：done 后立即销毁进程池（两版 zsdtdx 均需调用方显式 destroy）
    destroy_summary = dict(destroy_parallel_fetcher())
    _write_lifecycle(
        lifecycle_path,
        status=LIFECYCLE_POOL_DESTROYED,
        target=target,
        extra={"done": done_payload, "destroy_parallel_fetcher": destroy_summary},
    )

    # destroy 后再等待后台线程结束并传播异常
    job.result()

    return {
        "done_payload": done_payload,
        "destroy_summary": destroy_summary,
        "record_count": record_count,
        "data_event_count": data_event_count,
    }


def main() -> int:
    """
    执行单环境全量 async K 线基准。

    输入：环境变量 ZSDTDX_BENCH_TARGET。
    输出：写入 artifacts；lifecycle_status=completed 表示可进入下一阶段。
    """
    bench = _load_bench_cfg()
    target = _resolve_target()
    pkg_file = _bootstrap_import(target)

    from zsdtdx import (  # noqa: WPS433
        destroy_parallel_fetcher,
        get_stock_code_name,
        get_stock_kline,
        set_config_path,
    )

    cfg_rel = str(bench.get("config_path", "")).strip()
    config_path = (_MANUAL_DIR / cfg_rel).resolve()
    set_config_path(str(config_path))

    freqs = [str(x).strip() for x in list(bench.get("freqs") or [])]
    start_time = str(bench.get("start_time", "")).strip()
    end_time = str(bench.get("end_time", "")).strip()
    queue_timeout_seconds = float(bench.get("queue_timeout_seconds", 7200) or 7200)
    flush_every = int(bench.get("lifecycle_flush_every", 200) or 200)
    if not freqs or not start_time or not end_time:
        raise ValueError("freqs/start_time/end_time 不能为空")

    stock_map = get_stock_code_name()
    codes = sorted(stock_map.keys())
    stock_count = len(codes)
    tasks = _build_tasks(codes=codes, freqs=freqs, start_time=start_time, end_time=end_time)

    artifacts_root = _MANUAL_DIR / str(bench.get("artifacts_root", "artifacts/stock_kline_async_full_compare"))
    out_dir = artifacts_root / target
    out_dir.mkdir(parents=True, exist_ok=True)

    meta_path = out_dir / "run_meta.json"
    lifecycle_path = out_dir / "lifecycle.json"
    records_path = out_dir / "task_records.jsonl"

    # 新一轮测试清空旧产物，避免主 Agent 误读上次 completed 状态
    if records_path.is_file():
        records_path.unlink()
    if meta_path.is_file():
        meta_path.unlink()

    t0 = time.perf_counter()
    run_summary: Dict[str, Any] = {}

    _write_lifecycle(
        lifecycle_path,
        status=LIFECYCLE_RUNNING,
        target=target,
        extra={"phase": "starting", "task_count": len(tasks)},
    )

    try:
        job = get_stock_kline(task=tasks, mode=str(bench.get("mode", "async")))
        run_summary = _consume_async_job(
            job,
            records_path=records_path,
            lifecycle_path=lifecycle_path,
            target=target,
            queue_timeout_seconds=queue_timeout_seconds,
            flush_every=flush_every,
        )
    except Exception as exc:
        _write_lifecycle(
            lifecycle_path,
            status=LIFECYCLE_FAILED,
            target=target,
            extra={"error": str(exc)},
        )
        try:
            destroy_parallel_fetcher()
        except Exception:
            pass
        raise
    finally:
        elapsed = time.perf_counter() - t0

    done_payload = dict(run_summary.get("done_payload") or {})
    destroy_summary = dict(run_summary.get("destroy_summary") or {})
    record_count = int(run_summary.get("record_count", 0) or 0)

    meta = {
        "target": target,
        "zsdtdx_file": pkg_file,
        "config_path": str(config_path),
        "stock_count": stock_count,
        "expected_stock_count": int(bench.get("expected_stock_count", 0) or 0),
        "task_count": len(tasks),
        "freqs": freqs,
        "start_time": start_time,
        "end_time": end_time,
        "elapsed_seconds": elapsed,
        "done": done_payload,
        "record_count": record_count,
        "destroy_parallel_fetcher": destroy_summary,
        "lifecycle_status": LIFECYCLE_COMPLETED,
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_lifecycle(
        lifecycle_path,
        status=LIFECYCLE_COMPLETED,
        target=target,
        extra={
            "done": done_payload,
            "destroy_parallel_fetcher": destroy_summary,
            "record_count": record_count,
            "elapsed_seconds": elapsed,
            "run_meta_path": str(meta_path),
        },
    )
    print(json.dumps(meta, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
