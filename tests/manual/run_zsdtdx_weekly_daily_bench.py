# -*- coding: utf-8 -*-
"""
模块：run_zsdtdx_weekly_daily_bench.py

用途：
1. 使用本工程 zsdtdx（get_stock_kline mode=async）拉取本周一至五京沪深日线。
2. 使用用户指定行情地址配置，落盘供与 TdxQuant 离线对比。

边界：
1. 由主 Agent 在杀掉全部 py 进程后单独启动。
2. 收到 event=done 后立即 destroy_parallel_fetcher()。
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

_MANUAL_DIR = Path(__file__).resolve().parent
_ROOT = _MANUAL_DIR.parents[1]
_CFG_PATH = _MANUAL_DIR / "tdxquant_zsdtdx_weekly_compare_config.yaml"

LIFECYCLE_RUNNING = "running"
LIFECYCLE_DONE_RECEIVED = "done_received"
LIFECYCLE_POOL_DESTROYED = "pool_destroyed"
LIFECYCLE_COMPLETED = "completed"
LIFECYCLE_FAILED = "failed"


def _load_cfg() -> Dict[str, Any]:
    with _CFG_PATH.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh) or {}
    cfg = dict(doc.get("compare") or {})
    if not cfg:
        raise ValueError("缺少 compare 配置段")
    return cfg


def _bootstrap_workspace() -> str:
    src = str((_ROOT / "src").resolve())
    sys.path[:] = [p for p in sys.path if p != src]
    sys.path.insert(0, src)
    import zsdtdx

    return str(zsdtdx.__file__)


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


def _write_lifecycle(path: Path, status: str, extra: Optional[Dict[str, Any]] = None) -> None:
    payload: Dict[str, Any] = {
        "lifecycle_status": status,
        "runner": "zsdtdx",
        "updated_at_unix": time.time(),
    }
    if extra:
        payload.update(extra)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_record(records_path: Path, rec: Dict[str, Any]) -> None:
    with records_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _consume_async_job(
    job: Any,
    *,
    records_path: Path,
    lifecycle_path: Path,
    queue_timeout_seconds: float,
    flush_every: int,
) -> Dict[str, Any]:
    from zsdtdx import destroy_parallel_fetcher

    q = job.queue
    done_payload: Dict[str, Any] = {}
    destroy_summary: Dict[str, Any] = {}
    record_count = 0
    data_event_count = 0

    _write_lifecycle(lifecycle_path, LIFECYCLE_RUNNING, {"phase": "consuming_queue"})

    while True:
        event = q.get(timeout=float(queue_timeout_seconds))
        event_name = str(event.get("event", "")).strip().lower()
        if event_name == "done":
            done_payload = dict(event)
            _write_lifecycle(lifecycle_path, LIFECYCLE_DONE_RECEIVED, {"done": done_payload})
            break
        if event_name != "data":
            continue
        data_event_count += 1
        task = dict(event.get("task") or {})
        rec = {
            "task_key": _task_key(task),
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
                LIFECYCLE_RUNNING,
                {
                    "phase": "consuming_queue",
                    "data_event_count": data_event_count,
                    "record_count": record_count,
                },
            )

    destroy_summary = dict(destroy_parallel_fetcher())
    _write_lifecycle(
        lifecycle_path,
        LIFECYCLE_POOL_DESTROYED,
        {"done": done_payload, "destroy_parallel_fetcher": destroy_summary},
    )
    job.result()
    return {
        "done_payload": done_payload,
        "destroy_summary": destroy_summary,
        "record_count": record_count,
        "data_event_count": data_event_count,
    }


def main() -> int:
    cfg = _load_cfg()
    pkg_file = _bootstrap_workspace()

    from zsdtdx import (
        destroy_parallel_fetcher,
        get_client,
        get_stock_code_name,
        get_stock_kline,
        set_config_path,
    )

    config_path = (_MANUAL_DIR / str(cfg.get("zsdtdx_config_path", "zsdtdx_bench_hosts_config.yaml"))).resolve()
    set_config_path(str(config_path))

    start_time = str(cfg.get("start_time", "")).strip()
    end_time = str(cfg.get("end_time", "")).strip()
    freq = str(cfg.get("freq", "d")).strip()
    queue_timeout_seconds = float(cfg.get("queue_timeout_seconds", 7200) or 7200)
    flush_every = int(cfg.get("lifecycle_flush_every", 200) or 200)

    out_dir = _MANUAL_DIR / str(cfg.get("artifacts_root", "artifacts/tdxquant_vs_zsdtdx_weekly_d")) / "zsdtdx"
    out_dir.mkdir(parents=True, exist_ok=True)
    lifecycle_path = out_dir / "lifecycle.json"
    records_path = out_dir / "task_records.jsonl"
    meta_path = out_dir / "run_meta.json"
    if records_path.is_file():
        records_path.unlink()
    if meta_path.is_file():
        meta_path.unlink()

    with get_client():
        stock_map = get_stock_code_name()
    codes = sorted(stock_map.keys())
    tasks = [
        {"code": code, "freq": freq, "start_time": start_time, "end_time": end_time}
        for code in codes
    ]

    t0 = time.perf_counter()
    _write_lifecycle(
        lifecycle_path,
        LIFECYCLE_RUNNING,
        {"phase": "starting", "task_count": len(tasks), "stock_count": len(codes)},
    )

    try:
        job = get_stock_kline(task=tasks, mode="async")
        run_summary = _consume_async_job(
            job,
            records_path=records_path,
            lifecycle_path=lifecycle_path,
            queue_timeout_seconds=queue_timeout_seconds,
            flush_every=flush_every,
        )
    except Exception as exc:
        _write_lifecycle(lifecycle_path, LIFECYCLE_FAILED, {"error": str(exc)})
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
    ok_tasks = int(done_payload.get("success_tasks", 0) or 0)
    failed_tasks = int(done_payload.get("failed_tasks", 0) or 0)

    meta = {
        "runner": "zsdtdx",
        "zsdtdx_file": pkg_file,
        "config_path": str(config_path),
        "stock_count": len(codes),
        "task_count": len(tasks),
        "freq": freq,
        "start_time": start_time,
        "end_time": end_time,
        "elapsed_seconds": elapsed,
        "done": done_payload,
        "record_count": record_count,
        "ok_tasks": ok_tasks,
        "failed_tasks": failed_tasks,
        "destroy_parallel_fetcher": destroy_summary,
        "lifecycle_status": LIFECYCLE_COMPLETED,
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_lifecycle(lifecycle_path, LIFECYCLE_COMPLETED, meta)
    print(json.dumps(meta, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
