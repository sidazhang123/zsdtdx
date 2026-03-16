"""
对 simple_api.get_stock_kline 的 async mode 执行一轮真实 1000*5*15 测试，
并将队列返回结果逐条落盘。

输出文件（默认写到 examples 目录）：
1. async_1000x5x15_queue_events.jsonl  - 队列事件逐行 JSON（data + done）
2. async_1000x5x15_queue_summary.json  - 本次运行摘要（耗时、成功失败计数、文件路径）

运行：
    python examples/run_async_1000x5x15_dump_queue_results.py
"""

from __future__ import annotations

import datetime as dt
import json
import queue as py_queue
import random
import traceback
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from zsdtdx import (
    destroy_parallel_fetcher,
    get_client,
    get_stock_code_name,
    get_stock_kline,
    prewarm_parallel_fetcher,
    set_config_path,
)


# ---------------------------
# 可调参数
# ---------------------------
TARGET_STOCK_COUNT = 1000
FREQS: List[str] = ["15", "30", "60", "d", "w"]
SAMPLE_COUNT = 15
SAMPLE_SEED = 20260316
DATE_RANGE_START = "2024-06-01"
DATE_RANGE_END = "2026-01-31"
EXCLUDED_SAMPLE_MONTHS = {1, 2, 5, 10}

QUEUE_GET_TIMEOUT_SECONDS = 120.0
PREWARM_TIMEOUT_SECONDS = 60.0
PREWARM_MAX_ROUNDS = 3

OUT_EVENTS_FILENAME = "async_1000x5x15_queue_events.jsonl"
OUT_SUMMARY_FILENAME = "async_1000x5x15_queue_summary.json"


def _resolve_config_path() -> Path:
    """
    解析测试使用的 config.yaml 路径并校验存在性。

    优先级：
    1. examples 同级目录下的 config.yaml
    2. 项目 src/zsdtdx/config.yaml
    """
    base_dir = Path(__file__).resolve().parent
    candidates = [
        base_dir / "config.yaml",
        base_dir.parent / "src" / "zsdtdx" / "config.yaml",
    ]
    for item in candidates:
        if item.exists():
            return item.resolve()
    raise RuntimeError("未找到可用的 config.yaml（已检查 examples/config.yaml 与 src/zsdtdx/config.yaml）")


def _to_date(date_text: str):
    return dt.datetime.strptime(str(date_text), "%Y-%m-%d").date()


def _business_days(start_date: str, end_date: str) -> List[dt.date]:
    start = _to_date(start_date)
    end = _to_date(end_date)
    if end < start:
        raise ValueError(f"date range invalid: {start_date}..{end_date}")

    days: List[dt.date] = []
    cursor = start
    one_day = dt.timedelta(days=1)
    while cursor <= end:
        if cursor.weekday() < 5:  # Mon..Fri
            days.append(cursor)
        cursor += one_day
    return days


def _build_samples(sample_count: int, seed: int) -> List[Tuple[str, str]]:
    all_biz_days = _business_days(DATE_RANGE_START, DATE_RANGE_END)
    candidates = [d for d in all_biz_days if d.month not in EXCLUDED_SAMPLE_MONTHS]
    if len(candidates) < sample_count:
        raise RuntimeError(
            f"not enough candidate business days: need={sample_count}, actual={len(candidates)}"
        )

    rng = random.Random(int(seed))
    samples: List[Tuple[str, str]] = []
    used = set()

    # 1~3 个连续交易日窗口
    while len(samples) < sample_count:
        start_day = rng.choice(candidates)
        span_days = int(rng.choice([1, 2, 3]))

        # 从 all_biz_days 中找连续窗口
        try:
            start_idx = all_biz_days.index(start_day)
        except ValueError:
            continue
        end_idx = min(start_idx + span_days - 1, len(all_biz_days) - 1)
        end_day = all_biz_days[end_idx]

        key = (start_day.isoformat(), end_day.isoformat())
        if key in used:
            continue
        used.add(key)
        samples.append(key)

    samples.sort(key=lambda x: (x[0], x[1]))
    return samples


def _collect_a_share_codes(limit: int) -> List[str]:
    with get_client():
        stock_map = get_stock_code_name(use_cache=True)

    # 常见 A 股前缀过滤：sh/sz；去掉市场前缀，仅保留 6 位数字代码
    codes: List[str] = []
    for full_code in sorted(stock_map.keys()):
        text = str(full_code or "").strip().lower()
        if text == "" or "." not in text:
            continue
        market, raw = text.split(".", 1)
        if market not in {"sh", "sz"}:
            continue
        raw_code = raw.strip()
        if len(raw_code) != 6 or not raw_code.isdigit():
            continue
        # 常见股票代码段（覆盖主板/创业板/科创板）
        if not (
            raw_code.startswith("6")
            or raw_code.startswith("0")
            or raw_code.startswith("3")
        ):
            continue
        codes.append(raw_code)
        if len(codes) >= int(limit):
            break

    if len(codes) < int(limit):
        raise RuntimeError(f"A-share codes not enough: need={limit}, actual={len(codes)}")
    return codes


def _build_tasks(codes: Sequence[str], freqs: Sequence[str], samples: Sequence[Tuple[str, str]]) -> List[Dict[str, str]]:
    tasks: List[Dict[str, str]] = []
    for code in list(codes):
        for freq in list(freqs):
            for start_time, end_time in list(samples):
                tasks.append(
                    {
                        "code": str(code),
                        "freq": str(freq),
                        "start_time": str(start_time),
                        "end_time": str(end_time),
                    }
                )
    return tasks


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    out_events_path = base_dir / OUT_EVENTS_FILENAME
    out_summary_path = base_dir / OUT_SUMMARY_FILENAME
    config_path = _resolve_config_path()
    set_config_path(str(config_path))

    run_started_at = dt.datetime.now()

    codes = _collect_a_share_codes(TARGET_STOCK_COUNT)
    samples = _build_samples(sample_count=SAMPLE_COUNT, seed=SAMPLE_SEED)
    tasks = _build_tasks(codes=codes, freqs=FREQS, samples=samples)

    expected_task_count = int(TARGET_STOCK_COUNT * len(FREQS) * SAMPLE_COUNT)
    if len(tasks) != expected_task_count:
        raise RuntimeError(
            f"task_count mismatch: expected={expected_task_count}, actual={len(tasks)}"
        )

    print("=" * 96)
    print("get_stock_kline async real-run 1000*5*15 (queue dump)")
    print("=" * 96)
    print(f"stock_count : {len(codes)}")
    print(f"freqs       : {FREQS}")
    print(f"sample_count: {len(samples)}")
    print(f"task_count  : {len(tasks)}")
    print(f"events_file : {out_events_path}")
    print(f"summary_file: {out_summary_path}")
    print(f"config_file : {config_path}")

    runtime_error = None
    queue_events = 0
    queue_data_events = 0
    queue_done_events = 0
    rows_total = 0
    failed_tasks = 0
    success_tasks = 0
    done_event: Dict[str, Any] = {}

    q: py_queue.Queue = py_queue.Queue()

    try:
        # 可选预热：减少首次调用冷启动抖动
        prewarm_info = prewarm_parallel_fetcher()
        print(
            "prewarm done:",
            json.dumps(
                {
                    "target_workers": prewarm_info.get("target_workers"),
                    "ready_workers": prewarm_info.get("ready_workers"),
                    "elapsed_seconds": prewarm_info.get("elapsed_seconds"),
                },
                ensure_ascii=False,
            ),
        )

        job = get_stock_kline(task=tasks, queue=q, mode="async")

        with out_events_path.open("w", encoding="utf-8") as fw:
            while True:
                item = q.get(timeout=float(QUEUE_GET_TIMEOUT_SECONDS))
                queue_events += 1

                # 逐条落盘，避免将 7.5 万事件全部堆在内存
                fw.write(json.dumps(item, ensure_ascii=False) + "\n")

                event_type = str((item or {}).get("event", "")).strip().lower()
                if event_type == "data":
                    queue_data_events += 1
                    error_text = str((item or {}).get("error") or "").strip()
                    rows = list((item or {}).get("rows") or [])
                    rows_total += int(len(rows))
                    if error_text:
                        failed_tasks += 1
                    else:
                        success_tasks += 1
                elif event_type == "done":
                    queue_done_events += 1
                    done_event = dict(item or {})
                    break

            # 等待后台任务彻底完成（并传播异常）
            result_payloads = job.result()
            print(f"job.result() done, payload_count={len(result_payloads)}")

    except Exception as exc:
        runtime_error = f"{type(exc).__name__}: {exc}"
        print("\n[ERROR] run failed:")
        print(runtime_error)
        print(traceback.format_exc())
    finally:
        destroy_info = destroy_parallel_fetcher()
        print(
            "destroy pool:",
            json.dumps(
                {
                    "had_pool": destroy_info.get("had_pool"),
                    "elapsed_seconds": destroy_info.get("elapsed_seconds"),
                },
                ensure_ascii=False,
            ),
        )

    run_finished_at = dt.datetime.now()
    elapsed_seconds = (run_finished_at - run_started_at).total_seconds()

    summary: Dict[str, Any] = {
        "run_started_at": run_started_at.isoformat(timespec="seconds"),
        "run_finished_at": run_finished_at.isoformat(timespec="seconds"),
        "elapsed_seconds": float(round(elapsed_seconds, 3)),
        "task_profile": {
            "stock_count": int(TARGET_STOCK_COUNT),
            "freqs": list(FREQS),
            "sample_count": int(SAMPLE_COUNT),
            "sample_seed": int(SAMPLE_SEED),
            "excluded_sample_months": sorted(list(EXCLUDED_SAMPLE_MONTHS)),
            "task_count": int(len(tasks)),
            "expected_task_count": int(expected_task_count),
        },
        "queue_stats": {
            "queue_events": int(queue_events),
            "queue_data_events": int(queue_data_events),
            "queue_done_events": int(queue_done_events),
            "success_tasks": int(success_tasks),
            "failed_tasks": int(failed_tasks),
            "rows_total": int(rows_total),
        },
        "done_event": done_event,
        "output_files": {
            "events_jsonl": str(out_events_path),
            "summary_json": str(out_summary_path),
        },
        "runtime_error": runtime_error,
    }

    out_summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("-" * 96)
    print(json.dumps(summary["queue_stats"], ensure_ascii=False, indent=2))
    print(f"saved events : {out_events_path}")
    print(f"saved summary: {out_summary_path}")


if __name__ == "__main__":
    main()
