"""
sync-only 基准脚本：200 code * 5 freq * 15 samples。

用途：
1. 仅运行 sync 档位，避免在 async 优化阶段重复跑 sync。
2. 输出与完整对比脚本一致口径的统计字段，便于后续手工对比。
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict

import sync_async_benchmark_1000x5x15 as bench

TASKSET_FILENAME = "benchmark_taskset_200x5x15.json"


def _build_meta(
    *,
    codes: list[str],
    samples: list[tuple[str, str]],
    tasks: list[dict[str, str]],
    profile: Dict[str, Any],
    taskset_path: Path,
    taskset_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """
    构建 sync-only 结果元数据。

    输入：
    1. codes: 股票代码列表。
    2. samples: 时间样本列表。
    3. tasks: 全量任务列表。
    4. profile: 本次 sync 生效配置快照。
    输出：
    1. 元数据字典。
    用途：
    1. 保持与完整脚本的元数据结构尽量一致。
    边界条件：
    1. 仅包含 sync 档位，不包含 async 档位。
    """
    return {
        "stock_count": int(len(codes)),
        "freqs": list(bench.FREQS),
        "sample_count": int(len(samples)),
        "task_count": int(len(tasks)),
        "batch_size": int(bench.TASK_BATCH_SIZE),
        "samples": list(samples),
        "date_range": "2018-01-01..2020-12-31",
        "rule": "15 non-contiguous samples, each 1 day or max 2 consecutive weekdays; exclude months [1,2,10]",
        "early_stop_enabled": bool(bench.ENABLE_EARLY_STOP),
        "benchmark_profile": profile,
        "taskset_file": str(taskset_path),
        "taskset_hash_sha256": str(taskset_meta.get("task_hash_sha256", "")),
    }


def _load_taskset() -> tuple[list[str], list[tuple[str, str]], list[dict[str, str]], Dict[str, Any], Path]:
    """
    读取固定任务集文件并做基础校验。

    输入：
    1. 无显式输入参数。
    输出：
    1. `(codes, samples, tasks, taskset_meta, taskset_path)`。
    用途：
    1. 保证 sync-only 与 async-only 使用完全一致的任务输入。
    边界条件：
    1. 文件不存在或结构不合法时抛 RuntimeError。
    """
    taskset_path = Path(__file__).with_name(TASKSET_FILENAME)
    if not taskset_path.exists():
        raise RuntimeError(
            f"任务集文件不存在: {taskset_path}；请先运行 "
            "python examples/generate_benchmark_taskset_200x5x15.py"
        )

    raw = json.loads(taskset_path.read_text(encoding="utf-8"))
    meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
    taskset_meta = dict(meta)
    taskset_meta.setdefault("task_hash_sha256", "")
    tasks_raw = list(raw.get("tasks") or [])
    codes_raw = list(raw.get("codes") or [])
    samples_raw = list(raw.get("samples") or [])

    if not tasks_raw:
        raise RuntimeError(f"任务集为空: {taskset_path}")

    tasks: list[dict[str, str]] = []
    for item in tasks_raw:
        if not isinstance(item, dict):
            raise RuntimeError("任务集结构非法：tasks 元素必须为 dict")
        tasks.append(
            {
                "code": str(item.get("code", "")).strip(),
                "freq": str(item.get("freq", "")).strip(),
                "start_time": str(item.get("start_time", "")).strip(),
                "end_time": str(item.get("end_time", "")).strip(),
            }
        )

    for item in tasks:
        if not item["code"] or not item["freq"] or not item["start_time"] or not item["end_time"]:
            raise RuntimeError("任务集结构非法：task 存在空字段")

    codes = [str(item).strip() for item in codes_raw if str(item).strip()]
    samples: list[tuple[str, str]] = []
    for item in samples_raw:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            samples.append((str(item[0]), str(item[1])))

    expected_count = int(taskset_meta.get("task_count", len(tasks)) or len(tasks))
    if expected_count != len(tasks):
        raise RuntimeError(
            f"任务集数量不一致: meta.task_count={expected_count}, actual={len(tasks)}"
        )
    return codes, samples, tasks, taskset_meta, taskset_path


def main() -> None:
    """
    运行 sync-only 基准并写出结果文件。

    输入：
    1. 无显式输入参数。
    输出：
    1. 控制台进度日志 + `benchmark_sync_200x5x15_result.json`。
    用途：
    1. 作为 async 优化阶段的固定 sync 对照基线。
    边界条件：
    1. 异常退出时由 finally 统一回收并行资源，避免残留 python 进程。
    """
    codes, samples, tasks, taskset_meta, taskset_path = _load_taskset()

    fetcher = bench.get_fetcher()
    if hasattr(fetcher, "parallel_total_timeout_seconds"):
        fetcher.parallel_total_timeout_seconds = 3600.0
    if hasattr(fetcher, "parallel_result_timeout_seconds"):
        fetcher.parallel_result_timeout_seconds = 3600.0

    default_process_workers = int(max(1, int(getattr(fetcher, "num_processes", 1) or 1)))
    async_process_workers = int(max(bench.ASYNC_MIN_PROCESS_WORKERS, default_process_workers))

    print("=" * 96)
    print("get_stock_kline sync-only benchmark (200*5*15)")
    print("=" * 96)
    print(f"stock_count  : {len(codes)}")
    print(f"freqs        : {bench.FREQS}")
    print(f"sample_count : {len(samples)}")
    print(f"task_count   : {len(tasks)}")
    print(f"batch_size   : {bench.TASK_BATCH_SIZE}")
    print(f"samples      : {samples}")
    print(f"taskset_file : {taskset_path}")
    print(f"taskset_hash : {taskset_meta.get('task_hash_sha256', '')}")
    print(f"default_workers(auto) : {default_process_workers}")

    _ = bench.get_supported_markets(return_df=False)
    sync_profile = bench._configure_fetcher_for_mode(
        fetcher=fetcher,
        mode="sync",
        async_process_workers=async_process_workers,
    )
    sync_stat = bench.run_mode_batched(mode="sync", tasks=tasks, batch_size=bench.TASK_BATCH_SIZE)

    report: Dict[str, Any] = {
        "meta": _build_meta(
            codes=codes,
            samples=samples,
            tasks=tasks,
            profile=sync_profile,
            taskset_path=taskset_path,
            taskset_meta=taskset_meta,
        ),
        "sync": sync_stat,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
    }

    out_path = Path(__file__).with_name(
        f"benchmark_sync_{int(len(codes))}x{int(len(bench.FREQS))}x{int(len(samples))}_result.json"
    )
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("-" * 96)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"saved: {out_path}")


if __name__ == "__main__":
    try:
        main()
    finally:
        bench._cleanup_parallel_pool(run_name="benchmark-sync-exit")
