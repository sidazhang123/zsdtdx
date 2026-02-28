"""
sync/async 对比脚本（固定 taskset，单次输出两种模式结果）。

用途：
1. 先跑 sync，再跑 async，生成同口径对比结果文件。
2. 复用已落盘 taskset，避免每次随机采样导致口径漂移。
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
    sync_profile: Dict[str, Any],
    async_profile: Dict[str, Any],
    taskset_path: Path,
    taskset_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """
    构建 sync/async 对比结果元数据。

    输入：
    1. codes/samples/tasks: 基准输入集合。
    2. sync_profile/async_profile: 两种模式生效配置快照。
    3. taskset_path/taskset_meta: taskset 文件及其元数据。
    输出：
    1. 元数据字典。
    用途：
    1. 固化“输入一致 + 配置一致”的可复现上下文。
    边界条件：
    1. taskset_meta 缺字段时会回退默认值，不影响报告产出。
    """
    return {
        "stock_count": int(len(codes)),
        "freqs": list(bench.FREQS),
        "sample_count": int(len(samples)),
        "task_count": int(len(tasks)),
        "batch_size": int(bench.TASK_BATCH_SIZE),
        "samples": list(samples),
        "date_range": "2018-01-01..2020-12-31",
        "rule": "15 random windows, each 1-3 consecutive business days; exclude months [1,2,5,10]",
        "early_stop_enabled": bool(bench.ENABLE_EARLY_STOP),
        "sync_profile": sync_profile,
        "async_profile": async_profile,
        "taskset_file": str(taskset_path),
        "taskset_hash_sha256": str(taskset_meta.get("task_hash_sha256", "")),
    }


def _load_taskset() -> tuple[list[str], list[tuple[str, str]], list[dict[str, str]], Dict[str, Any], Path]:
    """
    读取固定 taskset 并做结构校验。

    输入：
    1. 无显式输入参数。
    输出：
    1. `(codes, samples, tasks, taskset_meta, taskset_path)`。
    用途：
    1. 保证 sync/async 对比时输入完全一致。
    边界条件：
    1. 文件不存在、结构非法或任务数不一致时抛 RuntimeError。
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


def _build_diagnosis(sync_stat: Dict[str, Any], async_stat: Dict[str, Any]) -> Dict[str, Any]:
    """
    构建基础性能诊断结论。

    输入：
    1. sync_stat/async_stat: 两种模式统计字典。
    输出：
    1. 诊断结论字典（速度比、一致性、提示）。
    用途：
    1. 在脚本输出层直接给出可读结论，便于快速判断是否异常。
    边界条件：
    1. async 耗时为 0 时 speedup_ratio 返回 None。
    """
    sync_elapsed = float(sync_stat.get("elapsed_seconds", 0.0) or 0.0)
    async_elapsed = float(async_stat.get("elapsed_seconds", 0.0) or 0.0)
    speedup_ratio = (sync_elapsed / async_elapsed) if async_elapsed > 0 else None
    quality_consistent = (
        int(sync_stat.get("rows_total", 0) or 0) == int(async_stat.get("rows_total", 0) or 0)
        and int(sync_stat.get("failed_tasks", 0) or 0) == int(async_stat.get("failed_tasks", 0) or 0)
    )
    hints: list[str] = []
    if speedup_ratio is None:
        hints.append("async_elapsed=0，无法计算 speedup_ratio")
    elif speedup_ratio < 1.0:
        hints.append("async 慢于 sync，优先检查预热、chunk 负载与网络瓶颈")
    else:
        hints.append("async 吞吐优于 sync（或持平）")
    if not quality_consistent:
        hints.append("rows_total/failed_tasks 不一致，需排查数据一致性")

    return {
        "sync_elapsed_seconds": sync_elapsed,
        "async_elapsed_seconds": async_elapsed,
        "sync_over_async_ratio": round(speedup_ratio, 4) if speedup_ratio is not None else None,
        "quality_consistent": bool(quality_consistent),
        "hints": hints,
    }


def main() -> None:
    """
    执行固定 taskset 的 sync/async 对比测试并落盘。

    输入：
    1. 无显式输入参数。
    输出：
    1. 控制台日志 + `benchmark_sync_async_200x5x15_result.json`。
    用途：
    1. 作为“第一次 sync/async 对比测试”的标准执行入口。
    边界条件：
    1. 异常退出时 finally 仍会回收并行进程池。
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
    print("get_stock_kline sync/async compare benchmark (200*5*15)")
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

    async_profile = bench._configure_fetcher_for_mode(
        fetcher=fetcher,
        mode="async",
        async_process_workers=async_process_workers,
    )
    async_stat = bench.run_mode_batched(mode="async", tasks=tasks, batch_size=bench.TASK_BATCH_SIZE)

    diagnosis = _build_diagnosis(sync_stat=sync_stat, async_stat=async_stat)
    report: Dict[str, Any] = {
        "meta": _build_meta(
            codes=codes,
            samples=samples,
            tasks=tasks,
            sync_profile=sync_profile,
            async_profile=async_profile,
            taskset_path=taskset_path,
            taskset_meta=taskset_meta,
        ),
        "sync": sync_stat,
        "async": async_stat,
        "diagnosis": diagnosis,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
    }

    out_path = Path(__file__).with_name(
        f"benchmark_sync_async_{int(len(codes))}x{int(len(bench.FREQS))}x{int(len(samples))}_result.json"
    )
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("-" * 96)
    print(json.dumps(diagnosis, ensure_ascii=False, indent=2))
    print(f"saved: {out_path}")


if __name__ == "__main__":
    try:
        main()
    finally:
        bench._cleanup_parallel_pool(run_name="benchmark-sync-async-exit")
