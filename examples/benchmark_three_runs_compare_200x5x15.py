"""
三轮基准对比脚本（严格影响边界）：
1. 轮次1：sync，完成后清理进程池；
2. 轮次2：先 prewarm，再 async，完成后清理进程池；
3. 轮次3：先 prewarm，再 async（禁用进程内 chunk future 并发，顺序跑 chunk），完成后清理进程池。

用途：
1. 在同一 taskset 下比较三种运行策略的耗时差异；
2. 输出总体与分批耗时对比，并给出瓶颈分析提示。
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sync_async_benchmark_1000x5x15 as bench

TASKSET_FILENAME = "benchmark_taskset_200x5x15.json"
RESULT_FILENAME = "benchmark_three_runs_compare_200x5x15_result.json"


def _safe_float(value: Any, default: float = 0.0) -> float:
    """
    安全转 float。

    输入：
    1. value: 任意值。
    2. default: 失败默认值。
    输出：
    1. float 值。
    用途：
    1. 防御报告字段类型抖动导致的计算异常。
    边界条件：
    1. 解析失败返回 default。
    """
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    """
    安全转 int。

    输入：
    1. value: 任意值。
    2. default: 失败默认值。
    输出：
    1. int 值。
    用途：
    1. 统一处理计数字段，避免类型异常。
    边界条件：
    1. 解析失败返回 default。
    """
    try:
        return int(value)
    except Exception:
        return int(default)


def _series_quantiles(values: List[float]) -> Dict[str, Any]:
    """
    计算分位数统计。

    输入：
    1. values: 浮点序列。
    输出：
    1. min/p50/p90/p95/max/avg 统计字典。
    用途：
    1. 观察批次耗时抖动与尾部慢批次。
    边界条件：
    1. 空序列时返回 None 统计。
    """
    cleaned = sorted([float(item) for item in values])
    if not cleaned:
        return {"min": None, "p50": None, "p90": None, "p95": None, "max": None, "avg": None}

    def _pick(percent: float) -> float:
        if len(cleaned) == 1:
            return cleaned[0]
        pos = (len(cleaned) - 1) * percent
        left = int(pos)
        right = min(left + 1, len(cleaned) - 1)
        frac = pos - left
        return cleaned[left] * (1.0 - frac) + cleaned[right] * frac

    return {
        "min": round(cleaned[0], 4),
        "p50": round(_pick(0.50), 4),
        "p90": round(_pick(0.90), 4),
        "p95": round(_pick(0.95), 4),
        "max": round(cleaned[-1], 4),
        "avg": round(sum(cleaned) / len(cleaned), 4),
    }


def _load_taskset() -> Tuple[list[str], list[tuple[str, str]], list[dict[str, str]], Dict[str, Any], Path]:
    """
    读取固定 taskset 并校验结构。

    输入：
    1. 无显式输入参数。
    输出：
    1. `(codes, samples, tasks, taskset_meta, taskset_path)`。
    用途：
    1. 保证三轮测试始终使用同一份任务输入。
    边界条件：
    1. 文件不存在或结构非法时抛 RuntimeError。
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
        task = {
            "code": str(item.get("code", "")).strip(),
            "freq": str(item.get("freq", "")).strip(),
            "start_time": str(item.get("start_time", "")).strip(),
            "end_time": str(item.get("end_time", "")).strip(),
        }
        if not task["code"] or not task["freq"] or not task["start_time"] or not task["end_time"]:
            raise RuntimeError("任务集结构非法：task 存在空字段")
        tasks.append(task)

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


def _extract_elapsed_stats(mode_stat: Dict[str, Any]) -> Dict[str, Any]:
    """
    提取模式级和分批级耗时指标。

    输入：
    1. mode_stat: `bench.run_mode_batched` 返回字典。
    输出：
    1. 总体耗时/吞吐/失败率/批次分位数字典。
    用途：
    1. 统一三轮对比口径。
    边界条件：
    1. batches 为空时分位数字段返回空值。
    """
    batches = list(mode_stat.get("batches") or [])
    elapsed_list = [_safe_float(item.get("elapsed_seconds"), 0.0) for item in batches if isinstance(item, dict)]
    return {
        "elapsed_seconds": _safe_float(mode_stat.get("elapsed_seconds"), 0.0),
        "tasks_per_sec": _safe_float(mode_stat.get("tasks_per_sec"), 0.0),
        "rows_per_sec": _safe_float(mode_stat.get("rows_per_sec"), 0.0),
        "failed_tasks": _safe_int(mode_stat.get("failed_tasks"), 0),
        "rows_total": _safe_int(mode_stat.get("rows_total"), 0),
        "unique_worker_pids": _safe_int(mode_stat.get("unique_worker_pids"), 0),
        "avg_pages_per_chunk": _safe_float(mode_stat.get("avg_pages_per_chunk"), 0.0),
        "batch_elapsed_quantiles": _series_quantiles(elapsed_list),
    }


def _run_one_round(
    *,
    run_name: str,
    mode: str,
    fetcher: Any,
    tasks: list[dict[str, str]],
    async_process_workers: int,
    prewarm_before_run: bool,
    inproc_chunk_workers_override: Optional[int] = None,
) -> Dict[str, Any]:
    """
    执行单轮基准（可选 prewarm + 可选 inproc 并发覆盖）并在末尾清理进程池。

    输入：
    1. run_name: 轮次标识。
    2. mode: `sync` 或 `async`。
    3. fetcher: 并行抓取器实例。
    4. tasks: 固定任务集。
    5. async_process_workers: 目标进程数。
    6. prewarm_before_run: 是否在执行前显式 prewarm。
    7. inproc_chunk_workers_override: 可选覆盖 `task_chunk_inproc_future_workers`。
    输出：
    1. 轮次结果字典（profile/prewarm/stat/cleanup）。
    用途：
    1. 实现严格影响边界：每轮执行后强制清理进程池。
    边界条件：
    1. 轮次异常也会执行 cleanup，再将异常上抛。
    """
    profile = bench._configure_fetcher_for_mode(
        fetcher=fetcher,
        mode=mode,
        async_process_workers=async_process_workers,
    )
    if inproc_chunk_workers_override is not None:
        fetcher.task_chunk_inproc_future_workers = max(1, int(inproc_chunk_workers_override))
        profile["task_chunk_inproc_future_workers"] = int(fetcher.task_chunk_inproc_future_workers)

    prewarm_summary: Optional[Dict[str, Any]] = None
    run_stat: Optional[Dict[str, Any]] = None
    error_text: Optional[str] = None

    try:
        if bool(prewarm_before_run):
            prewarm_summary = bench.parallel_fetcher.prewarm_parallel_fetcher(
                require_all_workers=True,
                timeout_seconds=max(1.0, _safe_float(getattr(fetcher, "auto_prewarm_timeout_seconds", 60.0), 60.0)),
                max_rounds=max(1, _safe_int(getattr(fetcher, "auto_prewarm_max_rounds", 3), 3)),
                target_workers=max(1, _safe_int(fetcher.num_processes, 1)),
            )
        run_stat = bench.run_mode_batched(mode=mode, tasks=tasks, batch_size=bench.TASK_BATCH_SIZE)
    except Exception as exc:
        error_text = str(exc)
        raise
    finally:
        cleanup_summary = bench._cleanup_parallel_pool(run_name=run_name)

    return {
        "run_name": str(run_name),
        "mode": str(mode),
        "profile": profile,
        "prewarm_before_run": bool(prewarm_before_run),
        "prewarm_summary": prewarm_summary,
        "stat": run_stat if isinstance(run_stat, dict) else {},
        "cleanup_summary": cleanup_summary,
        "error": error_text,
    }


def _build_analysis(run1_sync: Dict[str, Any], run2_async: Dict[str, Any], run3_async_seq_chunk: Dict[str, Any]) -> Dict[str, Any]:
    """
    构建三轮对比分析结论。

    输入：
    1. run1_sync: 第一轮 sync 统计。
    2. run2_async: 第二轮 prewarm+async 统计。
    3. run3_async_seq_chunk: 第三轮 prewarm+async(顺序chunk)统计。
    输出：
    1. 比值 + 结论提示。
    用途：
    1. 快速定位瓶颈是否来自 async 调度或进程内 chunk future 并发。
    边界条件：
    1. 分母耗时为 0 时对应比值返回 None。
    """
    run1_elapsed = _safe_float(run1_sync.get("elapsed_seconds"), 0.0)
    run2_elapsed = _safe_float(run2_async.get("elapsed_seconds"), 0.0)
    run3_elapsed = _safe_float(run3_async_seq_chunk.get("elapsed_seconds"), 0.0)

    run1_over_run2 = (run1_elapsed / run2_elapsed) if run2_elapsed > 0 else None
    run3_over_run2 = (run3_elapsed / run2_elapsed) if run2_elapsed > 0 else None
    run3_over_run1 = (run3_elapsed / run1_elapsed) if run1_elapsed > 0 else None

    hints: List[str] = []
    if run1_over_run2 is not None and run1_over_run2 > 1.0:
        hints.append("prewarm+async 相比 sync 有加速，说明异步并发与预热对吞吐有效。")
    else:
        hints.append("prewarm+async 未明显快于 sync，瓶颈更可能在网络或远端接口。")

    if run3_over_run2 is not None and run3_over_run2 > 1.05:
        hints.append("禁用进程内 chunk future 后 async 变慢，瓶颈包含进程内 chunk 并发能力。")
    else:
        hints.append("禁用进程内 chunk future 影响有限，主要瓶颈不在该层。")

    return {
        "run1_sync_over_run2_async": round(run1_over_run2, 4) if run1_over_run2 is not None else None,
        "run3_async_seq_chunk_over_run2_async": round(run3_over_run2, 4) if run3_over_run2 is not None else None,
        "run3_async_seq_chunk_over_run1_sync": round(run3_over_run1, 4) if run3_over_run1 is not None else None,
        "hints": hints,
    }


def main() -> None:
    """
    执行严格边界的三轮基准并落盘。

    输入：
    1. 无显式输入参数。
    输出：
    1. `benchmark_three_runs_compare_200x5x15_result.json`。
    用途：
    1. 满足“每轮后清理进程池”的可复现对比实验。
    边界条件：
    1. 任一轮失败会中止并抛错；已执行轮次仍会保留 cleanup 信息。
    """
    codes, samples, tasks, taskset_meta, taskset_path = _load_taskset()

    fetcher = bench.get_fetcher()
    if hasattr(fetcher, "parallel_total_timeout_seconds"):
        fetcher.parallel_total_timeout_seconds = 3600.0
    if hasattr(fetcher, "parallel_result_timeout_seconds"):
        fetcher.parallel_result_timeout_seconds = 3600.0

    default_workers = max(1, _safe_int(getattr(fetcher, "num_processes", 1), 1))
    async_workers = max(bench.ASYNC_MIN_PROCESS_WORKERS, default_workers)

    print("=" * 96)
    print("three runs benchmark compare with strict process-boundary control")
    print("=" * 96)
    print(f"stock_count  : {len(codes)}")
    print(f"freqs        : {bench.FREQS}")
    print(f"sample_count : {len(samples)}")
    print(f"task_count   : {len(tasks)}")
    print(f"batch_size   : {bench.TASK_BATCH_SIZE}")
    print(f"taskset_file : {taskset_path}")
    print(f"taskset_hash : {taskset_meta.get('task_hash_sha256', '')}")
    print(f"default_workers(auto) : {default_workers}")

    _ = bench.get_supported_markets(return_df=False)
    pre_run_cleanup = bench._cleanup_parallel_pool(run_name="pre-run-cleanup")

    run1 = _run_one_round(
        run_name="run1_sync_then_cleanup",
        mode="sync",
        fetcher=fetcher,
        tasks=tasks,
        async_process_workers=async_workers,
        prewarm_before_run=False,
        inproc_chunk_workers_override=None,
    )
    run2 = _run_one_round(
        run_name="run2_prewarm_async_then_cleanup",
        mode="async",
        fetcher=fetcher,
        tasks=tasks,
        async_process_workers=async_workers,
        prewarm_before_run=True,
        inproc_chunk_workers_override=None,
    )
    run3 = _run_one_round(
        run_name="run3_prewarm_async_seqchunk_then_cleanup",
        mode="async",
        fetcher=fetcher,
        tasks=tasks,
        async_process_workers=async_workers,
        prewarm_before_run=True,
        inproc_chunk_workers_override=1,
    )

    run_rows = [
        {
            "run_name": "run1_sync_then_cleanup",
            **_extract_elapsed_stats(run1["stat"]),
            "profile": run1["profile"],
            "prewarm_before_run": run1["prewarm_before_run"],
        },
        {
            "run_name": "run2_prewarm_async_then_cleanup",
            **_extract_elapsed_stats(run2["stat"]),
            "profile": run2["profile"],
            "prewarm_before_run": run2["prewarm_before_run"],
        },
        {
            "run_name": "run3_prewarm_async_seqchunk_then_cleanup",
            **_extract_elapsed_stats(run3["stat"]),
            "profile": run3["profile"],
            "prewarm_before_run": run3["prewarm_before_run"],
        },
    ]

    analysis = _build_analysis(
        run1_sync=run1["stat"],
        run2_async=run2["stat"],
        run3_async_seq_chunk=run3["stat"],
    )

    report: Dict[str, Any] = {
        "meta": {
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "taskset_file": str(taskset_path),
            "taskset_hash_sha256": str(taskset_meta.get("task_hash_sha256", "")),
            "stock_count": int(len(codes)),
            "freq_count": int(len(bench.FREQS)),
            "sample_count": int(len(samples)),
            "task_count": int(len(tasks)),
            "batch_size": int(bench.TASK_BATCH_SIZE),
            "default_process_workers": int(default_workers),
            "async_process_workers": int(async_workers),
            "boundary_policy": [
                "run1: sync -> cleanup",
                "run2: prewarm -> async -> cleanup",
                "run3: prewarm -> async(inproc_chunk_workers=1) -> cleanup",
            ],
        },
        "pre_run_cleanup": pre_run_cleanup,
        "run_details": [run1, run2, run3],
        "three_runs_compare": run_rows,
        "analysis": analysis,
    }

    out_path = Path(__file__).with_name(RESULT_FILENAME)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("-" * 96)
    print(json.dumps(analysis, ensure_ascii=False, indent=2))
    print(f"saved: {out_path}")


if __name__ == "__main__":
    try:
        main()
    finally:
        bench._cleanup_parallel_pool(run_name="benchmark-three-runs-exit")

