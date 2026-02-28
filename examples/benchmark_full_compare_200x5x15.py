"""
一键完整对比脚本：调起 3 个脚本并输出综合分析结论。

流程：
1. 生成固定任务集（taskset）。
2. 执行 sync-only 基准。
3. 执行 async-only 基准。
4. 聚合三份结果文件，生成更全面的分析报告。
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

RUN_GENERATE_TASKSET = True
RUN_SYNC_BENCHMARK = True
RUN_ASYNC_BENCHMARK = True
ASYNC_SIGNIFICANT_SPEEDUP_THRESHOLD = 1.2

EXAMPLES_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EXAMPLES_DIR.parent

TASKSET_SCRIPT = EXAMPLES_DIR / "generate_benchmark_taskset_200x5x15.py"
SYNC_SCRIPT = EXAMPLES_DIR / "benchmark_sync_200x5x15.py"
ASYNC_SCRIPT = EXAMPLES_DIR / "benchmark_async_200x5x15.py"

TASKSET_FILE = EXAMPLES_DIR / "benchmark_taskset_200x5x15.json"
SYNC_RESULT_FILE = EXAMPLES_DIR / "benchmark_sync_200x5x15_result.json"
ASYNC_RESULT_FILE = EXAMPLES_DIR / "benchmark_async_200x5x15_result.json"
FULL_REPORT_FILE = EXAMPLES_DIR / "benchmark_full_compare_200x5x15_result.json"


def _run_script(script_path: Path, stage_name: str) -> float:
    """
    运行单个阶段脚本并返回耗时。

    输入：
    1. script_path: 待执行脚本路径。
    2. stage_name: 阶段名称（用于日志输出）。
    输出：
    1. 阶段执行耗时（秒）。
    用途：
    1. 统一管理子脚本执行、环境变量和耗时统计。
    边界条件：
    1. 脚本不存在或执行失败时抛 RuntimeError。
    """
    if not script_path.exists():
        raise RuntimeError(f"{stage_name} 脚本不存在: {script_path}")

    env = dict(os.environ)
    existing_pythonpath = str(env.get("PYTHONPATH", "")).strip()
    src_path = str((PROJECT_ROOT / "src").resolve())
    env["PYTHONPATH"] = src_path if existing_pythonpath == "" else f"{src_path}{os.pathsep}{existing_pythonpath}"
    env["PYTHONIOENCODING"] = "utf-8"

    command = [sys.executable, str(script_path)]
    print(f"\n>>> stage={stage_name} run: {' '.join(command)}")
    started = time.perf_counter()
    try:
        subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            env=env,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"{stage_name} 失败，返回码={exc.returncode}") from exc

    elapsed = time.perf_counter() - started
    print(f"<<< stage={stage_name} done: elapsed={elapsed:.3f}s")
    return float(round(elapsed, 3))


def _read_json_file(path: Path, label: str) -> Dict[str, Any]:
    """
    读取 JSON 文件并返回字典对象。

    输入：
    1. path: 文件路径。
    2. label: 文件标签（用于错误提示）。
    输出：
    1. JSON 字典对象。
    用途：
    1. 统一读取 taskset/sync/async 结果文件。
    边界条件：
    1. 文件不存在、JSON 非法或根对象非 dict 时抛 RuntimeError。
    """
    if not path.exists():
        raise RuntimeError(f"{label} 文件不存在: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"{label} 读取失败: {path}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"{label} 格式非法（根对象必须为 dict）: {path}")
    return raw


def _safe_float(value: Any, default: float = 0.0) -> float:
    """
    安全转 float。

    输入：
    1. value: 任意值。
    2. default: 解析失败默认值。
    输出：
    1. float 值。
    用途：
    1. 统一处理报告字段中的数值解析。
    边界条件：
    1. 解析失败时返回 default。
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
    2. default: 解析失败默认值。
    输出：
    1. int 值。
    用途：
    1. 统一处理报告字段中的整数解析。
    边界条件：
    1. 解析失败时返回 default。
    """
    try:
        return int(value)
    except Exception:
        return int(default)


def _series_stats(values: List[float]) -> Dict[str, Any]:
    """
    计算序列统计。

    输入：
    1. values: 浮点数序列。
    输出：
    1. `count/min/max/avg` 统计字典。
    用途：
    1. 汇总 batch 级耗时与吞吐稳定性。
    边界条件：
    1. 空序列返回 count=0，其余字段为 None。
    """
    cleaned = [float(v) for v in values]
    if not cleaned:
        return {"count": 0, "min": None, "max": None, "avg": None}
    return {
        "count": int(len(cleaned)),
        "min": round(min(cleaned), 3),
        "max": round(max(cleaned), 3),
        "avg": round(sum(cleaned) / len(cleaned), 3),
    }


def _extract_batch_metrics(mode_stat: Dict[str, Any]) -> Dict[str, Any]:
    """
    提取 batch 级统计信息。

    输入：
    1. mode_stat: sync 或 async 模式聚合结果字典。
    输出：
    1. batch 级耗时与吞吐统计字典。
    用途：
    1. 补充模式级总耗时之外的稳定性观察指标。
    边界条件：
    1. batches 为空时返回空统计。
    """
    batches = list(mode_stat.get("batches") or [])
    elapsed_list: List[float] = []
    tps_list: List[float] = []
    avg_pages_list: List[float] = []
    for item in batches:
        if not isinstance(item, dict):
            continue
        elapsed_list.append(_safe_float(item.get("elapsed_seconds"), 0.0))
        monitor = item.get("monitor") if isinstance(item.get("monitor"), dict) else {}
        tps_list.append(_safe_float(monitor.get("tasks_per_sec"), 0.0))
        avg_pages_list.append(_safe_float(monitor.get("avg_pages_per_chunk"), 0.0))
    return {
        "elapsed_seconds": _series_stats(elapsed_list),
        "tasks_per_sec": _series_stats(tps_list),
        "avg_pages_per_chunk": _series_stats(avg_pages_list),
    }


def _build_diagnosis(
    *,
    taskset: Dict[str, Any],
    sync_result: Dict[str, Any],
    async_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    构建综合诊断结论。

    输入：
    1. taskset: 任务集文件字典。
    2. sync_result: sync-only 结果字典。
    3. async_result: async-only 结果字典。
    输出：
    1. 结构化诊断结论字典。
    用途：
    1. 统一输出“输入一致性 + 性能 + 质量 + 并发利用”分析结论。
    边界条件：
    1. 任一字段缺失时会降级为保守判断并给出提示。
    """
    taskset_meta = taskset.get("meta") if isinstance(taskset.get("meta"), dict) else {}
    sync_meta = sync_result.get("meta") if isinstance(sync_result.get("meta"), dict) else {}
    async_meta = async_result.get("meta") if isinstance(async_result.get("meta"), dict) else {}
    sync_stat = sync_result.get("sync") if isinstance(sync_result.get("sync"), dict) else {}
    async_stat = async_result.get("async") if isinstance(async_result.get("async"), dict) else {}

    task_count_taskset = _safe_int(taskset_meta.get("task_count"), 0)
    task_count_sync = _safe_int(sync_meta.get("task_count"), _safe_int(sync_stat.get("total_tasks"), 0))
    task_count_async = _safe_int(async_meta.get("task_count"), _safe_int(async_stat.get("total_tasks"), 0))
    task_count_consistent = bool(task_count_taskset > 0 and task_count_taskset == task_count_sync == task_count_async)

    hash_taskset = str(taskset_meta.get("task_hash_sha256", "")).strip()
    hash_sync = str(sync_meta.get("taskset_hash_sha256", "")).strip()
    hash_async = str(async_meta.get("taskset_hash_sha256", "")).strip()
    hash_consistent = bool(hash_taskset and hash_taskset == hash_sync == hash_async)

    sync_elapsed = _safe_float(sync_stat.get("elapsed_seconds"), 0.0)
    async_elapsed = _safe_float(async_stat.get("elapsed_seconds"), 0.0)
    sync_over_async_ratio = round(sync_elapsed / async_elapsed, 4) if async_elapsed > 0 else None
    is_async_significantly_faster = bool(
        sync_over_async_ratio is not None and sync_over_async_ratio >= ASYNC_SIGNIFICANT_SPEEDUP_THRESHOLD
    )

    sync_rows_total = _safe_int(sync_stat.get("rows_total"), 0)
    async_rows_total = _safe_int(async_stat.get("rows_total"), 0)
    sync_failed = _safe_int(sync_stat.get("failed_tasks"), 0)
    async_failed = _safe_int(async_stat.get("failed_tasks"), 0)
    quality_consistent = bool(sync_rows_total == async_rows_total and sync_failed == async_failed)

    hints: List[str] = []
    if not task_count_consistent:
        hints.append("task_count 不一致，需检查是否读取同一份任务集")
    if not hash_consistent:
        hints.append("taskset_hash 不一致，sync/async 可能未使用同一份输入")
    if not quality_consistent:
        hints.append("sync/async 质量统计不一致（rows_total 或 failed_tasks 不同）")
    if not is_async_significantly_faster:
        hints.append("async 未达到显著加速阈值，建议继续检查并发利用率与缓存命中")
    if not hints:
        hints.append("输入一致性与质量一致性通过，async 性能优势达标")

    return {
        "input_consistency": {
            "task_count_taskset": int(task_count_taskset),
            "task_count_sync": int(task_count_sync),
            "task_count_async": int(task_count_async),
            "task_count_consistent": bool(task_count_consistent),
            "task_hash_taskset": hash_taskset,
            "task_hash_sync": hash_sync,
            "task_hash_async": hash_async,
            "task_hash_consistent": bool(hash_consistent),
        },
        "performance": {
            "sync_elapsed_seconds": float(sync_elapsed),
            "async_elapsed_seconds": float(async_elapsed),
            "sync_over_async_ratio": sync_over_async_ratio,
            "async_significant_speedup_threshold": float(ASYNC_SIGNIFICANT_SPEEDUP_THRESHOLD),
            "is_async_significantly_faster": bool(is_async_significantly_faster),
            "speedup_percent": round((sync_over_async_ratio - 1.0) * 100.0, 2)
            if sync_over_async_ratio is not None
            else None,
        },
        "quality": {
            "sync_rows_total": int(sync_rows_total),
            "async_rows_total": int(async_rows_total),
            "sync_failed_tasks": int(sync_failed),
            "async_failed_tasks": int(async_failed),
            "quality_consistent": bool(quality_consistent),
        },
        "parallelism": {
            "sync_unique_worker_pids": _safe_int(sync_stat.get("unique_worker_pids"), 0),
            "async_unique_worker_pids": _safe_int(async_stat.get("unique_worker_pids"), 0),
            "sync_avg_pages_per_chunk": _safe_float(sync_stat.get("avg_pages_per_chunk"), 0.0),
            "async_avg_pages_per_chunk": _safe_float(async_stat.get("avg_pages_per_chunk"), 0.0),
            "sync_batch_metrics": _extract_batch_metrics(sync_stat),
            "async_batch_metrics": _extract_batch_metrics(async_stat),
        },
        "hints": hints,
    }


def main() -> None:
    """
    执行完整流水线并输出综合分析报告。

    输入：
    1. 无显式输入参数。
    输出：
    1. `benchmark_full_compare_200x5x15_result.json` 报告文件。
    用途：
    1. 一键完成“任务集生成 + sync 基线 + async 优化结果”统一对比。
    边界条件：
    1. 任一阶段失败会立即中止并抛错，避免输出不完整结论。
    """
    stage_elapsed: Dict[str, float] = {}

    print("=" * 96)
    print("benchmark full compare pipeline (taskset + sync + async + diagnosis)")
    print("=" * 96)

    if RUN_GENERATE_TASKSET:
        stage_elapsed["generate_taskset"] = _run_script(TASKSET_SCRIPT, "generate_taskset")
    if RUN_SYNC_BENCHMARK:
        stage_elapsed["run_sync"] = _run_script(SYNC_SCRIPT, "run_sync")
    if RUN_ASYNC_BENCHMARK:
        stage_elapsed["run_async"] = _run_script(ASYNC_SCRIPT, "run_async")

    taskset = _read_json_file(TASKSET_FILE, "taskset")
    sync_result = _read_json_file(SYNC_RESULT_FILE, "sync_result")
    async_result = _read_json_file(ASYNC_RESULT_FILE, "async_result")

    diagnosis = _build_diagnosis(taskset=taskset, sync_result=sync_result, async_result=async_result)

    report: Dict[str, Any] = {
        "meta": {
            "pipeline": {
                "run_generate_taskset": bool(RUN_GENERATE_TASKSET),
                "run_sync_benchmark": bool(RUN_SYNC_BENCHMARK),
                "run_async_benchmark": bool(RUN_ASYNC_BENCHMARK),
                "stage_elapsed_seconds": stage_elapsed,
            },
            "input_files": {
                "taskset_file": str(TASKSET_FILE),
                "sync_result_file": str(SYNC_RESULT_FILE),
                "async_result_file": str(ASYNC_RESULT_FILE),
            },
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        },
        "taskset_meta": taskset.get("meta", {}),
        "sync_meta": sync_result.get("meta", {}),
        "async_meta": async_result.get("meta", {}),
        "sync": sync_result.get("sync", {}),
        "async": async_result.get("async", {}),
        "diagnosis": diagnosis,
    }

    FULL_REPORT_FILE.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("-" * 96)
    print(json.dumps(diagnosis, ensure_ascii=False, indent=2))
    print(f"saved: {FULL_REPORT_FILE}")


if __name__ == "__main__":
    main()

