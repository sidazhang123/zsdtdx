"""
生成固定 benchmark 任务集并落盘：1000 code * 5 freq * 15 samples。

用途：
1. 将任务样本固定为文件，避免每次连接检查脚本运行时重新拉取股票代码。
2. 供 `run_async_1000x5x15_check_connection.py` 直接复用同一份任务集，保证口径一致。
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import sync_async_benchmark_1000x5x15 as bench

TASKSET_FILENAME = "benchmark_taskset_1000x5x15.json"
TARGET_STOCK_COUNT = 1000
SAMPLE_COUNT = 15
SAMPLE_SEED = int(bench.SAMPLE_SEED)


def _taskset_hash(tasks: List[Dict[str, str]]) -> str:
    """
    计算任务集哈希值（SHA256）。

    输入：
    1. tasks: task 字典列表。
    输出：
    1. 任务集哈希字符串。
    用途：
    1. 便于验证连接检查脚本读取到的任务集一致性。
    边界条件：
    1. 空列表仍会返回固定哈希值。
    """
    raw = json.dumps(tasks, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def main() -> None:
    """
    生成并写出固定任务集文件。

    输入：
    1. 无显式输入参数。
    输出：
    1. `benchmark_taskset_1000x5x15.json` 文件。
    用途：
    1. 固定 async 连接检查输入任务集。
    边界条件：
    1. 写文件失败时会抛出异常。
    """
    codes = bench._collect_stock_codes(TARGET_STOCK_COUNT)
    samples: List[Tuple[str, str]] = bench._build_samples(SAMPLE_COUNT, seed=SAMPLE_SEED)
    tasks = bench.build_tasks(codes, bench.FREQS, samples)
    task_hash = _taskset_hash(tasks)

    report: Dict[str, Any] = {
        "meta": {
            "taskset_name": "benchmark_taskset_1000x5x15",
            "stock_count": int(len(codes)),
            "freqs": list(bench.FREQS),
            "sample_count": int(len(samples)),
            "task_count": int(len(tasks)),
            "sample_seed": int(SAMPLE_SEED),
            "excluded_sample_months": sorted(list(bench.EXCLUDED_SAMPLE_MONTHS)),
            "task_hash_sha256": task_hash,
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "generator_script": "examples/generate_benchmark_taskset_1000x5x15.py",
        },
        "codes": list(codes),
        "samples": [list(item) for item in samples],
        "tasks": list(tasks),
    }

    out_path = Path(__file__).with_name(TASKSET_FILENAME)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 96)
    print("benchmark taskset generated")
    print("=" * 96)
    print(f"saved: {out_path}")
    print(f"task_count: {len(tasks)}")
    print(f"task_hash_sha256: {task_hash}")


if __name__ == "__main__":
    main()

