"""
模块：`run_full_catalog_kline_0914_0918.py`。

职责：
1. 把京沪深与港股通放进同一次 async 任务，拉取 2026-09-14~2026-09-18 的 15/30/60/d/w K 线。
2. 边收边统计两侧结果，并采样配额公式、进程峰值、拥塞回退等机制细节。

边界：
1. 需要访问真实行情服务器，不由 pytest 收集。
2. 不保留全量 K 线；全部结束后销毁进程池。
3. 周线允许落在窗口所在周，不按自然日裁剪。
4. 京沪深耗时是混合任务中该侧最后一条结果到达的时间，不是单独跑完的墙钟。
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# 环境变量 ZSDTDX_FULL_CATALOG_INCLUDE_HK=0 时只跑京沪深 A 股。
INCLUDE_HK = str(os.environ.get("ZSDTDX_FULL_CATALOG_INCLUDE_HK", "1")).strip() not in {
    "0",
    "false",
    "False",
    "no",
    "NO",
}
_ART_NAME = (
    "full_catalog_kline_0914_0918"
    if INCLUDE_HK
    else "full_catalog_kline_0914_0918_ashare_only"
)
_ART = _ROOT / "tests" / "manual" / "artifacts" / _ART_NAME
START_TEXT = "2026-09-14"
END_TEXT = "2026-09-18"
FREQS = ("15", "30", "60", "d", "w")
QUEUE_TIMEOUT = 900.0
PROGRESS_EVERY = 200
SAMPLE_INTERVAL = 0.25


def _log(message: str) -> None:
    """输入一行进度，输出到标准输出并立即刷新。"""
    print(message, flush=True)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    """输入路径和字典，以 UTF-8 写入 JSON。"""
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _load_codes() -> Dict[str, List[str]]:
    """
    输入无。
    输出：std 为京沪深前缀代码，hk 为港股通前缀代码。
    边界：任一侧为空时抛错。
    """
    from zsdtdx import get_client

    grouped = {"std": [], "hk": []}
    with get_client() as client:
        frame = client.get_all_stock_list(return_df=True)
        for record in frame.to_dict(orient="records"):
            code = str(record.get("code", "")).strip()
            source = str(record.get("source", "")).strip().lower()
            try:
                market = int(record.get("market", -1))
            except Exception:
                continue
            if code == "" or source not in {"std", "ex"} or market < 0:
                continue
            prefixed = client._stock_code_with_prefix(source, market, code)
            if prefixed.startswith(("sz.", "sh.", "bj.")):
                grouped["std"].append(prefixed)
            elif prefixed.startswith("hk."):
                grouped["hk"].append(prefixed)
    grouped["std"] = sorted(set(grouped["std"]))
    grouped["hk"] = sorted(set(grouped["hk"]))
    if not grouped["std"] or not grouped["hk"]:
        raise RuntimeError(
            f"码表分组不完整: std={len(grouped['std'])} hk={len(grouped['hk'])}"
        )
    return grouped


def _tasks_for(codes: List[str]) -> List[Dict[str, str]]:
    """输入代码列表，输出五个周期的任务。"""
    return [
        {
            "code": code,
            "freq": freq,
            "start_time": START_TEXT,
            "end_time": END_TEXT,
        }
        for code in codes
        for freq in FREQS
    ]


def _side_of(event: Dict[str, Any]) -> str:
    """输入一条任务事件，按代码前缀返回 std 或 ex。"""
    task = dict(event.get("task") or {})
    code = str(task.get("code") or "")
    if code.startswith("hk."):
        return "ex"
    return "std"


def _is_conn_error(error_text: str) -> bool:
    """输入错误文本，判断是否为连接或超时失败。"""
    lowered = error_text.lower()
    return any(
        word in lowered
        for word in ("无可用连接", "不可用", "unavailable", "timeout", "超时")
    )


def _host_side_stats(side_hosts: Dict[str, Any]) -> Dict[str, Any]:
    """
    输入某一侧 host 快照。
    输出：该侧 process_cap / inflight / 冷却中地址数。
    边界：空映射返回全 0。
    """
    if not side_hosts:
        return {
            "host_count": 0,
            "process_cap_sum": 0,
            "process_cap_max": 0,
            "process_cap_min": 0,
            "process_inflight_sum": 0,
            "cooling_hosts": 0,
            "hosts": {},
        }
    caps = [int(item.get("process_cap") or 0) for item in side_hosts.values()]
    inflights = [int(item.get("process_inflight") or 0) for item in side_hosts.values()]
    now = time.monotonic()
    cooling = sum(
        1
        for item in side_hosts.values()
        if float(item.get("cooldown_until") or 0.0) > now
    )
    return {
        "host_count": len(side_hosts),
        "process_cap_sum": int(sum(caps)),
        "process_cap_max": int(max(caps)),
        "process_cap_min": int(min(caps)),
        "process_inflight_sum": int(sum(inflights)),
        "cooling_hosts": int(cooling),
        "hosts": {
            key: {
                "process_cap": int(item.get("process_cap") or 0),
                "process_inflight": int(item.get("process_inflight") or 0),
                "failed_ceiling": item.get("failed_ceiling"),
                "cooling": float(item.get("cooldown_until") or 0.0) > now,
            }
            for key, item in side_hosts.items()
        },
    }


def _new_peak_state() -> Dict[str, Any]:
    """输入无，输出采样线程共享的峰值状态字典。"""
    return {
        "lock": threading.Lock(),
        "samples": 0,
        "overlap_samples": 0,
        "ex_budget_full_samples": 0,
        "std_budget_full_samples": 0,
        "ex_processes": 0,
        "std_processes": 0,
        "global_processes": 0,
        "waiting_count_max": 0,
        "first_snapshot": None,
        "last_snapshot": None,
        "time_to_std_budget_seconds": None,
        "time_to_ex_budget_seconds": None,
        "std_host_cap_max": 0,
        "ex_host_cap_max": 0,
        "std_host_cap_min_seen": None,
        "ex_host_cap_min_seen": None,
        "std_cooling_host_peak": 0,
        "ex_cooling_host_peak": 0,
        "congestion_events": 0,
        "host_cap_history": {"std": {}, "ex": {}},
        "budget": {"std": 0, "ex": 0},
        "per_host": {"std": 0, "ex": 0},
        "max_processes": 0,
        "inproc_limit": 0,
        "ex_hosts": [],
        "std_hosts": [],
        "started_at": time.perf_counter(),
    }


def _sample_scheduler(peak: Dict[str, Any], stop: threading.Event) -> None:
    """
    输入峰值字典和停止事件。
    输出：就地更新配额、进程峰值、拥塞与冷却等机制采样。
    边界：调度器尚未创建时跳过本次采样。
    """
    import zsdtdx.parallel_fetcher as pf

    prev_caps: Dict[str, Dict[str, int]] = {"std": {}, "ex": {}}
    while not stop.wait(SAMPLE_INTERVAL):
        controller = pf._global_adaptive_controller
        if controller is None:
            continue
        snap = controller.snapshot()
        by_source = dict(snap.get("inflight_processes_by_source") or {})
        budget = dict(snap.get("side_process_budget") or {})
        sides = dict(snap.get("sides") or {})
        std_now = int(by_source.get("std") or 0)
        ex_now = int(by_source.get("ex") or 0)
        std_budget = int(budget.get("std") or 0)
        ex_budget = int(budget.get("ex") or 0)
        std_hosts = dict(sides.get("std") or {})
        ex_hosts = dict(sides.get("ex") or {})
        std_side = _host_side_stats(std_hosts)
        ex_side = _host_side_stats(ex_hosts)
        elapsed = round(time.perf_counter() - float(peak["started_at"]), 3)

        congestion_delta = 0
        for source, host_map in (("std", std_hosts), ("ex", ex_hosts)):
            for host_key, item in host_map.items():
                cap_now = int(item.get("process_cap") or 0)
                prev = prev_caps[source].get(host_key)
                if prev is not None and cap_now < prev:
                    congestion_delta += 1
                prev_caps[source][host_key] = cap_now

        with peak["lock"]:
            peak["samples"] += 1
            peak["ex_processes"] = max(int(peak["ex_processes"]), ex_now)
            peak["std_processes"] = max(int(peak["std_processes"]), std_now)
            peak["global_processes"] = max(
                int(peak["global_processes"]), int(snap.get("inflight_processes") or 0)
            )
            peak["waiting_count_max"] = max(
                int(peak["waiting_count_max"]), int(snap.get("waiting_count") or 0)
            )
            peak["budget"] = {"std": std_budget, "ex": ex_budget}
            peak["per_host"] = dict(snap.get("processes_per_host") or {})
            peak["max_processes"] = int(snap.get("max_processes") or 0)
            peak["inproc_limit"] = int(snap.get("inproc_limit") or 0)
            peak["ex_hosts"] = sorted(ex_hosts)
            peak["std_hosts"] = sorted(std_hosts)
            if ex_now > 0 and std_now > 0:
                peak["overlap_samples"] += 1
            if ex_budget > 0 and ex_now >= ex_budget:
                peak["ex_budget_full_samples"] += 1
            if std_budget > 0 and std_now >= std_budget:
                peak["std_budget_full_samples"] += 1
            if (
                peak["time_to_std_budget_seconds"] is None
                and std_budget > 0
                and std_now >= std_budget
            ):
                peak["time_to_std_budget_seconds"] = elapsed
            if (
                peak["time_to_ex_budget_seconds"] is None
                and ex_budget > 0
                and ex_now >= ex_budget
            ):
                peak["time_to_ex_budget_seconds"] = elapsed
            peak["std_host_cap_max"] = max(
                int(peak["std_host_cap_max"]), int(std_side["process_cap_max"])
            )
            peak["ex_host_cap_max"] = max(
                int(peak["ex_host_cap_max"]), int(ex_side["process_cap_max"])
            )
            if std_side["host_count"] > 0:
                current_min = int(std_side["process_cap_min"])
                if peak["std_host_cap_min_seen"] is None:
                    peak["std_host_cap_min_seen"] = current_min
                else:
                    peak["std_host_cap_min_seen"] = min(
                        int(peak["std_host_cap_min_seen"]), current_min
                    )
            if ex_side["host_count"] > 0:
                current_min = int(ex_side["process_cap_min"])
                if peak["ex_host_cap_min_seen"] is None:
                    peak["ex_host_cap_min_seen"] = current_min
                else:
                    peak["ex_host_cap_min_seen"] = min(
                        int(peak["ex_host_cap_min_seen"]), current_min
                    )
            peak["std_cooling_host_peak"] = max(
                int(peak["std_cooling_host_peak"]), int(std_side["cooling_hosts"])
            )
            peak["ex_cooling_host_peak"] = max(
                int(peak["ex_cooling_host_peak"]), int(ex_side["cooling_hosts"])
            )
            peak["congestion_events"] = int(peak["congestion_events"]) + congestion_delta
            for source, host_map in (("std", std_hosts), ("ex", ex_hosts)):
                history = peak["host_cap_history"][source]
                for host_key, item in host_map.items():
                    cap_now = int(item.get("process_cap") or 0)
                    inflight_now = int(item.get("process_inflight") or 0)
                    entry = history.setdefault(
                        host_key,
                        {
                            "cap_max": 0,
                            "cap_min": None,
                            "inflight_max": 0,
                            "failed_ceiling_max": None,
                        },
                    )
                    entry["cap_max"] = max(int(entry["cap_max"]), cap_now)
                    if entry["cap_min"] is None:
                        entry["cap_min"] = cap_now
                    else:
                        entry["cap_min"] = min(int(entry["cap_min"]), cap_now)
                    entry["inflight_max"] = max(int(entry["inflight_max"]), inflight_now)
                    failed = item.get("failed_ceiling")
                    if failed is not None:
                        prev_failed = entry["failed_ceiling_max"]
                        entry["failed_ceiling_max"] = (
                            int(failed)
                            if prev_failed is None
                            else max(int(prev_failed), int(failed))
                        )
            compact = {
                "elapsed_seconds": elapsed,
                "inflight_processes": int(snap.get("inflight_processes") or 0),
                "inflight_by_source": {"std": std_now, "ex": ex_now},
                "budget": {"std": std_budget, "ex": ex_budget},
                "waiting_count": int(snap.get("waiting_count") or 0),
                "std": std_side,
                "ex": ex_side,
            }
            if peak["first_snapshot"] is None:
                peak["first_snapshot"] = compact
            peak["last_snapshot"] = compact


def _consume_side(stats: Dict[str, Any], event: Dict[str, Any]) -> None:
    """输入单侧统计和一条任务事件，就地累加成功、空数据和错误。"""
    error_text = str(event.get("error") or "").strip()
    rows = list(event.get("rows") or [])
    worker_pid = int(event.get("worker_pid") or 0)
    if worker_pid > 0:
        stats["worker_pids"].add(worker_pid)
    if error_text:
        stats["error"] += 1
        if _is_conn_error(error_text):
            stats["conn_errors"] += 1
        stats["error_kinds"][error_text[:80]] += 1
        if len(stats["error_samples"]) < 20:
            task = dict(event.get("task") or {})
            stats["error_samples"].append(
                {
                    "code": task.get("code"),
                    "freq": task.get("freq"),
                    "error": error_text[:200],
                }
            )
    elif not rows:
        stats["empty"] += 1
    else:
        stats["ok"] += 1


def _build_mechanism_stats(peak: Dict[str, Any], sides: Dict[str, Any]) -> Dict[str, Any]:
    """
    输入采样峰值与两侧任务统计。
    输出：可序列化的机制细节报告。
    边界：采样线程未拿到快照时仍返回峰值字段。
    """
    with peak["lock"]:
        budget = dict(peak["budget"])
        per_host = dict(peak["per_host"])
        max_processes = int(peak["max_processes"])
        std_budget = int(budget.get("std") or 0)
        ex_budget = int(budget.get("ex") or 0)
        std_hosts = list(peak["std_hosts"])
        ex_hosts = list(peak["ex_hosts"])
        host_history = {
            source: dict(items) for source, items in peak["host_cap_history"].items()
        }
        first_snapshot = peak["first_snapshot"]
        last_snapshot = peak["last_snapshot"]
        process_stats = {
            "sample_interval_seconds": SAMPLE_INTERVAL,
            "samples": int(peak["samples"]),
            "overlap_samples": int(peak["overlap_samples"]),
            "overlap_seconds_estimate": round(
                int(peak["overlap_samples"]) * SAMPLE_INTERVAL, 3
            ),
            "ex_budget_full_samples": int(peak["ex_budget_full_samples"]),
            "std_budget_full_samples": int(peak["std_budget_full_samples"]),
            "ex_budget_full_seconds_estimate": round(
                int(peak["ex_budget_full_samples"]) * SAMPLE_INTERVAL, 3
            ),
            "std_budget_full_seconds_estimate": round(
                int(peak["std_budget_full_samples"]) * SAMPLE_INTERVAL, 3
            ),
            "peak_ex_processes": int(peak["ex_processes"]),
            "peak_std_processes": int(peak["std_processes"]),
            "peak_global_processes": int(peak["global_processes"]),
            "waiting_count_max": int(peak["waiting_count_max"]),
            "time_to_std_budget_seconds": peak["time_to_std_budget_seconds"],
            "time_to_ex_budget_seconds": peak["time_to_ex_budget_seconds"],
            "std_host_cap_max": int(peak["std_host_cap_max"]),
            "ex_host_cap_max": int(peak["ex_host_cap_max"]),
            "std_host_cap_min_seen": peak["std_host_cap_min_seen"],
            "ex_host_cap_min_seen": peak["ex_host_cap_min_seen"],
            "std_cooling_host_peak": int(peak["std_cooling_host_peak"]),
            "ex_cooling_host_peak": int(peak["ex_cooling_host_peak"]),
            "congestion_events": int(peak["congestion_events"]),
            "distinct_ex_worker_pids": sides["ex"]["worker_pid_count"],
            "distinct_std_worker_pids": sides["std"]["worker_pid_count"],
        }

    formula = {
        "max_processes": max_processes,
        "inproc_limit": int(peak["inproc_limit"]),
        "processes_per_host": per_host,
        "reachable_hosts": {"std": std_hosts, "ex": ex_hosts},
        "host_count": {"std": len(std_hosts), "ex": len(ex_hosts)},
        "side_process_budget": budget,
        "expected_ex_budget": (
            min(max_processes, len(ex_hosts) * int(per_host.get("ex") or 0))
            if max_processes > 0 and ex_hosts
            else 0
        ),
        "expected_std_budget": (
            max_processes
            if max_processes > 0 and len(std_hosts) >= max_processes
            else min(max_processes, len(std_hosts) * int(per_host.get("std") or 0))
            if max_processes > 0 and std_hosts
            else 0
        ),
        "peak_vs_budget": {
            "std": {
                "peak": int(process_stats["peak_std_processes"]),
                "budget": std_budget,
                "within_budget": int(process_stats["peak_std_processes"]) <= std_budget
                if std_budget > 0
                else True,
            },
            "ex": {
                "peak": int(process_stats["peak_ex_processes"]),
                "budget": ex_budget,
                "within_budget": int(process_stats["peak_ex_processes"]) <= ex_budget
                if ex_budget > 0
                else True,
            },
        },
        "no_slow_start_evidence": {
            "std_reached_budget_in_seconds": process_stats["time_to_std_budget_seconds"],
            "ex_reached_budget_in_seconds": process_stats["time_to_ex_budget_seconds"],
            "std_started_near_full": (
                process_stats["time_to_std_budget_seconds"] is not None
                and float(process_stats["time_to_std_budget_seconds"]) <= 5.0
            ),
        },
    }
    return {
        "formula": formula,
        "process_stats": process_stats,
        "host_cap_history": host_history,
        "first_snapshot": first_snapshot,
        "last_snapshot": last_snapshot,
    }


def _run_mixed(
    std_tasks: List[Dict[str, str]],
    hk_tasks: List[Dict[str, str]],
    progress_path: Path,
) -> Dict[str, Any]:
    """
    输入京沪深与港股通任务。
    输出：混合任务总耗时、两侧完成时刻，以及机制细节统计。
    边界：队列超过 900 秒没有新事件则失败返回。
    """
    from zsdtdx import get_stock_kline

    sides = {
        "std": {
            "tasks": len(std_tasks),
            "seen": 0,
            "ok": 0,
            "empty": 0,
            "error": 0,
            "conn_errors": 0,
            "error_kinds": Counter(),
            "error_samples": [],
            "worker_pids": set(),
            "elapsed_seconds": None,
        },
        "ex": {
            "tasks": len(hk_tasks),
            "seen": 0,
            "ok": 0,
            "empty": 0,
            "error": 0,
            "conn_errors": 0,
            "error_kinds": Counter(),
            "error_samples": [],
            "worker_pids": set(),
            "elapsed_seconds": None,
        },
    }
    peak = _new_peak_state()
    all_tasks = list(std_tasks) + list(hk_tasks)
    total = len(all_tasks)
    phase = "mixed" if hk_tasks else "ashare_only"
    _log(f"[start] {phase} std={len(std_tasks)} hk={len(hk_tasks)} total={total}")
    _write_json(progress_path, {"phase": phase, "done_tasks": 0, "tasks": total})
    stop = threading.Event()
    sampler = threading.Thread(target=_sample_scheduler, args=(peak, stop), daemon=True)
    sampler.start()
    started = time.perf_counter()
    job = get_stock_kline(task=all_tasks, mode="async")
    seen = 0
    done: Dict[str, Any] = {}
    try:
        while True:
            event = job.queue.get(timeout=QUEUE_TIMEOUT)
            if str(event.get("event")) == "done":
                done = dict(event)
                break
            seen += 1
            side = _side_of(event)
            stats = sides[side]
            _consume_side(stats, event)
            stats["seen"] += 1
            if stats["seen"] == stats["tasks"] and stats["elapsed_seconds"] is None:
                stats["elapsed_seconds"] = round(time.perf_counter() - started, 3)
                _log(f"[side_done] {side} elapsed={stats['elapsed_seconds']}s")
            if seen % PROGRESS_EVERY == 0:
                elapsed = round(time.perf_counter() - started, 1)
                with peak["lock"]:
                    ex_peak = int(peak["ex_processes"])
                    std_peak = int(peak["std_processes"])
                    ex_budget = int((peak["budget"] or {}).get("ex") or 0)
                    std_budget = int((peak["budget"] or {}).get("std") or 0)
                    congestion = int(peak["congestion_events"])
                _log(
                    f"[progress] {phase} {seen}/{total} "
                    f"std={sides['std']['seen']}/{sides['std']['tasks']} "
                    f"ex={sides['ex']['seen']}/{sides['ex']['tasks']} "
                    f"error={sides['std']['error'] + sides['ex']['error']} "
                    f"peak_std={std_peak}/{std_budget} peak_ex={ex_peak}/{ex_budget} "
                    f"congestion={congestion} elapsed={elapsed}s"
                )
                _write_json(
                    progress_path,
                    {
                        "phase": phase,
                        "done_tasks": seen,
                        "tasks": total,
                        "std_seen": sides["std"]["seen"],
                        "ex_seen": sides["ex"]["seen"],
                        "peak_ex_processes": ex_peak,
                        "peak_std_processes": std_peak,
                        "ex_budget": ex_budget,
                        "std_budget": std_budget,
                        "congestion_events": congestion,
                        "elapsed_seconds": elapsed,
                    },
                )
    finally:
        stop.set()
        sampler.join(timeout=2)
    for stats in sides.values():
        stats["worker_pid_count"] = len(stats["worker_pids"])
        stats["worker_pids"] = sorted(stats["worker_pids"])
        stats["error_kinds"] = dict(stats["error_kinds"].most_common(10))
    mechanism = _build_mechanism_stats(peak, sides)
    result = {
        "tasks": total,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "std": sides["std"],
        "ex": sides["ex"],
        "mechanism": mechanism,
        "process_stats": mechanism["process_stats"],
        "done": done,
    }
    formula = mechanism["formula"]
    process_stats = mechanism["process_stats"]
    _log(
        f"[done] elapsed={result['elapsed_seconds']}s "
        f"std_elapsed={sides['std']['elapsed_seconds']}s "
        f"ex_elapsed={sides['ex']['elapsed_seconds']}s "
        f"budget_std={formula['side_process_budget'].get('std')} "
        f"budget_ex={formula['side_process_budget'].get('ex')} "
        f"peak_std={process_stats['peak_std_processes']} "
        f"peak_ex={process_stats['peak_ex_processes']} "
        f"congestion={process_stats['congestion_events']}"
    )
    return result


def main() -> int:
    """执行全码表测试并写入摘要；默认混合，INCLUDE_HK=0 时仅京沪深。"""
    from zsdtdx import destroy_parallel_fetcher, set_config_path

    _ART.mkdir(parents=True, exist_ok=True)
    progress_path = _ART / "progress.json"
    set_config_path(str(_SRC / "zsdtdx" / "config.yaml"), async_background_probe=False)
    report: Dict[str, Any] = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "scheduler_model": "host_count_x_processes_per_host",
        "include_hk": bool(INCLUDE_HK),
        "artifact_dir": str(_ART),
    }
    try:
        catalog_started = time.perf_counter()
        codes = _load_codes()
        report["catalog_seconds"] = round(time.perf_counter() - catalog_started, 3)
        report["counts"] = {
            "std": len(codes["std"]),
            "hk": len(codes["hk"]) if INCLUDE_HK else 0,
            "hk_available": len(codes["hk"]),
        }
        _log(
            f"[catalog] std={len(codes['std'])} hk_available={len(codes['hk'])} "
            f"include_hk={INCLUDE_HK} elapsed={report['catalog_seconds']}s"
        )
        hk_tasks = _tasks_for(codes["hk"]) if INCLUDE_HK else []
        report["run"] = _run_mixed(
            _tasks_for(codes["std"]),
            hk_tasks,
            progress_path,
        )
        # 兼容旧字段名，便于对照上次混合报告。
        report["mixed"] = report["run"]
        mixed = report["run"]
        ex_error = int(mixed["ex"].get("error") or 0) if INCLUDE_HK else 0
        report["passed"] = (
            int(mixed["std"].get("error") or 0) == 0
            and ex_error == 0
            and int(mixed["done"].get("failed_tasks") or 0) == 0
        )
    except Exception as exc:
        report["passed"] = False
        report["fatal"] = str(exc)
        _log(f"[fatal] {exc}")
    finally:
        try:
            report["destroy"] = destroy_parallel_fetcher()
        except Exception as exc:
            report["destroy_error"] = str(exc)
        report["finished_at"] = datetime.now().isoformat(timespec="seconds")
        _write_json(_ART / "report.json", report)
        mixed = report.get("mixed") or {}
        std_elapsed = (mixed.get("std") or {}).get("elapsed_seconds")
        mechanism = mixed.get("mechanism") or {}
        formula = mechanism.get("formula") or {}
        process_stats = mechanism.get("process_stats") or mixed.get("process_stats") or {}
        _log(f"[jing_hu_shen_elapsed_within_mixed] {std_elapsed}")
        _log(f"[budget] {formula.get('side_process_budget')}")
        _log(f"[peak_vs_budget] {formula.get('peak_vs_budget')}")
        _log(f"[ex_process_peak] {process_stats.get('peak_ex_processes')}")
        _log(f"[congestion_events] {process_stats.get('congestion_events')}")
        _log(f"[result] {'PASS' if report.get('passed') else 'FAIL'}")
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
