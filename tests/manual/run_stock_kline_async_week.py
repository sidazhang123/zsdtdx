# -*- coding: utf-8 -*-
"""
手工：get_stock_kline(mode=async) 拉取默认股票清单 2026-09-14~17 的 15/30/60/d/w。

边界：
1. 独立进程运行；收到 event=done 后立刻 destroy_parallel_fetcher。
2. 边收边统计，不落全量 K 线。
3. 报告写入 tests/manual/artifacts/stock_kline_async_week/。
"""

from __future__ import annotations

import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts" / "stock_kline_async_week"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")
START_DAY = date(2026, 9, 14)
END_DAY = date(2026, 9, 17)
START_TEXT = "2026-09-14"
END_TEXT = "2026-09-17"
FREQS = ["15", "30", "60", "d", "w"]
QUEUE_TIMEOUT = 1800.0
PROGRESS_EVERY = 500


def _ensure_test_config() -> Path:
    """复制包内配置并拉长并行超时，供全量 K 线跑完。"""
    src = _SRC / "zsdtdx" / "config.yaml"
    _ART.mkdir(parents=True, exist_ok=True)
    dst = _ART / "run_config.yaml"
    text = src.read_text(encoding="utf-8")
    repl = {
        "chunk_timeout_seconds: 15": "chunk_timeout_seconds: 30",
    }
    for old, new in repl.items():
        text = text.replace(old, new)
    dst.write_text(text, encoding="utf-8")
    return dst


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    """UTF-8 写入 JSON。"""
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _classify_error(text: str) -> str:
    """把 task.error 归到重试/超时/无数据等桶。"""
    raw = str(text or "").strip().lower()
    if not raw:
        return "empty"
    if "timeout" in raw or "超时" in raw:
        return "timeout"
    if "retry" in raw or "重试" in raw:
        return "retry"
    if "unavailable" in raw or "不可用" in raw or "disconnect" in raw:
        return "conn_unavailable"
    if "no_data" in raw or "nodata" in raw or "无数据" in raw:
        return "no_data"
    if "none" in raw:
        return "none"
    if "not_found" in raw or "code_not_found" in raw:
        return "not_found"
    return "other"


def _check_row(row: Dict[str, Any], freq: str) -> Optional[str]:
    """检查单根 K 线格式、OHLC 自洽、是否落在窗口内。"""
    dt_text = str(row.get("datetime", ""))
    if not _DT_RE.match(dt_text):
        return f"datetime格式非法:{dt_text}"
    try:
        bar_dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return f"datetime无法解析:{dt_text}"
    bar_day = bar_dt.date()
    if bar_day < START_DAY or bar_day > END_DAY:
        return f"日期越窗:{dt_text}"
    if freq in {"15", "30", "60"} and bar_day.weekday() >= 5:
        return f"分钟线落在周末:{dt_text}"
    try:
        o = float(row.get("open"))
        h = float(row.get("high"))
        l = float(row.get("low"))
        c = float(row.get("close"))
    except Exception:
        return "OHLC无法转float"
    if h < l:
        return "high<low"
    if h + 1e-9 < max(o, c) or l - 1e-9 > min(o, c):
        return "OHLC不自洽"
    vol = row.get("volume")
    if vol is not None:
        try:
            if float(vol) < 0:
                return "volume<0"
        except Exception:
            return "volume非法"
    return None


def _push_sample(bucket: List[Dict[str, Any]], item: Dict[str, Any], limit: int = 12) -> None:
    if len(bucket) < limit:
        bucket.append(item)


def main() -> int:
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_stock_code_name,
        get_stock_kline,
        set_config_path,
    )

    _ART.mkdir(parents=True, exist_ok=True)
    progress_path = _ART / "progress.json"
    summary_path = _ART / "summary.json"
    cfg = _ensure_test_config()
    set_config_path(str(cfg), async_background_probe=True)

    stock_map = get_stock_code_name()
    codes = sorted(stock_map.keys())
    tasks = [
        {
            "code": code,
            "freq": freq,
            "start_time": START_TEXT,
            "end_time": END_TEXT,
        }
        for code in codes
        for freq in FREQS
    ]
    expected = len(tasks)
    print(
        f"stocks={len(codes)} tasks={expected} window={START_TEXT}~{END_TEXT} freqs={FREQS}",
        flush=True,
    )

    stats: Dict[str, Any] = {
        "ok": False,
        "stock_count": len(codes),
        "expected_tasks": expected,
        "freqs": FREQS,
        "start_time": START_TEXT,
        "end_time": END_TEXT,
        "scope": "get_stock_code_name yaml default szsh",
        "data_events": 0,
        "ok_tasks": 0,
        "empty_rows": 0,
        "error_tasks": 0,
        "ohlc_bad": 0,
        "dup_datetime_tasks": 0,
        "unsorted_tasks": 0,
        "error_by_kind": Counter(),
        "empty_by_freq": Counter(),
        "ok_by_freq": Counter(),
        "error_by_freq": Counter(),
        "bar_count_by_freq": defaultdict(list),
        "error_samples": [],
        "ohlc_samples": [],
        "empty_samples": [],
        "error_codes": Counter(),
        "timeout_tasks": [],
        "done": {},
        "destroy": {},
    }
    t0 = time.perf_counter()
    _write_json(
        progress_path,
        {"phase": "starting", "expected_tasks": expected, "stock_count": len(codes)},
    )

    job = None
    try:
        job = get_stock_kline(task=tasks, mode="async")
        q = job.queue
        while True:
            event = q.get(timeout=QUEUE_TIMEOUT)
            name = str(event.get("event", "")).strip().lower()
            if name == "done":
                stats["done"] = {
                    "total_tasks": event.get("total_tasks"),
                    "success_tasks": event.get("success_tasks"),
                    "failed_tasks": event.get("failed_tasks"),
                }
                break
            if name != "data":
                continue
            stats["data_events"] += 1
            task = dict(event.get("task") or {})
            freq = str(task.get("freq", "")).strip()
            code = str(task.get("code", "")).strip()
            err = event.get("error")
            rows = list(event.get("rows") or [])
            if err:
                stats["error_tasks"] += 1
                stats["error_by_freq"][freq] += 1
                kind = _classify_error(str(err))
                stats["error_by_kind"][kind] += 1
                stats["error_codes"][code] += 1
                item = {
                    "code": code,
                    "freq": freq,
                    "kind": kind,
                    "error": str(err)[:240],
                }
                _push_sample(stats["error_samples"], item, limit=20)
                if kind == "timeout":
                    stats["timeout_tasks"].append(item)
            elif not rows:
                stats["empty_rows"] += 1
                stats["empty_by_freq"][freq] += 1
                _push_sample(stats["empty_samples"], {"code": code, "freq": freq})
            else:
                bad = None
                dts: List[str] = []
                for row in rows:
                    if not isinstance(row, dict):
                        bad = "row非dict"
                        break
                    dts.append(str(row.get("datetime", "")))
                    bad = _check_row(row, freq)
                    if bad:
                        break
                if bad:
                    stats["ohlc_bad"] += 1
                    _push_sample(
                        stats["ohlc_samples"],
                        {"code": code, "freq": freq, "reason": bad, "n": len(rows)},
                    )
                else:
                    if len(dts) != len(set(dts)):
                        stats["dup_datetime_tasks"] += 1
                    if dts != sorted(dts):
                        stats["unsorted_tasks"] += 1
                    stats["ok_tasks"] += 1
                    stats["ok_by_freq"][freq] += 1
                    stats["bar_count_by_freq"][freq].append(len(rows))
            if stats["data_events"] % PROGRESS_EVERY == 0:
                elapsed = time.perf_counter() - t0
                print(
                    f"progress events={stats['data_events']}/{expected} "
                    f"ok={stats['ok_tasks']} empty={stats['empty_rows']} "
                    f"err={stats['error_tasks']} ohlc_bad={stats['ohlc_bad']} "
                    f"elapsed={elapsed:.1f}s",
                    flush=True,
                )
                _write_json(
                    progress_path,
                    {
                        "phase": "consuming",
                        "elapsed_seconds": round(elapsed, 3),
                        "data_events": stats["data_events"],
                        "ok_tasks": stats["ok_tasks"],
                        "empty_rows": stats["empty_rows"],
                        "error_tasks": stats["error_tasks"],
                        "ohlc_bad": stats["ohlc_bad"],
                    },
                )
        stats["destroy"] = dict(destroy_parallel_fetcher())
        if job is not None:
            try:
                job.result()
            except Exception as exc:
                stats["job_result_error"] = str(exc)
        stats["ok"] = True
    except Exception as exc:
        stats["fatal"] = f"{type(exc).__name__}: {exc}"
        try:
            destroy_parallel_fetcher()
        except Exception:
            pass
        print("FATAL", stats["fatal"], flush=True)
        _write_json(summary_path, stats)
        return 1
    finally:
        stats["elapsed_seconds"] = round(time.perf_counter() - t0, 3)

    bar_summary = {}
    for freq, ns in stats["bar_count_by_freq"].items():
        if not ns:
            continue
        bar_summary[freq] = {
            "tasks": len(ns),
            "min": min(ns),
            "max": max(ns),
            "p50": sorted(ns)[len(ns) // 2],
            "mean": round(sum(ns) / len(ns), 2),
        }
    out = {
        **{
            k: v
            for k, v in stats.items()
            if k not in {"bar_count_by_freq", "error_codes"}
        },
        "error_by_kind": dict(stats["error_by_kind"]),
        "empty_by_freq": dict(stats["empty_by_freq"]),
        "ok_by_freq": dict(stats["ok_by_freq"]),
        "error_by_freq": dict(stats["error_by_freq"]),
        "error_code_count": len(stats["error_codes"]),
        "timeout_tasks": stats["timeout_tasks"],
        "top_error_codes": dict(stats["error_codes"].most_common(20)),
        "bar_count_by_freq": bar_summary,
        "coverage": (
            None
            if expected <= 0
            else round(stats["data_events"] / expected, 4)
        ),
    }
    _write_json(summary_path, out)
    _write_json(progress_path, {"phase": "completed", **out})
    print(json.dumps({k: out[k] for k in (
        "ok", "elapsed_seconds", "stock_count", "expected_tasks",
        "data_events", "ok_tasks", "empty_rows", "error_tasks", "ohlc_bad",
        "done", "coverage",
    ) if k in out}, ensure_ascii=False), flush=True)
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
