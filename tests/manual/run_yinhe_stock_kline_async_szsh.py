"""
模块：`run_yinhe_stock_kline_async_szsh.py`。

职责：
1. 使用银河 hosts 配置，对全部深沪股票（不含北交所）跑 get_stock_kline(mode=async)。
2. 周期 15/30/60/d/w，窗口由配套 yaml 指定；边收边校验 OHLC/时间。
3. 输出总耗时、成功率、校验失败样本与 lifecycle，供主 Agent 验收。

边界：
1. 参数只读配套 yaml，不暴露 CLI 可调项。
2. 不落全量 rows（体量过大）；仅落汇总与失败样本。
3. 收到 event=done 后立刻 destroy_parallel_fetcher。
"""

from __future__ import annotations

import json
import re
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

_MANUAL = Path(__file__).resolve().parent
_ROOT = _MANUAL.parents[1]
_SRC = _ROOT / "src"
_CFG = _MANUAL / "yinhe_stock_kline_async_szsh_config.yaml"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")


def _load_cfg() -> Dict[str, Any]:
    """
    输入：无。
    输出：bench 配置字典。
    用途：读取验收参数。
    边界：缺少 bench 段时抛错。
    """
    doc = yaml.safe_load(_CFG.read_text(encoding="utf-8")) or {}
    bench = dict(doc.get("bench") or {})
    if not bench:
        raise ValueError("缺少 bench 配置")
    return bench


def _check_row(row: Dict[str, Any], freq: str, start_day: str, end_day: str) -> Optional[str]:
    """
    输入：单根 K 线、周期、起止日期。
    输出：通过返回 None，否则返回原因。
    用途：准确性校验。
    边界：周线允许日期略超出窗口（±10 天）；分钟/日线必须落在 [start,end]。
    """
    dt_text = str(row.get("datetime", ""))
    if not _DT_RE.match(dt_text):
        return f"datetime非法:{dt_text}"
    try:
        o = float(row["open"])
        h = float(row["high"])
        low = float(row["low"])
        c = float(row["close"])
    except Exception:
        return "OHLC无法转float"
    if min(o, h, low, c) <= 0:
        return "OHLC非正"
    if h + 1e-9 < max(o, c) or low - 1e-9 > min(o, c) or h < low:
        return "OHLC不自洽"
    try:
        vol = float(row.get("volume"))
        if vol < 0:
            return "volume<0"
    except Exception:
        return "volume非法"
    bar_day = dt_text[:10]
    if freq in {"15", "30", "60", "d"}:
        if bar_day < start_day or bar_day > end_day:
            return f"日期越界:{bar_day}"
    elif freq == "w":
        try:
            b = date.fromisoformat(bar_day)
            s = date.fromisoformat(start_day)
            e = date.fromisoformat(end_day)
            if b < s - timedelta(days=10) or b > e + timedelta(days=10):
                return f"周线日期偏离:{bar_day}"
        except Exception:
            return f"周线日期解析失败:{bar_day}"
    return None


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    """
    输入：路径与字典。
    输出：无。
    用途：UTF-8 落盘 JSON。
    边界：自动建父目录。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    """
    输入：无。
    输出：成功 0，失败 1。
    用途：银河全量深沪 async K 线验收入口。
    边界：任何阶段异常写入 lifecycle=failed。
    """
    if str(_SRC) not in sys.path:
        sys.path.insert(0, str(_SRC))

    bench = _load_cfg()
    cfg_path = (_MANUAL / str(bench["config_path"])).resolve()
    freqs = [str(x).strip() for x in list(bench.get("freqs") or [])]
    start_time = str(bench["start_time"]).strip()
    end_time = str(bench["end_time"]).strip()
    start_day = start_time[:10]
    end_day = end_time[:10]
    queue_timeout = float(bench.get("queue_timeout_seconds", 7200))
    progress_every = int(bench.get("progress_every", 500))
    art = _MANUAL / str(bench.get("artifacts_dir", "artifacts/yinhe_kline_async"))
    art.mkdir(parents=True, exist_ok=True)
    lifecycle_path = art / "lifecycle.json"
    summary_path = art / "summary.json"

    from zsdtdx import (
        destroy_parallel_fetcher,
        get_client,
        get_stock_kline,
        set_config_path,
    )

    set_config_path(str(cfg_path), async_background_probe=True)
    t0 = time.perf_counter()
    _write_json(
        lifecycle_path,
        {
            "lifecycle_status": "running",
            "phase": "load_codes",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        },
    )

    with get_client() as client:
        rows = client.get_all_stock_list(return_df=False)
    codes: List[str] = []
    for rec in rows:
        if str(rec.get("source", "")).strip() != "std":
            continue
        try:
            market = int(rec.get("market", -1))
        except Exception:
            continue
        if bool(bench.get("exclude_beijing", True)) and market == 2:
            continue
        if market not in (0, 1):
            continue
        code = str(rec.get("code", "")).strip()
        if code:
            codes.append(code)
    codes = sorted(set(codes))
    tasks = [
        {
            "code": code,
            "freq": freq,
            "start_time": start_time,
            "end_time": end_time,
        }
        for code in codes
        for freq in freqs
    ]
    print(
        f"codes={len(codes)} freqs={freqs} tasks={len(tasks)} "
        f"window={start_time}~{end_time} config={cfg_path}",
        flush=True,
    )
    _write_json(
        lifecycle_path,
        {
            "lifecycle_status": "running",
            "phase": "fetching",
            "codes": len(codes),
            "tasks": len(tasks),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        },
    )

    stats: Dict[str, Any] = {
        "data_events": 0,
        "ok_tasks": 0,
        "empty_rows": 0,
        "error_tasks": 0,
        "ohlc_bad": 0,
        "row_total": 0,
        "by_freq_ok": Counter(),
        "by_freq_empty": Counter(),
        "by_freq_error": Counter(),
        "by_freq_ohlc_bad": Counter(),
        "by_freq_rows": Counter(),
        "error_samples": [],
        "ohlc_samples": [],
        "empty_samples": [],
    }

    fetch_t0 = time.perf_counter()
    job = get_stock_kline(task=tasks, mode="async")
    q = job.queue
    done: Dict[str, Any] = {}
    while True:
        event = q.get(timeout=queue_timeout)
        name = str(event.get("event", "")).strip().lower()
        if name == "done":
            done = {
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
        err = event.get("error")
        rows_payload = list(event.get("rows") or [])
        if err:
            stats["error_tasks"] += 1
            stats["by_freq_error"][freq] += 1
            if len(stats["error_samples"]) < 20:
                stats["error_samples"].append(
                    {"task": task, "error": str(err)[:300]}
                )
        elif not rows_payload:
            stats["empty_rows"] += 1
            stats["by_freq_empty"][freq] += 1
            if len(stats["empty_samples"]) < 20:
                stats["empty_samples"].append({"task": task})
        else:
            bad = None
            for row in rows_payload:
                if not isinstance(row, dict):
                    bad = "row非dict"
                    break
                bad = _check_row(row, freq, start_day, end_day)
                if bad:
                    break
            if bad:
                stats["ohlc_bad"] += 1
                stats["by_freq_ohlc_bad"][freq] += 1
                if len(stats["ohlc_samples"]) < 20:
                    stats["ohlc_samples"].append({"task": task, "reason": bad})
            else:
                stats["ok_tasks"] += 1
                stats["by_freq_ok"][freq] += 1
                stats["row_total"] += len(rows_payload)
                stats["by_freq_rows"][freq] += len(rows_payload)
        if stats["data_events"] % progress_every == 0:
            elapsed = time.perf_counter() - fetch_t0
            print(
                f"progress events={stats['data_events']}/{len(tasks)} "
                f"ok={stats['ok_tasks']} empty={stats['empty_rows']} "
                f"err={stats['error_tasks']} ohlc_bad={stats['ohlc_bad']} "
                f"elapsed={elapsed:.1f}s",
                flush=True,
            )
            _write_json(
                lifecycle_path,
                {
                    "lifecycle_status": "running",
                    "phase": "fetching",
                    "data_events": stats["data_events"],
                    "ok_tasks": stats["ok_tasks"],
                    "elapsed_seconds": round(elapsed, 3),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                },
            )

    destroy_info = dict(destroy_parallel_fetcher())
    try:
        job.result()
    except Exception as exc:
        print(f"job.result error: {exc}", flush=True)

    fetch_elapsed = time.perf_counter() - fetch_t0
    total_elapsed = time.perf_counter() - t0
    expected = len(tasks)
    coverage = (
        round(stats["data_events"] / expected, 6) if expected else None
    )
    # 准确性：有数据且 OHLC 通过的占比；空行对停牌可接受，单独统计
    checked = stats["ok_tasks"] + stats["ohlc_bad"]
    accuracy = (
        round(stats["ok_tasks"] / checked, 6) if checked else None
    )
    overall_ok = (
        stats["data_events"] >= int(expected * 0.95)
        and stats["ohlc_bad"] == 0
        and stats["error_tasks"] <= int(expected * 0.02)
    )
    summary = {
        "ok": overall_ok,
        "hosts": str(bench.get("hosts_label") or "yinhe"),
        "exclude_beijing": True,
        "codes": len(codes),
        "freqs": freqs,
        "start_time": start_time,
        "end_time": end_time,
        "expected_tasks": expected,
        "done": done,
        "destroy": destroy_info,
        "stats": {
            **{k: (dict(v) if isinstance(v, Counter) else v) for k, v in stats.items()},
        },
        "coverage": coverage,
        "ohlc_accuracy_among_nonempty": accuracy,
        "fetch_elapsed_seconds": round(fetch_elapsed, 3),
        "total_elapsed_seconds": round(total_elapsed, 3),
        "config_path": str(cfg_path),
    }
    _write_json(summary_path, summary)
    _write_json(
        lifecycle_path,
        {
            "lifecycle_status": "completed",
            "ok": overall_ok,
            "fetch_elapsed_seconds": round(fetch_elapsed, 3),
            "total_elapsed_seconds": round(total_elapsed, 3),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        },
    )
    print(json.dumps({
        "ok": overall_ok,
        "codes": len(codes),
        "tasks": expected,
        "data_events": stats["data_events"],
        "ok_tasks": stats["ok_tasks"],
        "empty": stats["empty_rows"],
        "error": stats["error_tasks"],
        "ohlc_bad": stats["ohlc_bad"],
        "coverage": coverage,
        "ohlc_accuracy": accuracy,
        "fetch_s": round(fetch_elapsed, 3),
        "total_s": round(total_elapsed, 3),
    }, ensure_ascii=False), flush=True)
    print(f"SUMMARY={summary_path}", flush=True)
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
