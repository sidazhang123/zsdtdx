# -*- coding: utf-8 -*-
"""
模块：`run_block_kline_full_anomaly.py`。

职责：
1. 实盘调用 `get_block_names` 拉取全部板块名称。
2. 对每个名称构造 15/30/60/d/w 任务，用 `get_block_kline(mode=async)`
   拉取 2026-09-21～2026-09-24 全部 OHLCV。
3. 扫描名称与 OHLCV 奇异值，写出验收报告与抽样明细。

边界：
1. 需访问真实行情服务器；不由 pytest 收集。
2. 参数写死本文件，不暴露 CLI。
3. 结束后销毁并行进程池；不保留全量原始 rows 到磁盘（仅报告与异常抽样）。
"""

from __future__ import annotations

import json
import math
import multiprocessing as mp
import queue as std_queue
import re
import sys
import time
import traceback
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts" / "block_kline_full_anomaly"
_TEST_CONFIG = (
    _ROOT / "tests" / "manual" / "artifacts" / "live_full_api" / "live_test_config.yaml"
)
START_TEXT = "2026-09-21"
END_TEXT = "2026-09-24"
FREQS = ("15", "30", "60", "d", "w")
QUEUE_TIMEOUT = 600.0
PROGRESS_EVERY = 200
# 短窗口下各周期期望 bar 下限（过少记为 thin，不记硬失败）。
MIN_ROWS_BY_FREQ = {"15": 8, "30": 4, "60": 2, "d": 1, "w": 1}

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _log(msg: str) -> None:
    """输入消息，输出无。用途：带时间戳进度，防卡死误判。边界：立即 flush。"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _write_json(path: Path, payload: Any) -> None:
    """输入路径与对象，以 UTF-8 写入 JSON。边界：不可序列化字段转 str。"""
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _force_cleanup_parallel(*, label: str) -> Dict[str, Any]:
    """
    输入段落标签。
    输出清理摘要。
    用途：async 结束后回收进程池与残留子进程。
    边界：只处理本进程 multiprocessing 子进程。
    """
    from zsdtdx import destroy_parallel_fetcher

    summary: Dict[str, Any] = {
        "label": label,
        "destroy": {},
        "terminated": [],
        "killed": [],
    }
    try:
        summary["destroy"] = dict(destroy_parallel_fetcher())
    except Exception as exc:
        summary["destroy_error"] = str(exc)
    for child in list(mp.active_children()):
        try:
            pid = int(getattr(child, "pid", 0) or 0)
        except Exception:
            pid = 0
        try:
            if child.is_alive():
                child.terminate()
                child.join(timeout=2.0)
                summary["terminated"].append(pid)
            if child.is_alive():
                child.kill()
                child.join(timeout=2.0)
                summary["killed"].append(pid)
        except Exception:
            pass
    _log(
        f"cleanup[{label}] destroy={summary.get('destroy')} "
        f"term={summary['terminated']} kill={summary['killed']}"
    )
    return summary


def _check_name(name: str) -> Optional[str]:
    """输入板块名，输出问题或 None。用途：空名/控制字符/替换符。边界：仅单条。"""
    text = "" if name is None else str(name)
    if not text.strip():
        return "空名称"
    if _CTRL_RE.search(text) or "\ufffd" in text:
        return "含控制字符或替换符"
    if len(text) > 40:
        return f"名称过长 len={len(text)}"
    return None


def _check_ohlcv(row: Dict[str, Any]) -> Optional[str]:
    """
    输入一行 K 线，输出问题或 None。
    用途：datetime/OHLC/volume/amount 奇异值。
    边界：缺字段或类型异常均记问题。
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
    if any(not math.isfinite(x) for x in (o, h, low, c)):
        return "OHLC非有限"
    if min(o, h, low, c) <= 0:
        return f"OHLC非正:{o}/{h}/{low}/{c}"
    if h + 1e-9 < max(o, c) or low - 1e-9 > min(o, c) or h < low:
        return f"OHLC不自洽:o={o}/h={h}/l={low}/c={c}"
    # 板块指数价格通常在合理量级；极端大值视为奇异。
    if max(o, h, low, c) > 1e7:
        return f"OHLC过大:{max(o, h, low, c)}"
    try:
        vol = float(row.get("volume", 0) or 0)
    except Exception:
        return "volume非法"
    if not math.isfinite(vol) or vol < 0:
        return f"volume非法:{vol}"
    if "amount" in row and row.get("amount") is not None:
        try:
            amt = float(row["amount"])
        except Exception:
            return "amount非法"
        if not math.isfinite(amt) or amt < 0:
            return f"amount非法:{amt}"
    return None


def _scan_names(names: Sequence[str]) -> Dict[str, Any]:
    """
    输入名称列表。
    输出名称侧奇异汇总。
    用途：空名、乱码、重复、长度异常。
    边界：仅采样前若干条。
    """
    empty: List[str] = []
    bad: List[Dict[str, str]] = []
    dups: List[str] = []
    seen: Dict[str, int] = {}
    for raw in names:
        name = str(raw)
        seen[name] = seen.get(name, 0) + 1
        reason = _check_name(name)
        if reason == "空名称":
            if len(empty) < 30:
                empty.append(repr(raw))
        elif reason:
            if len(bad) < 30:
                bad.append({"name": name[:60], "reason": reason})
    for name, cnt in seen.items():
        if cnt > 1 and len(dups) < 30:
            dups.append(f"{name} x{cnt}")
    return {
        "n": len(names),
        "unique_n": len(seen),
        "empty_n": len(empty),
        "empty_sample": empty,
        "bad_name_n": len(bad),
        "bad_name_sample": bad,
        "dup_n": len(dups),
        "dup_sample": dups,
    }


def _task_key(task: Dict[str, Any]) -> str:
    """输入 task 字典，输出 block_name:freq 主键。"""
    return f"{task.get('block_name')}:{task.get('freq')}"


def _scan_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    输入一条 data 事件。
    输出该任务扫描结果。
    用途：边收边检，避免堆积全量 rows。
    边界：error/empty/ohlc/dup_dt/thin 分类互斥优先 error。
    """
    task = dict(event.get("task") or {})
    key = _task_key(task)
    freq = str(task.get("freq") or "")
    err = event.get("error")
    rows = list(event.get("rows") or [])
    out: Dict[str, Any] = {
        "key": key,
        "block_name": task.get("block_name"),
        "freq": freq,
        "row_n": len(rows),
        "status": "ok",
        "reason": None,
        "sample_row": None,
    }
    if err:
        out["status"] = "error"
        out["reason"] = str(err)[:300]
        return out
    if not rows:
        out["status"] = "empty"
        out["reason"] = "无bars"
        return out

    seen_dt: set[str] = set()
    prev_close: Optional[float] = None
    jump_flags: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            out["status"] = "ohlc_bad"
            out["reason"] = "row非dict"
            return out
        dt = str(row.get("datetime", ""))
        if dt in seen_dt:
            out["status"] = "ohlc_bad"
            out["reason"] = f"datetime重复:{dt}"
            out["sample_row"] = {
                "datetime": dt,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
            }
            return out
        seen_dt.add(dt)
        bad = _check_ohlcv(row)
        if bad:
            out["status"] = "ohlc_bad"
            out["reason"] = bad
            out["sample_row"] = {
                "datetime": dt,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
                "amount": row.get("amount"),
            }
            return out
        try:
            close = float(row["close"])
        except Exception:
            close = None
        if prev_close is not None and close is not None and prev_close > 0:
            ratio = abs(close - prev_close) / prev_close
            # 单根相对前收涨跌超 50% 记为可疑跳变（板块指数极少见）。
            if ratio > 0.5:
                jump_flags.append(f"{dt} jump={ratio:.3f}")
        if close is not None:
            prev_close = close

    if jump_flags:
        out["status"] = "ohlc_bad"
        out["reason"] = "相邻收盘跳变过大:" + ";".join(jump_flags[:3])
        return out

    min_n = MIN_ROWS_BY_FREQ.get(freq, 1)
    if len(rows) < min_n:
        out["status"] = "thin"
        out["reason"] = f"bars过少 {len(rows)}<{min_n}"
        return out

    # 保留首尾各一行便于人工抽查。
    first = rows[0]
    last = rows[-1]
    out["sample_row"] = {
        "first": {
            "datetime": first.get("datetime"),
            "open": first.get("open"),
            "high": first.get("high"),
            "low": first.get("low"),
            "close": first.get("close"),
            "volume": first.get("volume"),
        },
        "last": {
            "datetime": last.get("datetime"),
            "open": last.get("open"),
            "high": last.get("high"),
            "low": last.get("low"),
            "close": last.get("close"),
            "volume": last.get("volume"),
        },
    }
    return out


def main() -> int:
    """
    输入：无。
    输出：成功 0；存在 error/ohlc_bad 硬问题返回 1。
    用途：全量板块名 + 五周期 OHLCV 奇异值验收入口。
    边界：finally 销毁并行池。
    """
    from zsdtdx import (
        BlockKlineTask,
        get_block_kline,
        get_block_names,
        get_client,
        prewarm_parallel_fetcher,
        set_config_path,
    )

    _ART.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    report: Dict[str, Any] = {
        "start_text": START_TEXT,
        "end_text": END_TEXT,
        "freqs": list(FREQS),
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }
    cleanup: Dict[str, Any] = {}
    try:
        set_config_path(str(_TEST_CONFIG), async_background_probe=True)
        _log("get_block_names ...")
        with get_client():
            names = list(get_block_names() or [])
        name_scan = _scan_names(names)
        report["names"] = {
            "count": len(names),
            "sample_head": names[:20],
            "sample_tail": names[-10:] if len(names) > 10 else names,
            "scan": name_scan,
        }
        _write_json(_ART / "block_names.json", {"names": names, "scan": name_scan})
        _log(f"block_names n={len(names)} unique={name_scan['unique_n']}")

        if not names:
            report["fatal"] = "get_block_names 返回空"
            _write_json(_ART / "report.json", report)
            _log("FAIL: 名称为空")
            return 1

        tasks = [
            BlockKlineTask(
                block_name=name,
                freq=freq,
                start_time=START_TEXT,
                end_time=END_TEXT,
            )
            for name in names
            for freq in FREQS
        ]
        report["task_n"] = len(tasks)
        _log(f"prewarm + get_block_kline async tasks={len(tasks)} ...")
        prewarm = prewarm_parallel_fetcher()
        report["prewarm"] = prewarm

        job = get_block_kline(task=tasks, mode="async")
        stats = Counter()
        by_freq: Dict[str, Counter] = {f: Counter() for f in FREQS}
        samples: Dict[str, List[Dict[str, Any]]] = {
            "error": [],
            "empty": [],
            "ohlc_bad": [],
            "thin": [],
            "ok": [],
        }
        row_total = 0
        done_evt = None
        got = 0
        expected = len(tasks)

        while True:
            try:
                event = job.queue.get(timeout=QUEUE_TIMEOUT)
            except std_queue.Empty:
                raise TimeoutError(
                    f"queue 超时 {QUEUE_TIMEOUT}s，已收 {got}/{expected}"
                )
            kind = str(event.get("event", "")).strip().lower()
            if kind == "done":
                done_evt = event
                break
            if kind != "data":
                continue
            got += 1
            scanned = _scan_payload(event)
            status = str(scanned["status"])
            stats[status] += 1
            freq = str(scanned.get("freq") or "")
            if freq in by_freq:
                by_freq[freq][status] += 1
            row_total += int(scanned.get("row_n") or 0)
            bucket = samples.get(status)
            if bucket is not None and len(bucket) < (20 if status != "ok" else 5):
                bucket.append(scanned)
            if got % PROGRESS_EVERY == 0 or got == expected:
                elapsed = time.perf_counter() - t0
                _log(
                    f"progress {got}/{expected} "
                    f"ok={stats['ok']} empty={stats['empty']} "
                    f"error={stats['error']} ohlc_bad={stats['ohlc_bad']} "
                    f"thin={stats['thin']} rows={row_total} "
                    f"elapsed={elapsed:.1f}s"
                )

        try:
            job.result()
        except Exception as exc:
            report["job_result_error"] = str(exc)

        cleanup = _force_cleanup_parallel(label="after_block_async")
        report["cleanup"] = cleanup
        report["done"] = done_evt
        report["received_n"] = got
        report["row_total"] = row_total
        report["stats"] = dict(stats)
        report["by_freq"] = {k: dict(v) for k, v in by_freq.items()}
        report["samples"] = samples
        report["elapsed_sec"] = round(time.perf_counter() - t0, 3)

        hard_bad = int(stats["error"]) + int(stats["ohlc_bad"])
        name_hard = int(name_scan["empty_n"]) + int(name_scan["bad_name_n"])
        report["hard_problem_n"] = hard_bad + name_hard
        report["pass"] = hard_bad == 0 and name_hard == 0 and got == expected

        _write_json(_ART / "report.json", report)
        _write_json(
            _ART / "anomaly_samples.json",
            {
                "error": samples["error"],
                "empty": samples["empty"],
                "ohlc_bad": samples["ohlc_bad"],
                "thin": samples["thin"],
            },
        )

        _log(
            f"DONE pass={report['pass']} received={got}/{expected} "
            f"ok={stats['ok']} empty={stats['empty']} error={stats['error']} "
            f"ohlc_bad={stats['ohlc_bad']} thin={stats['thin']} "
            f"rows={row_total} elapsed={report['elapsed_sec']}s"
        )
        _log(f"report -> {_ART / 'report.json'}")
        return 0 if report["pass"] else 1
    except Exception as exc:
        report["fatal"] = str(exc)
        report["traceback"] = traceback.format_exc()
        report["elapsed_sec"] = round(time.perf_counter() - t0, 3)
        try:
            cleanup = _force_cleanup_parallel(label="on_fatal")
            report["cleanup"] = cleanup
        except Exception:
            pass
        _write_json(_ART / "report.json", report)
        _log(f"FATAL: {exc}")
        _log(report["traceback"])
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
