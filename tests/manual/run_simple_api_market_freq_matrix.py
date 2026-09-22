"""
模块：`run_simple_api_market_freq_matrix.py`。

职责：
1. 从 simple_api 对沪、深、京、港股及标准/扩展指数做各周期现场验收。
2. 覆盖 sync、纯 std async、纯 ex async、混合 async，以及两个 async job 同时运行。
3. 只保留任务级摘要，不落全量 K 线。

边界：
1. 需要访问真实行情服务器，不由 pytest 收集。
2. 分钟线使用短窗口，日/周/月使用更长窗口，避免把短地址池打满。
3. 进程退出前销毁并行进程池。
"""

from __future__ import annotations

import json
import queue
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts" / "simple_api_market_freq_matrix"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")
FREQS = ("5", "15", "30", "60", "d", "w", "m")
WINDOWS = {
    "5": ("2026-09-18", "2026-09-18"),
    "15": ("2026-09-17", "2026-09-18"),
    "30": ("2026-09-17", "2026-09-18"),
    "60": ("2026-09-15", "2026-09-18"),
    "d": ("2026-09-01", "2026-09-18"),
    "w": ("2026-08-01", "2026-09-18"),
    "m": ("2026-01-01", "2026-09-18"),
}
PREFERRED = {
    "sh": ("sh.600000", "sh.601318"),
    "sz": ("sz.000001", "sz.300750"),
    "bj": ("bj.830799", "bj.920000"),
    "hk": ("hk.00700", "hk.09988"),
}
INDEX_STD = ("上证指数", "深证成指", "创业板指")
INDEX_EX = ("中证500", "中证1000", "中证2000")
LIQUID = {"sh.600000", "sz.000001", "hk.00700", "上证指数", "中证500"}


def _log(message: str) -> None:
    """输入一行进度文本，输出到标准输出并立即刷新。"""
    print(message, flush=True)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    """输入路径和字典，以 UTF-8 写入 JSON。"""
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _market_of(symbol: str) -> str:
    """输入带前缀代码或指数名，输出 sh/sz/bj/hk/index。"""
    text = str(symbol or "")
    if "." in text:
        return text.split(".", 1)[0]
    return "index"


def _window(freq: str) -> Tuple[str, str]:
    """输入周期，输出对应的短日期窗口。"""
    return WINDOWS[str(freq)]


def _check_row(
    row: Dict[str, Any], freq: str, start_day: str, end_day: str
) -> Optional[str]:
    """
    输入单根 K 线、周期和日期窗口。
    输出：通过返回 None，否则返回失败原因。
    边界：周/月线允许落在窗口所在周或月，不按自然日严格裁剪。
    """
    dt_text = str(row.get("datetime", ""))
    if not _DT_RE.match(dt_text):
        return f"datetime格式非法:{dt_text}"
    try:
        bar_day = datetime.strptime(dt_text, "%Y-%m-%d %H:%M:%S").date()
        start = datetime.strptime(start_day, "%Y-%m-%d").date()
        end = datetime.strptime(end_day, "%Y-%m-%d").date()
    except Exception:
        return f"datetime无法解析:{dt_text}"
    if freq in {"5", "15", "30", "60", "d"} and (bar_day < start or bar_day > end):
        return f"日期越窗:{dt_text}"
    try:
        o = float(row["open"])
        h = float(row["high"])
        low = float(row["low"])
        c = float(row["close"])
    except Exception:
        return "OHLC无法转float"
    if min(o, h, low, c) <= 0:
        return "OHLC非正"
    if h < low or h + 1e-9 < max(o, c) or low - 1e-9 > min(o, c):
        return "OHLC不自洽"
    try:
        if float(row.get("volume")) < 0:
            return "volume<0"
    except Exception:
        return "volume非法"
    return None


def _consume_async(
    job: Any, *, timeout: float
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    输入 async job 和单次队列等待秒数。
    输出：(data payloads, done payload)。
    边界：超时或缺少 done 时抛错。
    """
    payloads: List[Dict[str, Any]] = []
    deadline = time.monotonic() + timeout
    while True:
        remain = deadline - time.monotonic()
        if remain <= 0:
            raise TimeoutError("async 队列等待超时")
        try:
            event = job.queue.get(timeout=remain)
        except queue.Empty as exc:
            job_error = ""
            try:
                job_error = str(job.exception(timeout=0) or "")
            except Exception as inner:
                job_error = str(inner)
            raise TimeoutError(
                f"async 队列等待超时{(': ' + job_error) if job_error else ''}"
            ) from exc
        if str(event.get("event")) == "done":
            return payloads, dict(event)
        payloads.append(dict(event))


def _summarize(
    label: str,
    payloads: Sequence[Dict[str, Any]],
    *,
    elapsed: float,
    done: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    输入场景名、payload 和耗时。
    输出：按市场/周期聚合的 ok、empty、error 与失败样本。
    边界：连接类错误单独计数，不与普通 no_data 混在一起。
    """
    by_key: Dict[str, Dict[str, int]] = {}
    errors: List[Dict[str, Any]] = []
    conn_errors = 0
    for payload in payloads:
        task = dict(payload.get("task") or {})
        symbol = str(task.get("code") or task.get("index_name") or "")
        freq = str(task.get("freq") or "")
        key = f"{_market_of(symbol)}|{freq}"
        bucket = by_key.setdefault(key, {"ok": 0, "empty": 0, "error": 0})
        error_text = str(payload.get("error") or "").strip()
        rows = list(payload.get("rows") or [])
        if error_text:
            bucket["error"] += 1
            if any(
                word in error_text.lower()
                for word in ("不可用", "unavailable", "timeout", "超时")
            ):
                conn_errors += 1
            if len(errors) < 20:
                errors.append(
                    {"symbol": symbol, "freq": freq, "error": error_text[:240]}
                )
            continue
        if not rows:
            bucket["empty"] += 1
            if symbol in LIQUID and freq in {"15", "d"} and len(errors) < 20:
                errors.append({"symbol": symbol, "freq": freq, "error": "liquid_empty"})
            continue
        start_day, end_day = _window(freq)
        bad = None
        for row in rows:
            if not isinstance(row, dict):
                bad = "row非dict"
                break
            bad = _check_row(row, freq, start_day, end_day)
            if bad:
                break
        if bad:
            bucket["error"] += 1
            if len(errors) < 20:
                errors.append({"symbol": symbol, "freq": freq, "error": bad})
            continue
        bucket["ok"] += 1
    return {
        "label": label,
        "elapsed_seconds": round(elapsed, 3),
        "tasks": len(payloads),
        "conn_errors": conn_errors,
        "by_market_freq": by_key,
        "errors": errors,
        "done": done or {},
    }


def _select_codes(code_map: Dict[str, str]) -> Dict[str, List[str]]:
    """
    输入代码名称字典。
    输出：每个市场最多两个优先代码，缺失时用目录中的前两个补齐。
    边界：某市场目录为空时保留空列表，由调用方判定失败。
    """
    selected: Dict[str, List[str]] = {}
    for market, preferred in PREFERRED.items():
        present = [code for code in preferred if code in code_map]
        if len(present) < 2:
            extras = [
                code
                for code in code_map
                if code.startswith(f"{market}.") and code not in present
            ]
            present.extend(extras[: 2 - len(present)])
        # 代码表默认范围不含港股通，K 线接口仍接受显式港股代码。
        if len(present) < 2:
            present.extend(code for code in preferred if code not in present)
        selected[market] = present[:2]
    return selected


def _stock_tasks(codes: Iterable[str], freqs: Sequence[str]) -> List[Dict[str, str]]:
    """输入代码和周期，输出 simple_api 股票任务列表。"""
    tasks: List[Dict[str, str]] = []
    for code in codes:
        for freq in freqs:
            start_day, end_day = _window(freq)
            tasks.append(
                {
                    "code": code,
                    "freq": freq,
                    "start_time": start_day,
                    "end_time": end_day,
                }
            )
    return tasks


def _index_tasks(names: Sequence[str], freqs: Sequence[str]) -> List[Dict[str, str]]:
    """输入指数名和周期，输出 simple_api 指数任务列表。"""
    tasks: List[Dict[str, str]] = []
    for name in names:
        for freq in freqs:
            start_day, end_day = _window(freq)
            tasks.append(
                {
                    "index_name": name,
                    "freq": freq,
                    "start_time": start_day,
                    "end_time": end_day,
                }
            )
    return tasks


def _run_stock(label: str, tasks: List[Dict[str, str]], mode: str) -> Dict[str, Any]:
    """输入股票任务和模式，输出该场景摘要。"""
    from zsdtdx import get_stock_kline

    _log(f"[start] {label} tasks={len(tasks)} mode={mode}")
    started = time.perf_counter()
    if mode == "sync":
        payloads = list(get_stock_kline(task=tasks, mode="sync"))
        done = None
    else:
        job = get_stock_kline(task=tasks, mode="async")
        payloads, done = _consume_async(job, timeout=max(180.0, len(tasks) * 8.0))
    elapsed = time.perf_counter() - started
    summary = _summarize(label, payloads, elapsed=elapsed, done=done)
    _log(
        f"[done] {label} elapsed={summary['elapsed_seconds']}s "
        f"conn_errors={summary['conn_errors']} errors={len(summary['errors'])}"
    )
    return summary


def _run_index(label: str, tasks: List[Dict[str, str]], mode: str) -> Dict[str, Any]:
    """输入指数任务和模式，输出该场景摘要。"""
    from zsdtdx import get_index_kline

    _log(f"[start] {label} tasks={len(tasks)} mode={mode}")
    started = time.perf_counter()
    if mode == "sync":
        payloads = list(get_index_kline(task=tasks, mode="sync"))
        done = None
    else:
        job = get_index_kline(task=tasks, mode="async")
        payloads, done = _consume_async(job, timeout=max(180.0, len(tasks) * 8.0))
    elapsed = time.perf_counter() - started
    summary = _summarize(label, payloads, elapsed=elapsed, done=done)
    _log(
        f"[done] {label} elapsed={summary['elapsed_seconds']}s "
        f"conn_errors={summary['conn_errors']} errors={len(summary['errors'])}"
    )
    return summary


def _run_concurrent_jobs(
    std_tasks: List[Dict[str, str]], ex_tasks: List[Dict[str, str]]
) -> Dict[str, Any]:
    """
    输入标准侧和扩展侧任务。
    输出：两个同时运行的 async job 摘要。
    边界：任一侧队列超时视为该场景失败。
    """
    from zsdtdx import get_stock_kline

    _log(f"[start] concurrent_std_ex std={len(std_tasks)} ex={len(ex_tasks)}")
    started = time.perf_counter()
    std_job = get_stock_kline(task=std_tasks, mode="async", queue=queue.Queue())
    ex_job = get_stock_kline(task=ex_tasks, mode="async", queue=queue.Queue())
    std_payloads, std_done = _consume_async(std_job, timeout=240)
    ex_payloads, ex_done = _consume_async(ex_job, timeout=240)
    elapsed = time.perf_counter() - started
    std_summary = _summarize(
        "concurrent_std", std_payloads, elapsed=elapsed, done=std_done
    )
    ex_summary = _summarize("concurrent_ex", ex_payloads, elapsed=elapsed, done=ex_done)
    _log(
        "[done] concurrent_std_ex "
        f"std_errors={len(std_summary['errors'])} ex_errors={len(ex_summary['errors'])}"
    )
    return {
        "label": "concurrent_std_ex",
        "elapsed_seconds": round(elapsed, 3),
        "sides": [std_summary, ex_summary],
    }


def _scenario_failed(summary: Dict[str, Any]) -> bool:
    """输入场景摘要，输出是否存在连接错误、质量错误或活跃标的空数据。"""
    if int(summary.get("conn_errors") or 0) > 0 or list(summary.get("errors") or []):
        return True
    done = dict(summary.get("done") or {})
    if done and int(done.get("failed_tasks") or 0) > 0:
        return True
    return False


def main() -> int:
    """执行全市场、全周期矩阵并写入摘要；失败返回 1。"""
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_client,
        get_stock_code_name,
        set_config_path,
    )

    _ART.mkdir(parents=True, exist_ok=True)
    _log("[start] catalog_and_matrix")
    report: Dict[str, Any] = {
        "started_at": datetime.now().isoformat(timespec="seconds")
    }
    failed = False
    try:
        cfg = set_config_path(
            str(_SRC / "zsdtdx" / "config.yaml"), async_background_probe=False
        )
        report["config_path"] = cfg
        with get_client():
            code_map = get_stock_code_name()
        selected = _select_codes(code_map)
        report["selected_codes"] = {
            market: [{"code": code, "name": code_map.get(code, "")} for code in codes]
            for market, codes in selected.items()
        }
        _log(f"[catalog] {json.dumps(report['selected_codes'], ensure_ascii=False)}")
        for market, codes in selected.items():
            if len(codes) < 2:
                raise RuntimeError(f"{market} 可用样本不足 2 个")

        std_codes = selected["sh"] + selected["sz"] + selected["bj"]
        ex_codes = selected["hk"]
        scenarios = [
            _run_stock(
                "sync_all_markets", _stock_tasks(std_codes + ex_codes, FREQS), "sync"
            ),
            _run_stock("async_std_only", _stock_tasks(std_codes, FREQS), "async"),
            _run_stock("async_ex_only", _stock_tasks(ex_codes, FREQS), "async"),
            _run_stock(
                "async_mixed",
                _stock_tasks(std_codes + ex_codes, ("15", "60", "d")),
                "async",
            ),
            _run_index("sync_index_std", _index_tasks(INDEX_STD, FREQS), "sync"),
            _run_index("async_index_std", _index_tasks(INDEX_STD, FREQS), "async"),
            _run_index("sync_index_ex", _index_tasks(INDEX_EX, FREQS), "sync"),
            _run_index("async_index_ex", _index_tasks(INDEX_EX, FREQS), "async"),
            _run_index(
                "async_index_mixed",
                _index_tasks(INDEX_STD + INDEX_EX, ("15", "d", "w")),
                "async",
            ),
            _run_concurrent_jobs(
                _stock_tasks(std_codes, ("15", "d")),
                _stock_tasks(ex_codes, ("15", "d")),
            ),
        ]
        report["scenarios"] = scenarios
        for scenario in scenarios:
            if scenario["label"] == "concurrent_std_ex":
                failed = failed or any(
                    _scenario_failed(side) for side in scenario["sides"]
                )
            else:
                failed = failed or _scenario_failed(scenario)
        report["passed"] = not failed
    except Exception as exc:
        failed = True
        report["passed"] = False
        report["fatal"] = {
            "error": str(exc),
            "traceback": traceback.format_exc()[-2000:],
        }
        _log(f"[fatal] {exc}")
    finally:
        try:
            report["destroy"] = destroy_parallel_fetcher()
        except Exception as exc:
            report["destroy_error"] = str(exc)
        report["finished_at"] = datetime.now().isoformat(timespec="seconds")
        _write_json(_ART / "report.json", report)
        _log(f"[report] {_ART / 'report.json'}")
        _log(f"[result] {'PASS' if report.get('passed') else 'FAIL'}")
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
