# -*- coding: utf-8 -*-
"""
手工分轮验收 simple_api：每次命令只跑一个 round（由主 Agent 分进程调度）。

用法：
  py tests/manual/live_full_api/run_one.py <round_id>

边界：
1. 禁止在本进程内串联多个 round。
2. async K 线收到 event=done 后立刻 destroy_parallel_fetcher。
3. 报告写入 tests/manual/artifacts/live_full_api/<round_id>.json。
"""

from __future__ import annotations

import json
import re
import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[3]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts" / "live_full_api"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")
TODAY = "2026-09-17"
FREQS = ["15", "30", "60", "d", "w"]
QUEUE_TIMEOUT = 1800.0


def _ensure_test_config() -> Path:
    """复制包内配置并拉长并行超时，供全量 K 线跑完。"""
    src = _SRC / "zsdtdx" / "config.yaml"
    _ART.mkdir(parents=True, exist_ok=True)
    dst = _ART / "live_test_config.yaml"
    text = src.read_text(encoding="utf-8")
    repl = {
        "parallel_total_timeout_seconds: 300": "parallel_total_timeout_seconds: 7200",
        "parallel_result_timeout_seconds: 600": "parallel_result_timeout_seconds: 7200",
        "auto_prewarm_timeout_seconds: 60": "auto_prewarm_timeout_seconds: 180",
        "chunk_timeout_seconds: 15": "chunk_timeout_seconds: 30",
    }
    for old, new in repl.items():
        text = text.replace(old, new)
    dst.write_text(text, encoding="utf-8")
    return dst


def _boot() -> None:
    from zsdtdx import set_config_path

    set_config_path(str(_ensure_test_config()), async_background_probe=True)


def _write_report(round_id: str, payload: Dict[str, Any]) -> Path:
    _ART.mkdir(parents=True, exist_ok=True)
    path = _ART / f"{round_id}.json"
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    print(f"REPORT={path}")
    return path


def _ok(round_id: str, elapsed: float, **extra: Any) -> int:
    payload = {
        "round_id": round_id,
        "ok": True,
        "elapsed_seconds": round(elapsed, 3),
        "today": TODAY,
        **extra,
    }
    _write_report(round_id, payload)
    print(json.dumps({k: payload[k] for k in ("round_id", "ok", "elapsed_seconds") if k in payload}, ensure_ascii=False))
    return 0


def _fail(round_id: str, elapsed: float, error: str, **extra: Any) -> int:
    payload = {
        "round_id": round_id,
        "ok": False,
        "elapsed_seconds": round(elapsed, 3),
        "error": error,
        **extra,
    }
    _write_report(round_id, payload)
    print("FAIL", error)
    return 1


def _check_kline_row(row: Dict[str, Any], freq: str) -> Optional[str]:
    dt_text = str(row.get("datetime", ""))
    if not _DT_RE.match(dt_text):
        return f"datetime格式非法:{dt_text}"
    try:
        o, h, l, c = (
            float(row.get("open")),
            float(row.get("high")),
            float(row.get("low")),
            float(row.get("close")),
        )
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
    bar_day = dt_text[:10]
    if freq in {"15", "30", "60", "d"} and bar_day != TODAY:
        return f"日期不是今日:{bar_day}"
    if freq == "w":
        bar = date.fromisoformat(bar_day)
        today = date.fromisoformat(TODAY)
        if abs((today - bar).days) > 10:
            return f"周线日期偏离过大:{bar_day}"
    return None


def _consume_async(job: Any, *, expected_tasks: int) -> Dict[str, Any]:
    from zsdtdx import destroy_parallel_fetcher

    q = job.queue
    stats: Dict[str, Any] = {
        "data_events": 0,
        "ok_tasks": 0,
        "empty_rows": 0,
        "error_tasks": 0,
        "ohlc_bad": 0,
        "dt_bad": 0,
        "error_samples": [],
        "ohlc_samples": [],
        "done": {},
        "destroy": {},
    }
    t0 = time.time()
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
        rows = list(event.get("rows") or [])
        err = event.get("error")
        task = dict(event.get("task") or {})
        freq = str(task.get("freq", "")).strip()
        if err:
            stats["error_tasks"] += 1
            if len(stats["error_samples"]) < 8:
                stats["error_samples"].append(
                    {"task": task, "error": str(err)[:240]}
                )
        elif not rows:
            stats["empty_rows"] += 1
        else:
            bad = None
            for row in rows:
                if not isinstance(row, dict):
                    bad = "row非dict"
                    break
                bad = _check_kline_row(row, freq)
                if bad:
                    break
            if bad:
                stats["ohlc_bad"] += 1
                if len(stats["ohlc_samples"]) < 8:
                    stats["ohlc_samples"].append({"task": task, "reason": bad})
            else:
                stats["ok_tasks"] += 1
        if stats["data_events"] % 400 == 0:
            print(
                f"progress events={stats['data_events']}/{expected_tasks} "
                f"ok={stats['ok_tasks']} empty={stats['empty_rows']} "
                f"err={stats['error_tasks']} elapsed={time.time()-t0:.1f}s",
                flush=True,
            )
    stats["destroy"] = dict(destroy_parallel_fetcher())
    try:
        job.result()
    except Exception as exc:
        stats["job_result_error"] = str(exc)
    stats["expected_tasks"] = expected_tasks
    stats["coverage"] = (
        None
        if expected_tasks <= 0
        else round(stats["data_events"] / expected_tasks, 4)
    )
    return stats


def round_set_config_path() -> Dict[str, Any]:
    from zsdtdx import set_config_path

    path = set_config_path(str(_ensure_test_config()), async_background_probe=True)
    return {"config_path": path, "exists": Path(path).is_file()}


def round_get_client() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client

    with get_client() as client:
        return {"client_type": type(client).__name__, "entered": True}


def round_get_supported_markets() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_supported_markets

    with get_client():
        rows = get_supported_markets(return_df=False)
    names = sorted({str(r.get("name", "")) for r in rows})
    sources = sorted({str(r.get("source", "")) for r in rows})
    need = {"深圳", "上海", "北京", "香港主板", "上海期货"}
    missing = sorted(need - set(names))
    if missing:
        raise RuntimeError(f"市场名缺失: {missing}")
    return {"n": len(rows), "sources": sources, "sample_names": names[:12], "has_required": True}


def round_get_stock_code_name() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_stock_code_name

    with get_client():
        mp = get_stock_code_name(use_cache=True)
    prefixes = {}
    for k in mp:
        prefixes[k.split(".", 1)[0]] = prefixes.get(k.split(".", 1)[0], 0) + 1
    if "sh.600000" not in mp or "sz.000001" not in mp:
        raise RuntimeError("缺少浦发银行或平安银行")
    if any("退" in str(v) for v in mp.values()):
        raise RuntimeError("码表名称含退")
    return {"n": len(mp), "prefixes": prefixes, "yaml_scope": "szsh"}


def round_get_all_future_list() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_all_future_list, get_client

    with get_client():
        rows = get_all_future_list(return_df=False, use_cache=False)
    markets = sorted({str(r.get("market_name", "")) for r in rows})
    expect = {"郑州商品", "大连商品", "上海期货", "广州期货"}
    if set(markets) != expect:
        raise RuntimeError(f"期货市场不符: {markets}")
    codes = {str(r.get("code", "")).upper() for r in rows}
    if "CUL8" not in codes:
        raise RuntimeError("缺少CUL8")
    zhulian = sum(1 for r in rows if "主连" in str(r.get("name", "")))
    return {"n": len(rows), "markets": markets, "zhulian": zhulian}


def round_get_stock_latest_price() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_stock_latest_price

    with get_client() as client:
        rows = client.get_all_stock_list(return_df=False)
        codes = [str(r.get("code", "")).strip() for r in rows if str(r.get("code", "")).strip()]
        default_map = get_stock_latest_price()
        full_map = get_stock_latest_price(codes)
    def_ok = sum(1 for v in default_map.values() if v is not None and float(v) > 0)
    full_ok = sum(1 for v in full_map.values() if v is not None and float(v) > 0)
    if default_map.get("600000") in (None, 0) and default_map.get("000001") in (None, 0):
        # 收盘后仍应有最新价
        if def_ok < 1000:
            raise RuntimeError("默认szsh最新价有效数量过低")
    return {
        "default_n": len(default_map),
        "default_positive": def_ok,
        "full_n": len(full_map),
        "full_positive": full_ok,
        "sample_600000": default_map.get("600000"),
    }


def round_get_future_latest_price() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_future_latest_price

    with get_client():
        mp = get_future_latest_price()
        cu = get_future_latest_price("CU")
    pos = sum(1 for v in mp.values() if v is not None and float(v) > 0)
    if "CUL8" not in cu or cu.get("CUL8") in (None, 0):
        raise RuntimeError(f"CU主连最新价异常: {cu}")
    return {"n": len(mp), "positive": pos, "CUL8": cu.get("CUL8")}


def round_get_company_info() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_company_info

    out = {}
    with get_client():
        for code in ("600000", "000001"):
            rows = get_company_info(codes=[code], return_df=False, mode="sync")
            cats = sorted({str(r.get("category", "")) for r in (rows or [])})
            out[code] = {"n": len(rows or []), "categories": cats[:8]}
            if not rows:
                raise RuntimeError(f"{code} 公司信息为空")
            if not any(str(r.get("content", "")).strip() for r in rows):
                raise RuntimeError(f"{code} 公司信息正文全空")
        bj_rows = get_company_info(codes=["920002"], return_df=False, mode="sync")
        out["920002"] = {
            "n": len(bj_rows or []),
            "note": "北交所 HQ F10 目录可为空（服务端无公司信息分类）",
        }
    return out


def round_get_runtime_failures() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_runtime_failures, get_stock_latest_price

    with get_client():
        get_stock_latest_price("600000")
        df = get_runtime_failures()
    return {"type": type(df).__name__, "rows": int(getattr(df, "shape", [0])[0])}


def round_get_runtime_metadata() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_runtime_metadata

    with get_client():
        meta = get_runtime_metadata()
    if not isinstance(meta, dict) or not meta:
        raise RuntimeError("runtime metadata 为空")
    return {"keys": sorted(meta.keys()), "std_active_host": meta.get("std_active_host"), "config_path": meta.get("config_path")}


def round_prewarm() -> Dict[str, Any]:
    _boot()
    from zsdtdx import destroy_parallel_fetcher, prewarm_parallel_fetcher

    try:
        info = prewarm_parallel_fetcher()
        return {"prewarm": info}
    finally:
        destroy_parallel_fetcher()


def round_restart() -> Dict[str, Any]:
    _boot()
    from zsdtdx import destroy_parallel_fetcher, restart_parallel_fetcher

    try:
        info = restart_parallel_fetcher(prewarm=True, prewarm_timeout_seconds=180, max_rounds=3)
        return {"restart": info}
    finally:
        destroy_parallel_fetcher()


def round_destroy() -> Dict[str, Any]:
    _boot()
    from zsdtdx import destroy_parallel_fetcher, prewarm_parallel_fetcher

    prewarm_parallel_fetcher()
    info = destroy_parallel_fetcher()
    again = destroy_parallel_fetcher()
    return {"first": info, "second_idempotent": again}


def _run_stock_kline(codes: List[str], label: str) -> Dict[str, Any]:
    from zsdtdx import get_stock_kline

    tasks = [
        {"code": code, "freq": freq, "start_time": TODAY, "end_time": TODAY}
        for code in codes
        for freq in FREQS
    ]
    print(f"{label} tasks={len(tasks)} codes={len(codes)}", flush=True)
    job = get_stock_kline(task=tasks, mode="async")
    stats = _consume_async(job, expected_tasks=len(tasks))
    stats["codes"] = len(codes)
    stats["freqs"] = FREQS
    if stats["data_events"] < max(1, int(len(tasks) * 0.5)):
        raise RuntimeError(f"{label} 回收任务过少: {stats}")
    return stats


def round_kline_ashare() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client

    with get_client() as client:
        rows = client.get_all_stock_list(return_df=False)
    codes = [str(r["code"]).strip() for r in rows if str(r.get("source")) == "std"]
    return _run_stock_kline(codes, "ashare")


def round_kline_hk() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client

    with get_client() as client:
        rows = client.get_all_stock_list(return_df=False)
    codes = [str(r["code"]).strip() for r in rows if str(r.get("source")) == "ex"]
    if len(codes) < 1000:
        raise RuntimeError(f"港股代码过少: {len(codes)}，检查 include_hk_market_names")
    return _run_stock_kline(codes, "hk")


def round_kline_future() -> Dict[str, Any]:
    _boot()
    from zsdtdx import destroy_parallel_fetcher, get_client, get_future_kline

    t0 = time.time()
    try:
        with get_client():
            df = get_future_kline(
                codes=None,
                freq=FREQS,
                start_time=TODAY,
                end_time=TODAY,
            )
    finally:
        destroy_parallel_fetcher()
    n = 0 if df is None else int(len(df))
    bad = 0
    samples = []
    if df is not None and not df.empty:
        for rec in df.to_dict(orient="records"):
            row = {
                "datetime": str(rec.get("datetime", "")),
                "open": rec.get("open"),
                "high": rec.get("high"),
                "low": rec.get("low"),
                "close": rec.get("close"),
                "volume": rec.get("volume"),
            }
            reason = _check_kline_row(row, str(rec.get("freq", "")))
            if reason:
                bad += 1
                if len(samples) < 8:
                    samples.append({"code": rec.get("code"), "freq": rec.get("freq"), "reason": reason})
    codes = [] if df is None or df.empty else sorted(set(df["code"].astype(str)))
    freqs = [] if df is None or df.empty else sorted(set(df["freq"].astype(str)))
    if n < 100:
        raise RuntimeError(f"期货K线行数过少: {n}")
    return {
        "rows": n,
        "unique_codes": len(codes),
        "freqs": freqs,
        "ohlc_bad": bad,
        "ohlc_samples": samples,
        "has_CUL8": "CUL8" in codes,
        "inner_elapsed": round(time.time() - t0, 3),
    }


def round_kline_index() -> Dict[str, Any]:
    _boot()
    from zsdtdx import IndexKlineTask, get_client, get_index_kline

    with get_client() as client:
        records = client._discover_index_route_records(refresh=False)
    names: List[str] = []
    seen = set()
    for rec in records:
        name = str(rec.get("name", "")).strip()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    tasks = [
        IndexKlineTask(index_name=name, freq=freq, start_time=TODAY, end_time=TODAY).to_dict()
        for name in names
        for freq in FREQS
    ]
    print(f"index names={len(names)} tasks={len(tasks)}", flush=True)
    job = get_index_kline(task=tasks, mode="async")
    stats = _consume_async(job, expected_tasks=len(tasks))
    stats["index_names"] = len(names)
    if stats["data_events"] < max(1, int(len(tasks) * 0.3)):
        raise RuntimeError(f"指数回收过少: {stats}")
    return stats


ROUNDS = {
    "set_config_path": round_set_config_path,
    "get_client": round_get_client,
    "get_supported_markets": round_get_supported_markets,
    "get_stock_code_name": round_get_stock_code_name,
    "get_all_future_list": round_get_all_future_list,
    "get_stock_latest_price": round_get_stock_latest_price,
    "get_future_latest_price": round_get_future_latest_price,
    "get_company_info": round_get_company_info,
    "get_runtime_failures": round_get_runtime_failures,
    "get_runtime_metadata": round_get_runtime_metadata,
    "prewarm_parallel_fetcher": round_prewarm,
    "restart_parallel_fetcher": round_restart,
    "destroy_parallel_fetcher": round_destroy,
    "kline_ashare": round_kline_ashare,
    "kline_hk": round_kline_hk,
    "kline_future": round_kline_future,
    "kline_index": round_kline_index,
}


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in ROUNDS:
        print("usage: py tests/manual/live_full_api/run_one.py <round_id>")
        print("rounds:", " ".join(ROUNDS))
        return 2
    round_id = sys.argv[1]
    t0 = time.time()
    try:
        extra = ROUNDS[round_id]()
        return _ok(round_id, time.time() - t0, result=extra)
    except Exception:
        return _fail(round_id, time.time() - t0, traceback.format_exc())


if __name__ == "__main__":
    raise SystemExit(main())
