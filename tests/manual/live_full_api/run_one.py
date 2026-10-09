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
import math
import re
import sys
import time
import traceback
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[3]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts" / "live_full_api"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")
START_DAY = "2026-09-14"
END_DAY = "2026-09-18"
TODAY = END_DAY
FREQS = ["15", "30", "60", "d", "w"]
QUEUE_TIMEOUT = 1800.0


def _ensure_test_config() -> Path:
    """复制包内配置并拉长并行超时，供全量 K 线跑完。"""
    src = _SRC / "zsdtdx" / "config.yaml"
    _ART.mkdir(parents=True, exist_ok=True)
    cache_dir = (_ART / "catalog_cache").resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    dst = _ART / "live_test_config.yaml"
    text = src.read_text(encoding="utf-8")
    repl = {
        "chunk_timeout_seconds: 15": "chunk_timeout_seconds: 30",
        '  path: ""': f'  path: "{cache_dir.as_posix()}"',
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
    print(
        json.dumps(
            {
                k: payload[k]
                for k in ("round_id", "ok", "elapsed_seconds")
                if k in payload
            },
            ensure_ascii=False,
        )
    )
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
        o, h, low, c = (
            float(row.get("open")),
            float(row.get("high")),
            float(row.get("low")),
            float(row.get("close")),
        )
    except Exception:
        return "OHLC无法转float"
    if not all(math.isfinite(value) for value in (o, h, low, c)):
        return "OHLC包含非有限值"
    if min(o, h, low, c) <= 0:
        return "OHLC包含非正值"
    if h < low:
        return "high<low"
    if h + 1e-9 < max(o, c) or low - 1e-9 > min(o, c):
        return "OHLC不自洽"
    vol = row.get("volume")
    if vol is not None:
        try:
            vol_value = float(vol)
            if not math.isfinite(vol_value) or vol_value < 0:
                return "volume<0"
        except Exception:
            return "volume非法"
    amount = row.get("amount")
    if amount is not None:
        try:
            amount_value = float(amount)
            if not math.isfinite(amount_value) or amount_value < 0:
                return f"amount非法:{amount!r}"
        except Exception:
            return f"amount无法转float:{amount!r}"
    bar_day = dt_text[:10]
    if freq in {"15", "30", "60", "d"} and not (START_DAY <= bar_day <= END_DAY):
        return f"日期越界:{bar_day}"
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
        "empty_by_freq": {},
        "error_tasks": 0,
        "error_by_reason": {},
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
        if str(err or "").strip().lower() == "no_data":
            stats["empty_rows"] += 1
            stats["empty_by_freq"][freq] = stats["empty_by_freq"].get(freq, 0) + 1
        elif err:
            stats["error_tasks"] += 1
            reason = str(err).split(":", 1)[0][:80]
            stats["error_by_reason"][reason] = (
                stats["error_by_reason"].get(reason, 0) + 1
            )
            if len(stats["error_samples"]) < 8:
                stats["error_samples"].append({"task": task, "error": str(err)[:240]})
        elif not rows:
            stats["empty_rows"] += 1
            stats["empty_by_freq"][freq] = stats["empty_by_freq"].get(freq, 0) + 1
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
                f"err={stats['error_tasks']} elapsed={time.time() - t0:.1f}s",
                flush=True,
            )
    stats["destroy"] = dict(destroy_parallel_fetcher())
    try:
        job.result()
    except Exception as exc:
        stats["job_result_error"] = str(exc)
    stats["expected_tasks"] = expected_tasks
    stats["coverage"] = (
        None if expected_tasks <= 0 else round(stats["data_events"] / expected_tasks, 4)
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
    from zsdtdx import get_client

    with get_client() as client:
        rows = client.get_supported_markets(return_df=False)
    names = sorted({str(r.get("name", "")) for r in rows})
    sources = sorted({str(r.get("source", "")) for r in rows})
    need = {"深圳", "上海", "北京", "港股通", "上海期货"}
    missing = sorted(need - set(names))
    if missing:
        raise RuntimeError(f"市场名缺失: {missing}")
    return {
        "n": len(rows),
        "sources": sources,
        "sample_names": names[:12],
        "has_required": True,
    }


def round_get_stock_code_name() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_stock_code_name

    with get_client():
        mp = get_stock_code_name()
    prefixes = {}
    for k in mp:
        prefixes[k.split(".", 1)[0]] = prefixes.get(k.split(".", 1)[0], 0) + 1
    if "sh.600000" not in mp or "sz.000001" not in mp:
        raise RuntimeError("缺少浦发银行或平安银行")
    bad_names = [
        str(code)
        for code, name in mp.items()
        if not str(name).strip() or "\ufffd" in str(name)
    ]
    bad_codes = [
        str(code)
        for code in mp
        if not re.match(r"^(sz|sh|bj|hk)\.\d+$", str(code), re.I)
    ]
    if bad_names or bad_codes:
        raise RuntimeError(
            f"股票码表奇异值: bad_names={bad_names[:8]} bad_codes={bad_codes[:8]}"
        )
    retired_names = sorted({str(v) for v in mp.values() if "退" in str(v)})
    return {
        "n": len(mp),
        "prefixes": prefixes,
        "yaml_scope": "szsh",
        "retired_name_count": len(retired_names),
        "retired_name_samples": retired_names[:8],
        "bad_name_count": len(bad_names),
        "bad_code_count": len(bad_codes),
    }


def round_get_stock_concepts() -> Dict[str, Any]:
    """输入无，输出全量股票板块归属扫描；边界：板块名与股票名均不得为空。"""
    _boot()
    from zsdtdx import get_client, get_stock_concepts

    with get_client():
        payload = get_stock_concepts()
    names = list(payload.get("names") or [])
    mapping = dict(payload.get("map") or {})
    name_set = set(names)
    empty_stock = [str(k) for k in mapping if not str(k).strip()]
    empty_blocks = [
        str(stock)
        for stock, blocks in mapping.items()
        if not blocks or any(not str(item).strip() for item in blocks)
    ]
    unknown_blocks = [
        f"{stock}:{block}"
        for stock, blocks in mapping.items()
        for block in blocks
        if block not in name_set
    ]
    if len(names) < 100 or len(mapping) < 100:
        raise RuntimeError(f"板块归属规模过小: names={len(names)} map={len(mapping)}")
    if len(names) != len(name_set) or empty_stock or empty_blocks or unknown_blocks:
        raise RuntimeError(
            "板块归属奇异值: "
            f"dup_names={len(names) - len(name_set)} empty_stock={empty_stock[:8]} "
            f"empty_blocks={empty_blocks[:8]} unknown={unknown_blocks[:8]}"
        )
    return {
        "name_count": len(names),
        "stock_count": len(mapping),
        "duplicate_name_count": len(names) - len(name_set),
        "sample": {k: mapping[k] for k in list(mapping)[:5]},
    }


def round_get_etf_code_name() -> Dict[str, Any]:
    """输入无，输出全量 ETF/LOF 码表扫描；边界：仅接受沪深前缀及非空名称。"""
    _boot()
    from zsdtdx import get_client, get_etf_code_name

    with get_client():
        mp = get_etf_code_name()
    bad_codes = [
        str(code)
        for code in mp
        if not re.match(r"^(sz|sh)\.\d+$", str(code), re.I)
        or str(code).split(".", 1)[-1].startswith("399")
    ]
    bad_names = [
        str(code)
        for code, name in mp.items()
        if not str(name).strip() or "\ufffd" in str(name)
    ]
    if len(mp) < 100:
        raise RuntimeError(f"ETF/LOF 码表规模过小: {len(mp)}")
    if bad_codes or bad_names:
        raise RuntimeError(
            f"ETF/LOF 奇异值: bad_codes={bad_codes[:8]} bad_names={bad_names[:8]}"
        )
    return {
        "n": len(mp),
        "bad_code_count": len(bad_codes),
        "bad_name_count": len(bad_names),
        "sample": {k: mp[k] for k in list(mp)[:8]},
    }


def round_get_block_names() -> Dict[str, Any]:
    """输入无，输出全量板块名称扫描；边界：名称必须唯一、非空且无替换符。"""
    _boot()
    from zsdtdx import get_block_names, get_client

    with get_client():
        names = list(get_block_names() or [])
    bad = [name for name in names if not str(name).strip() or "\ufffd" in str(name)]
    duplicate_count = len(names) - len(set(names))
    if len(names) < 100:
        raise RuntimeError(f"板块名称规模过小: {len(names)}")
    if bad or duplicate_count:
        raise RuntimeError(
            f"板块名称奇异值: bad={bad[:8]} duplicate_count={duplicate_count}"
        )
    return {
        "n": len(names),
        "duplicate_count": duplicate_count,
        "bad_name_count": len(bad),
        "sample": names[:12],
    }


def round_get_all_future_list() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_all_future_list, get_client

    with get_client():
        rows = get_all_future_list(return_df=False)
    markets = sorted({str(r.get("market_name", "")) for r in rows})
    expect = {"郑州商品", "大连商品", "上海期货", "广州期货"}
    if set(markets) != expect:
        raise RuntimeError(f"期货市场不符: {markets}")
    codes = {str(r.get("code", "")).upper() for r in rows}
    if "CUL8" not in codes:
        raise RuntimeError("缺少CUL8")
    zhulian = sum(1 for r in rows if "主连" in str(r.get("name", "")))
    return {"n": len(rows), "markets": markets, "zhulian": zhulian}


def round_get_stock_stat() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_stock_stat
    from zsdtdx.biz.stock_stat import STOCK_STAT_COLUMN_LABELS

    with get_client():
        df = get_stock_stat()
    n = int(len(df))
    if n < 1000:
        raise RuntimeError(f"全市场宽表行数过低: {n}")
    code_col = STOCK_STAT_COLUMN_LABELS["code"]
    price_col = STOCK_STAT_COLUMN_LABELS["price"]
    by_code = {str(r[code_col]): r for _, r in df.iterrows()}
    sample = {}
    for code in ("600000", "000001"):
        row = by_code.get(code)
        if row is None:
            raise RuntimeError(f"缺少 {code}")
        px = row.get(price_col)
        if px is None or float(px) <= 0:
            raise RuntimeError(f"{code} 价异常: {px}")
        sample[code] = float(px)
    pos = int((df[price_col].fillna(0).astype(float) > 0).sum())
    numeric_anomalies = {}
    for column in (
        price_col,
        STOCK_STAT_COLUMN_LABELS["open"],
        STOCK_STAT_COLUMN_LABELS["high"],
        STOCK_STAT_COLUMN_LABELS["low"],
    ):
        values = df[column].dropna().astype(float)
        bad_count = int((~values.map(math.isfinite)).sum())
        if bad_count:
            numeric_anomalies[column] = bad_count
    if numeric_anomalies:
        raise RuntimeError(f"全市场宽表存在非有限数: {numeric_anomalies}")
    return {
        "n": n,
        "positive_price": pos,
        "non_positive_price": n - pos,
        "numeric_anomalies": numeric_anomalies,
        "sample": sample,
    }


def round_get_future_latest_price() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_future_latest_price, get_runtime_failures

    with get_client():
        mp = get_future_latest_price()
        cu = get_future_latest_price("CU")
        failures = get_runtime_failures()
    pos = sum(1 for v in mp.values() if v is not None and float(v) > 0)
    non_finite = [
        str(code)
        for code, value in mp.items()
        if value is not None and not math.isfinite(float(value))
    ]
    non_positive = [
        str(code)
        for code, value in mp.items()
        if value is not None and math.isfinite(float(value)) and float(value) <= 0
    ]
    if len(mp) < 100:
        raise RuntimeError(f"期货最新价样本过少: {len(mp)}")
    if non_finite or non_positive:
        raise RuntimeError(
            f"期货最新价奇异值: non_finite={non_finite[:8]} non_positive={non_positive[:8]}"
        )
    if "CUL8" not in cu:
        raise RuntimeError(f"CU 主连路由缺失: {cu}")
    cu_failure = failures[
        (failures["code"] == "CUL8") & (failures["reason"] == "no_valid_quote")
    ]
    if cu.get("CUL8") is None and cu_failure.empty:
        raise RuntimeError(f"CU 主连无报价但缺少失败明细: {cu}")
    return {
        "n": len(mp),
        "positive": pos,
        "unavailable": len(mp) - pos,
        "non_finite": len(non_finite),
        "non_positive": len(non_positive),
        "CUL8": cu.get("CUL8"),
        "CUL8_no_valid_quote": not cu_failure.empty,
    }


def round_get_company_info() -> Dict[str, Any]:
    """输入无，输出 100 只沪深股票 F10 实盘统计；边界：仅拉最新提示和公司概况。"""
    _boot()
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_client,
        get_company_info,
        get_stock_code_name,
    )

    with get_client():
        stock_map = get_stock_code_name()
    candidates = [
        code.split(".", 1)[1]
        for code, name in sorted(stock_map.items())
        if code.startswith(("sz.", "sh.")) and "退" not in str(name)
    ]
    step = max(1, len(candidates) // 100)
    codes = candidates[::step][:100]
    if len(codes) < 100:
        raise RuntimeError(f"公司信息候选样本不足: {len(codes)}")

    job = get_company_info(
        codes=codes,
        category=["最新提示", "公司概况"],
        return_df=False,
        mode="async",
    )
    events = 0
    errors = []
    empty = []
    rows_by_code: Dict[str, int] = {}
    done = {}
    try:
        while True:
            event = job.queue.get(timeout=QUEUE_TIMEOUT)
            kind = str(event.get("event", "")).strip().lower()
            if kind == "done":
                done = dict(event)
                break
            if kind != "data":
                continue
            events += 1
            code = str(event.get("code", ""))
            err = event.get("error")
            rows = list(event.get("rows") or [])
            if err:
                errors.append({"code": code, "error": str(err)[:240]})
            elif not rows:
                empty.append(code)
            else:
                bad_rows = [
                    row
                    for row in rows
                    if not str(row.get("category", "")).strip()
                    or not str(row.get("content", "")).strip()
                    or "\ufffd" in str(row.get("content", ""))
                ]
                if bad_rows:
                    errors.append({"code": code, "error": "分类/正文为空或含替换符"})
                rows_by_code[code] = len(rows)
        result_rows = list(job.result() or [])
    finally:
        destroy_parallel_fetcher()

    missing = sorted(
        set(codes) - set(rows_by_code) - set(empty) - {x["code"] for x in errors}
    )
    if events != len(codes) or errors or empty or missing:
        raise RuntimeError(
            f"公司信息异常: events={events}/100 errors={errors[:8]} "
            f"empty={empty[:8]} missing={missing[:8]}"
        )
    return {
        "sample_codes": len(codes),
        "data_events": events,
        "rows": len(result_rows),
        "empty": len(empty),
        "errors": len(errors),
        "done": done,
        "row_count_sample": dict(list(rows_by_code.items())[:8]),
    }


def round_get_runtime_failures() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_runtime_failures

    with get_client():
        df = get_runtime_failures()
    expected = {"timestamp", "task", "code", "freq", "reason", "detail"}
    columns = set(getattr(df, "columns", []))
    if not expected.issubset(columns):
        raise RuntimeError(f"runtime failures 字段不完整: {sorted(columns)}")
    return {
        "type": type(df).__name__,
        "rows": int(getattr(df, "shape", [0])[0]),
        "columns": list(df.columns),
    }


def round_get_runtime_metadata() -> Dict[str, Any]:
    _boot()
    from zsdtdx import get_client, get_runtime_metadata

    with get_client():
        meta = get_runtime_metadata()
    if not isinstance(meta, dict) or not meta:
        raise RuntimeError("runtime metadata 为空")
    return {
        "keys": sorted(meta.keys()),
        "std_active_host": meta.get("std_active_host"),
        "config_path": meta.get("config_path"),
    }


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
        info = restart_parallel_fetcher(
            prewarm=True, prewarm_timeout_seconds=180, max_rounds=3
        )
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
        {"code": code, "freq": freq, "start_time": START_DAY, "end_time": END_DAY}
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
    if stats["error_tasks"] or stats["ohlc_bad"]:
        raise RuntimeError(f"{label} 存在错误或奇异 K 线: {stats}")
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
        raise RuntimeError(f"港股通代码过少: {len(codes)}")
    return _run_stock_kline(codes, "hk")


def round_kline_future() -> Dict[str, Any]:
    _boot()
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_all_future_list,
        get_client,
        get_future_kline,
    )

    t0 = time.time()
    try:
        with get_client():
            catalog = get_all_future_list(return_df=False)
            all_codes = sorted({str(row.get("code", "")).strip() for row in catalog})
            all_codes = [code for code in all_codes if code]
            step = max(1, len(all_codes) // 120)
            requested_codes = all_codes[::step][:120]
            df = get_future_kline(
                codes=requested_codes,
                freq=FREQS,
                start_time=START_DAY,
                end_time=END_DAY,
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
                    samples.append(
                        {
                            "code": rec.get("code"),
                            "freq": rec.get("freq"),
                            "reason": reason,
                        }
                    )
    codes = [] if df is None or df.empty else sorted(set(df["code"].astype(str)))
    freqs = [] if df is None or df.empty else sorted(set(df["freq"].astype(str)))
    if n < 100:
        raise RuntimeError(f"期货K线行数过少: {n}")
    return {
        "rows": n,
        "catalog_codes": len(all_codes),
        "requested_codes": len(requested_codes),
        "unique_codes": len(codes),
        "missing_codes": sorted(set(requested_codes) - set(codes)),
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
        if name and name not in seen and not re.match(r"^\d{2}", name):
            seen.add(name)
            names.append(name)
    catalog_names = len(names)
    step = max(1, catalog_names // 120)
    names = names[::step][:120]
    tasks = [
        IndexKlineTask(
            index_name=name, freq=freq, start_time=START_DAY, end_time=END_DAY
        ).to_dict()
        for name in names
        for freq in FREQS
    ]
    print(f"index names={len(names)} tasks={len(tasks)}", flush=True)
    job = get_index_kline(task=tasks, mode="async")
    stats = _consume_async(job, expected_tasks=len(tasks))
    stats["index_names"] = len(names)
    stats["filtered_catalog_names"] = catalog_names
    if stats["data_events"] < max(1, int(len(tasks) * 0.3)):
        raise RuntimeError(f"指数回收过少: {stats}")
    if stats["error_tasks"] or stats["ohlc_bad"]:
        raise RuntimeError(f"指数存在错误或奇异 K 线: {stats}")
    return stats


ROUNDS = {
    "set_config_path": round_set_config_path,
    "get_client": round_get_client,
    "get_supported_markets": round_get_supported_markets,
    "get_stock_code_name": round_get_stock_code_name,
    "get_stock_concepts": round_get_stock_concepts,
    "get_etf_code_name": round_get_etf_code_name,
    "get_block_names": round_get_block_names,
    "get_all_future_list": round_get_all_future_list,
    "get_stock_stat": round_get_stock_stat,
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
