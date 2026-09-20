"""
模块：`run_simple_api_full_acceptance.py`。

职责：
1. 对 simple_api 做多市场全量验收：京沪深/港股、指定指数、期货。
2. 覆盖全部支持 K 线周期（5/15/30/60/d/w/m），校验 OHLC 与时间完备性。
3. 对京沪深 F10 按目录 length 校验正文完整度。

边界：
1. 参数写死在本文件，不暴露 CLI。
2. 需访问银河标准/扩展行情；结果写入 artifacts JSON。
3. 1 分钟周期当前 PERIOD_MAP 不支持，不测。
"""

from __future__ import annotations

import json
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts" / "simple_api_full_acceptance"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")

# 覆盖全部支持周期；窗口需能覆盖周/月线。
FREQS = ["5", "15", "30", "60", "d", "w", "m"]
START = "2026-08-01"
END = "2026-09-18"

STOCKS = {
    "sz": "000001",
    "sh": "600000",
    "bj": "920000",
    "hk": "09988",
}
INDICES = ["中证1000", "科创50", "沪深300"]
FUTURES = ["CU", "AL"]
F10_CODES = ["000001", "600000", "920000"]  # 仅京沪深


def _check_ohlc(row: Dict[str, Any]) -> Optional[str]:
    """
    输入：K 线行。
    输出：通过返回 None，否则原因。
    用途：校验 datetime/OHLC/volume。
    边界：缺字段或非正判失败。
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
        if float(row.get("volume", 0)) < 0:
            return "volume<0"
    except Exception:
        return "volume非法"
    return None


def _expect_min_rows(freq: str) -> int:
    """
    输入：周期。
    输出：期望最少根数（宽松下界）。
    用途：完备性粗检。
    边界：停牌票可能低于下界，记为 warn 而非硬失败时可再分。
    """
    return {
        "5": 20,
        "15": 10,
        "30": 8,
        "60": 4,
        "d": 5,
        "w": 1,
        "m": 1,
    }.get(freq, 1)


def main() -> int:
    """
    输入：无。
    输出：成功 0，有问题 1。
    用途：全量验收入口。
    边界：异常写入 report.errors。
    """
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_all_future_list,
        get_client,
        get_company_info,
        get_future_kline,
        get_future_latest_price,
        get_index_kline,
        get_runtime_failures,
        get_runtime_metadata,
        get_stock_code_name,
        get_stock_kline,
        get_stock_latest_price,
        get_supported_markets,
        set_config_path,
    )

    _ART.mkdir(parents=True, exist_ok=True)
    problems: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    sections: Dict[str, Any] = {}
    t0 = time.perf_counter()

    def prob(section: str, detail: str, **extra: Any) -> None:
        item = {"section": section, "detail": detail, **extra}
        problems.append(item)
        print(f"PROBLEM [{section}] {detail}", flush=True)

    def warn(section: str, detail: str, **extra: Any) -> None:
        item = {"section": section, "detail": detail, **extra}
        warnings.append(item)
        print(f"WARN [{section}] {detail}", flush=True)

    cfg = set_config_path(str(_SRC / "zsdtdx" / "config.yaml"), async_background_probe=False)
    print(f"config={cfg}", flush=True)

    try:
        _run_body(
            problems,
            warnings,
            sections,
            prob,
            warn,
            get_supported_markets,
            get_stock_code_name,
            get_all_future_list,
            get_stock_latest_price,
            get_future_latest_price,
            get_client,
            get_stock_kline,
            get_index_kline,
            get_future_kline,
            get_company_info,
            get_runtime_failures,
            get_runtime_metadata,
        )
    finally:
        # 期货/async 会起多进程池；无论成败都必须销毁，避免残留 worker 卡死。
        try:
            info = destroy_parallel_fetcher()
            print(f"destroy_parallel_fetcher={info}", flush=True)
        except Exception as exc:
            print(f"destroy_parallel_fetcher_fail={exc}", flush=True)
            warnings.append({"section": "cleanup", "detail": f"destroy失败:{exc}"})

    report = {
        "ok": len(problems) == 0,
        "problem_count": len(problems),
        "warning_count": len(warnings),
        "problems": problems,
        "warnings": warnings,
        "sections": sections,
        "freqs": FREQS,
        "window": {"start": START, "end": END},
        "elapsed_seconds": round(time.perf_counter() - t0, 3),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    out = _ART / "summary.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "ok": report["ok"],
        "problems": len(problems),
        "warnings": len(warnings),
        "elapsed": report["elapsed_seconds"],
        "path": str(out),
    }, ensure_ascii=False), flush=True)
    return 0 if report["ok"] else 1


def _run_body(
    problems,
    warnings,
    sections,
    prob,
    warn,
    get_supported_markets,
    get_stock_code_name,
    get_all_future_list,
    get_stock_latest_price,
    get_future_latest_price,
    get_client,
    get_stock_kline,
    get_index_kline,
    get_future_kline,
    get_company_info,
    get_runtime_failures,
    get_runtime_metadata,
) -> None:
    """
    输入：收集器与 API 符号。
    输出：无；结果写入 problems/warnings/sections。
    用途：主体验收逻辑，便于外层 finally 回收进程池。
    边界：不负责 destroy_parallel_fetcher。
    """
    # ---- markets / catalogs ----
    with get_client() as client:
        markets = get_supported_markets(return_df=False)
        names = {str(r.get("name", "")).strip() for r in markets}
        for need in ("深圳", "上海", "北京"):
            if need not in names:
                prob("get_supported_markets", f"缺少市场 {need}")
        sections["markets"] = {"n": len(markets), "has_sz_sh_bj": all(x in names for x in ("深圳", "上海", "北京"))}

        code_map = get_stock_code_name(use_cache=True)
        sections["stock_code_name"] = {
            "n": len(code_map),
            "has_000001": bool(code_map.get("sz.000001") or code_map.get("000001")),
            "has_600000": bool(code_map.get("sh.600000") or code_map.get("600000")),
            "note": "默认 scope=szsh，不含 bj/hk 属配置预期",
        }
        if not (code_map.get("sz.000001") or code_map.get("000001")):
            prob("get_stock_code_name", "缺少平安银行")
        if not (code_map.get("sh.600000") or code_map.get("600000")):
            prob("get_stock_code_name", "缺少浦发银行")

        futures = get_all_future_list(return_df=False, use_cache=True)
        fmarkets = {str(r.get("market_name", "")).strip() for r in futures}
        expect_fm = {"郑州商品", "大连商品", "上海期货", "广州期货"}
        if not expect_fm.issubset(fmarkets):
            prob("get_all_future_list", f"期货市场不全: {sorted(fmarkets)}")
        sections["future_list"] = {"n": len(futures), "markets": sorted(fmarkets)}

        price_codes = list(STOCKS.values())
        prices = get_stock_latest_price(price_codes)
        price_detail = {}
        for label, code in STOCKS.items():
            v = prices.get(code)
            price_detail[label] = v
            if v is None or float(v) <= 0:
                prob("get_stock_latest_price", f"{label}:{code} 无效价={v}")
        sections["stock_latest_price"] = price_detail

        fprices = get_future_latest_price(["CUL8", "ALL8"])
        for code, v in fprices.items():
            if v is None or float(v) <= 0:
                prob("get_future_latest_price", f"{code} 无效价={v}")
        sections["future_latest_price"] = fprices

    stock_tasks = [
        {"code": code, "freq": freq, "start_time": START, "end_time": END}
        for code in STOCKS.values()
        for freq in FREQS
    ]
    print(f"stock_kline tasks={len(stock_tasks)}", flush=True)
    stock_payloads = get_stock_kline(task=stock_tasks, mode="sync")
    stock_stats: Dict[str, Any] = {"ok": 0, "err": 0, "thin": 0, "by": {}}
    for p in stock_payloads:
        task = dict(p.get("task") or {})
        code = str(task.get("code", ""))
        freq = str(task.get("freq", ""))
        key = f"{code}:{freq}"
        err = p.get("error")
        rows = list(p.get("rows") or [])
        if err:
            stock_stats["err"] += 1
            prob("get_stock_kline", f"{key} error={err}")
            continue
        bad = None
        for row in rows:
            bad = _check_ohlc(row) if isinstance(row, dict) else "row非dict"
            if bad:
                break
        if bad:
            stock_stats["err"] += 1
            prob("get_stock_kline", f"{key} ohlc={bad}")
            continue
        if len(rows) < _expect_min_rows(freq):
            stock_stats["thin"] += 1
            warn("get_stock_kline", f"{key} 行数偏少 n={len(rows)} expect>={_expect_min_rows(freq)}")
        stock_stats["ok"] += 1
        stock_stats["by"][key] = len(rows)
    sections["stock_kline"] = stock_stats

    index_tasks = [
        {"index_name": name, "freq": freq, "start_time": START, "end_time": END}
        for name in INDICES
        for freq in FREQS
    ]
    print(f"index_kline tasks={len(index_tasks)}", flush=True)
    index_payloads = get_index_kline(task=index_tasks, mode="sync")
    index_stats: Dict[str, Any] = {"ok": 0, "err": 0, "thin": 0, "by": {}}
    for p in index_payloads:
        task = dict(p.get("task") or {})
        name = str(task.get("index_name", ""))
        freq = str(task.get("freq", ""))
        key = f"{name}:{freq}"
        err = p.get("error")
        rows = list(p.get("rows") or [])
        if err:
            index_stats["err"] += 1
            prob("get_index_kline", f"{key} error={err}")
            continue
        bad = None
        for row in rows:
            bad = _check_ohlc(row) if isinstance(row, dict) else "row非dict"
            if bad:
                break
        if bad:
            index_stats["err"] += 1
            prob("get_index_kline", f"{key} ohlc={bad}")
            continue
        if len(rows) < _expect_min_rows(freq):
            index_stats["thin"] += 1
            warn("get_index_kline", f"{key} 行数偏少 n={len(rows)}")
        index_stats["ok"] += 1
        index_stats["by"][key] = len(rows)
    sections["index_kline"] = index_stats

    print("future_kline ...", flush=True)
    with get_client():
        fut_df = get_future_kline(
            codes=FUTURES,
            freq=FREQS,
            start_time=START,
            end_time=END,
        )
    # 期货并行刚结束，立刻回收进程池，避免后续 F10 长时间占用时 worker 残留。
    from zsdtdx import destroy_parallel_fetcher as _destroy_now

    print(f"mid_destroy={_destroy_now()}", flush=True)

    fut_stats: Dict[str, Any] = {"ok": 0, "err": 0, "thin": 0, "by": {}}
    try:
        if hasattr(fut_df, "empty"):
            if fut_df is None or fut_df.empty:
                prob("get_future_kline", "返回空 DataFrame")
            else:
                for (code, freq), grp in fut_df.groupby(["code", "freq"]):
                    key = f"{code}:{freq}"
                    rows = grp.to_dict(orient="records")
                    bad = None
                    for row in rows:
                        bad = _check_ohlc(row)
                        if bad:
                            break
                    if bad:
                        fut_stats["err"] += 1
                        prob("get_future_kline", f"{key} ohlc={bad}")
                        continue
                    if len(rows) < _expect_min_rows(str(freq)):
                        fut_stats["thin"] += 1
                        warn("get_future_kline", f"{key} 行数偏少 n={len(rows)}")
                    fut_stats["ok"] += 1
                    fut_stats["by"][key] = len(rows)
                resolved_codes = sorted({k.split(":")[0] for k in fut_stats["by"]})
                fut_stats["resolved_codes"] = resolved_codes
                for rc in resolved_codes:
                    for freq in FREQS:
                        k = f"{rc}:{freq}"
                        if k not in fut_stats["by"]:
                            prob("get_future_kline", f"缺少组合 {k}")
                if not any(x.endswith("L8") for x in resolved_codes):
                    prob("get_future_kline", f"未解析到 L8 主连: {resolved_codes}")
        else:
            prob("get_future_kline", f"返回类型异常: {type(fut_df)}")
    except Exception as exc:
        prob("get_future_kline", f"异常: {exc}")
        traceback.print_exc()
    sections["future_kline"] = fut_stats

    print("f10 ...", flush=True)
    f10_stats: Dict[str, Any] = {}
    with get_client() as client:
        for code in F10_CODES:
            route = client._lookup_stock_route(code)  # noqa: SLF001
            if route is None:
                client.get_all_stock_list(return_df=True, refresh=True)
                route = client._lookup_stock_route(code)  # noqa: SLF001
            if route is None:
                prob("get_company_info", f"{code} 无路由")
                continue
            market = client._company_info_protocol_market(route)  # noqa: SLF001
            cats = client.std_pool.call(
                "get_company_info_category", market, code, allow_none=True
            ) or []
            rows = get_company_info(codes=[code], return_df=False, mode="sync")
            by_name = {str(r.get("category", "")).strip(): r for r in rows}
            short = []
            ok_n = 0
            for cat in cats:
                name = str(cat.get("name", "")).strip()
                expect = int(cat.get("length", 0))
                content = str((by_name.get(name) or {}).get("content") or "")
                gbk_len = len(content.encode("gbk", "ignore"))
                ratio = (gbk_len / expect) if expect else 1.0
                if expect and ratio < 0.995:
                    short.append({"name": name, "expect": expect, "got": gbk_len, "ratio": round(ratio, 4)})
                else:
                    ok_n += 1
            f10_stats[code] = {
                "tabs": len(cats),
                "rows": len(rows),
                "ok_tabs": ok_n,
                "short": short,
            }
            if len(cats) == 0:
                prob("get_company_info", f"{code} 目录为空")
            if short:
                prob("get_company_info", f"{code} 正文不完整", short=short)
            if len(rows) < len(cats):
                warn("get_company_info", f"{code} 返回行数<{len(cats)}")
    sections["f10"] = f10_stats

    fails = get_runtime_failures()
    meta = get_runtime_metadata()
    sections["runtime"] = {
        "failures_rows": int(getattr(fails, "shape", [0])[0]) if fails is not None else 0,
        "std_host": meta.get("std_active_host"),
        "ex_host": meta.get("ex_active_host"),
    }


if __name__ == "__main__":
    raise SystemExit(main())
