# -*- coding: utf-8 -*-
"""
钉死当前 config.yaml hosts.standard 各站，对比同一已收盘 A 股 15 分钟 K 线是否跨站不一致。

边界：
1. 直连 TdxHq_API，不走连接池轮换。
2. 只用已收盘 15 分钟 bar，排除当天未走完的时段。
3. 默认 100 只股票 × 100 个时间点；日线不参与本轮。
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Sequence, Set, Tuple

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts" / "std_host_bar_compare"
STOCK_N = 100
TIME_N = 100
FIELDS = ("open", "high", "low", "close", "vol", "amount")
PRICE_FIELDS = {"open", "high", "low", "close"}
CONNECT_TIMEOUT = 3.0
WORKERS = 12
PAGE_COUNT = 800
LIST_PAGE = 1600
ANCHOR_CODE = (1, "600000")
STOCK_CODE_RE = re.compile(
    r"^(000|001|002|003|300|301|600|601|603|605|688)\d{3}$"
)
FORCE_STOCKS: List[Tuple[int, str]] = [
    (1, "600000"),
    (0, "000001"),
    (0, "000002"),
    (0, "000010"),
    (0, "000858"),
    (0, "002107"),
    (0, "300221"),
    (0, "300750"),
    (1, "600825"),
    (1, "601288"),
    (1, "601318"),
    (1, "603159"),
    (1, "688001"),
]
PREFIX_QUOTA = {
    "000": 16,
    "001": 4,
    "002": 14,
    "003": 4,
    "300": 12,
    "301": 6,
    "600": 16,
    "601": 10,
    "603": 8,
    "605": 4,
    "688": 6,
}


def _load_std_hosts() -> List[Tuple[str, int]]:
    """读取包内标准行情 host 列表。"""
    raw = yaml.safe_load((_SRC / "zsdtdx" / "config.yaml").read_text(encoding="utf-8"))
    out: List[Tuple[str, int]] = []
    for item in ((raw.get("hosts") or {}).get("standard") or []):
        text = str(item).strip()
        if ":" not in text:
            continue
        host, port = text.rsplit(":", 1)
        out.append((host.strip(), int(port)))
    return out


def _market_of(code: str) -> int:
    """按代码前缀判定标准行情市场号。"""
    return 1 if code.startswith(("6", "9")) else 0


def _is_ashare_stock(code: str, name: str) -> bool:
    """过滤指数、退市占位，只保留深沪 A 股代码。"""
    if not STOCK_CODE_RE.match(code):
        return False
    if "退" in (name or ""):
        return False
    if code.startswith("000") and _market_of(code) == 1:
        return False
    return True


def _connect_first(hosts: Sequence[Tuple[str, int]]):
    """依次探测直到连上第一台可用 std host。"""
    from zsdtdx.hq import TdxHq_API

    last_err = "no_hosts"
    for host, port in hosts:
        api = TdxHq_API(raise_exception=False)
        try:
            if api.connect(host, port, time_out=CONNECT_TIMEOUT):
                return api, host, port
            last_err = f"connect_failed:{host}:{port}"
        except Exception as exc:
            last_err = f"{host}:{port}:{type(exc).__name__}"
        try:
            api.disconnect()
        except Exception:
            pass
    raise RuntimeError(f"seed_connect_failed:{last_err}")


def _iter_security_list(api: Any, market: int) -> List[Dict[str, Any]]:
    """分页拉取单个市场码表。"""
    rows: List[Dict[str, Any]] = []
    start = 0
    while True:
        page = api.get_security_list(market, start) or []
        if not page:
            break
        rows.extend(page)
        if len(page) < LIST_PAGE:
            break
        start += len(page)
        if start > 20000:
            break
    return rows


def _pick_stocks(api: Any, n: int = STOCK_N) -> List[Tuple[int, str]]:
    """从深沪码表分层抽取 n 只股票，并强制纳入对照样本。"""
    pooled: Dict[str, List[str]] = defaultdict(list)
    seen: Set[str] = set()
    for market in (0, 1):
        for rec in _iter_security_list(api, market):
            code = str(rec.get("code") or "").strip().rstrip("\x00")
            name = str(rec.get("name") or "")
            if code in seen or not _is_ashare_stock(code, name):
                continue
            if _market_of(code) != int(market):
                continue
            seen.add(code)
            pooled[code[:3]].append(code)

    picked: List[Tuple[int, str]] = []
    used: Set[str] = set()
    for market, code in FORCE_STOCKS:
        if code in used:
            continue
        picked.append((market, code))
        used.add(code)

    for prefix, quota in PREFIX_QUOTA.items():
        codes = sorted(pooled.get(prefix) or [])
        if not codes:
            continue
        need = max(0, quota)
        stride = max(1, len(codes) // max(need, 1))
        idx = 0
        got = 0
        while got < need and idx < len(codes):
            code = codes[idx]
            idx += stride
            if code in used:
                continue
            picked.append((_market_of(code), code))
            used.add(code)
            got += 1
            if len(picked) >= n:
                return picked[:n]

    for prefix in sorted(pooled):
        for code in pooled[prefix]:
            if code in used:
                continue
            picked.append((_market_of(code), code))
            used.add(code)
            if len(picked) >= n:
                return picked[:n]
    return picked[:n]


def _sample_times(rows: Any, n: int = TIME_N) -> List[str]:
    """从锚点股票 15 分钟序列中均匀抽取 n 个已收盘时间点。"""
    today = date.today().isoformat()
    dts: List[str] = []
    seen: Set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        dt_text = str(row.get("datetime") or "")
        if len(dt_text) < 19 or dt_text[:10] >= today:
            continue
        if dt_text in seen:
            continue
        seen.add(dt_text)
        dts.append(dt_text)
    dts.sort()
    if len(dts) <= n:
        return dts
    return [dts[i * len(dts) // n] for i in range(n)]


def _pick_bars(rows: Any, times: Set[str]) -> List[Dict[str, Any]]:
    """只保留目标 15 分钟时间点上的 OHLCV。"""
    if not rows:
        return []
    picked = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        dt_text = str(row.get("datetime", ""))
        if dt_text not in times:
            continue
        picked.append(
            {
                "datetime": dt_text,
                "open": float(row.get("open", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "close": float(row.get("close", 0)),
                "vol": int(row.get("vol") or 0),
                "amount": int(row.get("amount") or 0),
            }
        )
    return picked


def _fetch_one_host(
    host: str,
    port: int,
    stocks: Sequence[Tuple[int, str]],
    times: Set[str],
) -> Dict[str, Any]:
    """单站拉取全部样本股票最近一页 15 分钟 K 线并裁剪到目标时间点。"""
    from zsdtdx.hq import TdxHq_API

    endpoint = f"{host}:{port}"
    api = TdxHq_API(raise_exception=False)
    t0 = time.perf_counter()
    rec: Dict[str, Any] = {
        "endpoint": endpoint,
        "ok": False,
        "bars": {},
        "error": None,
        "stock_errors": [],
    }
    try:
        if not api.connect(host, port, time_out=CONNECT_TIMEOUT):
            rec["error"] = "connect_failed"
            rec["elapsed"] = round(time.perf_counter() - t0, 3)
            return rec
        for qfq in (False, True):
            tag = "qfq" if qfq else "raw"
            for market, code in stocks:
                key = f"{code}|15|{tag}"
                try:
                    rows = api.get_security_bars(
                        1, market, code, 0, PAGE_COUNT, qfq=qfq
                    )
                    rec["bars"][key] = _pick_bars(rows, times)
                except Exception as exc:
                    rec["bars"][key] = []
                    rec["stock_errors"].append(
                        f"{code}|{tag}:{type(exc).__name__}"
                    )
        rec["ok"] = True
    except Exception as exc:
        rec["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            api.disconnect()
        except Exception:
            pass
        rec["elapsed"] = round(time.perf_counter() - t0, 3)
    return rec


def _series_key(code_freq_tag: str, dt_text: str, field: str) -> str:
    """拼对比主键。"""
    return f"{code_freq_tag}|{dt_text}|{field}"


def _is_material(field: str, values: List[float]) -> bool:
    """价格差超过 1 分或量额有差视为实质差异。"""
    span = max(values) - min(values)
    if field in PRICE_FIELDS:
        return span > 0.011
    return span > 0


def main() -> int:
    """探测码表、抽取样本、全站对比 15 分钟 K 线。"""
    logging.getLogger("ZSDTDX").setLevel(logging.ERROR)
    _ART.mkdir(parents=True, exist_ok=True)
    hosts = _load_std_hosts()
    print(f"std_hosts={len(hosts)} stock_n={STOCK_N} time_n={TIME_N}", flush=True)

    seed_api, seed_host, seed_port = _connect_first(hosts)
    try:
        stocks = _pick_stocks(seed_api, STOCK_N)
        print(
            f"seed={seed_host}:{seed_port} stocks={len(stocks)} "
            f"sample={[c for _, c in stocks[:12]]}",
            flush=True,
        )
        anchor_rows = seed_api.get_security_bars(
            1, ANCHOR_CODE[0], ANCHOR_CODE[1], 0, PAGE_COUNT, qfq=False
        )
        target_times = _sample_times(anchor_rows, TIME_N)
    finally:
        try:
            seed_api.disconnect()
        except Exception:
            pass

    if len(stocks) < STOCK_N:
        print(f"WARN stock_shortfall {len(stocks)}/{STOCK_N}", flush=True)
    if len(target_times) < TIME_N:
        print(f"WARN time_shortfall {len(target_times)}/{TIME_N}", flush=True)
    if not stocks or not target_times:
        print("FATAL empty sample", flush=True)
        return 2

    time_set = set(target_times)
    print(
        json.dumps(
            {
                "time_count": len(target_times),
                "time_first": target_times[0],
                "time_last": target_times[-1],
                "time_sample": target_times[:: max(1, len(target_times) // 8)][:8],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = {
            pool.submit(_fetch_one_host, h, p, stocks, time_set): (h, p)
            for h, p in hosts
        }
        done_n = 0
        for fut in as_completed(futs):
            rec = fut.result()
            results.append(rec)
            done_n += 1
            if done_n % 4 == 0 or not rec.get("ok"):
                print(
                    f"progress {done_n}/{len(hosts)} last={rec.get('endpoint')} "
                    f"ok={rec.get('ok')} err={rec.get('error')} "
                    f"stock_err={len(rec.get('stock_errors') or [])} "
                    f"elapsed={rec.get('elapsed')}",
                    flush=True,
                )

    ok_hosts = [r for r in results if r.get("ok")]
    fail_hosts = [
        {"endpoint": r["endpoint"], "error": r.get("error")}
        for r in results
        if not r.get("ok")
    ]

    series: Dict[str, Dict[str, float]] = defaultdict(dict)
    for rec in ok_hosts:
        ep = rec["endpoint"]
        for series_name, bars in (rec.get("bars") or {}).items():
            for bar in bars:
                dt_text = bar["datetime"]
                for field in FIELDS:
                    series[_series_key(series_name, dt_text, field)][ep] = bar[field]

    mismatches: List[Dict[str, Any]] = []
    material: List[Dict[str, Any]] = []
    tiny: List[Dict[str, Any]] = []
    for key, by_host in series.items():
        uniq = sorted(set(by_host.values()))
        if len(uniq) <= 1:
            continue
        field = key.rsplit("|", 1)[-1]
        item = {
            "key": key,
            "n_values": len(uniq),
            "min": uniq[0],
            "max": uniq[-1],
            "span": round(uniq[-1] - uniq[0], 6),
            "values": uniq[:8],
            "host_groups": {},
        }
        groups: Dict[str, List[str]] = defaultdict(list)
        for ep, val in by_host.items():
            groups[str(val)].append(ep)
        item["host_groups"] = {
            k: v[:6] + ([f"...+{len(v) - 6}"] if len(v) > 6 else [])
            for k, v in groups.items()
        }
        item["host_counts"] = {k: len(v) for k, v in groups.items()}
        mismatches.append(item)
        if _is_material(field, [float(x) for x in uniq]):
            material.append(item)
        else:
            tiny.append(item)

    coverage_miss: List[Dict[str, Any]] = []
    host_n = len(ok_hosts)
    bar_hosts: Dict[str, set] = defaultdict(set)
    for rec in ok_hosts:
        ep = rec["endpoint"]
        for series_name, bars in (rec.get("bars") or {}).items():
            for bar in bars:
                bar_hosts[f"{series_name}|{bar['datetime']}"].add(ep)
    for bar_key, eps in bar_hosts.items():
        if 0 < len(eps) < host_n:
            coverage_miss.append(
                {
                    "key": bar_key,
                    "have": len(eps),
                    "ok_hosts": host_n,
                    "missing_n": host_n - len(eps),
                    "missing_sample": sorted(
                        {r["endpoint"] for r in ok_hosts} - eps
                    )[:8],
                }
            )

    summary = {
        "freq": "15",
        "stock_n": len(stocks),
        "time_n": len(target_times),
        "stocks": [c for _, c in stocks],
        "times": target_times,
        "std_host_count": len(hosts),
        "ok_hosts": len(ok_hosts),
        "fail_hosts": fail_hosts,
        "series_count": len(series),
        "mismatch_count": len(mismatches),
        "material_count": len(material),
        "tiny_count": len(tiny),
        "coverage_miss_count": len(coverage_miss),
        "coverage_miss_samples": coverage_miss[:20],
        "material_samples": material[:30],
        "tiny_samples": tiny[:12],
        "ok_endpoints": [r["endpoint"] for r in ok_hosts],
        "stock_error_hosts": [
            {"endpoint": r["endpoint"], "n": len(r.get("stock_errors") or [])}
            for r in ok_hosts
            if r.get("stock_errors")
        ],
    }
    out_path = _ART / "summary_15min_100.json"
    out_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                k: summary[k]
                for k in (
                    "freq",
                    "stock_n",
                    "time_n",
                    "std_host_count",
                    "ok_hosts",
                    "series_count",
                    "mismatch_count",
                    "material_count",
                    "tiny_count",
                    "coverage_miss_count",
                )
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    if material:
        print("MATERIAL", json.dumps(material[:8], ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
