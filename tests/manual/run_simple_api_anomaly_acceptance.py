# -*- coding: utf-8 -*-
"""
模块：`run_simple_api_anomaly_acceptance.py`。

职责：
1. 对 `simple_api` 全部对外函数做实盘可用性验收。
2. 样本放大：码表全量扫描奇异值，最新价/K 线/F10 取多样本。
3. 从返回值中挖掘空名、乱码、OHLC 不自洽、价格非正、前缀异常等。

边界：
1. 需访问银河标准/扩展行情；不由 pytest 收集。
2. 参数写死本文件，不暴露 CLI。
3. 每次 async/并行调用结束后必须 destroy_parallel_fetcher，并清掉残留 worker 子进程，
   避免段与段之间进程池叠加导致内存爆炸。
"""

from __future__ import annotations

import json
import math
import multiprocessing as mp
import os
import re
import sys
import time
import traceback
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts" / "simple_api_anomaly_acceptance"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_PREFIX_RE = re.compile(r"^(sz|sh|bj|hk)\.\S+$", re.I)

# 窗口覆盖周/月；收盘后仍应能拉到历史 bar。
END_DAY = date.today().isoformat()
START_DAY = (date.today() - timedelta(days=45)).isoformat()
FREQS = ["15", "30", "60", "d", "w"]

# 股票/港股/北交所固定锚点 + 从码表抽样。
ANCHOR_STOCKS = [
    "000001",
    "000002",
    "300750",
    "600000",
    "600519",
    "688981",
    "920000",
    "920001",
    "09988",
    "00700",
]
ANCHOR_INDICES = [
    "上证指数",
    "深证成指",
    "沪深300",
    "中证500",
    "中证1000",
    "科创50",
    "创业板指",
]
ANCHOR_FUTURES = ["CU", "AL", "AU", "RB", "M", "TA", "SI"]
F10_CODES = ["000001", "600000", "600519", "300750", "920000", "688981"]
PRICE_SAMPLE_N = 120
STOCK_KLINE_SAMPLE_N = 24
ETF_PRICE_SAMPLE_N = 40
FUTURE_PRICE_SAMPLE_N = 40


def _log(msg: str) -> None:
    """输入消息，输出无。用途：带时间戳进度，防卡死误判。边界：立即 flush。"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _extract_worker_pids(*blobs: Any) -> List[int]:
    """
    输入：prewarm/restart 摘要等对象。
    输出：其中出现的 worker pid 列表。
    用途：destroy 后对残留 pid 做二次强杀。
    边界：只收集正整数；字段缺失时返回空。
    """
    out: List[int] = []
    for blob in blobs:
        if not isinstance(blob, dict):
            continue
        for key in (
            "warmed_pids",
            "old_pids",
            "terminated_pids",
            "killed_pids",
            "failed_pids",
        ):
            raw = blob.get(key) or []
            if not isinstance(raw, (list, tuple, set)):
                continue
            for item in raw:
                try:
                    pid = int(item)
                except Exception:
                    continue
                if pid > 0:
                    out.append(pid)
        nested = blob.get("prewarm_summary")
        if isinstance(nested, dict):
            out.extend(_extract_worker_pids(nested))
    return sorted(set(out))


def _force_cleanup_parallel(
    *,
    label: str,
    known_pids: Optional[Sequence[int]] = None,
) -> Dict[str, Any]:
    """
    输入：段落标签与已知 worker pid。
    输出：清理摘要。
    用途：async/并行段之间强制回收进程池与残留 py 子进程。
    边界：只杀本进程的 multiprocessing 子进程与已知 worker pid，不扫全机 py。
    """
    from zsdtdx import destroy_parallel_fetcher

    summary: Dict[str, Any] = {
        "label": label,
        "destroy": {},
        "active_children_before": [],
        "terminated": [],
        "killed": [],
        "still_alive": [],
    }
    try:
        summary["destroy"] = dict(destroy_parallel_fetcher())
    except Exception as exc:
        summary["destroy_error"] = str(exc)

    children = []
    try:
        children = list(mp.active_children())
    except Exception:
        children = []
    for child in children:
        try:
            pid = int(getattr(child, "pid", 0) or 0)
        except Exception:
            pid = 0
        if pid > 0:
            summary["active_children_before"].append(pid)
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
            if pid > 0:
                summary["still_alive"].append(pid)

    targets = sorted(
        {
            int(p)
            for p in list(known_pids or []) + summary["active_children_before"]
            if int(p) > 0 and int(p) != os.getpid()
        }
    )
    try:
        import psutil
    except Exception:
        psutil = None  # type: ignore[assignment]

    if psutil is not None:
        me = os.getpid()
        try:
            for child in psutil.Process(me).children(recursive=True):
                try:
                    name = str(child.name() or "").lower()
                except Exception:
                    name = ""
                if "python" not in name and name not in {"py.exe", "python.exe"}:
                    continue
                try:
                    targets.append(int(child.pid))
                except Exception:
                    continue
        except Exception:
            pass
        targets = sorted(set(targets))
        for pid in targets:
            try:
                proc = psutil.Process(pid)
            except Exception:
                continue
            try:
                if not proc.is_running():
                    continue
            except Exception:
                continue
            try:
                proc.terminate()
                summary["terminated"].append(pid)
            except Exception:
                pass
            try:
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                    summary["killed"].append(pid)
                except Exception:
                    summary["still_alive"].append(pid)
            try:
                if proc.is_running():
                    summary["still_alive"].append(pid)
            except Exception:
                pass
    else:
        # 无 psutil 时仅对已知 pid 尽最大努力（Windows taskkill）。
        for pid in targets:
            try:
                os.kill(pid, 9)
                summary["killed"].append(pid)
            except Exception:
                summary["still_alive"].append(pid)

    summary["terminated"] = sorted(set(int(x) for x in summary["terminated"] if x))
    summary["killed"] = sorted(set(int(x) for x in summary["killed"] if x))
    summary["still_alive"] = sorted(set(int(x) for x in summary["still_alive"] if x))
    _log(
        f"cleanup[{label}] destroy={summary.get('destroy')} "
        f"term={summary['terminated']} kill={summary['killed']} "
        f"alive={summary['still_alive']}"
    )
    return summary


def _check_ohlc(row: Dict[str, Any]) -> Optional[str]:
    """输入 K 线行，输出问题或 None。用途：OHLC/时间/量校验。边界：缺字段失败。"""
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
        return "OHLC不自洽"
    try:
        vol = float(row.get("volume", 0) or 0)
        if not math.isfinite(vol) or vol < 0:
            return "volume非法"
    except Exception:
        return "volume非法"
    return None


def _scan_name_map(
    mp: Dict[str, str],
    *,
    kind: str,
    expect_prefixes: Tuple[str, ...],
) -> Dict[str, Any]:
    """
    输入代码名称字典与期望前缀。
    输出奇异值汇总。
    用途：全量扫描空名、乱码、重复码、前缀异常。
    边界：只采样前若干条问题，不全量打印。
    """
    empty_name: List[str] = []
    ctrl_name: List[str] = []
    bad_prefix: List[str] = []
    dup_codes: List[str] = []
    weird_len: List[str] = []
    seen_bare: Dict[str, str] = {}
    prefix_counter: Counter[str] = Counter()
    for key, name in mp.items():
        k = str(key).strip()
        n = "" if name is None else str(name)
        if "." in k:
            prefix_counter[k.split(".", 1)[0].lower()] += 1
        if not n.strip():
            if len(empty_name) < 20:
                empty_name.append(k)
            continue
        if _CTRL_RE.search(n) or "\ufffd" in n:
            if len(ctrl_name) < 20:
                ctrl_name.append(f"{k}={n[:40]}")
        if not _PREFIX_RE.match(k):
            if len(bad_prefix) < 20:
                bad_prefix.append(k)
        else:
            pref = k.split(".", 1)[0].lower()
            if pref not in expect_prefixes and len(bad_prefix) < 20:
                bad_prefix.append(k)
            bare = k.split(".", 1)[1]
            if bare in seen_bare and seen_bare[bare] != k:
                if len(dup_codes) < 20:
                    dup_codes.append(f"{seen_bare[bare]} vs {k}")
            else:
                seen_bare[bare] = k
        if len(n) > 40 or len(n) < 1:
            if len(weird_len) < 20:
                weird_len.append(f"{k} len={len(n)} name={n[:40]}")
    return {
        "kind": kind,
        "n": len(mp),
        "prefix_counts": dict(prefix_counter),
        "empty_name_n": len(empty_name),
        "empty_name_sample": empty_name,
        "ctrl_or_replacement_n": len(ctrl_name),
        "ctrl_sample": ctrl_name,
        "bad_prefix_n": len(bad_prefix),
        "bad_prefix_sample": bad_prefix,
        "dup_bare_n": len(dup_codes),
        "dup_sample": dup_codes,
        "weird_len_n": len(weird_len),
        "weird_len_sample": weird_len,
    }


def _sample_codes(keys: List[str], n: int, anchors: List[str]) -> List[str]:
    """输入键列表与锚点，输出去重抽样 bare code。用途：扩大最新价/K线样本。边界：锚点优先。"""
    bare_anchors = [str(x).strip() for x in anchors]
    out: List[str] = []
    seen: set[str] = set()
    for code in bare_anchors:
        if code and code not in seen:
            seen.add(code)
            out.append(code)
    step = max(1, len(keys) // max(1, n))
    for i in range(0, len(keys), step):
        raw = str(keys[i]).strip()
        bare = raw.split(".", 1)[-1]
        if bare and bare not in seen:
            seen.add(bare)
            out.append(bare)
        if len(out) >= n:
            break
    return out[:n]


def _scan_prices(mp: Dict[str, Optional[float]], *, label: str) -> Dict[str, Any]:
    """输入最新价字典，输出奇异值。用途：None/非正/非有限/缺键。边界：采样列出。"""
    none_keys: List[str] = []
    non_positive: List[str] = []
    non_finite: List[str] = []
    ok = 0
    for k, v in mp.items():
        if v is None:
            if len(none_keys) < 30:
                none_keys.append(str(k))
            continue
        try:
            fv = float(v)
        except Exception:
            if len(non_finite) < 20:
                non_finite.append(f"{k}={v}")
            continue
        if not math.isfinite(fv):
            if len(non_finite) < 20:
                non_finite.append(f"{k}={fv}")
            continue
        if fv <= 0:
            if len(non_positive) < 20:
                non_positive.append(f"{k}={fv}")
            continue
        ok += 1
    return {
        "label": label,
        "n": len(mp),
        "ok": ok,
        "none_n": len(none_keys),
        "none_sample": none_keys,
        "non_positive_n": len(non_positive),
        "non_positive_sample": non_positive,
        "non_finite_n": len(non_finite),
        "non_finite_sample": non_finite,
    }


def _scan_kline_payloads(payloads: List[Dict[str, Any]], *, label: str) -> Dict[str, Any]:
    """输入 sync K 线 payload，输出统计与奇异样本。用途：可用性+OHLC。边界：采样。"""
    stats: Dict[str, Any] = {
        "label": label,
        "n": len(payloads),
        "ok": 0,
        "empty": 0,
        "error": 0,
        "ohlc_bad": 0,
        "thin": 0,
        "error_sample": [],
        "ohlc_sample": [],
        "row_count_by": {},
    }
    for p in payloads:
        if not isinstance(p, dict):
            stats["error"] += 1
            continue
        task = dict(p.get("task") or {})
        key = (
            f"{task.get('code') or task.get('index_name')}:{task.get('freq')}"
        )
        err = p.get("error")
        rows = list(p.get("rows") or [])
        if err:
            stats["error"] += 1
            if len(stats["error_sample"]) < 15:
                stats["error_sample"].append({"key": key, "error": str(err)[:200]})
            continue
        if not rows:
            stats["empty"] += 1
            continue
        bad = None
        for row in rows:
            if not isinstance(row, dict):
                bad = "row非dict"
                break
            bad = _check_ohlc(row)
            if bad:
                break
        if bad:
            stats["ohlc_bad"] += 1
            if len(stats["ohlc_sample"]) < 15:
                stats["ohlc_sample"].append({"key": key, "reason": bad})
            continue
        freq = str(task.get("freq", ""))
        min_n = {"15": 8, "30": 6, "60": 4, "d": 5, "w": 1, "m": 1}.get(freq, 1)
        if len(rows) < min_n:
            stats["thin"] += 1
        stats["ok"] += 1
        stats["row_count_by"][key] = len(rows)
    return stats


def main() -> int:
    """
    输入：无。
    输出：成功 0，有硬问题 1。
    用途：全函数多样本奇异值验收入口。
    边界：finally 销毁并行池。
    """
    from zsdtdx import (
        get_all_future_list,
        get_client,
        get_company_info,
        get_etf_code_name,
        get_future_kline,
        get_future_latest_price,
        get_index_kline,
        get_runtime_failures,
        get_runtime_metadata,
        get_stock_code_name,
        get_stock_kline,
        get_stock_latest_price,
        get_supported_markets,
        prewarm_parallel_fetcher,
        restart_parallel_fetcher,
        set_config_path,
    )

    _ART.mkdir(parents=True, exist_ok=True)
    problems: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    sections: Dict[str, Any] = {}
    t0 = time.perf_counter()

    def prob(section: str, detail: str, **extra: Any) -> None:
        problems.append({"section": section, "detail": detail, **extra})
        _log(f"PROBLEM [{section}] {detail}")

    def warn(section: str, detail: str, **extra: Any) -> None:
        warnings.append({"section": section, "detail": detail, **extra})
        _log(f"WARN [{section}] {detail}")

    try:
        cfg = set_config_path(
            str(_SRC / "zsdtdx" / "config.yaml"), async_background_probe=False
        )
        sections["set_config_path"] = {"path": cfg, "ok": Path(cfg).is_file()}
        if not Path(cfg).is_file():
            prob("set_config_path", f"配置不存在: {cfg}")
            return 1
        _log(f"config={cfg}")

        # ---- catalogs ----
        with get_client() as client:
            sections["get_client"] = {"type": type(client).__name__}
            _log("get_supported_markets ...")
            markets = get_supported_markets(return_df=False)
            mnames = {str(r.get("name", "")).strip() for r in markets}
            for need in ("深圳", "上海", "北京"):
                if need not in mnames:
                    prob("get_supported_markets", f"缺少 {need}")
            if "港股通" not in mnames:
                warn("get_supported_markets", "未见到市场名「港股通」（扩展侧可能命名不同）")
            empty_m = [
                r
                for r in markets
                if not str(r.get("name", "")).strip()
                or r.get("market") is None
            ]
            if empty_m:
                prob("get_supported_markets", f"存在空 name/market: {len(empty_m)}")
            sections["get_supported_markets"] = {
                "n": len(markets),
                "names_sample": sorted(mnames)[:30],
                "empty_fields": len(empty_m),
            }

            _log("get_stock_code_name ...")
            stock_map = get_stock_code_name(use_cache=True)
            stock_scan = _scan_name_map(
                stock_map, kind="stock", expect_prefixes=("sz", "sh")
            )
            sections["get_stock_code_name"] = stock_scan
            if stock_scan["n"] < 4000:
                prob("get_stock_code_name", f"数量过少 n={stock_scan['n']}")
            if stock_map.get("sz.000001") != "平安银行":
                prob(
                    "get_stock_code_name",
                    f"sz.000001 异常: {stock_map.get('sz.000001')}",
                )
            if stock_map.get("sh.600000") != "浦发银行":
                prob(
                    "get_stock_code_name",
                    f"sh.600000 异常: {stock_map.get('sh.600000')}",
                )
            if stock_scan["empty_name_n"]:
                prob(
                    "get_stock_code_name",
                    f"空名 {stock_scan['empty_name_n']}",
                    sample=stock_scan["empty_name_sample"],
                )
            if stock_scan["ctrl_or_replacement_n"]:
                warn(
                    "get_stock_code_name",
                    f"控制符/替换符名 {stock_scan['ctrl_or_replacement_n']}",
                    sample=stock_scan["ctrl_sample"],
                )
            if stock_scan["bad_prefix_n"]:
                warn(
                    "get_stock_code_name",
                    f"前缀异常 {stock_scan['bad_prefix_n']}",
                    sample=stock_scan["bad_prefix_sample"],
                )

            _log("get_etf_code_name ...")
            etf_map = get_etf_code_name(use_cache=True)
            etf_scan = _scan_name_map(
                etf_map, kind="etf", expect_prefixes=("sz", "sh")
            )
            # 商品 LOF 锚点（截图常见）
            commodity_lof = [
                "sz.160216",
                "sz.160723",
                "sz.161116",
                "sz.161226",
                "sz.162411",
                "sz.162719",
                "sz.163208",
                "sz.165513",
                "sz.169103",
                "sz.169201",
            ]
            missing_lof = [c for c in commodity_lof if c not in etf_map]
            sections["get_etf_code_name"] = {
                **etf_scan,
                "missing_commodity_lof": missing_lof,
                "sample": {
                    k: etf_map.get(k)
                    for k in (
                        "sz.159915",
                        "sh.510050",
                        "sz.159105",
                        "sz.161725",
                    )
                    if k in etf_map
                },
            }
            if etf_scan["n"] < 1000:
                prob("get_etf_code_name", f"数量过少 n={etf_scan['n']}")
            if etf_scan["empty_name_n"]:
                prob(
                    "get_etf_code_name",
                    f"空名 {etf_scan['empty_name_n']}",
                    sample=etf_scan["empty_name_sample"],
                )
            if etf_scan["ctrl_or_replacement_n"]:
                warn(
                    "get_etf_code_name",
                    f"控制符/替换符名 {etf_scan['ctrl_or_replacement_n']}",
                    sample=etf_scan["ctrl_sample"],
                )
            if missing_lof:
                warn(
                    "get_etf_code_name",
                    f"商品LOF锚点缺失 {len(missing_lof)}",
                    sample=missing_lof,
                )
            # ETF 不应扩宽进默认股票码表
            overlap = set(etf_map) & set(stock_map)
            # 允许极少数误交叉？目标态：默认 szsh A 股不含 ETF。抽样检查 15/51/16 段。
            etf_like_in_stock = [
                k
                for k in stock_map
                if k.startswith(("sz.15", "sz.16", "sz.18", "sh.51", "sh.56", "sh.58"))
            ]
            sections["etf_vs_stock"] = {
                "overlap_n": len(overlap),
                "etf_like_prefix_in_stock_n": len(etf_like_in_stock),
                "etf_like_sample": etf_like_in_stock[:20],
            }
            if etf_like_in_stock:
                warn(
                    "get_stock_code_name",
                    f"疑似基金前缀混入 A 股码表 {len(etf_like_in_stock)}",
                    sample=etf_like_in_stock[:20],
                )

            _log("get_all_future_list ...")
            futures = get_all_future_list(return_df=False, use_cache=True)
            fmarkets = {str(r.get("market_name", "")).strip() for r in futures}
            expect_fm = {"郑州商品", "大连商品", "上海期货", "广州期货"}
            if not expect_fm.issubset(fmarkets):
                prob("get_all_future_list", f"市场不全: {sorted(fmarkets)}")
            empty_f = [
                r
                for r in futures
                if not str(r.get("code", "")).strip()
                or not str(r.get("name", "")).strip()
            ]
            if empty_f:
                prob("get_all_future_list", f"空 code/name: {len(empty_f)}")
            l8 = [
                r
                for r in futures
                if str(r.get("code", "")).upper().endswith("L8")
                or "主连" in str(r.get("name", ""))
            ]
            sections["get_all_future_list"] = {
                "n": len(futures),
                "markets": sorted(fmarkets),
                "empty_fields": len(empty_f),
                "l8_or_main_n": len(l8),
                "l8_sample": [
                    {"code": r.get("code"), "name": r.get("name")} for r in l8[:15]
                ],
            }

            # ---- latest prices（多样本）----
            stock_keys = sorted(stock_map.keys())
            price_codes = _sample_codes(stock_keys, PRICE_SAMPLE_N, ANCHOR_STOCKS)
            _log(f"get_stock_latest_price n={len(price_codes)} ...")
            stock_prices = get_stock_latest_price(price_codes)
            missing_price_keys = [c for c in price_codes if c not in stock_prices]
            price_scan = _scan_prices(stock_prices, label="stock")
            sections["get_stock_latest_price"] = {
                **price_scan,
                "missing_keys": missing_price_keys[:20],
                "anchor": {c: stock_prices.get(c) for c in ANCHOR_STOCKS if c in price_codes},
            }
            if missing_price_keys:
                warn(
                    "get_stock_latest_price",
                    f"缺键 {len(missing_price_keys)}",
                    sample=missing_price_keys[:20],
                )
            # A 股锚点必须正价；港股允许 None（收盘后偶发）
            for code in ("000001", "600000", "600519", "300750"):
                v = stock_prices.get(code)
                if v is None or float(v) <= 0:
                    prob("get_stock_latest_price", f"锚点 {code} 无效价={v}")
            for code in ("09988", "00700", "920000"):
                v = stock_prices.get(code)
                if v is not None and float(v) <= 0:
                    prob("get_stock_latest_price", f"{code} 非正价={v}")
                elif v is None:
                    warn("get_stock_latest_price", f"{code} 价为 None（可能未上市/无报价）")

            etf_keys = sorted(etf_map.keys())
            etf_codes = _sample_codes(etf_keys, ETF_PRICE_SAMPLE_N, ["159915", "510050", "159105", "161725"])
            _log(f"get_stock_latest_price(etf sample) n={len(etf_codes)} ...")
            etf_prices = get_stock_latest_price(etf_codes)
            sections["etf_latest_price"] = _scan_prices(etf_prices, label="etf")
            for code in ("159915", "510050"):
                v = etf_prices.get(code)
                if v is None or float(v) <= 0:
                    warn("etf_latest_price", f"{code} 无效价={v}")

            fut_codes_all = [
                str(r.get("code", "")).strip()
                for r in futures
                if str(r.get("code", "")).upper().endswith("L8")
            ]
            fut_price_codes = fut_codes_all[:FUTURE_PRICE_SAMPLE_N] or ["CUL8", "ALL8", "AUL8"]
            _log(f"get_future_latest_price n={len(fut_price_codes)} ...")
            fut_prices = get_future_latest_price(fut_price_codes)
            fut_price_scan = _scan_prices(fut_prices, label="future")
            sections["get_future_latest_price"] = fut_price_scan
            if fut_prices.get("CUL8") is None or float(fut_prices.get("CUL8") or 0) <= 0:
                # 也可能按品种返回
                cu = get_future_latest_price("CU")
                sections["get_future_latest_price"]["CU_resolve"] = cu
                if cu.get("CUL8") is None or float(cu.get("CUL8") or 0) <= 0:
                    prob("get_future_latest_price", f"CUL8 无效: {cu}")

        # ---- pool lifecycle：每段测完立刻清残留 worker ----
        cleanup_log: List[Dict[str, Any]] = []
        _log("prewarm_parallel_fetcher ...")
        prewarm = prewarm_parallel_fetcher()
        sections["prewarm_parallel_fetcher"] = prewarm
        if not isinstance(prewarm, dict):
            prob("prewarm_parallel_fetcher", f"类型异常 {type(prewarm)}")
        cleanup_log.append(
            _force_cleanup_parallel(
                label="after_prewarm",
                known_pids=_extract_worker_pids(prewarm),
            )
        )

        _log("restart_parallel_fetcher ...")
        restart = restart_parallel_fetcher(
            prewarm=True, prewarm_timeout_seconds=120, max_rounds=2
        )
        sections["restart_parallel_fetcher"] = restart
        if not isinstance(restart, dict):
            prob("restart_parallel_fetcher", f"类型异常 {type(restart)}")
        cleanup_log.append(
            _force_cleanup_parallel(
                label="after_restart",
                known_pids=_extract_worker_pids(restart),
            )
        )

        # ---- stock / index kline sync 多样本（sync 走主进程 inproc，不启池）----
        kline_codes = _sample_codes(sorted(stock_map.keys()), STOCK_KLINE_SAMPLE_N, ANCHOR_STOCKS)
        # 港股/北交所也纳入
        for extra in ("09988", "920000", "688981", "300750"):
            if extra not in kline_codes:
                kline_codes.append(extra)
        stock_tasks = [
            {"code": code, "freq": freq, "start_time": START_DAY, "end_time": END_DAY}
            for code in kline_codes
            for freq in FREQS
        ]
        _log(f"get_stock_kline sync tasks={len(stock_tasks)} ...")
        stock_payloads = get_stock_kline(task=stock_tasks, mode="sync")
        stock_k = _scan_kline_payloads(list(stock_payloads or []), label="stock_sync")
        sections["get_stock_kline_sync"] = stock_k
        if stock_k["ok"] < max(1, len(stock_tasks) // 3):
            prob(
                "get_stock_kline",
                f"可解析过少 ok={stock_k['ok']}/{stock_k['n']}",
            )
        if stock_k["ohlc_bad"]:
            prob(
                "get_stock_kline",
                f"OHLC异常 {stock_k['ohlc_bad']}",
                sample=stock_k["ohlc_sample"],
            )
        if stock_k["error"]:
            warn(
                "get_stock_kline",
                f"error任务 {stock_k['error']}",
                sample=stock_k["error_sample"],
            )

        index_tasks = [
            {
                "index_name": name,
                "freq": freq,
                "start_time": START_DAY,
                "end_time": END_DAY,
            }
            for name in ANCHOR_INDICES
            for freq in FREQS
        ]
        _log(f"get_index_kline sync tasks={len(index_tasks)} ...")
        index_payloads = get_index_kline(task=index_tasks, mode="sync")
        index_k = _scan_kline_payloads(list(index_payloads or []), label="index_sync")
        sections["get_index_kline_sync"] = index_k
        if index_k["ok"] < len(ANCHOR_INDICES):
            warn(
                "get_index_kline",
                f"可解析偏少 ok={index_k['ok']}/{index_k['n']}",
                sample=index_k["error_sample"],
            )
        if index_k["ohlc_bad"]:
            prob(
                "get_index_kline",
                f"OHLC异常 {index_k['ohlc_bad']}",
                sample=index_k["ohlc_sample"],
            )

        # ---- async 小样本：done 后立刻清池 + 残留子进程 ----
        _log("get_stock_kline async ...")
        job = get_stock_kline(
            task=[
                {
                    "code": "600000",
                    "freq": "d",
                    "start_time": START_DAY,
                    "end_time": END_DAY,
                },
                {
                    "code": "000001",
                    "freq": "60",
                    "start_time": START_DAY,
                    "end_time": END_DAY,
                },
            ],
            mode="async",
        )
        async_payloads: List[Dict[str, Any]] = []
        done_evt = None
        while True:
            evt = job.queue.get(timeout=300)
            if str(evt.get("event", "")).lower() == "done":
                done_evt = evt
                break
            if str(evt.get("event", "")).lower() == "data":
                async_payloads.append(evt)
        async_stats = _scan_kline_payloads(async_payloads, label="stock_async")
        cleanup_log.append(_force_cleanup_parallel(label="after_stock_async"))
        try:
            job.result()
        except Exception as exc:
            warn("get_stock_kline_async", f"job.result: {exc}")
        sections["get_stock_kline_async"] = {
            "stats": async_stats,
            "done": done_evt,
        }
        if async_stats["ok"] < 1:
            prob("get_stock_kline_async", f"无可解析: {async_stats}")

        # ---- future kline（内部走进程池）----
        _log(f"get_future_kline codes={ANCHOR_FUTURES} freqs={FREQS} ...")
        with get_client():
            fut_df = get_future_kline(
                codes=ANCHOR_FUTURES,
                freq=FREQS,
                start_time=START_DAY,
                end_time=END_DAY,
            )
        fut_stats: Dict[str, Any] = {
            "ok": 0,
            "err": 0,
            "thin": 0,
            "by": {},
            "ohlc_sample": [],
        }
        if fut_df is None or not hasattr(fut_df, "empty") or fut_df.empty:
            prob("get_future_kline", "返回空")
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
                    if len(fut_stats["ohlc_sample"]) < 10:
                        fut_stats["ohlc_sample"].append({"key": key, "reason": bad})
                    continue
                fut_stats["ok"] += 1
                fut_stats["by"][key] = len(rows)
            if fut_stats["err"]:
                prob(
                    "get_future_kline",
                    f"OHLC异常 {fut_stats['err']}",
                    sample=fut_stats["ohlc_sample"],
                )
            if fut_stats["ok"] < len(ANCHOR_FUTURES):
                warn(
                    "get_future_kline",
                    f"可解析组合偏少 ok={fut_stats['ok']}",
                )
        sections["get_future_kline"] = fut_stats
        cleanup_log.append(_force_cleanup_parallel(label="after_future_kline"))
        sections["parallel_cleanups"] = cleanup_log

        # ---- F10 ----
        _log(f"get_company_info codes={F10_CODES} ...")
        f10: Dict[str, Any] = {}
        with get_client():
            for code in F10_CODES:
                rows = get_company_info(
                    codes=[code], return_df=False, mode="sync"
                )
                if not isinstance(rows, list) or not rows:
                    prob("get_company_info", f"{code} 空")
                    f10[code] = {"n": 0}
                    continue
                empty_content = [
                    str(r.get("category", ""))
                    for r in rows
                    if not str(r.get("content", "")).strip()
                ]
                cats = [str(r.get("category", "")).strip() for r in rows]
                if any(not c for c in cats):
                    warn("get_company_info", f"{code} 存在空分类名")
                f10[code] = {
                    "n": len(rows),
                    "empty_content_n": len(empty_content),
                    "empty_content_sample": empty_content[:8],
                    "categories_sample": cats[:8],
                    "content_len_max": max(
                        len(str(r.get("content", ""))) for r in rows
                    ),
                }
                if len(empty_content) == len(rows):
                    prob("get_company_info", f"{code} 正文全空")
        sections["get_company_info"] = f10

        # ---- runtime ----
        with get_client():
            get_stock_latest_price(["600000"])
            fails = get_runtime_failures()
            meta = get_runtime_metadata()
        sections["get_runtime_failures"] = {
            "type": type(fails).__name__,
            "rows": int(getattr(fails, "shape", [0])[0]) if fails is not None else 0,
        }
        sections["get_runtime_metadata"] = {
            "keys": sorted(meta.keys()) if isinstance(meta, dict) else [],
            "std_active_host": (meta or {}).get("std_active_host"),
            "ex_active_host": (meta or {}).get("ex_active_host"),
        }
        if not isinstance(meta, dict) or not meta:
            prob("get_runtime_metadata", "空或非 dict")

        cleanup_log.append(_force_cleanup_parallel(label="before_exit"))

    except Exception as exc:
        prob("fatal", f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
    finally:
        try:
            info = _force_cleanup_parallel(label="final")
            _log(f"final_cleanup={info}")
            sections["final_cleanup"] = info
        except Exception as exc:
            warn("cleanup", f"destroy失败:{exc}")

    report = {
        "ok": len(problems) == 0,
        "problem_count": len(problems),
        "warning_count": len(warnings),
        "problems": problems,
        "warnings": warnings,
        "sections": sections,
        "window": {"start": START_DAY, "end": END_DAY},
        "freqs": FREQS,
        "elapsed_seconds": round(time.perf_counter() - t0, 3),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    out = _ART / "summary.json"
    out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    _log(
        json.dumps(
            {
                "ok": report["ok"],
                "problems": len(problems),
                "warnings": len(warnings),
                "elapsed": report["elapsed_seconds"],
                "path": str(out),
            },
            ensure_ascii=False,
        )
    )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
