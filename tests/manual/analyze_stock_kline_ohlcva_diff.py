# -*- coding: utf-8 -*-
"""
模块：analyze_stock_kline_ohlcva_diff.py

用途：
1. 离线分析 workspace vs site_packages 落盘产物中 OHLCVA 显著差异。
2. OHLC：相对偏差 >0.5% 视为显著；VA：int 不等视为显著。

边界：
1. 仅读取 artifacts，不发起网络请求。
2. datetime 对齐到分钟（SS=00 截断）。
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

_MANUAL_DIR = Path(__file__).resolve().parent
_CFG_PATH = _MANUAL_DIR / "stock_kline_async_full_compare_config.yaml"

OHLC_FIELDS = ("open", "high", "low", "close")
VA_FIELDS = ("volume", "amount")
ALL_FIELDS = OHLC_FIELDS + VA_FIELDS

PAT_HMS = re.compile(r"(\d{2}:\d{2}):00$")


def _load_cfg_root() -> Path:
    with _CFG_PATH.open("r", encoding="utf-8") as fh:
        bench = dict((yaml.safe_load(fh) or {}).get("bench") or {})
    return _MANUAL_DIR / str(bench.get("artifacts_root", "artifacts/stock_kline_async_full_compare"))


def _normalize_bar_datetime(raw: Any) -> str:
    dt = str(raw or "").strip()
    if dt == "":
        return dt
    if " " not in dt:
        return dt
    date_part, time_part = dt.split(" ", 1)
    time_part = time_part.strip()
    if time_part.count(":") == 2:
        hour, minute, second = time_part.split(":", 2)
        if second in {"0", "00"}:
            return f"{date_part} {hour}:{minute}"
    return dt


def _pct_diff(a: float, b: float) -> float:
    da = float(a)
    db = float(b)
    if da == db:
        return 0.0
    denom = max(abs(da), abs(db), 1e-12)
    return abs(da - db) / denom * 100.0


def _ohlc_significant(a: Any, b: Any, tol_pct: float = 0.5) -> Tuple[bool, float]:
    try:
        pa = float(a)
        pb = float(b)
    except (TypeError, ValueError):
        return True, 100.0
    pct = _pct_diff(pa, pb)
    return pct > tol_pct, pct


def _va_significant(a: Any, b: Any) -> Tuple[bool, float]:
    try:
        ia = int(a)
        ib = int(b)
    except (TypeError, ValueError):
        return True, 100.0
    if ia == ib:
        return False, 0.0
    denom = max(abs(ia), abs(ib), 1)
    return True, abs(ia - ib) / denom * 100.0


def _load_rows_index(path: Path) -> Dict[str, Dict[str, Any]]:
    """读取 jsonl，仅保留 task_key -> {rows_by_dt, error}。"""
    out: Dict[str, Dict[str, Any]] = {}
    if not path.is_file():
        return out
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if not text:
                continue
            rec = json.loads(text)
            key = str(rec.get("task_key") or "").strip()
            if not key:
                continue
            rows_by_dt: Dict[str, Dict[str, Any]] = {}
            for row in list(rec.get("rows") or []):
                if not isinstance(row, dict):
                    continue
                dk = _normalize_bar_datetime(row.get("datetime"))
                if dk:
                    rows_by_dt[dk] = row
            out[key] = {
                "error": str(rec.get("error") or "").strip(),
                "rows_by_dt": rows_by_dt,
                "bar_count": len(rows_by_dt),
            }
    return out


def _infer_reason(
    *,
    task_key: str,
    err_l: str,
    err_r: str,
    only_left: List[str],
    only_right: List[str],
    field_diffs: Dict[str, Dict[str, Any]],
) -> str:
    """根据上下文推断显著差异原因。"""
    if err_l and not err_r:
        return f"workspace 任务失败({err_l})，site_packages 有数据"
    if err_r and not err_l:
        return f"site_packages 任务失败({err_r})，workspace 有数据"
    if err_l and err_r and err_l != err_r:
        return f"两侧 error 不同: workspace={err_l}; site={err_r}"
    if only_left and not only_right:
        return "workspace 多 bar（或 site 缺 bar）"
    if only_right and not only_left:
        return "site_packages 多 bar（或 workspace 缺 bar）"
    if only_left or only_right:
        return "datetime 键集合部分不一致"

    va_only = all(f in VA_FIELDS for f in field_diffs)
    ohlc_only = all(f in OHLC_FIELDS for f in field_diffs)
    if va_only:
        return "仅 VA 取整/量级差异（OHLC 在容差内一致）"
    if ohlc_only:
        return "OHLC 价格字段显著偏离（可能路由/复权/解析刻度差异）"

  # 混合
    if "volume" in field_diffs or "amount" in field_diffs:
        if any(f in OHLC_FIELDS for f in field_diffs):
            return "OHLC 与 VA 同时偏离（可能非同源 bar 或分页边界差异）"
    return "OHLC/VA 多字段显著偏离"


def main() -> int:
    root = _load_cfg_root()
    left_path = root / "workspace" / "task_records.jsonl"
    right_path = root / "site_packages" / "task_records.jsonl"
    if not left_path.is_file() or not right_path.is_file():
        print("缺少 task_records.jsonl", file=sys.stderr)
        return 2

    print("加载 site_packages 索引...", flush=True)
    right_idx = _load_rows_index(right_path)
    print("加载 workspace 索引...", flush=True)
    left_idx = _load_rows_index(left_path)

    common = sorted(set(left_idx.keys()) & set(right_idx.keys()))
    bar_diffs: List[Dict[str, Any]] = []
    task_only_dt: List[Dict[str, Any]] = []
    reason_counter: Counter[str] = Counter()

    for task_key in common:
        lr = left_idx[task_key]
        rr = right_idx[task_key]
        lm = dict(lr["rows_by_dt"])
        rm = dict(rr["rows_by_dt"])
        keys_l = set(lm.keys())
        keys_r = set(rm.keys())
        only_l = sorted(keys_l - keys_r)
        only_r = sorted(keys_r - keys_l)
        common_dt = sorted(keys_l & keys_r)

        task_has_significant = False

        for dk in common_dt:
            a = lm[dk]
            b = rm[dk]
            field_diffs: Dict[str, Dict[str, Any]] = {}
            max_pct = 0.0
            for f in OHLC_FIELDS:
                sig, pct = _ohlc_significant(a.get(f), b.get(f))
                if sig:
                    field_diffs[f] = {
                        "left": a.get(f),
                        "right": b.get(f),
                        "pct_diff": round(pct, 6),
                    }
                    max_pct = max(max_pct, pct)
            for f in VA_FIELDS:
                sig, pct = _va_significant(a.get(f), b.get(f))
                if sig:
                    field_diffs[f] = {
                        "left": a.get(f),
                        "right": b.get(f),
                        "pct_diff": round(pct, 6),
                    }
                    max_pct = max(max_pct, pct)
            if not field_diffs:
                continue
            task_has_significant = True
            reason = _infer_reason(
                task_key=task_key,
                err_l=str(lr["error"]),
                err_r=str(rr["error"]),
                only_left=only_l,
                only_right=only_r,
                field_diffs=field_diffs,
            )
            reason_counter[reason] += 1
            bar_diffs.append(
                {
                    "task_key": task_key,
                    "datetime": dk,
                    "max_pct_diff": round(max_pct, 6),
                    "field_diffs": field_diffs,
                    "reason": reason,
                    "error": {"workspace": lr["error"], "site_packages": rr["error"]},
                }
            )

        if only_l or only_r:
            # 键集合差异：若无共同 bar 显著差，也记录 task 级
            if not task_has_significant and (only_l or only_r):
                reason = _infer_reason(
                    task_key=task_key,
                    err_l=str(lr["error"]),
                    err_r=str(rr["error"]),
                    only_left=only_l,
                    only_right=only_r,
                    field_diffs={},
                )
                reason_counter[reason] += 1
                task_only_dt.append(
                    {
                        "task_key": task_key,
                        "max_pct_diff": 100.0,
                        "only_left_count": len(only_l),
                        "only_right_count": len(only_r),
                        "only_left_samples": only_l[:5],
                        "only_right_samples": only_r[:5],
                        "reason": reason,
                        "error": {"workspace": lr["error"], "site_packages": rr["error"]},
                        "bar_count": {"workspace": lr["bar_count"], "site_packages": rr["bar_count"]},
                    }
                )

    bar_diffs.sort(key=lambda x: float(x.get("max_pct_diff", 0)), reverse=True)
    task_only_dt.sort(key=lambda x: float(x.get("max_pct_diff", 0)), reverse=True)

    summary = {
        "common_tasks": len(common),
        "significant_bar_diff_count": len(bar_diffs),
        "task_datetime_only_diff_count": len(task_only_dt),
        "ohlc_tolerance_pct": 0.5,
        "va_rule": "int(left) != int(right) 视为显著",
        "reason_distribution": dict(reason_counter.most_common()),
        "top_bar_diffs": bar_diffs[:200],
        "top_task_datetime_diffs": task_only_dt[:100],
    }

    out_path = root / "ohlcva_significant_diff_report.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(
        {
            "report": str(out_path),
            "significant_bar_diff_count": len(bar_diffs),
            "task_datetime_only_diff_count": len(task_only_dt),
            "reason_distribution": dict(reason_counter.most_common(10)),
            "top5_max_pct": [
                {
                    "task_key": x["task_key"],
                    "datetime": x.get("datetime"),
                    "max_pct_diff": x["max_pct_diff"],
                    "fields": list(x.get("field_diffs", {}).keys()),
                    "reason": x.get("reason"),
                }
                for x in bar_diffs[:5]
            ],
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
