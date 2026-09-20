# -*- coding: utf-8 -*-
"""
模块：analyze_tdxquant_vs_zsdtdx_weekly.py

用途：
1. 离线对比 TdxQuant 与 zsdtdx 本周日线 OHLCVA 完整性与耗时。
2. 输出缺失统计、字段差异与两侧 run_meta 摘要。

边界：
1. 仅读取 artifacts，不发起网络请求。
2. 两侧 task_key 使用 zsdtdx 代码前缀格式对齐。
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

_MANUAL_DIR = Path(__file__).resolve().parent
_CFG_PATH = _MANUAL_DIR / "tdxquant_zsdtdx_weekly_compare_config.yaml"

OHLC_FIELDS = ("open", "high", "low", "close")
VA_FIELDS = ("volume", "amount")
ALL_FIELDS = OHLC_FIELDS + VA_FIELDS


def _load_root() -> Path:
    with _CFG_PATH.open("r", encoding="utf-8") as fh:
        cfg = dict((yaml.safe_load(fh) or {}).get("compare") or {})
    return _MANUAL_DIR / str(cfg.get("artifacts_root", "artifacts/tdxquant_vs_zsdtdx_weekly_d"))


def _normalize_bar_datetime(raw: Any) -> str:
    dt = str(raw or "").strip()
    if dt == "":
        return dt
    if " " not in dt:
        return dt
    date_part, time_part = dt.split(" ", 1)
    time_part = time_part.strip()
    if time_part.count(":") == 2:
        hour, minute, _second = time_part.split(":", 2)
        return f"{date_part} {hour}:{minute}"
    return dt


def _load_rows_index(path: Path) -> Dict[str, Dict[str, Any]]:
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
                "has_ohlcv": _has_complete_ohlcv(rows_by_dt),
            }
    return out


def _has_complete_ohlcv(rows_by_dt: Dict[str, Dict[str, Any]]) -> bool:
    if not rows_by_dt:
        return False
    for row in rows_by_dt.values():
        for f in ALL_FIELDS:
            if f not in row:
                return False
    return True


def _pct_diff(a: float, b: float) -> float:
    if a == b:
        return 0.0
    denom = max(abs(a), abs(b), 1e-12)
    return abs(a - b) / denom * 100.0


def _field_significant(a: Any, b: Any, field: str) -> Tuple[bool, float]:
    if field in OHLC_FIELDS:
        try:
            pa = float(a)
            pb = float(b)
        except (TypeError, ValueError):
            return True, 100.0
        pct = _pct_diff(pa, pb)
        return pct > 0.5, pct
    try:
        ia = int(a)
        ib = int(b)
    except (TypeError, ValueError):
        return True, 100.0
    if ia == ib:
        return False, 0.0
    denom = max(abs(ia), abs(ib), 1)
    return True, abs(ia - ib) / denom * 100.0


def _load_meta(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    root = _load_root()
    left_path = root / "tdxquant" / "task_records.jsonl"
    right_path = root / "zsdtdx" / "task_records.jsonl"
    left_meta = _load_meta(root / "tdxquant" / "run_meta.json")
    right_meta = _load_meta(root / "zsdtdx" / "run_meta.json")

    if not left_path.is_file() or not right_path.is_file():
        print("缺少 task_records.jsonl，请先分别完成两侧基准", file=sys.stderr)
        return 2

    left_idx = _load_rows_index(left_path)
    right_idx = _load_rows_index(right_path)
    common = sorted(set(left_idx.keys()) & set(right_idx.keys()))
    only_left = sorted(set(left_idx.keys()) - set(right_idx.keys()))
    only_right = sorted(set(right_idx.keys()) - set(left_idx.keys()))

    left_complete = sum(1 for v in left_idx.values() if v["has_ohlcv"] and not v["error"])
    right_complete = sum(1 for v in right_idx.values() if v["has_ohlcv"] and not v["error"])
    left_empty = sum(1 for v in left_idx.values() if v["bar_count"] == 0 or v["error"])
    right_empty = sum(1 for v in right_idx.values() if v["bar_count"] == 0 or v["error"])

    bar_diff_count = 0
    field_counter: Counter[str] = Counter()
    sample_diffs: List[Dict[str, Any]] = []

    for task_key in common:
        lm = left_idx[task_key]["rows_by_dt"]
        rm = right_idx[task_key]["rows_by_dt"]
        common_dt = sorted(set(lm.keys()) & set(rm.keys()))
        for dk in common_dt:
            diffs: Dict[str, Dict[str, Any]] = {}
            for f in ALL_FIELDS:
                sig, pct = _field_significant(lm[dk].get(f), rm[dk].get(f), f)
                if sig:
                    diffs[f] = {"left": lm[dk].get(f), "right": rm[dk].get(f), "pct_diff": round(pct, 6)}
                    field_counter[f] += 1
            if diffs:
                bar_diff_count += 1
                if len(sample_diffs) < 20:
                    sample_diffs.append({"task_key": task_key, "datetime": dk, "field_diffs": diffs})

    report = {
        "timing": {
            "tdxquant_elapsed_seconds": left_meta.get("elapsed_seconds"),
            "zsdtdx_elapsed_seconds": right_meta.get("elapsed_seconds"),
            "tdxquant_fetch_elapsed_seconds": left_meta.get("fetch_elapsed_seconds"),
        },
        "stock_counts": {
            "tdxquant_stock_count": left_meta.get("stock_count"),
            "zsdtdx_stock_count": right_meta.get("stock_count"),
            "tdxquant_ok_stock_count": left_meta.get("ok_stock_count"),
            "zsdtdx_success_tasks": right_meta.get("ok_tasks"),
            "zsdtdx_failed_tasks": right_meta.get("failed_tasks"),
        },
        "completeness": {
            "tdxquant_tasks": len(left_idx),
            "zsdtdx_tasks": len(right_idx),
            "common_tasks": len(common),
            "only_tdxquant_tasks": len(only_left),
            "only_zsdtdx_tasks": len(only_right),
            "tdxquant_complete_ohlcv_tasks": left_complete,
            "zsdtdx_complete_ohlcv_tasks": right_complete,
            "tdxquant_empty_or_error_tasks": left_empty,
            "zsdtdx_empty_or_error_tasks": right_empty,
        },
        "value_diff": {
            "significant_bar_diff_count": bar_diff_count,
            "field_distribution": dict(field_counter),
            "sample_diffs": sample_diffs,
        },
        "only_tdxquant_samples": only_left[:10],
        "only_zsdtdx_samples": only_right[:10],
    }

    out_path = root / "compare_report.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
