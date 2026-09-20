# -*- coding: utf-8 -*-
"""
模块：check_boc_ohlcv_quality.py

用途：
1. 用 TdxQuant 拉取中国银行 601988.SH 在 15/30/60/d/w 周期下 2019 年至今的 OHLCV。
2. 检查字段齐全性、价格逻辑、空值与零成交占位等数据质量项。

边界：
1. 需通达信客户端已登录；分钟线依赖本地盘后/分钟数据是否已下载。
2. TdxQuant 周期映射：15m/30m/1h(60m)/1d/1w。
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

TDX_USER = r"C:\new_tdx_test\PYPlugins\user"
SCRIPT = str(Path(__file__).resolve())
CODE = "601988.SH"
START = "20190101"
END = "20260703"
PERIODS = [
    ("15", "15m"),
    ("30", "30m"),
    ("60", "1h"),
    ("d", "1d"),
    ("w", "1w"),
]
OHLCV = ["Open", "High", "Low", "Close", "Volume"]
OUT_DIR = Path(__file__).resolve().parent / "artifacts" / "boc_ohlcv_quality"


def _boot() -> Any:
    if TDX_USER not in sys.path:
        sys.path.insert(0, TDX_USER)
    from tqcenter import tq

    tq.initialize(SCRIPT)
    return tq


def _df_field(data: Dict[str, Any], field: str, code: str) -> Optional[pd.Series]:
    block = data.get(field) if isinstance(data, dict) else None
    if block is None or getattr(block, "empty", True):
        return None
    if code in block.columns:
        return block[code]
    return None


def _fetch(tq: Any, period: str) -> Dict[str, Any]:
    """优先区间拉取，空则回退 count 大窗口。"""
    data = tq.get_market_data(
        field_list=OHLCV + ["Amount"],
        stock_list=[CODE],
        period=period,
        start_time=START,
        end_time=END,
        count=-1,
        dividend_type="none",
        fill_data=True,
    )
    close = _df_field(data, "Close", CODE)
    if close is not None and len(close) > 0:
        return data
    # 回退：从今日往前取大 count
    data2 = tq.get_market_data(
        field_list=OHLCV + ["Amount"],
        stock_list=[CODE],
        period=period,
        start_time="",
        end_time=END,
        count=50000,
        dividend_type="none",
        fill_data=True,
    )
    close2 = _df_field(data2, "Close", CODE)
    if close2 is not None and len(close2) > 0:
        # 过滤 2019 及以后
        idx = close2.index
        mask = pd.to_datetime(idx) >= pd.Timestamp("2019-01-01")
        out: Dict[str, Any] = {}
        for f in OHLCV + ["Amount"]:
            ser = _df_field(data2, f, CODE)
            if ser is not None:
                out[f] = pd.DataFrame({CODE: ser[mask]})
        return out
    return data if isinstance(data, dict) else {}


def _analyze(data: Dict[str, Any], label: str, period: str) -> Dict[str, Any]:
    """对单周期数据做 OHLCV 质量审计。"""
    frames: Dict[str, pd.Series] = {}
    for f in OHLCV + ["Amount"]:
        ser = _df_field(data, f, CODE)
        if ser is not None:
            frames[f.lower()] = ser

    bar_count = len(frames.get("close", []))
    report: Dict[str, Any] = {
        "freq_label": label,
        "period": period,
        "bar_count": bar_count,
        "fields_present": sorted(frames.keys()),
        "missing_fields": [x for x in ["open", "high", "low", "close", "volume"] if x not in frames],
    }
    if bar_count == 0:
        report["status"] = "empty"
        return report

    df = pd.DataFrame(frames)
    df.index = pd.to_datetime(df.index)

    # 2019 过滤（双保险）
    df = df[df.index >= pd.Timestamp("2019-01-01")]
    report["bar_count"] = int(len(df))
    if len(df) == 0:
        report["status"] = "empty_after_2019_filter"
        return report

    report["date_first"] = str(df.index.min())
    report["date_last"] = str(df.index.max())

    # 字段空值
    nulls = {c: int(df[c].isna().sum()) for c in df.columns}
    report["null_counts"] = nulls
    report["has_null_ohlcv"] = any(nulls.get(c, 0) > 0 for c in ["open", "high", "low", "close", "volume"])

    # 零成交但 OHLC 全 0（疑似占位）
    zero_ohlc = (
        (df["open"] == 0) & (df["high"] == 0) & (df["low"] == 0) & (df["close"] == 0)
    )
    zero_vol = df["volume"] == 0
    report["zero_volume_bars"] = int(zero_vol.sum())
    report["zero_ohlc_bars"] = int(zero_ohlc.sum())
    report["zero_ohlc_and_zero_volume_bars"] = int((zero_ohlc & zero_vol).sum())

    # 价格逻辑
    bad_high_low = df["high"] < df["low"]
    bad_high = (df["high"] < df["open"]) | (df["high"] < df["close"])
    bad_low = (df["low"] > df["open"]) | (df["low"] > df["close"])
    nonpos_price = (df["open"] <= 0) | (df["high"] <= 0) | (df["low"] <= 0) | (df["close"] <= 0)

    report["logic_violations"] = {
        "high_lt_low": int(bad_high_low.sum()),
        "high_lt_open_or_close": int(bad_high.sum()),
        "low_gt_open_or_close": int(bad_low.sum()),
        "non_positive_price": int(nonpos_price.sum()),
    }
    report["logic_ok"] = sum(report["logic_violations"].values()) == 0

    # 有成交量但价格为 0
    vol_gt0_price0 = (df["volume"] > 0) & nonpos_price
    report["volume_gt0_but_price_nonpositive"] = int(vol_gt0_price0.sum())

    # 完整性判定
    complete = (
        not report["missing_fields"]
        and not report["has_null_ohlcv"]
        and report["logic_ok"]
        and report["volume_gt0_but_price_nonpositive"] == 0
        and bar_count > 0
    )
    report["ohlcv_complete"] = complete
    report["status"] = "ok" if complete else "issues"

    # 样例问题 bar
    issues_mask = bad_high_low | bad_high | bad_low | nonpos_price | vol_gt0_price0
    if issues_mask.any():
        sample = df[issues_mask].head(5)
        report["issue_samples"] = [
            {"datetime": str(idx), **{k: float(v) if k != "volume" else int(v) for k, v in row.items()}}
            for idx, row in sample.iterrows()
        ]

    # 日线额外：估算交易日缺口（仅 d）
    if label == "d" and len(df) > 1:
        daily_idx = df.index.normalize().unique()
        span_days = (daily_idx.max() - daily_idx.min()).days
        report["trading_day_span_calendar_days"] = int(span_days)
        report["trading_bars"] = int(len(daily_idx))

    return report


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tq = _boot()

    # 预热缓存：日线必刷，分钟尝试 1m 刷盘（15/30/60 依赖分钟底层）
    for p in ["1d", "1m"]:
        try:
            tq.refresh_kline(stock_list=[CODE], period=p)
        except Exception as exc:
            print(f"refresh_kline {p} warn: {exc}")
        time.sleep(1)

    all_reports: List[Dict[str, Any]] = []
    for label, period in PERIODS:
        t0 = time.perf_counter()
        data = _fetch(tq, period)
        rep = _analyze(data, label, period)
        rep["elapsed_seconds"] = round(time.perf_counter() - t0, 3)
        all_reports.append(rep)
        print(json.dumps(rep, ensure_ascii=False))

    summary = {
        "code": CODE,
        "name": "中国银行",
        "range": f"{START}~{END}",
        "freqs": [x[0] for x in PERIODS],
        "all_complete": all(r.get("ohlcv_complete") for r in all_reports if r.get("bar_count", 0) > 0),
        "reports": all_reports,
    }
    out_path = OUT_DIR / "quality_report.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=== SUMMARY ===")
    print(json.dumps({k: v for k, v in summary.items() if k != "reports"}, ensure_ascii=False, indent=2))
    try:
        tq.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
