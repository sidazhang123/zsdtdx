# -*- coding: utf-8 -*-
"""离线解析 run.log 中的 failure error 分布。"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOG = ROOT / "tests/manual/artifacts/kline_universe_week_validate/run.log"
SUMMARY = ROOT / "tests/manual/artifacts/kline_universe_week_validate/summary.json"
OUT = ROOT / "tests/manual/artifacts/kline_universe_week_validate/error_breakdown.json"

c = Counter()
by_prefix = Counter()
n = 0
# Tee-Object on Windows 常写出 UTF-16 LE
enc = "utf-16" if LOG.read_bytes()[:2] == b"\xff\xfe" else "utf-8"
with LOG.open("r", encoding=enc, errors="replace") as fh:
    for line in fh:
        if '"chunk_failure_count": 1' not in line and '"chunk_failure_count":1' not in line:
            continue
        idx = line.find("{")
        if idx < 0:
            continue
        try:
            obj = json.loads(line[idx:])
        except Exception:
            continue
        for task in obj.get("failure_tasks") or []:
            if not isinstance(task, dict):
                continue
            err = str(task.get("error", "")).strip() or "<empty>"
            code = str(task.get("code", "")).strip()
            c[err] += 1
            n += 1
            pref = code.split(".", 1)[0] if "." in code else "?"
            by_prefix[(pref, err)] += 1

# ohlc reason breakdown from summary
summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
ohlc_reasons = Counter()
for s in summary.get("stock", {}).get("ohlc_samples", []):
    reason = str(s.get("reason", ""))
    if "amount" in reason:
        ohlc_reasons["amount_negative"] += 1
    elif "OHLC" in reason or "不自洽" in reason:
        ohlc_reasons["ohlc_inconsistent"] += 1
    elif "非正" in reason:
        ohlc_reasons["ohlc_nonpositive"] += 1
    else:
        ohlc_reasons[reason[:40] or "unknown"] += 1

payload = {
    "failure_task_error_matches": n,
    "error_counts": dict(c.most_common(50)),
    "top_prefix_error": [
        {"prefix": a, "error": b, "n": v} for (a, b), v in by_prefix.most_common(40)
    ],
    "ohlc_sample_reason_buckets": dict(ohlc_reasons),
}
OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(payload, ensure_ascii=False, indent=2))
