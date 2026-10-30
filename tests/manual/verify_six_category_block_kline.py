"""
模块：`verify_six_category_block_kline.py`。

职责：
1. 通过公开接口验证全部六类板块在五个周期的K线覆盖情况。
2. 分别统计有行情、无代码空结果、服务器空页/区间无行情及真实请求失败。

边界：
1. 需要真实网络；输出仅为本次窗口和下载快照的覆盖记录。
2. 不更改正式缓存；原始文件、报告及明细均放在独立验收目录。
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from zsdtdx import (  # noqa: E402
    BlockKlineTask,
    destroy_parallel_fetcher,
    get_block_kline,
    get_block_names,
    get_client,
    get_stock_concepts,
    set_config_path,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient  # noqa: E402


def main():
    """按分类与周期对账全部任务，保存可重现的快照、空项和失败项。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-10-01")
    parser.add_argument("--end", default="2026-10-30")
    parser.add_argument("--freq", nargs="+", default=["15", "30", "60", "d", "w"])
    args = parser.parse_args()
    out = (
        ROOT
        / "tests/manual/artifacts/six_category_block_kline"
        / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    out.mkdir(parents=True)
    cache = out / "cache"
    config = out / "config.yaml"
    config.write_text(
        f'logging:\n  level: "ERROR"\ncatalog_cache:\n  path: "{cache.as_posix()}"\n',
        encoding="utf-8",
    )
    started = time.monotonic()
    details, sync_samples, snapshot = [], [], {}
    try:
        set_config_path(str(config))
        with get_client() as client:
            names = get_block_names()
            catalog = client._load_block_index_catalog()
            raw_files = client.get_block_named_files()
            concepts = get_stock_concepts()
            derived = client._ensure_block_catalog_snapshot()
            fresh_client = UnifiedTdxClient(config_path=str(config))
            fresh_client._download_named_hq_file = lambda *a: (_ for _ in ()).throw(
                AssertionError("磁盘命中后不应下载源文件")
            )
            fresh_client.get_stock_code_name_map = lambda: (_ for _ in ()).throw(
                AssertionError("磁盘命中后不应重新拉码表")
            )
            assert fresh_client.get_block_names() == names
            assert fresh_client.get_stock_concepts() == concepts
            assert set(concepts["names"]) <= set(names)
            cache_check = {
                "filename": "block_catalog.pkl",
                "build_date": derived["cache_date"],
                "ttl_seconds": 6 * 3600,
                "source_fetched_at": derived["source_fetched_at"],
                "expires_at": derived["source_fetched_at"] + 6 * 3600,
                "stocks": len(derived["code_name"]),
                "membership_records": sum(
                    len(r["members"]) for r in derived["records"]
                ),
                "stock_board_relationships": sum(
                    len(v) for v in concepts["map"].values()
                ),
                "disk_reload_matches": True,
            }
            snapshot = {
                name: hashlib.sha256(raw).hexdigest() for name, raw in raw_files.items()
            }
            for name, raw in raw_files.items():
                (out / name).write_bytes(raw)
            routes = {
                identity: dict(record) for identity, record in catalog.by_name.items()
            }
            print(
                json.dumps(
                    {
                        "stage": "catalog",
                        "names": len(names),
                        "categories": dict(Counter(n[0] for n in names)),
                        "out": str(out),
                    },
                    ensure_ascii=True,
                ),
                flush=True,
            )
            sample_ids = [
                identity
                for identity in [
                    ("通达信基础行业", "元器件"),
                    ("研究行业中类", "元器件"),
                ]
                if identity in routes
            ]
            sync_samples = get_block_kline(
                task=[BlockKlineTask(i, "d", args.start, args.end) for i in sample_ids],
                mode="sync",
            )
            job = get_block_kline(
                task=[
                    BlockKlineTask(i, freq, args.start, args.end)
                    for i in names
                    for freq in args.freq
                ],
                mode="async",
            )
        while True:
            event = job.queue.get(timeout=600)
            if event["event"] == "done":
                done = event
                break
            identity = tuple(event["task"]["block_name"])
            rows = event.get("rows") or []
            error = event.get("error")
            status = (
                "failed"
                if error
                else "data"
                if rows
                else "empty_no_code"
                if not routes[identity]["code"]
                else "empty_server_or_window"
            )
            assert all(tuple(row["block_name"]) == identity for row in rows)
            bad = [
                row
                for row in rows
                if not row["low"]
                <= min(row["open"], row["close"])
                <= max(row["open"], row["close"])
                <= row["high"]
            ]
            details.append(
                {
                    "category": identity[0],
                    "name": identity[1],
                    "kind": routes[identity]["kind"],
                    "code": routes[identity]["code"],
                    "freq": event["task"]["freq"],
                    "status": status,
                    "rows": len(rows),
                    "error": error,
                    "empty_reason": routes[identity]["empty_reason"]
                    if not rows
                    else "",
                    "ohlc_bad": len(bad),
                    "sample": rows[:1],
                    "bad_sample": bad[:1],
                }
            )
            if len(details) % 250 == 0:
                print(
                    json.dumps(
                        {
                            "stage": "progress",
                            "received": len(details),
                            "status": dict(Counter(d["status"] for d in details)),
                        }
                    ),
                    flush=True,
                )
        summary = job.result(timeout=10)
        assert len(details) == len(names) * len(args.freq) == summary["total_tasks"]
        assert len({(d["category"], d["name"], d["freq"]) for d in details}) == len(
            details
        )
        by_category, by_frequency = defaultdict(Counter), defaultdict(Counter)
        for d in details:
            by_category[d["category"]][d["status"]] += 1
            by_frequency[d["freq"]][d["status"]] += 1
        report = {
            "window": [args.start, args.end],
            "frequencies": args.freq,
            "snapshot": snapshot,
            "names": len(names),
            "cache": cache_check,
            "category_names": dict(Counter(n[0] for n in names)),
            "status": dict(Counter(d["status"] for d in details)),
            "by_category": dict(by_category),
            "by_frequency": dict(by_frequency),
            "done": done,
            "summary": summary,
            "ohlc_bad": sum(d["ohlc_bad"] for d in details),
            "sync_samples": [
                {
                    "task": r["task"],
                    "rows": len(r["rows"]),
                    "sample": r["rows"][:1],
                    "error": r["error"],
                }
                for r in sync_samples
            ],
            "elapsed_seconds": round(time.monotonic() - started, 2),
        }
        (out / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(report, ensure_ascii=True), flush=True)
    finally:
        (out / "details.json").write_text(
            json.dumps(details, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        destroy_parallel_fetcher()


if __name__ == "__main__":
    main()
