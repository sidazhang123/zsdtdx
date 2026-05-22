# -*- coding: utf-8 -*-
"""
实测 TDX K 线分页方向（走 zsdtdx socket 池，不读注释下结论）。

用法:
  py tests/manual/probe_kline_pagination_direction.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
DEFAULT_CFG = REPO / "src" / "zsdtdx" / "config.yaml"


def _parse_dt(text: str) -> datetime:
    return datetime.strptime(str(text).strip(), "%Y-%m-%d %H:%M:%S")


def _probe(fetch_page, *, pages: int, page_size: int) -> List[str]:
    lines: List[str] = []
    start = 0
    prev: Optional[tuple[str, str]] = None
    for i in range(pages):
        page = list(fetch_page(start, page_size) or [])
        if not page:
            lines.append(f"  page{i} start={start}: EMPTY")
            break
        dts = sorted(str(x.get("datetime", "")) for x in page)
        lines.append(
            f"  page{i} start={start} n={len(page)} "
            f"[0]={page[0].get('datetime')} [-1]={page[-1].get('datetime')} "
            f"min={dts[0]} max={dts[-1]}"
        )
        if prev:
            pmn, pmx = prev
            cmn, cmx = dts[0], dts[-1]
            if cmx < pmn:
                rel = "next_page_OLDER (start+=n -> earlier)"
            elif cmn > pmx:
                rel = "next_page_NEWER (unexpected)"
            else:
                rel = "overlap"
            lines.append(f"    vs_prev: {rel}")
        prev = (dts[0], dts[-1])
        if len(page) < page_size:
            break
        start += len(page)
    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(DEFAULT_CFG))
    parser.add_argument("--pages", type=int, default=6)
    parser.add_argument("--page-size", type=int, default=15)
    args = parser.parse_args()

    from zsdtdx import get_client, set_config_path

    set_config_path(str(Path(args.config).resolve()))
    client = get_client()
    cat = client._freq_to_category("15")
    route = client.resolve_index_name("中证2000")
    m, code = int(route["market"]), str(route["code"])

    print("=== get_instrument_bars ex 中证2000 15min ===")
    for line in _probe(
        lambda start, count: client.ex_pool.call(
            "get_instrument_bars", cat, m, code, int(start), int(count), allow_none=True
        ),
        pages=args.pages,
        page_size=args.page_size,
    ):
        print(line)

    print("\n=== get_index_bars std 中证1000 15min ===")
    for line in _probe(
        lambda start, count: client.std_pool.call(
            "get_index_bars", cat, 0, "399852", int(start), int(count), allow_none=True
        ),
        pages=args.pages,
        page_size=args.page_size,
    ):
        print(line)

    print(
        "\n=== CONCLUSION ===\n"
        "1. 页内: page[0]=min, page[-1]=max（升序）\n"
        "2. start=0 一段最新 bar；start+=len(page) 后下一页 vs_prev=OLDER\n"
        "3. 拉 [start_time,end_time]: 从 start=0 递增，直到 page[0] <= start_time\n"
        "4. 13:15 等「更晚」的 bar 在 start=0 那页（page[-1] 侧），不会在更旧页\n"
    )


if __name__ == "__main__":
    main()
