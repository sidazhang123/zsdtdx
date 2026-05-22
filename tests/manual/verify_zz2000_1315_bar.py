# -*- coding: utf-8 -*-
"""验证 TDX 服务端 start=0 是否返回 中证2000 2026-05-18 13:15 15分钟 bar。"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

TARGET = "2026-05-18 13:15:00"
TARGET_DAY = "2026-05-18"
MARKET = 62
CODE = "932000"
CATEGORY = 1  # 15min


def may18_bars(page) -> list[str]:
    out = []
    for row in page or []:
        dt = str((row or {}).get("datetime", "")).strip()
        if dt.startswith(TARGET_DAY):
            out.append(dt)
    return sorted(out)


def report(label: str, page, start: int) -> bool:
    if not page:
        print(f"{label} start={start}: EMPTY")
        return False
    d = may18_bars(page)
    has = TARGET in d
    print(f"{label} start={start} n={len(page)}")
    print(f"  page[0]={page[0].get('datetime')}  page[-1]={page[-1].get('datetime')}")
    print(f"  5/18 ({len(d)}): {d}")
    print(f"  has {TARGET}: {has}")
    return has


def probe_zsdtdx_exhq(host: str) -> bool:
    from zsdtdx.exhq import TdxExHq_API

    h, p = host.rsplit(":", 1)
    api = TdxExHq_API()
    with api.connect(h, int(p)):
        p0 = list(api.get_instrument_bars(CATEGORY, MARKET, CODE, 0, 50) or [])
        p1 = list(api.get_instrument_bars(CATEGORY, MARKET, CODE, 50, 50) or [])
    ok0 = report(f"zsdtdx.exhq @ {host}", p0, 0)
    report(f"zsdtdx.exhq @ {host}", p1, 50)
    return ok0


def probe_pytdx(host: str) -> bool:
    from pytdx.exhq import TdxExHq_API

    h, p = host.rsplit(":", 1)
    api = TdxExHq_API()
    with api.connect(h, int(p)):
        p0 = list(api.get_instrument_bars(CATEGORY, MARKET, CODE, 0, 50) or [])
    return report(f"pytdx.exhq @ {host}", p0, 0)


def probe_zsdtdx_pool() -> bool:
    from zsdtdx import get_client, set_config_path

    cfg = REPO / "logs" / "index_bench_yaml7c" / "config_single_host.yaml"
    set_config_path(str(cfg.resolve()))
    c = get_client()
    route = c.resolve_index_name("中证2000")
    cat = c._freq_to_category("15")
    m, code = int(route["market"]), str(route["code"])
    active = getattr(c.ex_pool, "_hosts", None) or getattr(c.ex_pool, "hosts", None)
    print(f"\nzsdtdx ex_pool active hosts: {active}")
    p0 = list(
        c.ex_pool.call("get_instrument_bars", cat, m, code, 0, 50, allow_none=True) or []
    )
    return report("zsdtdx ex_pool (bench config)", p0, 0)


def main() -> None:
    hosts = ["47.112.95.207:7720", "118.31.28.30:7730"]
    print(f"probe 中证2000 market={MARKET} code={CODE} category={CATEGORY} (15min)")
    print(f"target bar: {TARGET}\n")

    results: dict[str, bool] = {}
    for host in hosts:
        print("\n" + "=" * 56)
        try:
            results[f"zsdtdx:{host}"] = probe_zsdtdx_exhq(host)
        except Exception as exc:
            print(f"zsdtdx.exhq @ {host} ERROR: {exc}")
            results[f"zsdtdx:{host}"] = False
        try:
            results[f"pytdx:{host}"] = probe_pytdx(host)
        except Exception as exc:
            print(f"pytdx.exhq @ {host} ERROR: {exc}")
            results[f"pytdx:{host}"] = False

    print("\n" + "=" * 56)
    try:
        results["ex_pool"] = probe_zsdtdx_pool()
    except Exception as exc:
        print(f"ex_pool ERROR: {exc}")
        results["ex_pool"] = False

    print("\n" + "=" * 56)
    print("SUMMARY")
    for k, v in results.items():
        print(f"  {k}: {'HAS 13:15' if v else 'NO 13:15 / failed'}")


if __name__ == "__main__":
    main()
