"""
模块：`verify_f10_markets_sz_sh_bj.py`。

职责：
1. 验收 get_company_info：深/沪/京各 3 只股票全部 F10 标签可完整获取。
2. 对照目录 length 与正文 GBK 字节，并检查文头文尾语义完整性。

边界：
1. 需访问行情服务器；不由 pytest 收集。
2. 北交所依赖统一客户端 F10 路由（行情 market=2，协议 market=0）。
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts"

SAMPLES = {
    "深圳": ["000001", "300063", "002415"],
    "上海": ["600000", "601318", "688981"],
    "北京": ["920002", "920008", "920000"],
}


def _check_one_category(
    code: str,
    cat_meta: Dict[str, Any],
    content: str,
) -> Dict[str, Any]:
    """
    输入：代码、目录元数据、正文。
    输出：单栏完整性结果。
    用途：长度与头尾语义检查。
    边界：允许 GBK ignore 造成少量字节差（≤32）。
    """
    name = str(cat_meta.get("name") or "").strip()
    exp = int(cat_meta.get("length") or 0)
    gbk_len = len(content.encode("gbk", "ignore"))
    issues: List[str] = []
    if not content.strip():
        issues.append("empty")
    if name and f"☆{name}☆" not in content[:150] and name not in content[:250]:
        issues.append("missing_title")
    if code not in content[:300]:
        issues.append("missing_code")
    if exp > 0 and exp - gbk_len > 32:
        issues.append(f"short_by_{exp - gbk_len}")
    # 允许以「暂无数据」等正常收束
    if content and not re.search(
        r"(暂无数据|[。；！？】）)\r?\n|[\r\n─━┘┐])\s*$", content
    ):
        if not content.rstrip().endswith(("数据", "。", "；")):
            issues.append("suspicious_tail")
    return {
        "name": name,
        "index": cat_meta.get("index"),
        "expect_bytes": exp,
        "got_gbk_bytes": gbk_len,
        "delta": gbk_len - exp,
        "ok": not issues,
        "issues": issues,
        "head": content[:50].replace("\r", "\\r").replace("\n", "\\n"),
        "tail": content[-60:].replace("\r", "\\r").replace("\n", "\\n")
        if content
        else "",
    }


def main() -> int:
    """
    输入：无。
    输出：全部通过返回 0，否则 1。
    用途：京沪深 F10 全标签验收入口。
    边界：单票失败不中断其余票。
    """
    from zsdtdx import get_client, get_company_info, set_config_path

    set_config_path(str(_SRC / "zsdtdx" / "config.yaml"), async_background_probe=False)
    report: Dict[str, Any] = {"ok": True, "markets": {}}

    with get_client() as client:
        client.get_all_stock_list(return_df=True)
        for market_name, codes in SAMPLES.items():
            market_block: Dict[str, Any] = {"ok": True, "stocks": []}
            for code in codes:
                route = client._lookup_stock_route(code)
                protocol_market = (
                    None
                    if route is None
                    else client._company_info_protocol_market(route)
                )
                cats = client.std_pool.call(
                    "get_company_info_category",
                    int(protocol_market),
                    str(code),
                    allow_none=True,
                )
                rows = get_company_info(codes=[code], return_df=False, mode="sync") or []
                by_name = {
                    str(r.get("category") or "").strip(): str(r.get("content") or "")
                    for r in rows
                }
                stock: Dict[str, Any] = {
                    "code": code,
                    "route_market": None if not route else route.get("market"),
                    "protocol_market": protocol_market,
                    "category_n": 0 if not cats else len(cats),
                    "rows_n": len(rows),
                    "ok": True,
                    "issues": [],
                    "categories": [],
                }
                if not route:
                    stock["ok"] = False
                    stock["issues"].append("route_missing")
                elif not cats:
                    stock["ok"] = False
                    stock["issues"].append("category_empty")
                elif len(rows) != len(cats):
                    stock["ok"] = False
                    stock["issues"].append(
                        f"row_count_mismatch rows={len(rows)} cats={len(cats)}"
                    )
                else:
                    for cat in cats:
                        cname = str(cat.get("name") or "").strip()
                        item = _check_one_category(code, cat, by_name.get(cname, ""))
                        if not item["ok"]:
                            stock["ok"] = False
                        stock["categories"].append(item)
                if not stock["ok"]:
                    market_block["ok"] = False
                    report["ok"] = False
                market_block["stocks"].append(stock)
                print(
                    f"==== {market_name} {code} route={stock['route_market']} "
                    f"proto={stock['protocol_market']} cats={stock['category_n']} "
                    f"ok={stock['ok']} {stock['issues'] or '-'}",
                    flush=True,
                )
                for item in stock["categories"]:
                    mark = "PASS" if item["ok"] else "FAIL"
                    print(
                        f"  {mark} [{item['index']}] {item['name']}: "
                        f"{item['got_gbk_bytes']}/{item['expect_bytes']} "
                        f"delta={item['delta']} {item['issues'] or ''}",
                        flush=True,
                    )
            report["markets"][market_name] = market_block

    _ART.mkdir(parents=True, exist_ok=True)
    out = _ART / "verify_f10_markets_sz_sh_bj.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OVERALL {report['ok']}", flush=True)
    print(f"REPORT={out}", flush=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
