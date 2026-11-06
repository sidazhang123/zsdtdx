"""
模块：`parser/stock_regions.py`。

职责：
1. 将批量财务的 DY 地区码换成股票地区归属。
2. 将地区归属合入股票板块的 `{names, map}` 返回值。

边界：
1. 不联网、不读写缓存；只处理当前股票码表中的沪深北代码。
2. 本模块仅处理股票地区归属；地区指数 K 线路由由板块目录独立解析。
"""

import re


def stock_region_targets(code_name):
    """输入股票码表，输出带前缀代码到财务请求参数；港股通不请求标准行情财务。"""
    markets = {"sz": 0, "sh": 1, "bj": 2}
    return {
        key: (markets[key[:2]], key[3:])
        for key in code_name
        if re.fullmatch(r"(?:sz|sh|bj)\.\d{6}", key) and key[3:] != "000000"
    }


def build_stock_region_map(targets, finance_map, region_names):
    """输入请求目标、完整财务和地区名称表，输出前缀代码到地区名；零码和未知地区不猜测。"""
    regions = {}
    for key, (_, code) in targets.items():
        row = finance_map[code]
        region_code = row["_region_code"]
        if row.get("parse_error") or type(region_code) is not int or region_code < 0:
            raise ValueError(f"股票地区字段无效: {key}")
        name = region_names.get(region_code) if region_code else None
        if name:
            regions[key] = name
    return regions


def merge_stock_regions(payload, code_name, regions):
    """输入板块归属及股票地区，输出隔离可变容器、去重排序的七类归属；同名股票取并集。"""
    names = set(payload["names"])
    mapping = {name: set(blocks) for name, blocks in payload["map"].items()}
    for code, region in regions.items():
        stock_name = str(code_name.get(code) or "").strip()
        if not stock_name:
            continue
        identity = ("地区", region)
        names.add(identity)
        mapping.setdefault(stock_name, set()).add(identity)
    return {
        "names": sorted(names),
        "map": {name: sorted(blocks) for name, blocks in mapping.items()},
    }
