"""
模块：`biz/stock_stat.py`。

职责：
1. 定义全市场股票统计宽表的对外列、单位折算与 A/B/C/D 行聚合。
2. 提供 `fetch_stock_stat` 门面，委托统一客户端拉数并返回 DataFrame。

边界：
1. 不组协议包、不管理连接池。
2. 不强制刷新板块命名文件缓存；由客户端复用 `block_file_cache`。
3. 无 codes 入参；范围由前缀过滤与配置 `include_indices` 决定。
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, List, Sequence

import pandas as pd

from zsdtdx.parser.tdxstat import STAT_MERGE_KEYS
from zsdtdx.util.helper import call_with_client
from zsdtdx.engine.unified_client import UnifiedTdxClient

# 对外宽表列（与看板脚本标定一致）
STOCK_STAT_COLUMNS: List[str] = [
    "market",
    "code",
    "price",
    "last_close",
    "open",
    "high",
    "low",
    "vol",
    "cur_vol",
    "amount",
    "bid1",
    "ask1",
    "bid_vol1",
    "ask_vol1",
    "s_vol",
    "b_vol",
    "change",
    "change_pct",
    "amplitude_pct",
    "avg_price",
    "io_ratio",
    "float_shares",
    "total_shares",
    "finance_date",
    "ipo_date",
    "report_period",
    "region",
    "industry",
    "total_assets",
    "current_assets",
    "fixed_assets",
    "intangible_assets",
    "shareholder_count",
    "current_liab",
    "minority_equity",
    "capital_reserve",
    "net_assets",
    "debt_ratio_pct",
    "equity_ratio_pct",
    "operating_revenue",
    "operating_cost",
    "accounts_receivable",
    "operating_profit",
    "invest_income",
    "operating_cashflow",
    "total_cashflow",
    "inventory",
    "total_profit",
    "profit_after_tax",
    "net_profit",
    "undistributed_profit",
    "bvps",
    "eps",
    "capital_reserve_ps",
    "undistributed_ps",
    "ocf_ps",
    "roe_pct",
    "gross_margin_pct",
    "op_margin_pct",
    "net_margin_pct",
    "float_mkt_cap",
    "total_mkt_cap",
    "turnover_pct",
    "pb",
    "ps",
    "pcf",
    "stat_asof",
    "beta",
    "consec_up_days",
    "chg_pct_prev",
    "chg_pct_3d",
    "chg_pct_5d",
    "chg_pct_10d",
    "chg_pct_20d",
    "chg_pct_30d",
    "chg_pct_60d",
    "chg_pct_1y",
    "chg_pct_mtd",
    "chg_pct_ytd",
    "pe_ttm",
    "pe_static",
    "dividend_yield_pct",
    "float_shares_z",
    "net_profit_ex",
    "employee_count",
    "rd_expense",
    "cash_funds",
    "contract_liab",
    "limit_up_days_1y",
]

_C_FIELD_KEYS = (
    "float_shares",
    "total_shares",
    "finance_date",
    "ipo_date",
    "report_period",
    "total_assets",
    "current_assets",
    "fixed_assets",
    "intangible_assets",
    "shareholder_count",
    "current_liab",
    "minority_equity",
    "capital_reserve",
    "net_assets",
    "debt_ratio_pct",
    "equity_ratio_pct",
    "operating_revenue",
    "operating_cost",
    "accounts_receivable",
    "operating_profit",
    "invest_income",
    "operating_cashflow",
    "total_cashflow",
    "inventory",
    "total_profit",
    "profit_after_tax",
    "net_profit",
    "undistributed_profit",
    "bvps",
    "eps",
    "capital_reserve_ps",
    "undistributed_ps",
    "ocf_ps",
    "roe_pct",
    "gross_margin_pct",
    "op_margin_pct",
    "net_margin_pct",
    "_region_code",
    "_industry_code",
)

_YUAN_TO_WAN_KEYS = ("amount", "float_mkt_cap", "total_mkt_cap")
_QIAN_TO_WAN_KEYS = (
    "total_assets",
    "current_assets",
    "fixed_assets",
    "intangible_assets",
    "current_liab",
    "minority_equity",
    "capital_reserve",
    "net_assets",
    "operating_revenue",
    "operating_cost",
    "accounts_receivable",
    "operating_profit",
    "invest_income",
    "operating_cashflow",
    "total_cashflow",
    "inventory",
    "total_profit",
    "profit_after_tax",
    "net_profit",
    "undistributed_profit",
)


def is_stock_stat_row(market: int, code: str, *, include_indices: bool) -> bool:
    """
    输入：市场号、代码、是否保留指数。
    输出：是否纳入全市场股票集合。
    用途：过滤板块/指数占位。
    边界：粗分代码形态，不读码表。
    """
    c = str(code).strip()
    if len(c) != 6 or not c.isdigit():
        return False
    if not include_indices:
        if c.startswith(("399", "880")) or c in {"999999", "999998", "999997"}:
            return False
    if market == 0:
        return c.startswith(("000", "001", "002", "003", "300", "301"))
    if market == 1:
        return c.startswith(("60", "68")) or include_indices
    if market == 2:
        return c.startswith(("43", "83", "87", "88", "92"))
    return False


def _div_unit(v: Any, divisor: float, ndigits: int) -> Any:
    """
    输入：原值、除数、小数位。
    输出：折算后浮点；None/NaN/非法保持 None。
    用途：导出前单位换算。
    边界：不修改非数值；除数为 0 时原样返回。
    """
    if v is None or divisor == 0:
        return v
    try:
        x = float(v)
    except (TypeError, ValueError):
        return v
    if x != x:
        return None
    return round(x / divisor, ndigits)


def scale_row_units_to_wan(row: Dict[str, Any]) -> OrderedDict:
    """
    输入：协议原单位聚合行（衍生字段已算完）。
    输出：金额已折为「万」的行副本。
    用途：对外统一万元；总量/现量保持手。
    边界：不改价格/比率/股本(万股)/成交量(手)/tdxstat 已是万元的字段。
    """
    out = OrderedDict(row)
    for k in _YUAN_TO_WAN_KEYS:
        if k in out:
            out[k] = _div_unit(out.get(k), 10000.0, 4)
    for k in _QIAN_TO_WAN_KEYS:
        if k in out:
            out[k] = _div_unit(out.get(k), 10.0, 4)
    return out


def project_stock_stat_row(row: Dict[str, Any]) -> OrderedDict:
    """
    输入：聚合行（协议原单位）。
    输出：仅保留对外字段，且金额折为万。
    用途：压缩输出、统一量纲。
    边界：缺键填 None；须在均价/换手/市销等衍生计算之后调用。
    """
    scaled = scale_row_units_to_wan(row)
    return OrderedDict((k, scaled.get(k)) for k in STOCK_STAT_COLUMNS)


def merge_c_into_ab(ab: Dict[str, Any], fin: Dict[str, Any]) -> None:
    """
    输入：A/B 行、C 行（就地写入 ab）。
    输出：无。
    用途：合并财务并计算市值/换手/市净市销市现。
    边界：财务缺失时 C 字段保持缺省；估值用现价优先、否则昨收。
    """
    for k in _C_FIELD_KEYS:
        ab[k] = fin.get(k)
    try:
        price = float(ab.get("price") or 0)
        last = float(ab.get("last_close") or 0)
        vol = float(ab.get("vol") or 0)
        float_shares = float(fin.get("float_shares") or 0)
        total_shares = float(fin.get("total_shares") or 0)
        px = price if price > 0 else last
    except (TypeError, ValueError):
        ab["float_mkt_cap"] = None
        ab["total_mkt_cap"] = None
        ab["turnover_pct"] = None
        ab["pb"] = ab["ps"] = ab["pcf"] = None
        return
    ab["float_mkt_cap"] = (
        round(price * float_shares * 10000, 2)
        if price > 0 and float_shares > 0
        else None
    )
    ab["total_mkt_cap"] = (
        round(price * total_shares * 10000, 2)
        if price > 0 and total_shares > 0
        else None
    )
    ab["turnover_pct"] = round(vol / float_shares, 4) if float_shares > 0 else None
    if ab["float_mkt_cap"] is None and last > 0 and float_shares > 0:
        ab["float_mkt_cap"] = round(last * float_shares * 10000, 2)

    bvps = fin.get("bvps")
    try:
        ab["pb"] = (
            round(px / float(bvps), 2) if px > 0 and bvps and float(bvps) != 0 else None
        )
    except (TypeError, ValueError, ZeroDivisionError):
        ab["pb"] = None
    try:
        rev = float(fin.get("operating_revenue") or 0) * 1000.0
        mcap = ab.get("total_mkt_cap") or (
            px * total_shares * 10000 if px > 0 and total_shares > 0 else 0
        )
        ab["ps"] = round(mcap / rev, 2) if mcap and rev else None
    except (TypeError, ValueError, ZeroDivisionError):
        ab["ps"] = None
    try:
        ocf_ps = fin.get("ocf_ps")
        ab["pcf"] = (
            round(px / float(ocf_ps), 2)
            if px > 0 and ocf_ps and float(ocf_ps) != 0
            else None
        )
    except (TypeError, ValueError, ZeroDivisionError):
        ab["pcf"] = None


def assemble_stock_stat_rows(
    ab_rows: Sequence[Dict[str, Any]],
    fin_map: Dict[str, Dict[str, Any]],
    stat_map: Dict[str, Dict[str, Any]],
    region_map: Dict[int, str],
    industry_name_map: Dict[str, str],
    code_hy_map: Dict[str, str],
) -> List[OrderedDict]:
    """
    输入：A/B 行、C 财务表、D 统计表、地区/行业名表。
    输出：已投影、单位折算后的宽表行列表。
    用途：离线/在线共用的聚合核心。
    边界：财务或统计缺失时对应列填 None。
    """
    rows: List[OrderedDict] = []
    for r in ab_rows:
        code = str(r["code"])
        row = OrderedDict(r)
        fin = fin_map.get(code)
        if fin:
            merge_c_into_ab(row, fin)
        else:
            for k in _C_FIELD_KEYS:
                if not str(k).startswith("_"):
                    row.setdefault(k, None)
            for k in (
                "float_mkt_cap",
                "total_mkt_cap",
                "turnover_pct",
                "pb",
                "ps",
                "pcf",
            ):
                row.setdefault(k, None)
        dy = fin.get("_region_code") if fin is not None else None
        row["region"] = region_map.get(int(dy)) if dy is not None else None
        t_code = code_hy_map.get(code)
        row["industry"] = industry_name_map.get(t_code) if t_code else None
        row.pop("_region_code", None)
        row.pop("_industry_code", None)
        row.pop("region_code", None)
        row.pop("industry_code", None)
        row.pop("report_period_code", None)
        if not row.get("report_period"):
            row["report_period"] = None
        st = stat_map.get(code) or {}
        for k in STAT_MERGE_KEYS:
            row[k] = st.get(k)
        rows.append(project_stock_stat_row(row))
    return rows


def rows_to_stock_stat_df(rows: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    """
    输入：投影后的宽表行列表。
    输出：按 `STOCK_STAT_COLUMNS` 列序的 DataFrame。
    用途：对外固定列宽表。
    边界：空列表返回空表（含全部列）。
    """
    if not rows:
        return pd.DataFrame(columns=STOCK_STAT_COLUMNS)
    return pd.DataFrame(list(rows), columns=STOCK_STAT_COLUMNS)


def fetch_stock_stat() -> pd.DataFrame:
    """
    输入：无。
    输出：全市场股票统计宽表 DataFrame。
    用途：simple_api 门面；需处于 `with get_client()`。
    边界：无 codes；配置见 `config.yaml` 的 `stock_stat`。
    """
    from zsdtdx.simple_api import get_client

    return call_with_client(
        lambda client: client.get_stock_stat(),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )
