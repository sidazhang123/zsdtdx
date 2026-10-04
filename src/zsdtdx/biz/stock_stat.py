"""
模块：`biz/stock_stat.py`。

职责：
1. 定义全市场股票统计宽表对外列（内部英文键 + 中文表头映射）。
2. 将行情行、财务行、tdxstat 快照按代码左连接，向量化计算市值/换手/估值后输出 DataFrame。
3. 假定 tdxstat 为昨收时点快照，按现价/昨收把 PE、股息率、n 日涨幅折算到当前价。
4. 提供 `fetch_stock_stat` 门面，委托统一客户端拉数。

边界：
1. 不组协议包、不管理连接池。
2. 不强制刷新板块命名文件缓存；由客户端复用 `block_file_cache`。
3. 无 codes 入参；范围由前缀过滤与配置 `include_indices` 决定。
4. 现价非正则估值价回退昨收；仅当行内统计基准日不是当日时才按现价折算价敏字段。
5. 不接码表；返回列为中文表头，见 `STOCK_STAT_COLUMN_LABELS`。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Sequence

import numpy as np
import pandas as pd

from zsdtdx.parser.tdxstat import TDXSTAT_FIELD_KEYS
from zsdtdx.util.helper import call_with_client
from zsdtdx.engine.unified_client import UnifiedTdxClient

# 对外宽表列顺序（内部英文键；返回前映射为中文表头）。
STOCK_STAT_COLUMNS: List[str] = [
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
]

# 英文键 → 中文表头；仅最终返回列写说明/单位。
# 返回量纲：价/涨跌额/均价为元（不标）；量为手；金额与市值为万元；股本为万股；
# 每股*为元；已是百分点的比率标(%)；市净/市销/市现/市盈/贝塔不标单位。
# 涨跌额=现价-昨收；涨幅=(现价-昨收)/昨收*100；振幅=(最高-最低)/昨收*100；
# 总量=当日成交量；现量=最近分笔量；内盘/外盘=主动卖/买；内外比=内盘/外盘；
# 换手=总量/流通股本；流通市值/总市值=对应股本×现价；
# 市净=现价/每股净资；市销=总市值/营业收入；市现=现价/每股现金流；
# 财报更新日期=财务数据更新日期；资产负债率=(总资产-净资产-少数股东权益)/总资产*100；
# 税后利润含少数股东损益；净利润=归母；净资产收益率=净利润/净资产*100。
STOCK_STAT_COLUMN_LABELS: Dict[str, str] = {
    "code": "代码",
    "price": "现价",
    "last_close": "昨收",
    "open": "开盘",
    "high": "最高",
    "low": "最低",
    "vol": "总量(手)",
    "cur_vol": "现量(手)",
    "amount": "成交额(万元)",
    "bid1": "买一价",
    "ask1": "卖一价",
    "bid_vol1": "买一量(手)",
    "ask_vol1": "卖一量(手)",
    "s_vol": "内盘(手)",
    "b_vol": "外盘(手)",
    "change": "涨跌额",
    "change_pct": "涨幅(%)",
    "amplitude_pct": "振幅(%)",
    "avg_price": "均价",
    "io_ratio": "内外比",
    "float_shares": "流通股本(万股)",
    "total_shares": "总股本(万股)",
    "finance_date": "财报更新日期",
    "ipo_date": "上市日期",
    "report_period": "报告期",
    "region": "地区",
    "industry": "行业",
    "total_assets": "总资产(万元)",
    "current_assets": "流动资产(万元)",
    "fixed_assets": "固定资产(万元)",
    "intangible_assets": "无形资产(万元)",
    "shareholder_count": "股东人数",
    "current_liab": "流动负债(万元)",
    "minority_equity": "少数股东权益(万元)",
    "capital_reserve": "资本公积(万元)",
    "net_assets": "净资产(万元)",
    "debt_ratio_pct": "资产负债率(%)",
    "equity_ratio_pct": "权益比(%)",
    "operating_revenue": "营业收入(万元)",
    "operating_cost": "营业成本(万元)",
    "accounts_receivable": "应收账款(万元)",
    "operating_profit": "营业利润(万元)",
    "invest_income": "投资收益(万元)",
    "operating_cashflow": "经营现金流(万元)",
    "total_cashflow": "总现金流(万元)",
    "inventory": "存货(万元)",
    "total_profit": "利润总额(万元)",
    "profit_after_tax": "税后利润(万元)",
    "net_profit": "净利润(万元)",
    "undistributed_profit": "未分配利润(万元)",
    "bvps": "每股净资(元)",
    "eps": "每股收益(元)",
    "capital_reserve_ps": "每股公积(元)",
    "undistributed_ps": "每股未分配(元)",
    "ocf_ps": "每股现金流(元)",
    "roe_pct": "净资产收益率(%)",
    "gross_margin_pct": "销售毛利率(%)",
    "op_margin_pct": "营业利润率(%)",
    "net_margin_pct": "净利润率(%)",
    "float_mkt_cap": "流通市值(万元)",
    "total_mkt_cap": "总市值(万元)",
    "turnover_pct": "换手(%)",
    "pb": "市净率",
    "ps": "市销率",
    "pcf": "市现率",
    "stat_asof": "统计基准日",  # 快照基准日；价敏列折算后仍保留
    "beta": "贝塔系数",  # 近60日相对大盘（沪→上证、深→深成指）
    "consec_up_days": "连涨天数",
    "chg_pct_prev": "统计日涨幅(%)",
    "chg_pct_3d": "3日涨幅(%)",
    "chg_pct_5d": "5日涨幅(%)",
    "chg_pct_10d": "10日涨幅(%)",
    "chg_pct_20d": "20日涨幅(%)",
    "chg_pct_30d": "30日涨幅(%)",
    "chg_pct_60d": "60日涨幅(%)",
    "chg_pct_1y": "一年涨幅(%)",
    "chg_pct_mtd": "月涨幅(%)",
    "chg_pct_ytd": "年涨幅(%)",
    "pe_ttm": "市盈率(TTM)",
    "pe_static": "市盈率(静)",
    "dividend_yield_pct": "股息率(%)",
    "float_shares_z": "自由流通股本(万股)",
    "net_profit_ex": "扣非净利润(万元)",
    "employee_count": "员工人数",
    "rd_expense": "研发费用(万元)",
    "cash_funds": "货币资金(万元)",
    "contract_liab": "合同负债(万元)",
}

if set(STOCK_STAT_COLUMN_LABELS) != set(STOCK_STAT_COLUMNS):
    raise RuntimeError("STOCK_STAT_COLUMN_LABELS 与 STOCK_STAT_COLUMNS 键不一致")

STOCK_STAT_DISPLAY_COLUMNS: List[str] = [
    STOCK_STAT_COLUMN_LABELS[k] for k in STOCK_STAT_COLUMNS
]

# 0010 财务字段（含地区码内部列，投影前丢弃）。
FINANCE_FIELD_KEYS = (
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
)

# tdxstat 假定挂在昨收：随现价/昨收因子重算的字段。
_TDXSTAT_PE_KEYS = ("pe_ttm", "pe_static")
_TDXSTAT_YIELD_KEYS = ("dividend_yield_pct",)
_TDXSTAT_CHG_PCT_KEYS = (
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


def _empty_stock_stat_df() -> pd.DataFrame:
    """
    输入：无。
    输出：仅含中文表头列的空表。
    用途：无行情行时的固定列契约。
    边界：0 行。
    """
    return pd.DataFrame(columns=STOCK_STAT_DISPLAY_COLUMNS)


def _map_to_frame(
    by_code: Mapping[str, Mapping[str, Any]],
    fields: Sequence[str],
    *,
    code_name: str = "code",
) -> pd.DataFrame:
    """
    输入：code→字段字典、需要保留的字段名。
    输出：含 `code` 列的 DataFrame；无记录时为空表（仅 code 列）。
    用途：财务/tdxstat 字典转表以便 merge。
    边界：缺字段填 NA；不拷贝未列出的键。
    """
    if not by_code:
        return pd.DataFrame(columns=[code_name, *fields])
    codes = [str(c) for c in by_code.keys()]
    cols: Dict[str, List[Any]] = {code_name: codes}
    for key in fields:
        cols[key] = [by_code[c].get(key) for c in by_code.keys()]
    return pd.DataFrame(cols)


def _today_stat_asof() -> str:
    """
    输入：无。
    输出：当日自然日 `YYYYMMDD`。
    用途：判断 tdxstat 行是否已是当日快照。
    边界：按本机日历日，不区分交易日。
    """
    return datetime.now().strftime("%Y%m%d")


def _valuation_price(price: pd.Series, last_close: pd.Series) -> pd.Series:
    """
    输入：现价列、昨收列。
    输出：估值用价（现价>0 用现价，否则昨收>0 用昨收，否则 NA）。
    用途：市值/市净/市销/市现与 `_quote_last_price` 同语义。
    边界：非数值视为 NA；双非正为 NA。
    """
    px = pd.to_numeric(price, errors="coerce")
    last = pd.to_numeric(last_close, errors="coerce")
    chosen = px.where(px > 0, last)
    return chosen.where(chosen > 0)


def _price_to_last_factor(price: pd.Series, last_close: pd.Series) -> pd.Series:
    """
    输入：现价列、昨收列。
    输出：估值价/昨收；无法计算时为 NA。
    用途：把昨收时点的 tdxstat 比率折到当前价。
    边界：估值价回退昨收时因子为 1；昨收非正则 NA。
    """
    px = _valuation_price(price, last_close)
    last = pd.to_numeric(last_close, errors="coerce")
    return (px / last).where((px > 0) & (last > 0))


def _derive_quote_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    输入：已合并财务列的宽表（协议原单位）。
    输出：就地写入市值/换手/pb/ps/pcf 后的同一 DataFrame。
    用途：向量化衍生；现价为 0 时回退昨收。
    边界：缺股本或分母非正则对应列为 NA；市值暂为「元」，折万放在返回前最后一步。
    """
    out = df
    px = _valuation_price(out.get("price"), out.get("last_close"))
    vol = pd.to_numeric(out.get("vol"), errors="coerce")
    float_shares = pd.to_numeric(out.get("float_shares"), errors="coerce")
    total_shares = pd.to_numeric(out.get("total_shares"), errors="coerce")
    bvps = pd.to_numeric(out.get("bvps"), errors="coerce")
    ocf_ps = pd.to_numeric(out.get("ocf_ps"), errors="coerce")
    revenue_qian = pd.to_numeric(out.get("operating_revenue"), errors="coerce")

    # 元/股 × 万股 × 10000 = 元；中间不取整，避免折万前丢精度。
    float_mkt_yuan = (px * float_shares * 10000.0).where((px > 0) & (float_shares > 0))
    total_mkt_yuan = (px * total_shares * 10000.0).where((px > 0) & (total_shares > 0))
    out["float_mkt_cap"] = float_mkt_yuan
    out["total_mkt_cap"] = total_mkt_yuan
    out["turnover_pct"] = (vol / float_shares).where(float_shares > 0).round(4)

    out["pb"] = (px / bvps).where((px > 0) & bvps.notna() & (bvps != 0)).round(2)
    revenue_yuan = revenue_qian * 1000.0
    out["ps"] = (
        (total_mkt_yuan / revenue_yuan)
        .where(total_mkt_yuan.notna() & (revenue_yuan > 0))
        .round(2)
    )
    out["pcf"] = (px / ocf_ps).where((px > 0) & ocf_ps.notna() & (ocf_ps != 0)).round(2)
    return out


def _rebase_tdxstat_to_live_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    输入：已合并 tdxstat 列的宽表。
    输出：按需折算 PE、股息率、n 日涨幅后的同一 DataFrame。
    用途：`stat_asof` 不是当日时，假定快照挂在昨收并折到现价；已是当日则原样送出。
    边界：
    1. 仅 `stat_asof != 今日` 的行参与折算；等于今日或无法比价时保持文件原值。
    2. PE_live = PE_asof × (估值价/昨收)。
    3. 股息率_live = 股息率_asof × (昨收/估值价)。
    4. 涨幅_live% = ((1 + 涨幅_asof/100) × 因子 − 1) × 100。
    5. `stat_asof`/`beta` 等非价敏字段不改。
    """
    out = df
    if "stat_asof" not in out.columns:
        return out
    today = _today_stat_asof()
    asof = out["stat_asof"].fillna("").astype(str).str.strip()
    need_rebase = asof != today
    factor = _price_to_last_factor(out.get("price"), out.get("last_close"))
    factor = factor.where(need_rebase)
    for key in _TDXSTAT_PE_KEYS:
        if key not in out.columns:
            continue
        base = pd.to_numeric(out[key], errors="coerce")
        adjusted = (base * factor).round(4)
        out[key] = base.where(factor.isna() | base.isna(), adjusted)
    for key in _TDXSTAT_YIELD_KEYS:
        if key not in out.columns:
            continue
        base = pd.to_numeric(out[key], errors="coerce")
        adjusted = (base / factor).round(4)
        out[key] = base.where(factor.isna() | base.isna(), adjusted)
    for key in _TDXSTAT_CHG_PCT_KEYS:
        if key not in out.columns:
            continue
        base = pd.to_numeric(out[key], errors="coerce")
        adjusted = ((1.0 + base / 100.0) * factor - 1.0) * 100.0
        out[key] = base.where(factor.isna() | base.isna(), adjusted.round(4))
    return out


def _scale_amount_columns_to_wan(df: pd.DataFrame) -> pd.DataFrame:
    """
    输入：协议原单位聚合宽表（市值/成交额为元，财务金额为千元）。
    输出：金额列折为万元后的表。
    用途：对外统一万元；仅在返回前调用一次。
    边界：不改价格/比率/股本(万股)/成交量(手)/tdxstat 已是万元的字段。
    """
    out = df
    for key in _YUAN_TO_WAN_KEYS:
        if key in out.columns:
            out[key] = (pd.to_numeric(out[key], errors="coerce") / 10000.0).round(4)
    for key in _QIAN_TO_WAN_KEYS:
        if key in out.columns:
            out[key] = (pd.to_numeric(out[key], errors="coerce") / 10.0).round(4)
    return out


def build_stock_stat_df(
    quote_rows: Sequence[Mapping[str, Any]],
    finance_by_code: Mapping[str, Mapping[str, Any]],
    tdxstat_by_code: Mapping[str, Mapping[str, Any]],
    region_by_code: Mapping[int, str],
    industry_name_by_hy: Mapping[str, str],
    hy_by_code: Mapping[str, str],
) -> pd.DataFrame:
    """
    输入：行情行列表、财务/tdxstat/地区/行业映射。
    输出：已投影、单位折算、中文表头后的宽表 DataFrame。
    用途：离线/在线共用的聚合核心（向量化，不逐票 Python 循环算估值）。
    边界：财务或统计缺失时对应列为 NA/None；空行情返回空表（含全部列）；
         仅统计基准日非当日的行才按现价/昨收折算价敏字段；
         表头为 `STOCK_STAT_COLUMN_LABELS` 中文（含单位）。
    """
    if not quote_rows:
        return _empty_stock_stat_df()

    quote = pd.DataFrame(list(quote_rows))
    if "code" not in quote.columns:
        return _empty_stock_stat_df()
    quote["code"] = quote["code"].astype(str)

    fin = _map_to_frame(finance_by_code, FINANCE_FIELD_KEYS)
    if not fin.empty:
        fin["code"] = fin["code"].astype(str)
        merged = quote.merge(fin, on="code", how="left", suffixes=("", "_drop"))
        drop_cols = [c for c in merged.columns if c.endswith("_drop")]
        if drop_cols:
            merged = merged.drop(columns=drop_cols)
    else:
        merged = quote.copy()
        for key in FINANCE_FIELD_KEYS:
            if key not in merged.columns:
                merged[key] = np.nan

    region_codes = pd.to_numeric(merged.get("_region_code"), errors="coerce")
    region_series = pd.Series(
        {int(k): v for k, v in region_by_code.items()},
        dtype=object,
    )
    merged["region"] = region_codes.map(region_series)

    hy_series = pd.Series(dict(hy_by_code), dtype=object)
    industry_series = pd.Series(dict(industry_name_by_hy), dtype=object)
    merged["industry"] = merged["code"].map(hy_series).map(industry_series)

    if "report_period" in merged.columns:
        rp = merged["report_period"]
        merged["report_period"] = rp.where(rp.notna() & (rp.astype(str).str.len() > 0))

    merged = _derive_quote_metrics(merged)

    stat = _map_to_frame(tdxstat_by_code, TDXSTAT_FIELD_KEYS)
    if not stat.empty:
        stat["code"] = stat["code"].astype(str)
        merged = merged.merge(stat, on="code", how="left", suffixes=("", "_stat"))
        drop_cols = [c for c in merged.columns if c.endswith("_stat")]
        if drop_cols:
            merged = merged.drop(columns=drop_cols)
    else:
        for key in TDXSTAT_FIELD_KEYS:
            if key not in merged.columns:
                merged[key] = np.nan

    merged = _rebase_tdxstat_to_live_price(merged)
    merged = _scale_amount_columns_to_wan(merged)

    for key in STOCK_STAT_COLUMNS:
        if key not in merged.columns:
            merged[key] = np.nan
    out = merged.loc[:, list(STOCK_STAT_COLUMNS)].copy()
    out = out.rename(columns=STOCK_STAT_COLUMN_LABELS)
    return out.replace({np.nan: None})


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
