"""离线验收 get_stock_stat 相关解析与宽表聚合（不联网）。"""

from __future__ import annotations

import struct
from collections import OrderedDict

from zsdtdx.biz.stock_stat import (
    STOCK_STAT_COLUMNS,
    assemble_stock_stat_rows,
    is_stock_stat_row,
    project_stock_stat_row,
    rows_to_stock_stat_df,
    scale_row_units_to_wan,
)
from zsdtdx.parser.get_board_quote_page import (
    GetBoardQuotePageCmd,
    enrich_board_quote_row,
    parse_board_quote_body,
)
from zsdtdx.parser.get_finance_info_batch import (
    GetFinanceInfoBatchCmd,
    parse_finance_info_batch_body,
)
from zsdtdx.parser.get_security_quotes import GetSecurityQuotesCmd
from zsdtdx.parser.tdxstat import (
    merge_tdxstat_maps,
    parse_industry_name_map_from_incon,
    parse_region_map_from_tdxzs,
    parse_tdxhy_code_map,
    parse_tdxstat2_cfg,
    parse_tdxstat_cfg,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient


def test_board_quote_page_pack_clamps_to_80():
    """
    输入：count>80。
    输出：请求包内条数字段为 80。
    用途：054B 硬顶。
    边界：组包不发网络。
    """
    cmd = GetBoardQuotePageCmd(None)
    cmd.setParams(0, 200)
    assert cmd.send_pkg is not None
    # hdr 12 字节 + body 前 3 个 H 后为 count
    count = struct.unpack_from("<H", cmd.send_pkg, 18)[0]
    assert count == 80


def test_board_quote_enrich_and_empty_body():
    """
    输入：空正文与手工价量行。
    输出：空列表；衍生涨跌幅算对。
    用途：B 类衍生与空页。
    边界：不依赖网络。
    """
    assert parse_board_quote_body(b"") == []
    row = enrich_board_quote_row(
        {
            "price": 11.0,
            "last_close": 10.0,
            "high": 12.0,
            "low": 9.0,
            "vol": 100.0,
            "amount": 110000.0,
            "s_vol": 40.0,
            "b_vol": 60.0,
        }
    )
    assert row["change"] == 1.0
    assert row["change_pct"] == 10.0
    assert row["amplitude_pct"] == 30.0
    assert row["avg_price"] == 11.0
    assert row["io_ratio"] == round(40.0 / 60.0, 4)


def test_finance_batch_pack_and_parse_one_record():
    """
    输入：单票组包 + 手工 143 字节财务记录。
    输出：组包含 0x0010；解析出 code/股本。
    用途：批量 0010 离线契约。
    边界：条数硬顶由 setParams 校验。
    """
    cmd = GetFinanceInfoBatchCmd(None)
    cmd.setParams([(1, "600000")])
    assert cmd.send_pkg[-8:-2]  # 非空
    assert struct.unpack_from("<H", cmd.send_pkg, 10)[0] == 0x0010

    rec = bytearray(143)
    rec[0] = 1
    rec[1:7] = b"600000"
    struct.pack_into("<f", rec, 7, 100.0)  # float_shares 万股
    struct.pack_into("<HH", rec, 11, 18, 1)
    struct.pack_into("<II", rec, 15, 20260331, 19991110)
    floats = [0.0] * 30
    floats[0] = 200.0  # total_shares
    floats[6] = 1.5  # eps
    floats[7] = 1000.0  # total_assets 千元
    floats[14] = 500.0  # net_assets
    floats[15] = 800.0  # operating_revenue
    floats[16] = 300.0  # operating_cost
    floats[25] = 50.0  # net_profit
    floats[27] = 2.5  # bvps
    floats[29] = 6.0  # 中报
    struct.pack_into("<" + "f" * 30, rec, 23, *floats)
    body = struct.pack("<H", 1) + bytes(rec)
    rows = parse_finance_info_batch_body(body)
    assert len(rows) == 1
    assert rows[0]["code"] == "600000"
    assert rows[0]["float_shares"] == 100.0
    assert rows[0]["report_period"] == "中报"
    assert rows[0]["_region_code"] == 18


def test_tdxstat_merge_and_maps():
    """
    输入：最小 tdxstat/tdxstat2/tdxzs/incon/tdxhy 文本。
    输出：合并涨幅、地区/行业映射正确。
    用途：D 类解析离线契约。
    边界：空单元格为 None。
    """
    # 至少 31 列以满足 [30]=10日
    parts = [""] * 31
    parts[0] = "1"
    parts[1] = "600000"
    parts[2] = "1.2"
    parts[3] = "8.5"
    parts[4] = "20260929"
    parts[5] = "3"
    parts[6] = "1.1"
    parts[28] = "2.2"
    parts[30] = "3.3"
    st1 = parse_tdxstat_cfg(("|".join(parts) + "\n").encode("gbk"))
    assert st1["600000"]["chg_pct_5d"] == 2.2
    assert st1["600000"]["stat_asof"] == "20260929"

    parts2 = [""] * 21
    parts2[1] = "600000"
    parts2[12] = "15.0"
    parts2[20] = "4.0"
    st2 = parse_tdxstat2_cfg(("|".join(parts2) + "\n").encode("gbk"))
    merged = merge_tdxstat_maps(st1, st2)
    assert merged["600000"]["chg_pct_1y"] == 15.0
    assert merged["600000"]["chg_pct_30d"] == 4.0

    zs = "深圳板块|880001|3|1|0|18\n钢铁|880002|2|1|0|T01\n".encode("gbk")
    assert parse_region_map_from_tdxzs(zs)[18] == "深圳板块"

    incon = "#TDXNHY\nT020603|银行\n#OTHER\nx|y\n".encode("gbk")
    assert parse_industry_name_map_from_incon(incon)["T020603"] == "银行"

    hy = "1|600000|T020603|||\n".encode("gbk")
    assert parse_tdxhy_code_map(hy)["600000"] == "T020603"


def test_is_stock_stat_row_filters_index():
    """
    输入：指数与股票代码。
    输出：默认过滤指数；include_indices 时保留。
    用途：全市场范围过滤。
    边界：不读码表。
    """
    assert is_stock_stat_row(1, "600000", include_indices=False)
    assert not is_stock_stat_row(0, "399001", include_indices=False)
    assert is_stock_stat_row(1, "880001", include_indices=True)
    assert not is_stock_stat_row(0, "399001", include_indices=True)


def test_wan_scale_and_assemble_dataframe():
    """
    输入：协议原单位聚合行。
    输出：万元折算；宽表列齐全。
    用途：对外量纲与列契约。
    边界：缺财务仍保留列。
    """
    ab = OrderedDict(
        [
            ("market", 1),
            ("code", "600000"),
            ("price", 10.0),
            ("last_close", 9.0),
            ("open", 9.5),
            ("high", 10.5),
            ("low", 9.0),
            ("vol", 100.0),
            ("cur_vol", 1.0),
            ("amount", 100000.0),
            ("bid1", 9.99),
            ("ask1", 10.01),
            ("bid_vol1", 1),
            ("ask_vol1", 1),
            ("s_vol", 40.0),
            ("b_vol", 60.0),
            ("change", 1.0),
            ("change_pct", 11.11),
            ("amplitude_pct", 16.67),
            ("avg_price", 10.0),
            ("io_ratio", 0.6667),
        ]
    )
    fin = {
        "float_shares": 100.0,
        "total_shares": 200.0,
        "finance_date": 20260331,
        "ipo_date": 19991110,
        "report_period": "中报",
        "total_assets": 1000.0,  # 千元
        "current_assets": None,
        "fixed_assets": None,
        "intangible_assets": None,
        "shareholder_count": 10,
        "current_liab": None,
        "minority_equity": 0.0,
        "capital_reserve": None,
        "net_assets": 500.0,
        "debt_ratio_pct": 50.0,
        "equity_ratio_pct": 50.0,
        "operating_revenue": 800.0,
        "operating_cost": 300.0,
        "accounts_receivable": None,
        "operating_profit": None,
        "invest_income": None,
        "operating_cashflow": None,
        "total_cashflow": None,
        "inventory": None,
        "total_profit": None,
        "profit_after_tax": None,
        "net_profit": 50.0,
        "undistributed_profit": None,
        "bvps": 2.5,
        "eps": 0.1,
        "capital_reserve_ps": None,
        "undistributed_ps": None,
        "ocf_ps": None,
        "roe_pct": 10.0,
        "gross_margin_pct": None,
        "op_margin_pct": None,
        "net_margin_pct": None,
        "_region_code": 18,
        "_industry_code": 1,
    }
    rows = assemble_stock_stat_rows(
        [ab],
        {"600000": fin},
        {"600000": {"chg_pct_5d": 2.2, "stat_asof": "20260929"}},
        {18: "深圳板块"},
        {"T020603": "银行"},
        {"600000": "T020603"},
    )
    assert len(rows) == 1
    assert rows[0]["amount"] == 10.0  # 元→万元
    assert rows[0]["total_assets"] == 100.0  # 千元→万元
    assert rows[0]["region"] == "深圳板块"
    assert rows[0]["industry"] == "银行"
    assert rows[0]["chg_pct_5d"] == 2.2
    df = rows_to_stock_stat_df(rows)
    assert list(df.columns) == STOCK_STAT_COLUMNS
    assert len(df) == 1

    scaled = scale_row_units_to_wan({"amount": 20000.0, "price": 1.0})
    assert scaled["amount"] == 2.0
    assert scaled["price"] == 1.0

    empty = rows_to_stock_stat_df([])
    assert list(empty.columns) == STOCK_STAT_COLUMNS
    assert len(empty) == 0


def test_quote_last_price_fallback_still_works():
    """
    输入：报价行。
    输出：现价优先、昨收回退。
    用途：期货最新价等仍用该辅助函数。
    边界：双非正为 None。
    """
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    assert client._quote_last_price({"price": 9.08, "last_close": 9.06}) == 9.08
    assert client._quote_last_price({"price": 0, "last_close": 9.06}) == 9.06
    assert client._quote_last_price({"price": 0.0, "last_close": 0.0}) is None


def test_project_keeps_column_order():
    """
    输入：多余字段行。
    输出：仅 STOCK_STAT_COLUMNS 且顺序固定。
    用途：投影契约。
    边界：缺键填 None。
    """
    out = project_stock_stat_row({"code": "600000", "noise": 1, "amount": 10000.0})
    assert list(out.keys()) == STOCK_STAT_COLUMNS
    assert out["code"] == "600000"
    assert out["amount"] == 1.0
    assert out["price"] is None


# 保留原五档解析用例所需最小导入（同文件历史 fixture 已迁走时仍可独立跑）
def test_security_quotes_unlisted_still_isolated():
    """
    输入：未上市占位回包（历史 fixture）。
    输出：单条解析不抛错。
    用途：054B 复用 GetSecurityQuotesCmd 的前置契约。
    边界：不测已删除的 get_stock_latest_price。
    """
    body = bytes.fromhex(
        "016001000033303135363900000000000000a401000000000000000000000000"
        "0000000000000000000000000000000000000000800000000000000000"
    )
    rows = GetSecurityQuotesCmd(None).parseResponse(body)
    assert len(rows) == 1
    assert rows[0]["code"] == "301569"
