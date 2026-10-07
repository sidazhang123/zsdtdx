"""离线验收 get_stock_stat 相关解析与宽表聚合（不联网）。"""

from __future__ import annotations

import struct
from collections import OrderedDict

import pytest

import zsdtdx.biz.stock_stat as stock_stat_module
from zsdtdx.biz.stock_stat import (
    STOCK_STAT_COLUMN_LABELS,
    STOCK_STAT_DISPLAY_COLUMNS,
    _fetch_stock_stat_with_client,
    build_stock_stat_df,
    is_stock_stat_row,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient
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


def test_stock_stat_short_board_page_is_terminal():
    """输入 054B 短页。输出不再请求后继空页，保留已拉取股票。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    board_starts: list[int] = []

    def pool_call(_pool, method, *args):
        if method == "get_board_quote_page":
            start = int(args[0])
            board_starts.append(start)
            if start != 0:
                raise AssertionError("054B 短页后不应再请求")
            return [{"market": 1, "code": "600000", "price": 10.0}]
        raise AssertionError(f"未预期的方法: {method}")

    client.std_pool = object()
    client._pool_call_allow_none = pool_call
    client._record_failure = lambda *args: None
    result = client.get_board_quote_rows(80)
    assert board_starts == [0]
    assert len(result) == 1


def test_stock_stat_board_none_is_failure_but_empty_page_is_normal():
    """输入 None 与空页。输出前者报错、后者正常结束，避免静默残缺。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.std_pool = object()
    client._record_failure = lambda *args: None
    client._pool_call_allow_none = lambda *_args: None
    with pytest.raises(RuntimeError, match="None"):
        client.get_board_quote_rows(80)

    client._pool_call_allow_none = lambda *_args: []
    assert client.get_board_quote_rows(80) == []


def test_stock_stat_suspended_quote_is_normal_row():
    """输入现价与成交量为 0 的停牌形态。输出仍纳入股票集。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.std_pool = object()
    client._record_failure = lambda *args: None
    client._pool_call_allow_none = lambda *_args: [
        {
            "market": 0,
            "code": "002667",
            "price": 0.0,
            "last_close": 22.19,
            "vol": 0,
        }
    ]
    rows = client.get_board_quote_rows(80)
    assert [row["code"] for row in rows] == ["002667"]


def test_stock_stat_finance_map_requires_complete_code_set():
    """输入完整与缺行的 0010 批次。输出完整映射或显式报错。"""

    class DummyPool:
        def __init__(self):
            self.cleaned = 0

        def cleanup_dead_thread_connections(self):
            self.cleaned += 1

    codes = [(1, f"60{i:04d}") for i in range(201)]
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.std_pool = DummyPool()
    client._record_failure = lambda *args: None

    def complete_call(_pool, method, chunk):
        assert method == "get_finance_info_batch"
        return [{"code": code} for _, code in chunk]

    client._pool_call_allow_none = complete_call
    result = client.get_finance_info_batch_map(codes, 100, 4)
    assert set(result) == {code for _, code in codes}
    assert client.std_pool.cleaned == 1

    client._pool_call_allow_none = lambda _pool, _method, chunk: [
        {"code": code} for _, code in chunk[:-1]
    ]
    with pytest.raises(RuntimeError, match="不完整"):
        client.get_finance_info_batch_map(codes[:2], 100, 1)


def test_stock_stat_business_orchestration_uses_generic_client_capabilities(
    monkeypatch,
):
    """
    输入同时含股票与指数的版面数据。
    输出业务层只向财务能力传入股票，并完成六源参数聚合。
    用途：固化 simple_api→biz→engine 的目标分层。
    边界：不发网络请求，不解析真实静态文件。
    """

    class FakeClient:
        config = {
            "stock_stat": {
                "page_size": 80,
                "finance_batch_size": 100,
                "finance_workers": 4,
            }
        }

        def __init__(self):
            self.finance_args = None

        def get_board_quote_rows(self, page_size, row_filter=None):
            assert page_size == 80
            rows = [
                {"market": 1, "code": "600000", "price": 10.0},
                {"market": 0, "code": "399001", "price": 20.0},
            ]
            return [row for row in rows if row_filter is None or row_filter(row)]

        def get_finance_info_batch_map(self, codes, batch_size, workers):
            self.finance_args = (codes, batch_size, workers)
            return {"600000": {"code": "600000"}}

    client = FakeClient()
    static_maps = (
        {"600000": {"stat_asof": "20260930"}},
        {18: "深圳板块"},
        {"T020603": "银行"},
        {"600000": "T020603"},
    )
    captured = {}
    monkeypatch.setattr(
        stock_stat_module,
        "_load_stock_stat_static_maps",
        lambda _client: static_maps,
    )

    def fake_build(quote_rows, finance_map, stat_map, region_map, industry_map, hy_map):
        captured["args"] = (
            quote_rows,
            finance_map,
            stat_map,
            region_map,
            industry_map,
            hy_map,
        )
        return "target-result"

    monkeypatch.setattr(stock_stat_module, "build_stock_stat_df", fake_build)
    result = _fetch_stock_stat_with_client(client)
    assert result == "target-result"
    assert client.finance_args == ([(1, "600000")], 100, 4)
    assert [row["code"] for row in captured["args"][0]] == ["600000"]
    assert captured["args"][1:] == ({"600000": {"code": "600000"}}, *static_maps)


def test_board_quote_enrich_and_empty_body():
    """
    输入：空正文与手工价量行。
    输出：空列表；衍生涨跌幅算对。
    用途：行情衍生字段。
    边界：不依赖网络。
    """
    with pytest.raises(ValueError, match="正文过短"):
        parse_board_quote_body(b"")
    assert parse_board_quote_body(struct.pack("<HH", 0, 0)) == []
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


def test_board_quote_parser_rejects_declared_count_mismatch():
    """输入声明 1 条但无记录的 054B 正文。输出结构错误而非空页。"""
    with pytest.raises(ValueError, match="记录数不匹配"):
        parse_board_quote_body(struct.pack("<HH", 0, 1))


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
    用途：统计快照解析离线契约。
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
    输出：始终过滤指数，仅保留股票代码前缀。
    用途：全市场范围过滤。
    边界：不读码表。
    """
    assert is_stock_stat_row(1, "600000")
    assert is_stock_stat_row(0, "300001")
    assert is_stock_stat_row(2, "920001")
    assert not is_stock_stat_row(0, "399001")
    assert not is_stock_stat_row(1, "880001")
    assert not is_stock_stat_row(1, "000001")


def test_build_stock_stat_df_units_and_columns():
    """
    输入：协议原单位行情 + 财务 + tdxstat。
    输出：万元折算；宽表列齐全；地区行业挂上。
    用途：向量化聚合契约。
    边界：空行情返回空表（含全部列）。
    """
    quote = OrderedDict(
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
        "ocf_ps": 1.0,
        "roe_pct": 10.0,
        "gross_margin_pct": None,
        "op_margin_pct": None,
        "net_margin_pct": None,
        "_region_code": 18,
        "_industry_code": 1,
    }
    df = build_stock_stat_df(
        [quote],
        {"600000": fin},
        {
            "600000": {
                "chg_pct_5d": 2.2,
                "pe_ttm": 9.0,
                "pe_static": 8.1,
                "dividend_yield_pct": 4.5,
                "stat_asof": "20260929",
            }
        },
        {18: "深圳板块"},
        {"T020603": "银行"},
        {"600000": "T020603"},
    )
    assert list(df.columns) == STOCK_STAT_DISPLAY_COLUMNS
    assert len(df) == 1
    row = df.iloc[0]
    L = STOCK_STAT_COLUMN_LABELS
    assert row[L["amount"]] == 10.0  # 元→万元
    assert row[L["total_assets"]] == 100.0  # 千元→万元
    assert row[L["region"]] == "深圳板块"
    assert row[L["industry"]] == "银行"
    # zhb/tdxstat 字段不推断交易日，不与实时涨幅聚合。
    assert row[L["stat_asof"]] == "20260929"
    assert row[L["chg_pct_5d"]] == 2.2
    assert row[L["pe_ttm"]] == 9.0
    assert row[L["pe_static"]] == 8.1
    assert row[L["dividend_yield_pct"]] == 4.5
    # 现价 10、流通股本 100 万股：市值=1000 万元；pb=10/2.5=4
    assert row[L["float_mkt_cap"]] == 1000.0
    assert row[L["total_mkt_cap"]] == 2000.0
    assert row[L["pb"]] == 4.0
    assert row[L["pcf"]] == 10.0

    empty = build_stock_stat_df([], {}, {}, {}, {}, {})
    assert list(empty.columns) == STOCK_STAT_DISPLAY_COLUMNS
    assert len(empty) == 0


def test_build_stock_stat_df_price_zero_falls_back_to_last_close():
    """
    输入：现价为 0、昨收有值的停牌形态行。
    输出：市值/pb/ps/pcf 均按昨收回退计算。
    用途：与 `_quote_last_price` 同语义。
    边界：现价与昨收双非正则估值为空。
    """
    quote = {
        "market": 0,
        "code": "002667",
        "price": 0.0,
        "last_close": 22.19,
        "vol": 0.0,
        "amount": 0.0,
    }
    fin = {
        "float_shares": 100.0,
        "total_shares": 200.0,
        "bvps": 2.219,
        "operating_revenue": 1000.0,  # 千元 → 元分母=1_000_000
        "ocf_ps": 2.219,
        "_region_code": 18,
    }
    df = build_stock_stat_df(
        [quote],
        {"002667": fin},
        {
            "002667": {
                "chg_pct_5d": 1.7,
                "pe_ttm": -13.29,
                "dividend_yield_pct": 0.54,
            }
        },
        {18: "深圳板块"},
        {},
        {},
    )
    row = df.iloc[0]
    L = STOCK_STAT_COLUMN_LABELS
    assert row[L["float_mkt_cap"]] == round(22.19 * 100.0, 4)
    assert row[L["total_mkt_cap"]] == round(22.19 * 200.0, 4)
    assert row[L["pb"]] == 10.0
    assert row[L["pcf"]] == 10.0
    # 市销用元口径：总市值元 / 营收元
    assert row[L["ps"]] == round((22.19 * 200 * 10000) / (1000.0 * 1000.0), 2)
    # tdxstat 字段始终保持文件原值，与估值价回退无关。
    assert row[L["chg_pct_5d"]] == 1.7
    assert row[L["pe_ttm"]] == -13.29
    assert row[L["dividend_yield_pct"]] == 0.54

    both_zero = build_stock_stat_df(
        [{"market": 0, "code": "301569", "price": 0.0, "last_close": 0.0, "vol": 0}],
        {
            "301569": {
                "float_shares": 10.0,
                "total_shares": 10.0,
                "bvps": 1.0,
                "ocf_ps": 1.0,
                "operating_revenue": 100.0,
            }
        },
        {},
        {},
        {},
        {},
    ).iloc[0]
    assert both_zero[L["float_mkt_cap"]] is None
    assert both_zero[L["total_mkt_cap"]] is None
    assert both_zero[L["pb"]] is None
    assert both_zero[L["ps"]] is None
    assert both_zero[L["pcf"]] is None


def test_build_stock_stat_df_keeps_tdxstat_raw_values_for_old_asof():
    """
    输入：`stat_asof` 明显早于行情自然日、现价≠昨收。
    输出：tdxstat 字段仍保持文件原值，不推断交易日或折算。
    用途：固化 zhb 原值返回契约。
    边界：市值等仍按现价计算。
    """
    stat_asof = "20000104"
    quote = {
        "market": 1,
        "code": "600000",
        "price": 10.0,
        "last_close": 9.0,
        "vol": 100.0,
        "amount": 100000.0,
    }
    fin = {
        "float_shares": 100.0,
        "total_shares": 200.0,
        "bvps": 2.5,
        "ocf_ps": 1.0,
        "operating_revenue": 800.0,
    }
    df = build_stock_stat_df(
        [quote],
        {"600000": fin},
        {
            "600000": {
                "stat_asof": stat_asof,
                "chg_pct_5d": 2.2,
                "pe_ttm": 9.0,
                "pe_static": 8.1,
                "dividend_yield_pct": 4.5,
            }
        },
        {},
        {},
        {},
    )
    row = df.iloc[0]
    L = STOCK_STAT_COLUMN_LABELS
    assert row[L["stat_asof"]] == stat_asof
    assert row[L["chg_pct_5d"]] == 2.2
    assert row[L["pe_ttm"]] == 9.0
    assert row[L["pe_static"]] == 8.1
    assert row[L["dividend_yield_pct"]] == 4.5
    assert row[L["float_mkt_cap"]] == 1000.0


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
