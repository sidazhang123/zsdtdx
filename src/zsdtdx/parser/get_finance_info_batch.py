"""
模块：`parser/get_finance_info_batch.py`。

职责：
1. 封装标准行情批量 0x0010 财务/股本组包与解包。
2. 字段名与 base.dbf / 银河看板标定一致（协议原单位：金额千元、股本万股）。
3. 计算资产负债率、ROE、毛利率等 C 类衍生比率。

边界：
1. 服务端硬顶 100 条/批。
2. GJG/FQRFRG/FRG/BG/HG 五槽为股改前名词，解析时跳过不导出。
3. 不在此做「千元→万元」折算；对外量纲由业务层统一处理。
"""

from __future__ import annotations

import struct
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Sequence, Tuple

from zsdtdx.parser.base import BaseParser

MAX_FINANCE_INFO_BATCH = 100

# 报告期码 → 界面文案（base.dbf ZBNB）
REPORT_PERIOD_MAP = {
    3: "一季报",
    6: "中报",
    9: "三季报",
    12: "年报",
}


def parse_finance_info_batch_body(body: bytes) -> List[Dict[str, Any]]:
    """
    输入：批量 0x0010 解压正文。
    输出：C 类字段列表（含内部 `_region_code` / `_industry_code`）。
    用途：解析财务批回包；供命令类与离线单测共用。
    边界：固定 143 字节/条 = market+code(7)+f+HH+II+30×f；
         金额千元、股本万股；部分 pytdx 旧名与看板不符，按 dbf+截图正名。
    """
    if not body or len(body) < 2:
        return []
    count = struct.unpack_from("<H", body, 0)[0]
    if count <= 0:
        return []
    avail = len(body) - 2
    rec_n = avail // count if count else 0
    if rec_n < 40:
        rec_n = 143
    rows: List[Dict[str, Any]] = []
    for i in range(count):
        off = 2 + i * rec_n
        rec = body[off : off + rec_n]
        if len(rec) < 143:
            break
        if rec[0] in (0, 1, 2) and rec[1:7].isdigit():
            code = rec[1:7].decode("ascii")
        elif rec[0:6].isdigit():
            code = rec[0:6].decode("ascii")
        else:
            continue
        try:
            # 布局对齐 base.dbf：LTAG/DY/HY/GXRQ/SSDATE/ZGB…/ZBNB
            float_shares = struct.unpack_from("<f", rec, 7)[0]  # LTAG
            region_code, industry_code = struct.unpack_from("<HH", rec, 11)
            finance_date, ipo_date = struct.unpack_from("<II", rec, 15)
            (
                total_shares,  # ZGB
                # 以下 5 槽对齐 base.dbf 的 GJG/FQRFRG/FRG/BG/HG：
                # 股改前名词（国家股/发起人法人股/法人股/B股/H股），语义已无现用意义，跳过不导出。
                _skip_gjg,
                _skip_fqrfrg,
                _skip_frg,
                _skip_bg,
                _skip_hg,
                eps,  # ZGG（旧名职工股，实为每股收益）
                total_assets,  # ZZC
                current_assets,  # LDZC
                fixed_assets,  # GDZC
                intangible_assets,  # WXZC
                shareholder_count,  # 截图「股东人数」；非长期投资
                current_liab,  # LDFZ
                minority_equity,  # CQFZ 槽；看板「少数股权」
                capital_reserve,  # ZBGJJ
                net_assets,  # JZC
                operating_revenue,  # ZYSY
                operating_cost,  # 截图「营业成本」；非主营利润
                accounts_receivable,  # QTLY 槽≈应收账款
                operating_profit,  # YYLY
                invest_income,  # TZSY
                operating_cashflow,  # BTSY 槽≈经营现金流
                total_cashflow,  # YYWSZ
                inventory,  # SNSYTZ 槽≈存货
                total_profit,  # LYZE
                profit_after_tax,  # SHLY
                net_profit,  # JLY
                undistributed_profit,  # WFPLY
                bvps,  # TZMGJZ
                report_period_code,  # ZBNB
            ) = struct.unpack_from("<" + "f" * 30, rec, 23)
            del _skip_gjg, _skip_fqrfrg, _skip_frg, _skip_bg, _skip_hg
        except Exception as exc:
            rows.append(OrderedDict([("code", code), ("parse_error", str(exc))]))
            continue

        rp_code = (
            int(round(float(report_period_code)))
            if report_period_code == report_period_code
            else None
        )

        # 每股*：金额千元 / (股本万股×10) = 元/股
        def _ps(num: float, zgb: float) -> Optional[float]:
            try:
                if zgb and zgb > 0 and num == num:
                    return round(float(num) / (float(zgb) * 10.0), 4)
            except (TypeError, ValueError, ZeroDivisionError):
                pass
            return None

        debt_ratio_pct = None
        equity_ratio_pct = None
        try:
            ta = float(total_assets)
            if ta > 0:
                debt_ratio_pct = round(
                    (ta - float(net_assets) - float(minority_equity)) / ta * 100.0, 2
                )
                equity_ratio_pct = round(float(net_assets) / ta * 100.0, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            pass

        # 净益率=净利润/净资；毛利率=(营收-成本)/营收；营业利润率=营业利润/营收；
        # 净利润率=税后利润/营收
        roe_pct = gross_margin_pct = op_margin_pct = net_margin_pct = None
        try:
            na = float(net_assets)
            rev = float(operating_revenue)
            np_ = float(net_profit)
            if na > 0 and np_ == np_:
                roe_pct = round(np_ / na * 100.0, 2)
            if rev > 0:
                if operating_cost == operating_cost:
                    gross_margin_pct = round(
                        (rev - float(operating_cost)) / rev * 100.0, 2
                    )
                if operating_profit == operating_profit:
                    op_margin_pct = round(float(operating_profit) / rev * 100.0, 2)
                if profit_after_tax == profit_after_tax:
                    net_margin_pct = round(float(profit_after_tax) / rev * 100.0, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            pass

        sh_cnt = None
        try:
            if shareholder_count == shareholder_count:
                sh_cnt = int(round(float(shareholder_count)))
        except (TypeError, ValueError, OverflowError):
            sh_cnt = None

        rows.append(
            OrderedDict(
                [
                    ("code", code),
                    ("float_shares", float(float_shares)),
                    ("total_shares", float(total_shares)),
                    ("finance_date", int(finance_date)),
                    ("ipo_date", int(ipo_date)),
                    ("report_period", REPORT_PERIOD_MAP.get(rp_code)),
                    ("total_assets", float(total_assets)),
                    ("current_assets", float(current_assets)),
                    ("fixed_assets", float(fixed_assets)),
                    ("intangible_assets", float(intangible_assets)),
                    ("shareholder_count", sh_cnt),
                    ("current_liab", float(current_liab)),
                    ("minority_equity", float(minority_equity)),
                    ("capital_reserve", float(capital_reserve)),
                    ("net_assets", float(net_assets)),
                    ("debt_ratio_pct", debt_ratio_pct),
                    ("equity_ratio_pct", equity_ratio_pct),
                    ("operating_revenue", float(operating_revenue)),
                    ("operating_cost", float(operating_cost)),
                    ("accounts_receivable", float(accounts_receivable)),
                    ("operating_profit", float(operating_profit)),
                    ("invest_income", float(invest_income)),
                    ("operating_cashflow", float(operating_cashflow)),
                    ("total_cashflow", float(total_cashflow)),
                    ("inventory", float(inventory)),
                    ("total_profit", float(total_profit)),
                    ("profit_after_tax", float(profit_after_tax)),
                    ("net_profit", float(net_profit)),
                    ("undistributed_profit", float(undistributed_profit)),
                    ("bvps", float(bvps)),
                    ("eps", float(eps)),
                    ("capital_reserve_ps", _ps(capital_reserve, total_shares)),
                    ("undistributed_ps", _ps(undistributed_profit, total_shares)),
                    ("ocf_ps", _ps(operating_cashflow, total_shares)),
                    ("roe_pct", roe_pct),
                    ("gross_margin_pct", gross_margin_pct),
                    ("op_margin_pct", op_margin_pct),
                    ("net_margin_pct", net_margin_pct),
                    ("_region_code", int(region_code)),
                    ("_industry_code", int(industry_code)),
                ]
            )
        )
    return rows


class GetFinanceInfoBatchCmd(BaseParser):
    """标准行情批量 0x0010：按 (market, code) 列表拉财务/股本。"""

    def setParams(self, stocks: Sequence[Tuple[int, str]]) -> None:
        """
        输入：[(market, code), ...]，1..100 条。
        输出：无（写入 `send_pkg`）。
        用途：组批量财务请求包。
        边界：代码须为 6 位数字；条数越界抛 ValueError。
        """
        items = []
        for market, code in stocks:
            c = str(code).strip()
            if len(c) != 6 or not c.isdigit():
                raise ValueError(f"非法代码: {code!r}")
            items.append(struct.pack("<B6s", int(market) & 0xFF, c.encode("ascii")))
        if not items or len(items) > MAX_FINANCE_INFO_BATCH:
            raise ValueError(f"0010 批量条数须为 1..{MAX_FINANCE_INFO_BATCH}")
        body_codes = b"".join(items)
        pkgdatalen = 2 + 2 + len(body_codes)
        self.send_pkg = (
            b"\x00" * 6
            + struct.pack("<HHH", pkgdatalen, pkgdatalen, 0x0010)
            + struct.pack("<H", len(items))
            + body_codes
        )

    def parseResponse(self, body_buf: bytes) -> List[Dict[str, Any]]:
        """
        输入：解压后的批量 0x0010 正文。
        输出：C 类字段行列表。
        用途：财务批解码。
        边界：空正文返回 []。
        """
        return parse_finance_info_batch_body(body_buf or b"")
