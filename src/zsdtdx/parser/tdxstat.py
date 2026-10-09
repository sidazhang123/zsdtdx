"""
模块：`parser/tdxstat.py`。

职责：
1. 解析 `zhb.zip` 内 `tdxstat.cfg` / `tdxstat2.cfg` 多日涨幅、估值快照与扩展统计。
2. 从 `tdxzs.cfg` 解析地区码→中文名；从 `incon.dat` 解析通达信行业码→中文名。
3. 从 `tdxhy.cfg` 解析股票代码→通达信行业码。

边界：
1. 纯解析，不发起网络、不写缓存。
2. tdxstat 文件无表头；列义按日线反算与截图金额标定；金额为文件原生万元。
3. 地区仅收分类字段=3 的行；行业名仅收 `#TDXNHY` 段。
4. 文件内无可验证的实时行情基准；所有统计字段由业务层按文件原值返回，不做跨日期推断。
"""

from __future__ import annotations

import io
import re
import zipfile
from collections import OrderedDict
from typing import Any, Dict, Optional, Sequence

from zsdtdx.params import TDXParams

# 挂到 get_stock_stat 宽表的 tdxstat/tdxstat2 字段（原样合并，不按现价重算）。
TDXSTAT_FIELD_KEYS = (
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
)


def _tdxstat_cell_float(parts: Sequence[str], i: int) -> Optional[float]:
    """
    输入：管道分割字段、下标。
    输出：浮点或 None。
    用途：tdxstat/tdxstat2 空单元格与非法值统一为空。
    边界：越界/空白/非数字 → None。
    """
    if i >= len(parts):
        return None
    s = parts[i].strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _tdxstat_cell_int(parts: Sequence[str], i: int) -> Optional[int]:
    """
    输入：管道分割字段、下标。
    输出：整数或 None。
    用途：连涨天数/员工数等整型列。
    边界：先按浮点解析再转 int；空→None。
    """
    v = _tdxstat_cell_float(parts, i)
    return None if v is None else int(v)


def parse_tdxstat_cfg(raw: bytes) -> Dict[str, Dict[str, Any]]:
    """
    输入：`tdxstat.cfg` 原文。
    输出：code → 多日涨幅 + 估值快照 + 扩展财务字段。
    用途：解析 zhb.zip 内统计主表。
    边界：无表头；空单元格→None；金额为文件原生万元；不含当日股价。
    """
    text = raw.decode("gbk", errors="replace")
    out: Dict[str, Dict[str, Any]] = {}
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 10:
            continue
        code = parts[1].strip()
        if len(code) != 6 or not code.isdigit():
            continue
        out[code] = OrderedDict(
            [
                ("beta", _tdxstat_cell_float(parts, 2)),
                ("pe_ttm", _tdxstat_cell_float(parts, 3)),
                ("stat_asof", parts[4].strip() or None),
                ("consec_up_days", _tdxstat_cell_int(parts, 5)),
                ("chg_pct_prev", _tdxstat_cell_float(parts, 6)),
                ("chg_pct_3d", _tdxstat_cell_float(parts, 7)),
                ("pe_static", _tdxstat_cell_float(parts, 9)),
                ("dividend_yield_pct", _tdxstat_cell_float(parts, 10)),
                ("float_shares_z", _tdxstat_cell_float(parts, 11)),
                ("net_profit_ex", _tdxstat_cell_float(parts, 14)),
                ("employee_count", _tdxstat_cell_int(parts, 15)),
                ("rd_expense", _tdxstat_cell_float(parts, 16)),
                ("chg_pct_mtd", _tdxstat_cell_float(parts, 17)),
                ("chg_pct_20d", _tdxstat_cell_float(parts, 18)),
                ("chg_pct_60d", _tdxstat_cell_float(parts, 20)),
                ("chg_pct_ytd", _tdxstat_cell_float(parts, 21)),
                ("cash_funds", _tdxstat_cell_float(parts, 24)),
                ("contract_liab", _tdxstat_cell_float(parts, 25)),
                ("chg_pct_5d", _tdxstat_cell_float(parts, 28)),
                ("chg_pct_10d", _tdxstat_cell_float(parts, 30)),
            ]
        )
    return out


def parse_tdxstat2_cfg(raw: bytes) -> Dict[str, Dict[str, Any]]:
    """
    输入：`tdxstat2.cfg` 原文。
    输出：code → 一年/30 日等补充涨幅字段。
    用途：与 tdxstat 主表按代码合并。
    边界：无表头；[12]=一年涨幅%、[20]=30日涨幅%。
    """
    text = raw.decode("gbk", errors="replace")
    out: Dict[str, Dict[str, Any]] = {}
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 13:
            continue
        code = parts[1].strip()
        if len(code) != 6 or not code.isdigit():
            continue
        out[code] = OrderedDict(
            [
                ("chg_pct_1y", _tdxstat_cell_float(parts, 12)),
                ("chg_pct_30d", _tdxstat_cell_float(parts, 20)),
            ]
        )
    return out


def merge_tdxstat_maps(
    stat: Dict[str, Dict[str, Any]],
    stat2: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    输入：tdxstat 主表、tdxstat2 补表。
    输出：按代码合并后的统计字典（stat2 同名键覆盖主表）。
    用途：一次给出完整多日涨幅与估值快照字段。
    边界：只出现在一侧的代码也会保留。
    """
    out: Dict[str, Dict[str, Any]] = {}
    for code, row in stat.items():
        out[code] = OrderedDict(row)
    for code, row in stat2.items():
        base = out.get(code)
        if base is None:
            out[code] = OrderedDict(row)
        else:
            base.update(row)
    return out


def parse_region_map_from_tdxzs(raw: bytes) -> Dict[int, str]:
    """
    输入：`tdxzs.cfg` 原文。
    输出：地区码 → 中文名。
    用途：把 0010 DY 数字码换成地区文字。
    边界：只收分类字段=3 的地区行。
    """
    out: Dict[int, str] = {}
    text = raw.decode("gbk", errors="replace")
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 6 or parts[2].strip() != "3":
            continue
        try:
            code = int(parts[-1].strip())
        except ValueError:
            continue
        name = parts[0].strip()
        if name:
            out[code] = name
    return out


def parse_industry_name_map_from_incon(raw: bytes) -> Dict[str, str]:
    """
    输入：`incon.dat` 原文。
    输出：通达信行业码（如 T020603）→ 中文名。
    用途：配合 tdxhy.cfg 把股票行业码换成文字。
    边界：只解析 `#TDXNHY` 段。
    """
    text = raw.decode("gbk", errors="replace")
    m = re.search(r"#TDXNHY\r?\n(.*?)(?:\r?\n#|\Z)", text, re.S)
    if not m:
        return {}
    out: Dict[str, str] = {}
    for line in m.group(1).splitlines():
        if "|" not in line:
            continue
        code, name = line.split("|", 1)
        code, name = code.strip(), name.strip()
        if code and name:
            out[code] = name
    return out


def parse_tdxhy_code_map(raw: bytes) -> Dict[str, str]:
    """
    输入：`tdxhy.cfg` 原文。
    输出：股票代码 → 通达信行业码（T…）。
    用途：行业中文名解析的中间表。
    边界：行格式 `市场|代码|T行业码|||…`。
    """
    out: Dict[str, str] = {}
    text = raw.decode("gbk", errors="replace")
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 3:
            continue
        code = parts[1].strip()
        hy = parts[2].strip()
        if len(code) == 6 and code.isdigit() and hy:
            out[code] = hy
    return out


def read_zhb_stat_payload(zhb_raw: bytes) -> Dict[str, bytes]:
    """
    输入：`zhb.zip` 全文。
    输出：成员小写名 → 原文（仅关心的统计相关成员）。
    用途：一次打开 zip 取出 tdxstat / tdxzs / incon。
    边界：损坏或缺成员时对应键不存在；不落盘。
    """
    wanted = {
        TDXParams.TDXSTAT_ZIP_MEMBER,
        TDXParams.TDXSTAT2_ZIP_MEMBER,
        TDXParams.TDXZS_ZIP_MEMBER,
        TDXParams.INCON_ZIP_MEMBER,
    }
    out: Dict[str, bytes] = {}
    if not zhb_raw:
        return out
    try:
        with zipfile.ZipFile(io.BytesIO(bytes(zhb_raw))) as zf:
            names = {n.lower(): n for n in zf.namelist()}
            for key in wanted:
                real = names.get(key)
                if real is not None:
                    out[key] = zf.read(real)
    except (zipfile.BadZipFile, KeyError, OSError):
        return {}
    return out
