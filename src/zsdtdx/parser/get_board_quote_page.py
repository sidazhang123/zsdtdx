"""
模块：`parser/get_board_quote_page.py`。

职责：
1. 封装标准行情 0x054B 版面分页组包与解包。
2. 单条记录复用 `GetSecurityQuotesCmd.parseResponse` 解析价量字段。
3. 附带涨跌/涨幅/振幅/均价/内外比等 B 类衍生字段。

边界：
1. 服务端硬顶约 80 条/页；请求更大也只回 ≤80。
2. 不发起网络；组包后由 `call_api` 发送。
3. 正文结构不完整或单条解析失败时抛错，不返回残缺页。
"""

from __future__ import annotations

import struct
from collections import OrderedDict
from typing import Any, Dict, List

from zsdtdx.parser.base import BaseParser
from zsdtdx.parser.get_security_quotes import GetSecurityQuotesCmd

MAX_BOARD_QUOTE_PAGE = 80


def enrich_board_quote_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    输入：价量行。
    输出：附加 B 类衍生字段的行副本。
    用途：涨跌/涨幅/振幅/均价/内外比。
    边界：分母为 0 时对应字段为 None。
    """
    out = OrderedDict(row)
    try:
        price = float(row.get("price") or 0)
        last = float(row.get("last_close") or 0)
        high = float(row.get("high") or 0)
        low = float(row.get("low") or 0)
        vol = float(row.get("vol") or 0)
        amount = float(row.get("amount") or 0)
        s_vol = float(row.get("s_vol") or 0)
        b_vol = float(row.get("b_vol") or 0)
    except (TypeError, ValueError):
        return out
    out["change"] = round(price - last, 4) if last or price else None
    out["change_pct"] = round((price - last) / last * 100, 4) if last else None
    out["amplitude_pct"] = (
        round((high - low) / last * 100, 4) if last and high and low else None
    )
    out["avg_price"] = round(amount / (vol * 100), 4) if vol > 0 else None
    out["io_ratio"] = round(s_vol / b_vol, 4) if b_vol else None
    return out


def find_board_record_starts(body: bytes, count: int, start_off: int = 4) -> List[int]:
    """
    输入：回包正文、期望条数、扫描起点。
    输出：记录起始偏移列表。
    用途：054B 变长行切分。
    边界：market∈{0,1,2} 且后跟 6 位数字；相邻 ≥70 字节。
    """
    starts: List[int] = []
    for off in range(start_off, max(start_off, len(body) - 6)):
        if body[off] not in (0, 1, 2):
            continue
        code_b = body[off + 1 : off + 7]
        if len(code_b) < 6 or not all(0x30 <= b <= 0x39 for b in code_b):
            continue
        if not starts or off - starts[-1] >= 70:
            starts.append(off)
            if len(starts) >= int(count):
                break
    return starts


def parse_board_quote_body(body: bytes) -> List[Dict[str, Any]]:
    """
    输入：0x054B 解压正文。
    输出：A/B 字段行列表。
    用途：解析版面分页；供命令类与离线单测共用。
    边界：声明 0 条返回空列表；正文过短、条数不匹配或单条失败抛 ValueError。
    """
    if not body or len(body) < 4:
        raise ValueError(f"054B 正文过短: {len(body or b'')} 字节")
    _unk, count = struct.unpack_from("<HH", body, 0)
    if count <= 0:
        return []
    starts = find_board_record_starts(body, count, 4)
    if len(starts) != count:
        raise ValueError(
            f"054B 记录数不匹配: 声明 {count} 条，定位 {len(starts)} 条"
        )
    cmd = GetSecurityQuotesCmd(client=None)
    rows: List[Dict[str, Any]] = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(body)
        rec = body[start:end]
        fake = struct.pack("<HH", 0, 1) + rec
        try:
            got = cmd.parseResponse(fake)
            if not got:
                code = rec[1:7].decode("ascii", errors="replace")
                rows.append(OrderedDict([("code", code), ("parse_error", "empty")]))
                continue
            rows.append(enrich_board_quote_row(OrderedDict(got[0])))
        except Exception as exc:
            code = rec[1:7].decode("ascii", errors="replace") if len(rec) >= 7 else ""
            rows.append(OrderedDict([("code", code), ("parse_error", str(exc))]))
    errors = [row for row in rows if row.get("parse_error")]
    if errors:
        detail = "; ".join(
            f"{row.get('code', '')}: {row.get('parse_error', '')}" for row in errors[:3]
        )
        raise ValueError(f"054B 记录解析失败 {len(errors)} 条: {detail}")
    return rows


class GetBoardQuotePageCmd(BaseParser):
    """标准行情 0x054B：按列表偏移分页拉取版面实时行。"""

    def setParams(self, start: int, count: int = 80, *, seq: int = 1) -> None:
        """
        输入：列表偏移、条数、包序号。
        输出：无（写入 `send_pkg`）。
        用途：组 0x054B 请求包。
        边界：count 钳制到 1..80。
        """
        count = max(1, min(int(count), MAX_BOARD_QUOTE_PAGE))
        hdr = struct.pack(
            "<HIHHH",
            0x000C | ((int(seq) & 0xFF) << 8),
            0x01000A04,
            20,
            20,
            0x054B,
        )
        body = struct.pack(
            "<HHHHHHHHH",
            6,
            0,
            int(start) & 0xFFFF,
            int(count) & 0xFFFF,
            0,
            5,
            0,
            1,
            0,
        )
        self.send_pkg = hdr + body

    def parseResponse(self, body_buf: bytes) -> List[Dict[str, Any]]:
        """
        输入：解压后的 0x054B 正文。
        输出：带 A/B 字段的行列表。
        用途：版面分页解码。
        边界：服务端声明 0 条返回 []；结构不完整抛 ValueError。
        """
        return parse_board_quote_body(body_buf or b"")
