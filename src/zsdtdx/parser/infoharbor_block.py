"""
模块：`parser/infoharbor_block.py`。

职责：
1. 解析 `infoharbor_block.dat` 正文，得到概念、风格、指数板块与成分代码。
2. 解析 `tdxhy.cfg` 与 `tdxzs.cfg`，得到通达信基础行业板块与成分代码。
3. 用股票代码名称表收成 `{names, map}`。

边界：
1. 不访问网络，不读写缓存，不生成 pkl 文件。
2. 成分只认市场号 0/1/2 与 6 位代码；没有股票名称字段。
3. 基础行业只收 `tdxzs.cfg` 里行业码为 `T` 加数字、且分类字段为 2 的行。
"""

from __future__ import annotations

import io
import re
import zipfile
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

# infoharbor 成分行里的市场号，与标准行情码表前缀一致。
_MARKET_PREFIX = {0: "sz", 1: "sh", 2: "bj"}
# tdxzs.cfg 分类字段：2 为通达信基础行业（钢铁、煤炭、水泥等）。
_TDX_INDUSTRY_KIND = "2"
_TDX_INDUSTRY_CODE = re.compile(r"^T\d+$")


def _prefixed_stock_code(market: int, code: str) -> str:
    """
    输入市场号与 6 位代码，输出 `sz.`/`sh.`/`bj.` 前缀代码。

    输入：
    1. market: 0 深圳、1 上海、2 北京。
    2. code: 6 位数字代码。
    输出：
    1. 前缀代码；市场号无法识别时为空串。
    用途：
    1. 与 `get_stock_code_name` 的键对齐。
    边界条件：
    1. 不校验代码是否在码表中。
    """
    prefix = _MARKET_PREFIX.get(int(market))
    if not prefix:
        return ""
    return f"{prefix}.{code}"


def _decode_gbk(raw: bytes) -> str:
    """
    输入字节，输出 GBK 文本。

    输入：
    1. raw: 文件原文。
    输出：
    1. 文本；空输入为空串。
    用途：
    1. 板块与行业配置都是 GBK。
    边界条件：
    1. 非法字节丢弃，不抛异常。
    """
    if not raw:
        return ""
    return bytes(raw).decode("gbk", "ignore")


def read_zip_entry(raw: bytes, member: str) -> bytes:
    """
    输入 zip 原文与成员名，输出该成员字节。

    输入：
    1. raw: `zhb.zip` 全文。
    2. member: 包内文件名，如 `tdxzs.cfg`。
    输出：
    1. 成员原文；包损坏或没有该成员时为空 bytes。
    用途：
    1. 从命名文件下载的 zip 中取出行业板块表。
    边界条件：
    1. 不落盘。
    """
    if not raw or not member:
        return b""
    try:
        with zipfile.ZipFile(io.BytesIO(bytes(raw))) as archive:
            return archive.read(member)
    except (zipfile.BadZipFile, KeyError, OSError):
        return b""


def parse_infoharbor_block(raw: bytes) -> List[dict]:
    """
    输入板块文件原文，输出按出现顺序排列的板块列表。

    输入：
    1. raw: `infoharbor_block.dat` 的 GBK 原文。
    输出：
    1. `[{"name": str, "kind": str, "codes": [(market, code), ...]}]`。
    用途：
    1. 抽出概念、风格、指数板块名称与成分代码。
    边界条件：
    1. 空原文返回空列表。
    2. 同一板块内重复代码只保留第一次。
    3. 非 `市场#6位数字`、全零代码不收录。
    4. 无成分的板块仍保留，名称为空的标题丢弃。
    """
    text = _decode_gbk(raw)
    if not text:
        return []

    blocks: List[dict] = []
    current: dict | None = None
    for line in text.splitlines():
        line = line.strip().replace("\x00", "")
        if not line:
            continue
        if line.startswith("#"):
            body = line[1:]
            if "_" not in body:
                current = None
                continue
            kind, rest = body.split("_", 1)
            name = rest.split(",", 1)[0].strip()
            if not name:
                current = None
                continue
            current = {"name": name, "kind": kind.strip(), "codes": []}
            blocks.append(current)
            continue
        if current is None:
            continue
        seen = {code for _market, code in current["codes"]}
        for part in line.split(","):
            part = part.strip()
            if "#" not in part:
                continue
            market_text, code = part.split("#", 1)
            code = code.strip()
            if not market_text.isdigit():
                continue
            market_id = int(market_text)
            if (
                market_id not in _MARKET_PREFIX
                or len(code) != 6
                or not code.isdigit()
                or code == "000000"
                or code in seen
            ):
                continue
            seen.add(code)
            current["codes"].append((market_id, code))
    return blocks


def parse_tdx_industry_blocks(hy_raw: bytes, zs_raw: bytes) -> List[dict]:
    """
    输入行业归属与板块名称表，输出基础行业板块列表。

    输入：
    1. hy_raw: `tdxhy.cfg`。每行 `市场|代码|T行业码|||X研究行业码`。
    2. zs_raw: `tdxzs.cfg`。行业行 `名称|板块代码|2|...|T行业码`。
    输出：
    1. 与 `parse_infoharbor_block` 相同结构，`kind` 为 `HY`。
    用途：
    1. 把通达信基础行业补进板块成分。
    边界条件：
    1. 地区板块、概念板块不在这里生成。
    2. 只使用股票行上的那个行业码，不把上级行业名写入结果。
    3. 行业码对不上名称的股票丢弃。没有成分的行业不返回。
    """
    industry_name: Dict[str, str] = {}
    for line in _decode_gbk(zs_raw).splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 6 or parts[2] != _TDX_INDUSTRY_KIND:
            continue
        code = parts[5]
        name = parts[0]
        if not name or _TDX_INDUSTRY_CODE.fullmatch(code) is None:
            continue
        industry_name[code] = name

    buckets: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    seen: Dict[str, set] = defaultdict(set)
    for line in _decode_gbk(hy_raw).splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 3 or not parts[0].isdigit():
            continue
        market_id = int(parts[0])
        code = parts[1]
        industry = parts[2]
        if (
            market_id not in _MARKET_PREFIX
            or len(code) != 6
            or not code.isdigit()
            or code == "000000"
        ):
            continue
        name = industry_name.get(industry, "")
        if not name or code in seen[name]:
            continue
        seen[name].add(code)
        buckets[name].append((market_id, code))
    return [
        {"name": name, "kind": "HY", "codes": codes}
        for name, codes in buckets.items()
        if codes
    ]


def build_stock_concept_payload(
    blocks: Sequence[dict],
    code_name: Dict[str, str],
) -> Dict[str, object]:
    """
    输入板块列表与代码名称表，输出 `{names, map}`。

    输入：
    1. blocks: 概念板块与基础行业板块。
    2. code_name: `get_stock_code_name` 的 `前缀代码 -> 股票名称`。
    输出：
    1. names: 有有效成分代码的板块名，去重后按字排序。
    2. map: 股票名称 -> 所属板块名列表；列表去重且按字排序。
    用途：
    1. 把「板块 -> 股票代码」收成「股票名称 -> 板块名」。
    边界条件：
    1. 码表中没有名称的代码丢弃，不因此删掉板块名。
    2. 多个代码对应同一股票名时，板块名取并集。
    3. 不写文件。
    """
    names: List[str] = []
    code_to_blocks: Dict[Tuple[int, str], List[str]] = {}
    for block in blocks:
        name = str(block.get("name") or "").strip()
        if not name:
            continue
        codes = []
        seen_codes = set()
        for market, code in block.get("codes") or []:
            try:
                market_id = int(market)
            except (TypeError, ValueError):
                continue
            code_text = str(code).strip()
            if code_text in seen_codes or market_id not in _MARKET_PREFIX:
                continue
            seen_codes.add(code_text)
            codes.append((market_id, code_text))
        if not codes:
            continue
        names.append(name)
        for market_id, code_text in codes:
            code_to_blocks.setdefault((market_id, code_text), []).append(name)

    name_map: Dict[str, List[str]] = {}
    for (market_id, code_text), block_names in code_to_blocks.items():
        prefixed = _prefixed_stock_code(market_id, code_text)
        stock_name = str(code_name.get(prefixed) or "").strip()
        if not stock_name:
            continue
        merged = set(block_names)
        if stock_name in name_map:
            name_map[stock_name] = sorted(set(name_map[stock_name]) | merged)
        else:
            name_map[stock_name] = sorted(merged)
    return {"names": sorted(set(names)), "map": name_map}
