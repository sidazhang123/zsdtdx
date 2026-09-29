"""
模块：`parser/block_index_catalog.py`。

职责：
1. 从 `zhb.zip` 内的 `tdxzs.cfg` / `tdxzs3.cfg` 选出「全部板块」指数目录。
2. 给出板块名称列表，以及名称到指数代码的内部映射。

边界：
1. 不访问网络，不读写缓存。
2. `infoharbor_block.dat` 与 `tdxhy.cfg` 必须随三份文件一起存在且非空，本模块不解析其成分。
3. 只收录概念、非统计风格、地区、研究行业中类；通达信 T 行业与统计风格不进入。
4. 同名只保留排序靠前的一条，保证名称可唯一反查代码。
"""

from __future__ import annotations

import re
from typing import Dict, List, Sequence

from zsdtdx.params import TDXParams
from zsdtdx.parser.infoharbor_block import read_zip_entry

# 板块指数代码。
_BLOCK_CODE = re.compile(r"^88\d{4}$")
# tdxzs.cfg 分类：3 地区，4 概念，5 风格。
_KIND_BY_TDXZS = {"4": "GN", "5": "FG", "3": "DQ"}
_KIND_ORDER = {"GN": 0, "FG": 1, "DQ": 2, "HY": 3}
# 研究行业中类：tdxzs3 分类 12，且行业码长度为 5（如 X4006）。
_RESEARCH_KIND = "12"
_RESEARCH_CODE_LEN = 5
# 「全部板块」不展示的统计风格：名称前缀，或固定全名。
_STATISTIC_STYLE_PREFIXES = ("昨", "历史", "最近")
_STATISTIC_STYLE_NAMES = frozenset({"轮动趋势", "板块趋势", "通达信热股", "通达信热"})

REQUIRED_BLOCK_FILES = (
    "infoharbor_block.dat",
    "tdxhy.cfg",
    "zhb.zip",
)


def _decode_gbk(raw: bytes) -> str:
    """
    输入字节，输出 GBK 文本。

    输入：raw 为文件原文。
    输出：文本；空输入为空串。
    用途：板块配置为 GBK。
    边界条件：非法字节丢弃，不抛异常。
    """
    if not raw:
        return ""
    return bytes(raw).decode("gbk", "ignore")


def _is_statistic_style(name: str) -> bool:
    """
    输入风格板块名，输出是否为「全部板块」不展示的统计板。

    输入：name 为 tdxzs 名称。
    输出：True 表示昨日涨停、历史新高等统计板。
    用途：风格分类共 158 个，其中约 27 个不进入 560 名单。
    边界条件：空名称视为统计板，不收录。
    """
    text = str(name or "").strip()
    if text == "":
        return True
    if text in _STATISTIC_STYLE_NAMES:
        return True
    return text.startswith(_STATISTIC_STYLE_PREFIXES)


def _pipe_rows(raw: bytes) -> List[List[str]]:
    """
    输入竖线分隔原文，输出字段行。

    输入：raw 为 cfg 字节。
    输出：每行至少 6 列且代码为 88xxxx 的字段列表。
    用途：解析 tdxzs.cfg / tdxzs3.cfg。
    边界条件：空文件返回 []；代码不是 88xxxx 的行丢弃。
    """
    rows: List[List[str]] = []
    for line in _decode_gbk(raw).splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 6 or _BLOCK_CODE.fullmatch(parts[1] or "") is None:
            continue
        rows.append(parts)
    return rows


class BlockIndexCatalog:
    """
    板块指数目录。

    输入：由 `parse_block_index_catalog` 构造。
    输出：`names` 为名称列表；`by_name` 为名称到代码记录。
    用途：`get_block_names` 读 names；K 线按名称取 code/market。
    边界条件：names 与 by_name 一一对应，不含重复名称。
    """

    def __init__(self, records: Sequence[dict]):
        """
        输入：records 为已排序且名称唯一的记录。
        输出：无。
        用途：冻结目录。
        边界条件：跳过空名称。
        """
        names: List[str] = []
        by_name: Dict[str, dict] = {}
        for record in records:
            name = str(record.get("name") or "").strip()
            if name == "" or name in by_name:
                continue
            item = {
                "name": name,
                "code": str(record.get("code") or "").strip(),
                "market": int(record.get("market", 1)),
                "kind": str(record.get("kind") or "").strip(),
            }
            names.append(name)
            by_name[name] = item
        self.names = names
        self.by_name = by_name


def parse_block_index_catalog(files: Dict[str, bytes]) -> BlockIndexCatalog:
    """
    输入三份板块文件，输出「全部板块」指数目录。

    输入：
    1. files: 键为 infoharbor_block.dat、tdxhy.cfg、zhb.zip 的原文字典。
    输出：
    1. BlockIndexCatalog。名称按种类（概念、风格、地区、研究行业）再按代码排序。
    用途：
    1. 从 zhb.zip 的两张表选出可请求 K 线的板块指数。
    边界条件：
    1. 任一文件缺失或为空时抛 ValueError。
    2. zip 中缺少 tdxzs.cfg 或 tdxzs3.cfg 时抛 ValueError。
    3. 板块指数 market 固定为 1。
    """
    payload = dict(files or {})
    for name in REQUIRED_BLOCK_FILES:
        if not payload.get(name):
            raise ValueError(f"板块文件为空: {name}")
    zs_raw = read_zip_entry(payload["zhb.zip"], TDXParams.TDXZS_ZIP_MEMBER)
    zs3_raw = read_zip_entry(payload["zhb.zip"], TDXParams.TDXZS3_ZIP_MEMBER)
    if not zs_raw or not zs3_raw:
        raise ValueError(
            f"zhb.zip 缺少 {TDXParams.TDXZS_ZIP_MEMBER} 或 {TDXParams.TDXZS3_ZIP_MEMBER}"
        )

    selected: List[dict] = []
    for parts in _pipe_rows(zs_raw):
        kind = _KIND_BY_TDXZS.get(parts[2], "")
        if kind == "":
            continue
        name = parts[0]
        if kind == "FG" and _is_statistic_style(name):
            continue
        selected.append({"name": name, "code": parts[1], "market": 1, "kind": kind})
    for parts in _pipe_rows(zs3_raw):
        if parts[2] != _RESEARCH_KIND or len(parts[5]) != _RESEARCH_CODE_LEN:
            continue
        selected.append({"name": parts[0], "code": parts[1], "market": 1, "kind": "HY"})
    selected.sort(key=lambda item: (_KIND_ORDER.get(item["kind"], 9), item["code"]))
    return BlockIndexCatalog(selected)
