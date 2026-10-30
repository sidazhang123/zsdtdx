"""
模块：`parser/block_index_catalog.py`。

职责：
1. 从三份命名文件建立概念、风格、指数成分、基础行业、研究中类、补充成分目录。
2. 按（中文分类名称，板块名称）解析 K 线路由。

边界：
1. 不访问网络，不读写缓存；空成分、无代码板块仍进入目录。
2. 同分类同名仅保留一项；不同代码冲突或缺少代码时不猜测路由。
3. 不纳入地区分类；风格不再过滤统计板块。
"""

from collections import defaultdict
import re
from typing import Dict, Sequence

from zsdtdx.block_identity import BLOCK_KIND_NAMES, block_identity
from zsdtdx.parser.infoharbor_block import (
    catalog_full_names,
    parse_infoharbor_block,
    parse_research_industry_blocks,
    parse_spblock,
    parse_tdx_industry_blocks,
    read_zip_entry,
)

_BLOCK_CODE = re.compile(r"^88\d{4}$")
REQUIRED_BLOCK_FILES = ("infoharbor_block.dat", "tdxhy.cfg", "zhb.zip")


def _pipe_rows(raw: bytes):
    """输出目录中至少六字段且代码为88开头的行。"""
    rows = []
    for line in raw.decode("gbk", "ignore").splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) >= 6 and _BLOCK_CODE.fullmatch(parts[1]):
            rows.append(parts)
    return rows


class BlockIndexCatalog:
    """六类板块目录；names 与 by_name 均以中文分类和名称的二元组为键。"""

    def __init__(self, records: Sequence[dict]):
        self.names = []
        self.by_name = {}
        candidates = defaultdict(set)
        members = defaultdict(set)
        ambiguous = set()
        for record in records:
            kind = record["kind"]
            identity = block_identity(kind, record["name"])
            if record.get("empty_reason") == "ambiguous_code":
                ambiguous.add(identity)
            if identity not in self.by_name:
                self.names.append(identity)
                self.by_name[identity] = {
                    "name": identity[1],
                    "identity": identity,
                    "kind": kind,
                    "code": "",
                    "market": 1,
                    "concept_order": None,
                }
            source_order = record.get("concept_order")
            previous_order = self.by_name[identity]["concept_order"]
            if source_order is not None and (
                previous_order is None or source_order < previous_order
            ):
                self.by_name[identity]["concept_order"] = source_order
            members[identity].update(tuple(pair) for pair in record.get("members", []))
            if record.get("code"):
                candidates[identity].add(str(record["code"]))
        for identity, record in self.by_name.items():
            codes = sorted(candidates[identity])
            if identity in ambiguous:
                codes = []
            record["code"] = codes[0] if len(codes) == 1 else ""
            record["members"] = sorted(members[identity])
            record["empty_reason"] = (
                ""
                if len(codes) == 1
                else "ambiguous_code"
                if codes or identity in ambiguous
                else "no_code"
            )


def parse_block_index_catalog(files: Dict[str, bytes]) -> BlockIndexCatalog:
    """输入三件套，输出全部六类目录；目录不要求成分非空或具备K线代码。"""
    for name in REQUIRED_BLOCK_FILES:
        if not files.get(name):
            raise ValueError(f"板块文件为空: {name}")
    zs = read_zip_entry(files["zhb.zip"], "tdxzs.cfg")
    zs3 = read_zip_entry(files["zhb.zip"], "tdxzs3.cfg")
    if not zs or not zs3:
        raise ValueError("zhb.zip 缺少 tdxzs.cfg 或 tdxzs3.cfg")
    full_names = catalog_full_names(zs, zs3)
    # 补充成分与无代码标题，只在目录名称唯一对应代码时关联。
    name_codes = defaultdict(set)
    for code, name in full_names.items():
        name_codes[name].add(code)
    selected = []
    typed_name_codes = defaultdict(set)
    for row in _pipe_rows(zs):
        kind = {"4": "GN", "5": "FG", "2": "HY"}.get(row[2])
        if kind == "HY" and re.fullmatch(r"T\d+", row[5]) is None:
            continue
        if kind and row[0]:
            selected.append({"name": row[0], "code": row[1], "kind": kind})
            typed_name_codes[(kind, row[0])].add(row[1])
    for row in _pipe_rows(zs3):
        if row[2] == "12" and re.fullmatch(r"X\d{4}", row[5]) and row[0]:
            selected.append({"name": row[0], "code": row[1], "kind": "RS"})
            typed_name_codes[("RS", row[0])].add(row[1])
    blocks = parse_infoharbor_block(files["infoharbor_block.dat"])
    blocks += parse_tdx_industry_blocks(files["tdxhy.cfg"], zs)
    blocks += parse_research_industry_blocks(files["tdxhy.cfg"], zs3)
    blocks += parse_spblock(read_zip_entry(files["zhb.zip"], "spblock.dat"))
    for source_order, block in enumerate(blocks):
        kind = block["kind"]
        if kind not in BLOCK_KIND_NAMES:
            continue
        code = block["block_code"]
        name = full_names.get(code, block["name"])
        candidates = (
            name_codes[name] if kind == "SP" else typed_name_codes[(kind, name)]
        )
        if not code and len(candidates) == 1:
            code = next(iter(candidates))
        selected.append(
            {
                "name": name,
                "code": code,
                "kind": kind,
                "members": block["codes"],
                "concept_order": source_order,
            }
        )
    order = {kind: index for index, kind in enumerate(BLOCK_KIND_NAMES)}
    selected.sort(key=lambda r: (order[r["kind"]], r.get("code") or "~", r["name"]))
    return BlockIndexCatalog(selected)
