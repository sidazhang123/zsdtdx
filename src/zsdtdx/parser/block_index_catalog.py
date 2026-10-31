"""
模块：`parser/block_index_catalog.py`。

职责：
1. 从三份命名文件建立概念、风格、指数成分、基础行业、研究中类、研究小类目录。不解析补充成分。
2. 按（中文分类名称，板块名称）解析 K 线路由。

边界：
1. 不访问网络，不读写缓存；空成分、无代码板块仍进入目录。
2. 同分类同名仅保留一项；不同代码冲突或缺少代码时不猜测路由。
3. 不纳入地区分类；风格不再过滤统计板块。
"""

from collections import defaultdict
import re
from typing import Dict, List, Sequence, Tuple

from zsdtdx.block_identity import BLOCK_KIND_NAMES, block_identity
from zsdtdx.parser.infoharbor_block import (
    catalog_full_names,
    parse_infoharbor_block,
    parse_research_industry_blocks,
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
    """六类板块目录；names 与 by_name 均以中文分类和名称的二元组为键。不含补充成分。"""

    def __init__(self, records: Sequence[dict], *, source_blocks=None, full_names=None):
        """输入目录记录及原始归属来源，输出按分类名称索引；歧义代码仅保留身份不猜路由。"""
        self.names = []
        self.by_name = {}
        # 展示目录会合并同名代码，归属处理仍需保留原始代码/成分对应及出现顺序。
        self.source_blocks = source_blocks or []
        self.full_names = full_names or {}
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
                    "industry_code": "",
                }
            industry_code = str(record.get("industry_code") or "").strip()
            if industry_code and not self.by_name[identity].get("industry_code"):
                self.by_name[identity]["industry_code"] = industry_code
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
    """输入三件套，输出全部六类目录；不解析补充成分。目录不要求成分非空或具备K线代码。研究行业大类不进入。"""
    for name in REQUIRED_BLOCK_FILES:
        if not files.get(name):
            raise ValueError(f"板块文件为空: {name}")
    zs = read_zip_entry(files["zhb.zip"], "tdxzs.cfg")
    zs3 = read_zip_entry(files["zhb.zip"], "tdxzs3.cfg")
    if not zs or not zs3:
        raise ValueError("zhb.zip 缺少 tdxzs.cfg 或 tdxzs3.cfg")
    full_names = catalog_full_names(zs, zs3)
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
        if row[2] != "12" or not row[0]:
            continue
        # 五位 X 码是中类，七位 X 码是小类；三位大类不进入板块目录。
        if re.fullmatch(r"X\d{4}", row[5]):
            kind = "RS"
        elif re.fullmatch(r"X\d{6}", row[5]):
            kind = "RL"
        else:
            continue
        selected.append(
            {
                "name": row[0],
                "code": row[1],
                "kind": kind,
                "industry_code": row[5],
            }
        )
        typed_name_codes[(kind, row[0])].add(row[1])
    blocks = parse_infoharbor_block(files["infoharbor_block.dat"])
    blocks += parse_tdx_industry_blocks(files["tdxhy.cfg"], zs)
    blocks += parse_research_industry_blocks(files["tdxhy.cfg"], zs3)
    for block in blocks:
        kind = block["kind"]
        if kind not in BLOCK_KIND_NAMES:
            continue
        code = block["block_code"]
        raw_name = str(block.get("name") or "").strip()
        if not raw_name:
            continue
        full_name = str(full_names.get(code, "") or "").strip() if code else ""
        labels = [raw_name]
        if full_name and full_name != raw_name:
            labels.append(full_name)
        for name in labels:
            resolved = code
            candidates = typed_name_codes[(kind, name)]
            if not resolved and len(candidates) == 1:
                resolved = next(iter(candidates))
            selected.append(
                {
                    "name": name,
                    "code": resolved,
                    "kind": kind,
                    "members": block["codes"],
                }
            )
    order = {kind: index for index, kind in enumerate(BLOCK_KIND_NAMES)}
    selected.sort(key=lambda r: (order[r["kind"]], r.get("code") or "~", r["name"]))
    return BlockIndexCatalog(selected, source_blocks=blocks, full_names=full_names)


def published_block_names(catalog: BlockIndexCatalog) -> List[Tuple[str, str]]:
    """输入完整目录，输出公开板块名单。

    输入：
    1. catalog: 已入库的六类目录，研究行业中类和小类都在其中。不含补充成分。
    输出：
    1. （中文分类名称，板块名称）。有小类的研究行业中类不出现；没有小类的中类和小类都出现。
    用途：
    1. `get_block_names` 的返回值。目录缓存仍保留全部二级和三级。
    边界条件：
    1. 缺少研究行业 X 码的中类视为没有小类，仍返回。
    2. 其它分类不筛选。
    """
    covered_mids = set()
    for record in catalog.by_name.values():
        industry_code = str(record.get("industry_code") or "")
        if record.get("kind") == "RL" and len(industry_code) == 7:
            covered_mids.add(industry_code[:5])
    names: List[Tuple[str, str]] = []
    for identity in catalog.names:
        record = catalog.by_name[identity]
        industry_code = str(record.get("industry_code") or "")
        if record.get("kind") == "RS" and industry_code in covered_mids:
            continue
        names.append(identity)
    return names
