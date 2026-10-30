"""
模块：`cache/block_catalog_cache.py`。

职责：
1. 按原始文件的6小时生命周期保存完整六类目录、路由、成分和股票归属。
2. 提供原子写入与结构校验，供三个板块公开接口共用。

边界：
1. 不联网；不缓存空码表或不完整的源文件。
2. 过期、原始快照变化、未来时间、旧格式或损坏数据不能命中。
"""

from datetime import datetime
import hashlib
import math
from pathlib import Path
import pickle
import re
import time

from zsdtdx.block_identity import (
    BLOCK_KIND_NAMES,
    block_identity,
    normalize_block_identity,
)
from zsdtdx.cache.block_file_cache import BLOCK_FILE_TTL_SECONDS, _atomic_write_bytes
from zsdtdx.parser.block_index_catalog import (
    REQUIRED_BLOCK_FILES,
    parse_block_index_catalog,
)
from zsdtdx.parser.infoharbor_block import (
    build_stock_concept_payload,
    collapse_identical_stock_sets,
    merge_concept_blocks,
)

BLOCK_CATALOG_CACHE_VERSION = 3
BLOCK_CATALOG_CACHE_FILENAME = "block_catalog.pkl"


def _stock_payload(records, code_name):
    """从同一份成分图派生股票归属，校验时也用此关系核对派生数据。"""
    blocks = []
    prefixes = {0: "sz", 1: "sh", 2: "bj"}
    # 目录展示顺序不决定归属别名；缺少目录全称时保持原始成分文件的先出现者。
    ordered = sorted(
        records,
        key=lambda r: r["concept_order"]
        if r["concept_order"] is not None
        else math.inf,
    )
    for record in ordered:
        members = record["members"]
        if record["kind"] == "SP":
            members = [
                (m, c) for m, c in members if code_name.get(f"{prefixes[m]}.{c}")
            ]
        blocks.append(
            {
                "name": record["name"],
                "kind": record["kind"],
                "codes": members,
                "block_code": record["code"],
            }
        )
    merged = merge_concept_blocks(blocks, {})
    return build_stock_concept_payload(
        collapse_identical_stock_sets(merged, code_name), code_name
    )


def build_block_catalog_snapshot(
    files, code_name, *, now=None, name_scope="", source_fetched_at=None
):
    """完整三文件和股票码表生成统一图；空成分保留目录，基金仍不进入股票名称映射。"""
    stamp = time.time() if now is None else float(now)
    if not code_name:
        raise ValueError("股票名称码表为空，不能生成完整板块映射缓存")
    catalog = parse_block_index_catalog(files)
    records = list(catalog.by_name.values())
    snapshot = {
        "version": BLOCK_CATALOG_CACHE_VERSION,
        "kind": "block_catalog",
        "cache_date": datetime.fromtimestamp(stamp).date().isoformat(),
        "fetched_at": stamp,
        "source_fetched_at": stamp
        if source_fetched_at is None
        else float(source_fetched_at),
        "name_scope": name_scope,
        "source_hashes": {
            n: hashlib.sha256(files[n]).hexdigest() for n in REQUIRED_BLOCK_FILES
        },
        "records": records,
        "code_name": dict(code_name),
        "stock_concepts": _stock_payload(records, code_name),
    }
    if not validate_block_catalog_snapshot(snapshot):
        raise ValueError("板块映射缓存结构或股票码表无效")
    return snapshot


def validate_block_catalog_snapshot(snapshot):
    """检查身份、标记、路由、原始成分与股票映射；不在热路径重复执行。"""
    try:
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("version") != BLOCK_CATALOG_CACHE_VERSION
            or snapshot.get("kind") != "block_catalog"
        ):
            return False
        stamp = snapshot["fetched_at"]
        if (
            not isinstance(stamp, (int, float))
            or not math.isfinite(stamp)
            or stamp <= 0
            or datetime.fromtimestamp(stamp).date().isoformat()
            != snapshot["cache_date"]
        ):
            return False
        if not isinstance(snapshot.get("name_scope"), str):
            return False
        source_stamp = snapshot["source_fetched_at"]
        if (
            not isinstance(source_stamp, (int, float))
            or not math.isfinite(source_stamp)
            or not 0 < source_stamp <= stamp
        ):
            return False
        hashes = snapshot["source_hashes"]
        if set(hashes) != set(REQUIRED_BLOCK_FILES) or any(
            not isinstance(h, str) or not re.fullmatch(r"[0-9a-f]{64}", h)
            for h in hashes.values()
        ):
            return False
        names = snapshot["code_name"]
        if (
            not isinstance(names, dict)
            or not names
            or any(
                not isinstance(k, str)
                or not re.fullmatch(r"(?:sz|sh|bj)\.\d{6}|hk\.\d{5}", k)
                or not isinstance(v, str)
                or not v.strip()
                for k, v in names.items()
            )
        ):
            return False
        identities = set()
        records = snapshot["records"]
        if not isinstance(records, list) or not records:
            return False
        for record in records:
            identity = normalize_block_identity(record["identity"])
            if (
                identity != block_identity(record["kind"], record["name"])
                or identity in identities
            ):
                return False
            identities.add(identity)
            order = record["concept_order"]
            if order is not None and (type(order) is not int or order < 0):
                return False
            if record["kind"] not in BLOCK_KIND_NAMES or record["market"] != 1:
                return False
            if record["code"] and not re.fullmatch(r"88\d{4}", record["code"]):
                return False
            if record["empty_reason"] not in {"", "no_code", "ambiguous_code"} or bool(
                record["code"]
            ) != (record["empty_reason"] == ""):
                return False
            if not isinstance(record["members"], list):
                return False
            for market, code in record["members"]:
                if (
                    market not in (0, 1, 2)
                    or not isinstance(code, str)
                    or not re.fullmatch(r"\d{6}", code)
                    or code == "000000"
                ):
                    return False
        concepts = snapshot["stock_concepts"]
        if (
            not isinstance(concepts, dict)
            or set(concepts) != {"names", "map"}
            or not isinstance(concepts["names"], list)
            or not isinstance(concepts["map"], dict)
        ):
            return False
        concept_names = {normalize_block_identity(i) for i in concepts["names"]}
        if not concept_names <= identities:
            return False
        for name, blocks in concepts["map"].items():
            if (
                not isinstance(name, str)
                or not isinstance(blocks, list)
                or not {normalize_block_identity(i) for i in blocks} <= concept_names
            ):
                return False
        return concepts == _stock_payload(records, names)
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return False


def block_catalog_cache_path(cache_dir):
    """输出统一图缓存路径；沿用catalog_cache配置的目录。"""
    return Path(cache_dir) / BLOCK_CATALOG_CACHE_FILENAME


def block_catalog_snapshot_matches(
    snapshot, *, now=None, name_scope="", source_fetched_at=None, source_hashes=None
):
    """轻量检查生命周期与来源；映射不能从建图时另算6小时而延长旧原文寿命。"""
    current = time.time() if now is None else float(now)
    try:
        if (
            snapshot.get("version") != BLOCK_CATALOG_CACHE_VERSION
            or snapshot.get("kind") != "block_catalog"
        ):
            return False
        age = current - float(snapshot["source_fetched_at"])
        if (
            not math.isfinite(current)
            or not math.isfinite(age)
            or not 0 <= age < BLOCK_FILE_TTL_SECONDS
            or float(snapshot["fetched_at"]) > current
            or snapshot["name_scope"] != name_scope
        ):
            return False
        if (
            source_fetched_at is not None
            and snapshot["source_fetched_at"] != source_fetched_at
        ):
            return False
        if source_hashes is not None and snapshot["source_hashes"] != source_hashes:
            return False
        return True
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def load_block_catalog_snapshot(
    cache_dir, *, now=None, name_scope="", source_fetched_at=None, source_hashes=None
):
    """返回仍在来源6小时期限内、且来源快照一致的完整图。"""
    path = block_catalog_cache_path(cache_dir)
    current = time.time() if now is None else float(now)
    try:
        with path.open("rb") as handle:
            snapshot = pickle.load(handle)
        if not block_catalog_snapshot_matches(
            snapshot,
            now=current,
            name_scope=name_scope,
            source_fetched_at=source_fetched_at,
            source_hashes=source_hashes,
        ) or not validate_block_catalog_snapshot(snapshot):
            return None
        return snapshot
    except Exception:
        return None


def save_block_catalog_snapshot(snapshot, cache_dir):
    """校验后原子替换，绝不把空结果或损坏图写为有效缓存。"""
    if not validate_block_catalog_snapshot(snapshot):
        raise ValueError("板块映射缓存结构无效")
    _atomic_write_bytes(
        block_catalog_cache_path(cache_dir),
        pickle.dumps(snapshot, protocol=pickle.HIGHEST_PROTOCOL),
    )
