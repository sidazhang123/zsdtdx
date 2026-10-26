"""
模块：`parser/infoharbor_block.py`。

职责：
1. 解析 `infoharbor_block.dat` 正文，得到概念、风格、指数板块与成分代码。
2. 解析 `tdxhy.cfg` 与 `tdxzs.cfg`，得到通达信基础行业板块与成分代码。
3. 解析 `zhb.zip` 的 `spblock.dat`，补进有股票成分的板块。
4. 解析 `tdxzs3.cfg` 的研究行业中类，并按板块代码或名称与已有板块合并。
5. 同一板块代码保留目录全称并合并成分；同一板块名再取并集。
6. 用股票代码名称表收成 `{names, map}`。

边界：
1. 不访问网络，不读写缓存，不生成 pkl 文件。
2. 成分只认市场号 0/1/2 与 6 位代码；没有股票名称字段。
3. 基础行业只收 `tdxzs.cfg` 里行业码为 `T` 加数字、且分类字段为 2 的行。
4. `spblock.dat` 只保留当日码表有名称的股票；基金、债券段不进入结果。
5. 研究行业只生成五位 X 码对应的中类。
"""

from __future__ import annotations

import io
import re
import zipfile
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

from zsdtdx.params import TDXParams

# infoharbor 成分行里的市场号，与标准行情码表前缀一致。
_MARKET_PREFIX = {0: "sz", 1: "sh", 2: "bj"}
# tdxzs.cfg 分类字段：2 为通达信基础行业（钢铁、煤炭、水泥等）。
_TDX_INDUSTRY_KIND = "2"
_TDX_INDUSTRY_CODE = re.compile(r"^T\d+$")
# tdxzs3.cfg 分类 12、行业码 5 位，是研究行业中类。
_RESEARCH_KIND = "12"
_RESEARCH_CODE_LEN = 5
# 板块指数代码。目录全称按这个代码对齐，不按简称对齐。
_BLOCK_CODE = re.compile(r"^88\d{4}$")


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
    1. `[{"name": str, "kind": str, "block_code": str, "codes": [(market, code), ...]}]`。
    用途：
    1. 抽出概念、风格、指数板块名称、板块代码与成分代码。
    边界条件：
    1. 空原文返回空列表。
    2. 同一板块内重复代码只保留第一次。
    3. 非 `市场#6位数字`、全零代码不收录。
    4. 无成分的板块仍保留，名称为空的标题丢弃。
    5. 板块代码只收 `88` 加四位数字，缺失时为空串。
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
            fields = rest.split(",")
            name = fields[0].strip()
            if not name:
                current = None
                continue
            block_code = fields[2].strip() if len(fields) > 2 else ""
            if _BLOCK_CODE.fullmatch(block_code) is None:
                block_code = ""
            current = {
                "name": name,
                "kind": kind.strip(),
                "block_code": block_code,
                "codes": [],
            }
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
    1. 与 `parse_infoharbor_block` 相同结构，`kind` 为 `HY`，`block_code` 为行业行上的 `88` 代码。
    用途：
    1. 把通达信基础行业补进板块成分。
    边界条件：
    1. 地区板块、概念板块不在这里生成。
    2. 只使用股票行上的那个行业码，不把上级行业名写入结果。
    3. 行业码对不上名称的股票丢弃。没有成分的行业不返回。
    """
    industry_name: Dict[str, str] = {}
    name_block_code: Dict[str, str] = {}
    for line in _decode_gbk(zs_raw).splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 6 or parts[2] != _TDX_INDUSTRY_KIND:
            continue
        code = parts[5]
        name = parts[0]
        if not name or _TDX_INDUSTRY_CODE.fullmatch(code) is None:
            continue
        industry_name[code] = name
        block_code = parts[1] if _BLOCK_CODE.fullmatch(parts[1] or "") else ""
        name_block_code[name] = block_code

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
        {
            "name": name,
            "kind": "HY",
            "block_code": name_block_code.get(name, ""),
            "codes": codes,
        }
        for name, codes in buckets.items()
        if codes
    ]


def parse_research_industry_blocks(hy_raw: bytes, zs3_raw: bytes) -> List[dict]:
    """
    输入行业归属与研究行业目录，输出研究行业中类板块。

    输入：
    1. hy_raw: `tdxhy.cfg`。第六字段是 X 研究行业码。
    2. zs3_raw: `tdxzs3.cfg`。中类行 `名称|板块代码|12|...|五位X码`。
    输出：
    1. 与 infoharbor 相同结构，`kind` 为 `RS`。
    用途：
    1. 把研究行业中类成分交给统一合并。
    边界条件：
    1. 大类、小类不生成板块。股票的长码按前五位归入中类。
    2. 对不上中类名称的 X 码丢弃。没有成分的中类不返回。
    3. 与基础行业同名时不在这里取舍，由后续按名称并集。
    """
    mids: Dict[str, Tuple[str, str]] = {}
    for line in _decode_gbk(zs3_raw).splitlines():
        parts = [part.strip() for part in line.split("|")]
        if (
            len(parts) < 6
            or parts[2] != _RESEARCH_KIND
            or len(parts[5]) != _RESEARCH_CODE_LEN
        ):
            continue
        name = parts[0]
        if not name:
            continue
        block_code = parts[1] if _BLOCK_CODE.fullmatch(parts[1] or "") else ""
        mids[parts[5]] = (name, block_code)
    buckets: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    seen: Dict[str, set] = defaultdict(set)
    for line in _decode_gbk(hy_raw).splitlines():
        parts = [part.strip() for part in line.split("|")]
        if (
            len(parts) < 6
            or not parts[0].isdigit()
            or len(parts[5]) < _RESEARCH_CODE_LEN
        ):
            continue
        market_id = int(parts[0])
        code = parts[1]
        mid = parts[5][:_RESEARCH_CODE_LEN]
        if (
            mid not in mids
            or market_id not in _MARKET_PREFIX
            or len(code) != 6
            or not code.isdigit()
            or code == "000000"
            or code in seen[mid]
        ):
            continue
        seen[mid].add(code)
        buckets[mid].append((market_id, code))
    return [
        {
            "name": mids[mid][0],
            "kind": "RS",
            "block_code": mids[mid][1],
            "codes": codes,
        }
        for mid, codes in buckets.items()
        if codes
    ]


def parse_spblock(raw: bytes) -> List[dict]:
    """
    输入 `spblock.dat` 原文，输出补充成分段。

    输入：
    1. raw: GBK 文本。`#名称` 下每行是市场号加六位代码。
    输出：
    1. 与 infoharbor 相同结构，`kind` 为 `SP`，`block_code` 先为空。
    用途：
    1. 读出 infoharbor 没有列出的股票成分。
    边界条件：
    1. 空原文或没有井号标题时返回空列表。
    2. 不是 `0/1/2` 加六位数字的行丢弃，全零代码丢弃。
    3. 这里不判断代码是否为股票。
    """
    text = _decode_gbk(raw)
    if not text:
        return []
    blocks: List[dict] = []
    current: dict | None = None
    seen: set = set()
    for line in text.splitlines():
        line = line.strip().replace("\x00", "")
        if not line:
            continue
        if line.startswith("#"):
            name = line[1:].split(",", 1)[0].strip()
            current = None
            seen = set()
            if not name:
                continue
            current = {"name": name, "kind": "SP", "block_code": "", "codes": []}
            blocks.append(current)
            continue
        if current is None:
            continue
        token = line.split(",")[0].strip()
        if len(token) != 7 or token[0] not in "012" or not token[1:].isdigit():
            continue
        pair = (int(token[0]), token[1:])
        if pair[1] == "000000" or pair in seen:
            continue
        seen.add(pair)
        current["codes"].append(pair)
    return blocks


def catalog_full_names(*raws: bytes) -> Dict[str, str]:
    """
    输入板块目录原文，输出板块代码到目录名称。

    输入：
    1. raws: `tdxzs.cfg`、`tdxzs3.cfg` 的 GBK 字节，可缺省。
    输出：
    1. `88xxxx -> 名称`。同一代码只保留第一次出现的名称。
    用途：
    1. 把 infoharbor 简称换成目录全称。
    边界条件：
    1. 代码不是 `88` 加四位数字、或名称为空的行丢弃。
    2. 同一代码若出现不同名称，保留先读到的名称。
    """
    names: Dict[str, str] = {}
    for raw in raws:
        for line in _decode_gbk(raw).splitlines():
            parts = [part.strip() for part in line.split("|")]
            if len(parts) < 2 or not parts[0]:
                continue
            code = parts[1]
            if _BLOCK_CODE.fullmatch(code) is None or code in names:
                continue
            names[code] = parts[0]
    return names


def _union_codes(left: List[Tuple[int, str]], right: Sequence[Tuple[int, str]]) -> None:
    """输入两个成分列表，把右侧尚未出现的市场和代码追加到左侧；不改右侧。"""
    seen = set(left)
    for market, code in right:
        pair = (int(market), str(code))
        if pair in seen:
            continue
        seen.add(pair)
        left.append(pair)


def merge_concept_blocks(
    blocks: Sequence[dict],
    code_to_name: Dict[str, str],
) -> List[dict]:
    """
    输入多来源板块和目录全称，输出合并后的板块。

    输入：
    1. blocks: 含 `name`、`codes`，可选 `block_code`、`kind`。
    2. code_to_name: 板块代码到目录全称。
    输出：
    1. 每个最终名称一块。有板块代码时名称用目录全称，成分取并集。
    用途：
    1. 同代码或同名不保留两套成分。
    边界条件：
    1. 没有目录全称时沿用成分文件里的名称。
    2. 没有成分的板块不返回。
    3. 无代码的补充段，仅当目录里该名称只对应一个板块代码时才挂上代码。
    """
    name_to_codes: Dict[str, List[str]] = defaultdict(list)
    for code, name in code_to_name.items():
        name_to_codes[name].append(code)
    by_code: Dict[str, dict] = {}
    loose: List[dict] = []
    for block in blocks:
        raw_name = str(block.get("name") or "").strip()
        if not raw_name:
            continue
        block_code = str(block.get("block_code") or "").strip()
        if (
            not block_code
            and str(block.get("kind") or "") == "SP"
            and len(name_to_codes.get(raw_name, [])) == 1
        ):
            block_code = name_to_codes[raw_name][0]
        members = [
            (int(market), str(code)) for market, code in (block.get("codes") or [])
        ]
        if block_code:
            slot = by_code.setdefault(
                block_code,
                {
                    "name": code_to_name.get(block_code) or raw_name,
                    "kind": str(block.get("kind") or ""),
                    "kinds": set(),
                    "block_code": block_code,
                    "codes": [],
                },
            )
            if code_to_name.get(block_code):
                slot["name"] = code_to_name[block_code]
            kind = str(block.get("kind") or "")
            if kind:
                slot["kinds"].add(kind)
            _union_codes(slot["codes"], members)
            continue
        loose_slot = {
            "name": raw_name,
            "kind": str(block.get("kind") or ""),
            "kinds": {str(block.get("kind") or "")} - {""},
            "block_code": "",
            "codes": [],
        }
        _union_codes(loose_slot["codes"], members)
        loose.append(loose_slot)
    by_name: Dict[str, dict] = {}
    for slot in list(by_code.values()) + loose:
        name = str(slot.get("name") or "").strip()
        if not name:
            continue
        dest = by_name.setdefault(
            name,
            {
                "name": name,
                "kind": slot.get("kind") or "",
                "kinds": set(),
                "block_code": slot.get("block_code") or "",
                "codes": [],
            },
        )
        dest["kinds"].update(slot.get("kinds") or [])
        _union_codes(dest["codes"], slot.get("codes") or [])
    return [item for item in by_name.values() if item["codes"]]


def _named_stock_pairs(block: dict, code_name: Dict[str, str]) -> frozenset:
    """输入板块和码表，输出有股票名称的成分身份；无名称的代码不参与相同成分判断。"""
    found = []
    for market, code in block.get("codes") or []:
        if str(code_name.get(_prefixed_stock_code(market, code)) or "").strip():
            found.append((int(market), str(code)))
    return frozenset(found)


def collapse_identical_stock_sets(
    blocks: Sequence[dict],
    code_name: Dict[str, str],
) -> List[dict]:
    """
    输入已按代码和名称合并的板块，输出成分相同者只留一个名称。

    输入：
    1. blocks: 每块含 `name` 与 `codes`。
    2. code_name: `前缀代码 -> 股票名称`。
    输出：
    1. 同一组有名称的股票只保留一个板块。
    用途：
    1. 去掉 a、c 都映射到同一组成分股 b 的重复板块。
    边界条件：
    1. 没有可发布股票的板块互不合并。
    2. 保留名称最长的一个。长度相同则保留字典序较后的名称。
    3. 被去掉的名称不再出现。
    """
    grouped: Dict[frozenset, List[dict]] = defaultdict(list)
    kept: List[dict] = []
    for block in blocks:
        stocks = _named_stock_pairs(block, code_name)
        if not stocks:
            kept.append(block)
            continue
        grouped[stocks].append(block)
    for items in grouped.values():
        winner = max(
            items,
            key=lambda item: (
                len(str(item.get("name") or "")),
                str(item.get("name") or ""),
            ),
        )
        chosen = dict(winner)
        kinds = set(chosen.get("kinds") or [])
        for item in items:
            kinds.update(item.get("kinds") or [])
            kind = str(item.get("kind") or "")
            if kind:
                kinds.add(kind)
        chosen["kinds"] = kinds
        kept.append(chosen)
    return kept


def prepare_concept_blocks(
    block_raw: bytes,
    hy_raw: bytes,
    zip_raw: bytes,
    code_name: Dict[str, str],
) -> List[dict]:
    """
    输入三份命名文件和码表，输出合并并去重后的板块。

    输入：
    1. block_raw、hy_raw、zip_raw: 概念文件、行业归属和 zhb.zip。
    2. code_name: 用于识别可发布股票，以及 spblock 是否有股票成分。
    输出：
    1. 板块列表。同一代码、同一名称、同一组成分股都只剩一个名称。
    用途：
    1. 概念函数和对照脚本共用这一条组装路径。
    边界条件：
    1. zip 缺少成员时跳过该来源。
    2. spblock 没有可发布股票的段不进入。
    """
    zs_raw = read_zip_entry(zip_raw, TDXParams.TDXZS_ZIP_MEMBER)
    zs3_raw = read_zip_entry(zip_raw, TDXParams.TDXZS3_ZIP_MEMBER)
    full_names = catalog_full_names(zs_raw, zs3_raw)
    blocks = parse_infoharbor_block(block_raw)
    blocks.extend(parse_tdx_industry_blocks(hy_raw, zs_raw))
    blocks.extend(parse_research_industry_blocks(hy_raw, zs3_raw))
    for block in parse_spblock(read_zip_entry(zip_raw, TDXParams.SPBLOCK_ZIP_MEMBER)):
        if not _named_stock_pairs(block, code_name):
            continue
        blocks.append(block)
    merged = merge_concept_blocks(blocks, full_names)
    return collapse_identical_stock_sets(merged, code_name)


def build_stock_concepts_from_named_files(
    block_raw: bytes,
    hy_raw: bytes,
    zip_raw: bytes,
    code_name: Dict[str, str],
) -> Dict[str, object]:
    """
    输入三份命名文件和股票码表，输出 `{names, map}`。

    输入：
    1. block_raw: `infoharbor_block.dat`。
    2. hy_raw: `tdxhy.cfg`。
    3. zip_raw: `zhb.zip`，需能读出行业目录和 `spblock.dat`。
    4. code_name: `前缀代码 -> 股票名称`。
    输出：
    1. 与 `build_stock_concept_payload` 相同。
    用途：
    1. 概念、风格、指数、基础行业、研究行业中类，以及 `spblock.dat` 里有股票的板块，合成一份归属。
    边界条件：
    1. zip 缺少成员时该来源为空，其它来源仍参与。
    2. `spblock.dat` 中码表没有名称的代码整段不纳入；有名称的代码才合并。
    3. 同板块代码使用目录全称。同名板块只保留一个名称，成分取并集。
    4. 研究行业只收五位 X 码对应的中类。
    5. 可发布股票集合相同的板块只保留名称最长的一个。
    """
    return build_stock_concept_payload(
        prepare_concept_blocks(block_raw, hy_raw, zip_raw, code_name),
        code_name,
    )


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
