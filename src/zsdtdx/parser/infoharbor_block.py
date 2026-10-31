"""
模块：`parser/infoharbor_block.py`。

职责：
1. 解析 `infoharbor_block.dat` 正文，得到概念、风格、指数板块与成分代码。
2. 解析 `tdxhy.cfg` 与 `tdxzs.cfg`，得到通达信基础行业板块与成分代码。
3. 解析 `tdxzs3.cfg` 的研究行业中类和小类。小类名称存在时归入小类，否则归入上一级中类。
4. 不解析 `spblock.dat`，不产生补充成分。
5. 原文板块名与不同的目录全称都保留；成分不同的同名板块不合并成分。
6. 用股票代码名称表收成 `{names, map}`，板块身份为（中文分类名称，板块名称）。

边界：
1. 不访问网络，不读写缓存，不生成 pkl 文件。
2. 成分只认市场号 0/1/2 与 6 位代码；没有股票名称字段。
3. 基础行业只收 `tdxzs.cfg` 里行业码为 `T` 加数字、且分类字段为 2 的行。
4. 不解析补充成分。
5. 研究行业中类只收五位 X 码，小类只收七位 X 码；大类不生成板块。小类码没有名称时，股票归入上一级中类。
"""

from __future__ import annotations

import io
import re
import zipfile
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

from zsdtdx.params import TDXParams
from zsdtdx.block_identity import BLOCK_KIND_NAMES, BlockIdentity, block_identity

# infoharbor 成分行里的市场号，与标准行情码表前缀一致。
_MARKET_PREFIX = {0: "sz", 1: "sh", 2: "bj"}
# tdxzs.cfg 分类字段：2 为通达信基础行业（钢铁、煤炭、水泥等）。
_TDX_INDUSTRY_KIND = "2"
_TDX_INDUSTRY_CODE = re.compile(r"^T\d+$")
# tdxzs3.cfg 分类 12。五位 X 码是中类，七位 X 码是小类。
_RESEARCH_KIND = "12"
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
    输入行业归属与研究行业目录，输出中类和小类板块。

    输入：
    1. hy_raw: `tdxhy.cfg`。第六字段是 X 研究行业码。
    2. zs3_raw: `tdxzs3.cfg`。中类为五位 X 码，小类为七位 X 码。
    输出：
    1. 与 infoharbor 相同结构。中类 `kind` 为 `RS`，小类 `kind` 为 `RL`。
    用途：
    1. 小类目录有名称时，股票归入该小类；没有时归入前五位对应的中类。
    边界条件：
    1. 大类不生成板块。已有小类名称的股票不改挂中类。
    2. 前五位也没有中类名称时，该股票不归入研究行业。没有成分的层级不返回。
    3. 与基础行业同名仍为两个分类。
    """
    nodes: Dict[str, Tuple[str, str, str]] = {}
    for line in _decode_gbk(zs3_raw).splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 6 or parts[2] != _RESEARCH_KIND or not parts[0]:
            continue
        industry_code = parts[5]
        if re.fullmatch(r"X\d{4}", industry_code):
            kind = "RS"
        elif re.fullmatch(r"X\d{6}", industry_code):
            kind = "RL"
        else:
            continue
        block_code = parts[1] if _BLOCK_CODE.fullmatch(parts[1] or "") else ""
        nodes[industry_code] = (parts[0], block_code, kind)
    buckets: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    seen: Dict[str, set] = defaultdict(set)
    for line in _decode_gbk(hy_raw).splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 6 or not parts[0].isdigit():
            continue
        market_id = int(parts[0])
        code = parts[1]
        industry_code = parts[5]
        target = industry_code
        if target not in nodes and len(industry_code) == 7:
            target = industry_code[:5]
        if (
            target not in nodes
            or market_id not in _MARKET_PREFIX
            or len(code) != 6
            or not code.isdigit()
            or code == "000000"
            or code in seen[target]
        ):
            continue
        seen[target].add(code)
        buckets[target].append((market_id, code))
    return [
        {
            "name": nodes[industry_code][0],
            "kind": nodes[industry_code][2],
            "block_code": nodes[industry_code][1],
            "codes": codes,
        }
        for industry_code, codes in buckets.items()
        if codes
    ]


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


def prepare_concept_blocks_from_parsed(
    blocks: Sequence[dict], full_names: Dict[str, str]
) -> List[dict]:
    """输入原文顺序的板块及目录全称，输出未合并成分的归属块。

    输入：
    1. blocks: 各来源解析出的板块，含 name、kind、codes。
    2. full_names: 板块代码到目录名称。
    输出：
    1. 保留原文名称。目录全称与原文不同时再追加同成分的一条。
    用途：
    1. 股票归属按文件中的映射原样进入 `{names, map}`。
    边界条件：
    1. 未知分类跳过，补充成分不进入。没有成分的板块不进入归属。
    2. 不按板块代码或名称合并成分，也不按名称长短删板块。
    """
    prepared: List[dict] = []
    for block in blocks:
        if block.get("kind") not in BLOCK_KIND_NAMES:
            continue
        if not block.get("codes"):
            continue
        prepared.append(block)
        raw_name = str(block.get("name") or "").strip()
        block_code = str(block.get("block_code") or "").strip()
        full_name = str(full_names.get(block_code) or "").strip()
        if full_name and full_name != raw_name:
            alias = dict(block)
            alias["name"] = full_name
            prepared.append(alias)
    return prepared


def prepare_concept_blocks(
    block_raw: bytes,
    hy_raw: bytes,
    zip_raw: bytes,
) -> List[dict]:
    """
    输入三份命名文件，输出未合并成分的板块。

    输入：
    1. block_raw、hy_raw、zip_raw: 概念文件、行业归属和 zhb.zip。
    输出：
    1. 板块列表。相同成分的不同名称都保留，同名板块不合并彼此的成分。
    用途：
    1. 概念函数和对照脚本共用这一条组装路径。
    边界条件：
    1. zip 缺少成员时跳过该来源。
    2. 不解析补充成分。
    """
    zs_raw = read_zip_entry(zip_raw, TDXParams.TDXZS_ZIP_MEMBER)
    zs3_raw = read_zip_entry(zip_raw, TDXParams.TDXZS3_ZIP_MEMBER)
    full_names = catalog_full_names(zs_raw, zs3_raw)
    blocks = parse_infoharbor_block(block_raw)
    blocks.extend(parse_tdx_industry_blocks(hy_raw, zs_raw))
    blocks.extend(parse_research_industry_blocks(hy_raw, zs3_raw))
    return prepare_concept_blocks_from_parsed(blocks, full_names)


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
    3. zip_raw: `zhb.zip`，需能读出行业目录。不读取 `spblock.dat`。
    4. code_name: `前缀代码 -> 股票名称`。
    输出：
    1. 与 `build_stock_concept_payload` 相同。
    用途：
    1. 概念、风格、指数、基础行业、研究行业中类和小类合成一份归属。不含补充成分。
    边界条件：
    1. zip 缺少成员时该来源为空，其它来源仍参与。
    2. 不解析 `spblock.dat`。
    3. 原文名称保留；目录全称不同时另外保留一条相同成分。
    4. 研究行业小类有名称时归入小类；小类名称不存在时归入上一级中类。
    5. 成分相同的不同板块名都返回；板块名相同但成分不同时不合并成分。
    """
    return build_stock_concept_payload(
        prepare_concept_blocks(block_raw, hy_raw, zip_raw),
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
    1. names: 有有效成分代码的（中文分类名称，板块名称），去重后按字排序。
    2. map: 股票名称 -> 所属板块二元组列表；列表去重且按字排序。
    用途：
    1. 把「板块 -> 股票代码」收成「股票名称 -> 板块名」。
    边界条件：
    1. 码表中没有名称的代码丢弃，不因此删掉板块名。
    2. 多个代码对应同一股票名时，板块名取并集。
    3. 不写文件。
    """
    names: List[BlockIdentity] = []
    code_to_blocks: Dict[Tuple[int, str], List[BlockIdentity]] = {}
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
        identity = block_identity(str(block.get("kind") or ""), name)
        names.append(identity)
        for market_id, code_text in codes:
            code_to_blocks.setdefault((market_id, code_text), []).append(identity)

    name_map: Dict[str, List[BlockIdentity]] = {}
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
