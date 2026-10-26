# -*- coding: utf-8 -*-
"""
模块：`test_infoharbor_block.py`。

职责：
1. 离线验收 infoharbor 板块正文与通达信基础行业解析。
2. 验收 `{names, map}` 与代码名称表的拼接规则。

边界：
1. 不访问网络，不写 pkl。
"""

import io
import zipfile

from zsdtdx.params import TDXParams
from zsdtdx.parser.infoharbor_block import (
    build_stock_concept_payload,
    build_stock_concepts_from_named_files,
    merge_concept_blocks,
    parse_infoharbor_block,
    parse_research_industry_blocks,
    parse_spblock,
    parse_tdx_industry_blocks,
    read_zip_entry,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient


def _sample_raw() -> bytes:
    """输入：无。输出：一小段 GBK 板块正文。用途：离线解析。边界：含空板块与重复代码。"""
    text = (
        "#GN_含可转债,0,880524,20050610,20260921,,\n"
        "#GN_芯片,3,880880,20050607,20260922,,\n"
        "0#000001,0#000001,1#600000,9#000002,\n"
        "#FG_融资融券,1,880001,20050607,20260922,,\n"
        "0#000001,\n"
        "#ZS_沪深300,0,880300,20050607,20260922,,\n"
    )
    return text.encode("gbk")


def _industry_files() -> tuple[bytes, bytes]:
    """输入：无。输出：行业归属与名称表。用途：离线拼接。边界：含地区行，不应进入行业板块。"""
    hy = (
        "1|605007|T0207|||\n0|000001|T0101|||\n1|600000|T010101|||\n0|000002|T9999|||\n"
    )
    zs = (
        "造纸|880350|2|1|1|T0207\n"
        "煤炭|880301|2|1|0|T0101\n"
        "煤炭开采|880302|2|1|1|T010101\n"
        "浙江板块|880228|3|1|0|28\n"
    )
    return hy.encode("gbk"), zs.encode("gbk")


def _zhb_zip(zs_raw: bytes) -> bytes:
    """输入：tdxzs 原文。输出：只含该成员的 zip。用途：离线解包。边界：不写磁盘。"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(TDXParams.TDXZS_ZIP_MEMBER, zs_raw)
    return buf.getvalue()


def test_parse_keeps_named_blocks_and_valid_codes():
    """输入样例正文。输出板块与代码。用途：确认代码去重与非法代码丢弃。边界：空板块保留。"""
    blocks = parse_infoharbor_block(_sample_raw())
    assert [item["name"] for item in blocks] == [
        "含可转债",
        "芯片",
        "融资融券",
        "沪深300",
    ]
    chip = blocks[1]
    assert chip["kind"] == "GN"
    assert chip["block_code"] == "880880"
    assert chip["codes"] == [(0, "000001"), (1, "600000")]
    assert parse_infoharbor_block(b"") == []


def test_payload_matches_concept_pkl_shape():
    """输入板块与码表。输出 names/map。用途：同名股票合并、无名称代码丢弃。边界：空板块不进 names。"""
    blocks = parse_infoharbor_block(_sample_raw())
    code_name = {"sz.000001": "平安银行", "sh.600000": "浦发银行"}
    payload = build_stock_concept_payload(blocks, code_name)
    assert payload["names"] == ["芯片", "融资融券"]
    assert payload["map"] == {
        "平安银行": ["芯片", "融资融券"],
        "浦发银行": ["芯片"],
    }


def test_same_stock_name_unions_blocks():
    """输入两个代码同一名称。输出并集。用途：同名合并。边界：板块名仍只出现一次。"""
    blocks = [
        {"name": "锂矿", "kind": "GN", "codes": [(0, "000001")]},
        {"name": "黄金概念", "kind": "GN", "codes": [(1, "600000")]},
    ]
    payload = build_stock_concept_payload(
        blocks, {"sz.000001": "藏格矿业", "sh.600000": "藏格矿业"}
    )
    assert payload["map"]["藏格矿业"] == ["锂矿", "黄金概念"]


def test_industry_blocks_join_t_code_and_skip_region():
    """输入行业两表。输出钢铁类板块。用途：T 码对上名称。边界：地区板块不进入。"""
    hy_raw, zs_raw = _industry_files()
    blocks = parse_tdx_industry_blocks(hy_raw, zs_raw)
    by_name = {item["name"]: item["codes"] for item in blocks}
    assert set(by_name) == {"造纸", "煤炭", "煤炭开采"}
    assert by_name["造纸"] == [(1, "605007")]
    assert by_name["煤炭"] == [(0, "000001")]
    assert by_name["煤炭开采"] == [(1, "600000")]
    assert blocks[0]["block_code"] == "880350"
    assert read_zip_entry(_zhb_zip(zs_raw), TDXParams.TDXZS_ZIP_MEMBER) == zs_raw
    assert read_zip_entry(b"not-zip", TDXParams.TDXZS_ZIP_MEMBER) == b""


def test_get_stock_concepts_merges_industry():
    """输入 mock 下载。输出概念与行业并集。用途：确认三份远程文件都参与。边界：无网络。成分相同的较短名称不保留。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    seen = []
    hy_raw, zs_raw = _industry_files()

    def _download(filename):
        seen.append(filename)
        if filename == TDXParams.INFOHARBOR_BLOCK_REMOTE_FILE:
            return _sample_raw()
        if filename == TDXParams.TDXHY_REMOTE_FILE:
            return hy_raw
        if filename == TDXParams.ZHB_ZIP_REMOTE_FILE:
            return _zhb_zip(zs_raw)
        return b""

    client.get_stock_code_name_map = lambda: {
        "sz.000001": "平安银行",
        "sh.600000": "浦发银行",
        "sh.605007": "五洲特纸",
    }
    client._download_named_hq_file = _download
    payload = client.get_stock_concepts()
    assert seen == [
        TDXParams.INFOHARBOR_BLOCK_REMOTE_FILE,
        TDXParams.TDXHY_REMOTE_FILE,
        TDXParams.ZHB_ZIP_REMOTE_FILE,
    ]
    assert payload["map"]["平安银行"] == ["芯片", "融资融券"]
    assert payload["map"]["浦发银行"] == ["煤炭开采", "芯片"]
    assert payload["map"]["五洲特纸"] == ["造纸"]
    assert "浙江板块" not in payload["names"]


def test_catalog_full_name_and_spblock_stock_members():
    """输入简称、目录全称和 spblock。输出全称、并集，以及有股票的新板块。边界：纯基金段不进入。"""
    info = (
        "#GN_智能机器,1,880904,20050607,20260922,,\n"
        "0#000001,\n"
        "#GN_含可转债,0,880524,20050610,20260921,,\n"
        "#FG_融资融券,1,880001,20050607,20260922,,\n"
        "0#000002,\n"
    ).encode("gbk")
    zs = "机器人概念|880904|4|1|0|智能机器\n含可转债|880524|4|1|0|含可转债\n"
    sp = "#含可转债\n0000001\n1600000\n#中证500\n0000001\n#深债ETF\n0512345\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(TDXParams.TDXZS_ZIP_MEMBER, zs.encode("gbk"))
        archive.writestr(TDXParams.SPBLOCK_ZIP_MEMBER, sp.encode("gbk"))
    payload = build_stock_concepts_from_named_files(
        info,
        b"",
        buf.getvalue(),
        {"sz.000001": "平安银行", "sz.000002": "万科Ａ", "sh.600000": "浦发银行"},
    )
    assert "智能机器" not in payload["names"]
    assert "深债ETF" not in payload["names"]
    assert payload["map"]["平安银行"] == ["含可转债", "机器人概念"]
    assert payload["map"]["浦发银行"] == ["含可转债"]
    assert payload["map"]["万科Ａ"] == ["融资融券"]
    assert parse_spblock(b"") == []


def test_same_name_unions_after_code_merge():
    """输入同名的有代码板块和无代码板块。输出一个名称和成分并集。边界：重名目录不猜测代码。"""
    merged = merge_concept_blocks(
        [
            {
                "name": "甲",
                "kind": "HY",
                "block_code": "880001",
                "codes": [(0, "000001")],
            },
            {"name": "甲", "kind": "SP", "block_code": "", "codes": [(1, "600000")]},
        ],
        {"880001": "甲", "881001": "甲"},
    )
    assert len(merged) == 1
    assert merged[0]["name"] == "甲"
    assert merged[0]["codes"] == [(0, "000001"), (1, "600000")]


def test_research_industry_unions_same_name():
    """输入基础行业与研究行业中类。输出同名并集，大类不进入。边界：小类股票归入前五位中类。"""
    hy = "0|000001|T1001|||X400401\n1|600000|T2001|||X5001\n".encode("gbk")
    zs = "元器件|880492|2|1|0|T1001\n"
    zs3 = (
        "元器件|881333|12|1|0|X4004\n"
        "全国性银行|881001|12|1|0|X5001\n"
        "能源|881000|12|1|0|X50\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(TDXParams.TDXZS_ZIP_MEMBER, zs.encode("gbk"))
        archive.writestr(TDXParams.TDXZS3_ZIP_MEMBER, zs3.encode("gbk"))
    payload = build_stock_concepts_from_named_files(
        b"",
        hy,
        buf.getvalue(),
        {"sz.000001": "平安银行", "sh.600000": "浦发银行"},
    )
    assert payload["names"] == ["元器件", "全国性银行"]
    assert payload["map"]["平安银行"] == ["元器件"]
    assert payload["map"]["浦发银行"] == ["全国性银行"]
    assert "能源" not in payload["names"]
    assert parse_research_industry_blocks(b"", b"") == []


def test_identical_constituents_keep_longest_name():
    """输入两个成分相同的板块。输出只留较长名称。边界：较短名称不再出现。"""
    info = (
        "#GN_中药,2,,\n"
        "0#000001,1#600000,\n"
        "#GN_中成药,2,,\n"
        "0#000001,1#600000,\n"
    ).encode("gbk")
    payload = build_stock_concepts_from_named_files(
        info,
        b"",
        b"",
        {"sz.000001": "平安银行", "sh.600000": "浦发银行"},
    )
    assert payload["names"] == ["中成药"]
    assert payload["map"]["平安银行"] == ["中成药"]
    assert payload["map"]["浦发银行"] == ["中成药"]


def test_get_stock_concepts_empty_download():
    """输入空下载。输出空结构。用途：失败时保持字段。边界：不抛异常。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.get_stock_code_name_map = lambda: {"sz.000001": "平安银行"}
    client._download_named_hq_file = lambda filename: b""
    assert client.get_stock_concepts() == {"names": [], "map": {}}
