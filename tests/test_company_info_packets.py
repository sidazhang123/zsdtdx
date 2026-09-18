"""离线验收公司信息目录与正文组包：分类下标、剩余 length、30720 分页。"""

from __future__ import annotations

import struct
from unittest.mock import MagicMock

from zsdtdx.parser.get_company_info_category import GetCompanyInfoCategory
from zsdtdx.parser.get_company_info_content import (
    COMPANY_INFO_CONTENT_PAGE_SIZE,
    GetCompanyInfoContent,
)
from zsdtdx.unified_client import UnifiedTdxClient


def _pack_category_body(rows: list[tuple[str, str, int, int]]) -> bytes:
    """
    输入：分类元组列表 (name, filename, start, length)。
    输出：目录回包正文。
    用途：构造与服务端相同的 152 字节记录。
    边界：名称按 GBK、文件名按 ASCII 写入并 0 填充。
    """
    body = struct.pack("<H", len(rows))
    for name, filename, start, length in rows:
        body += struct.pack(
            "<64s80sII",
            name.encode("gbk"),
            filename.encode("ascii"),
            start,
            length,
        )
    return body


def test_category_parse_assigns_index_and_vxx_filename():
    """输入：16 条 .Vxx 目录；输出：下标 0..15、文件名与 length 保持原值。"""
    rows = [
        ("最新提示", "300063.V11", 0, 15969),
        ("公司概况", "300063.V04", 0, 16508),
        ("财务分析", "300063.V02", 0, 40329),
        ("股东研究", "300063.V10", 0, 29160),
        ("股本结构", "300063.V03", 0, 29365),
        ("资本运作", "300063.V05", 0, 58913),
        ("业内点评", "300063.V07", 0, 46254),
        ("行业分析", "300063.V09", 0, 42507),
        ("公司大事", "300063.V14", 0, 361861),
        ("研究报告", "300063.V12", 0, 76342),
        ("经营分析", "300063.V08", 0, 23321),
        ("主力追踪", "300063.V01", 0, 18605),
        ("分红扩股", "300063.V13", 0, 42547),
        ("高层治理", "300063.V17", 0, 18649),
        ("龙虎榜单", "300063.V06", 0, 41446),
        ("关联个股", "300063.V16", 0, 12041),
    ]
    parser = GetCompanyInfoCategory(client=None)
    parsed = parser.parseResponse(_pack_category_body(rows))
    assert len(parsed) == 16
    assert parsed[0]["index"] == 0
    assert parsed[0]["name"] == "最新提示"
    assert parsed[0]["filename"] == "300063.V11"
    assert parsed[8]["index"] == 8
    assert parsed[8]["name"] == "公司大事"
    assert parsed[8]["filename"] == "300063.V14"
    assert parsed[8]["length"] == 361861
    assert parsed[8]["start"] == 0


def test_content_pack_uses_category_index_and_remaining_length():
    """输入：V14 第二页 start=30720、剩余 331141、index=8；输出：114 字节且字段对齐抓包。"""
    parser = GetCompanyInfoContent(client=None)
    parser.setParams(0, "300063", "300063.V14", 30720, 331141, 8)
    pkg = bytes(parser.send_pkg)
    assert len(pkg) == 114
    assert pkg[:12] == bytes.fromhex("0c 07 10 9c 00 01 68 00 68 00 d0 02")
    market, code, unk, filename, start, length, tail = struct.unpack_from(
        "<H6sH80sIII", pkg, 12
    )
    assert market == 0
    assert code.split(b"\x00", 1)[0] == b"300063"
    assert unk == 8
    assert filename.split(b"\x00", 1)[0] == b"300063.V14"
    assert start == 30720
    assert length == 331141
    assert tail == 0
    assert COMPANY_INFO_CONTENT_PAGE_SIZE == 30720


def test_content_parse_uses_uint16_chunk_length():
    """输入：10 字节前缀 + uint16 + GBK；输出：按本页字节数解码，不把剩余总长当本页长度。"""
    text = "☆公司大事☆"
    encoded = text.encode("gbk")
    body = b"\x00\x00300063\x00\x00" + struct.pack("<H", len(encoded)) + encoded
    parser = GetCompanyInfoContent(client=None)
    assert parser.parseResponse(body) == text


def test_fetch_company_content_pages_like_official_client():
    """输入：length=361861、index=8；输出：12 次请求，start 步进 30720，length 为剩余。"""
    calls: list[tuple] = []

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：连接池方法名与参数。
        输出：非空占位正文。
        用途：记录分页参数。
        边界：不访问网络。
        """
        calls.append((method_name, args))
        return "x"

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"company_info_chunk_size": 30720}
    client.std_pool = MagicMock()
    client.std_pool.call.side_effect = fake_call
    text, status = UnifiedTdxClient._fetch_company_content(
        client,
        market=0,
        code="300063",
        filename="300063.V14",
        start=0,
        length=361861,
        category_index=8,
    )
    assert status == "success"
    assert text == "x" * 12
    assert len(calls) == 12
    starts = [item[1][3] for item in calls]
    lengths = [item[1][4] for item in calls]
    indexes = [item[1][5] for item in calls]
    assert starts == [30720 * i for i in range(12)]
    assert lengths[0] == 361861
    assert lengths[1] == 331141
    assert lengths[-1] == 361861 - 30720 * 11
    assert indexes == [8] * 12
    assert all(item[0] == "get_company_info_content" for item in calls)
    assert all(item[1][2] == "300063.V14" for item in calls)


def test_company_info_protocol_market_beijing_uses_zero():
    """输入：北交所路由 market=2；输出：F10 协议 market=0；深沪仍沿用路由 market。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.market_rules = {"include_beijing_prefixes": ["92"]}
    assert (
        UnifiedTdxClient._company_info_protocol_market(
            client,
            {
                "code": "920002",
                "market": 2,
                "source": "std",
            },
        )
        == 0
    )
    assert (
        UnifiedTdxClient._company_info_protocol_market(
            client,
            {"code": "000001", "market": 0, "source": "std"},
        )
        == 0
    )
    assert (
        UnifiedTdxClient._company_info_protocol_market(
            client,
            {"code": "600000", "market": 1, "source": "std"},
        )
        == 1
    )


def test_get_company_info_content_beijing_queries_market_zero():
    """输入：920002；输出：目录与正文请求均使用 market=0，不再用行情路由 market=2。"""
    calls: list[tuple] = []

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：连接池方法名与参数。
        输出：目录一条或占位正文。
        用途：记录 F10 实际请求 market。
        边界：不访问网络。
        """
        calls.append((method_name, args))
        if method_name == "get_company_info_category":
            return [
                {
                    "index": 0,
                    "name": "公司概况",
                    "filename": "920002.V04",
                    "start": 0,
                    "length": 100,
                }
            ]
        return "正文"

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"company_info_chunk_size": 30720}
    client.market_rules = {"include_beijing_prefixes": ["92"]}
    client._stock_route = {
        "920002": {
            "code": "920002",
            "name": "万达轴承",
            "market": 2,
            "market_name": "北京",
            "source": "std",
            "asset_type": "stock",
        }
    }
    client._runtime_failures = []
    client.output = {"return_df_default": False}
    client.std_pool = MagicMock()
    client.std_pool.call.side_effect = fake_call

    rows = UnifiedTdxClient.get_company_info_content(
        client, code="920002", return_df=False
    )
    assert len(rows) == 1
    assert rows[0]["category"] == "公司概况"
    assert rows[0]["content"] == "正文"
    assert calls[0][0] == "get_company_info_category"
    assert calls[0][1][0] == 0
    assert calls[0][1][1] == "920002"
    content_calls = [c for c in calls if c[0] == "get_company_info_content"]
    assert content_calls
    assert all(c[1][0] == 0 for c in content_calls)
