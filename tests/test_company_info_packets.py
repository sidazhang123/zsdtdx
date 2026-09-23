"""离线验收公司信息目录与正文组包：分类下标、剩余 length、30720 分页。"""

from __future__ import annotations

import struct
import threading
from unittest.mock import MagicMock

from zsdtdx.parser.get_company_info_category import GetCompanyInfoCategory
from zsdtdx.parser.get_company_info_content import (
    COMPANY_INFO_CONTENT_PAGE_SIZE,
    GetCompanyInfoContent,
)
from zsdtdx.unified_client import UnifiedTdxClient


def _attach_pinned_pool(client, fake_call, hosts=None):
    """
    输入：客户端、请求替身、可选地址列表。
    输出：挂好的连接池替身。
    用途：公司信息单测走不换站接口。
    边界：不访问网络；`call` 被调用时直接失败，避免测到旧的换站入口。
    """
    pool = MagicMock()
    pool.hosts = list(hosts or [("127.0.0.1", 7709)])
    pool.get_active_host.return_value = f"{pool.hosts[0][0]}:{pool.hosts[0][1]}"
    pool.connect_thread_to_host.return_value = True
    pool.rotate_thread_host.return_value = False
    pool.call_current_host.side_effect = fake_call
    pool.call.side_effect = AssertionError("公司信息不应再走会换站的 call")
    client.std_pool = pool
    return pool


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
    """输入：10 字节前缀 + uint16 + GBK；输出：本页原始 bytes，不做解码。"""
    text = "☆公司大事☆"
    encoded = text.encode("gbk")
    body = b"\x00\x00300063\x00\x00" + struct.pack("<H", len(encoded)) + encoded
    parser = GetCompanyInfoContent(client=None)
    assert parser.parseResponse(body) == encoded


def test_fetch_company_content_pages_like_official_client():
    """输入：length=361861、index=8；输出：12 次请求，start 步进 30720，length 为剩余。"""
    calls: list[tuple] = []

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：连接池方法名与参数。
        输出：非空占位正文 bytes。
        用途：记录分页参数。
        边界：不访问网络。
        """
        calls.append((method_name, args))
        return b"x"

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"company_info_chunk_size": 30720}
    client.std_pool = MagicMock()
    client.std_pool.call_current_host.side_effect = fake_call
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


def test_fetch_company_content_joins_bytes_before_gbk_decode():
    """输入：页界切开双字节汉字；输出：整包解码保留该字，不因按页 ignore 丢字节。"""
    page_size = 30720
    # 「中」GBK=D6 D0：lead 落在第 1 页末字节，trail 落在第 2 页首字节。
    full = (b"A" * (page_size - 1)) + "中".encode("gbk") + b"B"
    assert len(full) == page_size + 2
    pages = [full[:page_size], full[page_size:]]
    assert pages[0][-1:] == b"\xd6"
    assert pages[1][:1] == b"\xd0"

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：连接池方法名与参数。
        输出：按 start 返回对应页 bytes。
        用途：模拟跨页 GBK 拆字。
        边界：不访问网络。
        """
        start = int(args[3])
        return pages[start // page_size]

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"company_info_chunk_size": page_size}
    client.std_pool = MagicMock()
    client.std_pool.call_current_host.side_effect = fake_call
    text, status = UnifiedTdxClient._fetch_company_content(
        client,
        market=0,
        code="000001",
        filename="000001.V14",
        start=0,
        length=len(full),
        category_index=8,
    )
    assert status == "success"
    assert text == ("A" * (page_size - 1)) + "中" + "B"
    # 对照：按页 ignore 无法还原跨页汉字（lead 被丢，trail 可能被误读成别字）。
    per_page = "".join(p.decode("gbk", "ignore") for p in pages)
    assert "中" not in per_page
    assert text.encode("gbk") == full


def test_decode_company_info_bytes_ignore_fallback():
    """输入：非法尾字节；输出：ignore 文本 + gbk_ignore_fallback 状态。"""
    text, status = UnifiedTdxClient._decode_company_info_bytes(b"OK\xff")
    assert status == "gbk_ignore_fallback"
    assert text == "OK"


def test_get_company_info_content_records_gbk_ignore_fallback():
    """输入：正文含非法 GBK 字节；输出：仍返回正文，并记入运行态失败。"""
    bad = b"\xff"

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：连接池方法名与参数。
        输出：目录一条或非法正文 bytes。
        用途：触发整包 GBK ignore 兜底。
        边界：不访问网络。
        """
        if method_name == "get_company_info_category":
            return [
                {
                    "index": 0,
                    "name": "公司概况",
                    "filename": "600000.V04",
                    "start": 0,
                    "length": len(bad),
                }
            ]
        return bad

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"company_info_chunk_size": 30720}
    client.market_rules = {"include_beijing_prefixes": ["92"]}
    client._stock_route = {
        "600000": {
            "code": "600000",
            "name": "浦发银行",
            "market": 1,
            "market_name": "上海",
            "source": "std",
            "asset_type": "stock",
        }
    }
    client._runtime_failures = []
    client.output = {"return_df_default": False}
    _attach_pinned_pool(client, fake_call)

    rows = UnifiedTdxClient.get_company_info_content(
        client, code="600000", return_df=False
    )
    assert len(rows) == 1
    assert rows[0]["content"] == ""
    assert any(
        f["reason"] == "gbk_ignore_fallback" and f["task"] == "company_info"
        for f in client._runtime_failures
    )


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
        输出：目录一条或占位正文 bytes。
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
        return "正文".encode("gbk")

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
    _attach_pinned_pool(client, fake_call)

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


def test_get_company_info_content_category_workers_preserves_order():
    """输入：两分类 + category_workers=2；输出：记录顺序与目录一致。"""

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：连接池方法名与参数。
        输出：目录两条或正文 bytes。
        用途：验证标签并发仍按目录顺序回填。
        边界：不访问网络。
        """
        if method_name == "get_company_info_category":
            return [
                {
                    "index": 0,
                    "name": "最新提示",
                    "filename": "600000.V04",
                    "start": 0,
                    "length": 2,
                },
                {
                    "index": 1,
                    "name": "公司概况",
                    "filename": "600000.V04",
                    "start": 2,
                    "length": 2,
                },
            ]
        # args: market, code, filename, start, length, category_index
        start = int(args[3])
        return b"A" if start == 0 else b"B"

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"company_info_chunk_size": 30720}
    client.market_rules = {"include_beijing_prefixes": ["92"]}
    client._stock_route = {
        "600000": {
            "code": "600000",
            "name": "浦发银行",
            "market": 1,
            "market_name": "上海",
            "source": "std",
            "asset_type": "stock",
        }
    }
    client._runtime_failures = []
    client.output = {"return_df_default": False}
    _attach_pinned_pool(client, fake_call)

    rows = UnifiedTdxClient.get_company_info_content(
        client,
        code="600000",
        return_df=False,
        category_workers=2,
    )
    assert [r["category"] for r in rows] == ["最新提示", "公司概况"]
    assert [r["content"] for r in rows] == ["A", "B"]


def _client_for_stock(code: str, market: int) -> UnifiedTdxClient:
    """
    输入：代码与市场。
    输出：只含该路由的客户端空壳。
    用途：公司信息钉站单测。
    边界：不建真连接。
    """
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"company_info_chunk_size": 30720}
    client.market_rules = {"include_beijing_prefixes": ["92"]}
    client._stock_route = {
        code: {
            "code": code,
            "name": "测试",
            "market": market,
            "market_name": "深圳",
            "source": "std",
            "asset_type": "stock",
        }
    }
    client._runtime_failures = []
    client.output = {"return_df_default": False}
    return client


def test_company_info_content_threads_bind_category_host():
    """输入：两分类并发；输出：正文线程只连目录所在站，且不走会换站的 call。"""
    binds: list[tuple[str, int, int]] = []
    caller = threading.get_ident()

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：方法名与协议参数。
        输出：两条目录或占位正文。
        用途：观察正文钉站。
        边界：不访问网络。
        """
        if method_name == "get_company_info_category":
            return [
                {
                    "index": 0,
                    "name": "股东研究",
                    "filename": "000559.txt",
                    "start": 10,
                    "length": 2,
                },
                {
                    "index": 1,
                    "name": "财务分析",
                    "filename": "000559.txt",
                    "start": 20,
                    "length": 2,
                },
            ]
        return b"OK"

    def connect(host, port):
        """
        输入：目标站。
        输出：记录调用并视为连上。
        用途：确认正文线程的目标站。
        边界：不建 socket。
        """
        binds.append((str(host), int(port), threading.get_ident()))
        return True

    client = _client_for_stock("000559", 0)
    pool = _attach_pinned_pool(
        client,
        fake_call,
        hosts=[("10.1.1.1", 7709), ("10.2.2.2", 7709)],
    )
    pool.get_active_host.return_value = "10.1.1.1:7709"
    pool.connect_thread_to_host.side_effect = connect

    rows = UnifiedTdxClient.get_company_info_content(
        client,
        code="000559",
        return_df=False,
        category_workers=2,
    )
    assert [row["content"] for row in rows] == ["OK", "OK"]
    assert binds
    assert {(host, port) for host, port, _ident in binds} == {("10.1.1.1", 7709)}
    assert any(ident != caller for _host, _port, ident in binds)
    pool.call.assert_not_called()


def test_company_info_retries_whole_stock_when_content_host_fails():
    """输入：第一台站正文中断；输出：换站重拉目录，正文使用第二台站自己的 start。"""
    state = {"host": ("10.0.0.1", 7709)}
    content_starts: list[tuple[str, int]] = []
    payload = "正文".encode("gbk")
    catalogs = {
        "10.0.0.1": {
            "index": 0,
            "name": "股东研究",
            "filename": "000559.txt",
            "start": 66477,
            "length": 4,
        },
        "10.0.0.2": {
            "index": 0,
            "name": "股东研究",
            "filename": "000559.txt",
            "start": 52879,
            "length": len(payload),
        },
    }

    def get_active_host():
        """输入无。输出当前替身站。边界：不访问网络。"""
        host, port = state["host"]
        return f"{host}:{port}"

    def rotate():
        """输入无。输出切到第二台站。边界：只改替身状态。"""
        state["host"] = ("10.0.0.2", 7709)
        return True

    def connect(host, port):
        """输入目标站。输出是否等于当前替身站。边界：不建 socket。"""
        return (str(host), int(port)) == state["host"]

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：方法名与协议参数。
        输出：按当前站返回目录；第一台站正文为 None，第二台站返回正文。
        用途：模拟跨站偏移不能混用。
        边界：不访问网络。
        """
        host = state["host"][0]
        if method_name == "get_company_info_category":
            return [dict(catalogs[host])]
        start = int(args[3])
        content_starts.append((host, start))
        if host == "10.0.0.1":
            return None
        return payload

    client = _client_for_stock("000559", 0)
    pool = _attach_pinned_pool(
        client,
        fake_call,
        hosts=[("10.0.0.1", 7709), ("10.0.0.2", 7709)],
    )
    pool.get_active_host.side_effect = get_active_host
    pool.rotate_thread_host.side_effect = rotate
    pool.connect_thread_to_host.side_effect = connect

    rows = UnifiedTdxClient.get_company_info_content(
        client, code="000559", return_df=False
    )
    assert rows[0]["category"] == "股东研究"
    assert rows[0]["content"] == "正文"
    assert content_starts == [("10.0.0.1", 66477), ("10.0.0.2", 52879)]
    assert client._runtime_failures == []
    pool.call.assert_not_called()


def test_company_info_raises_when_every_host_fails():
    """输入：两台站目录都抛错；输出：把最后一次异常抛出，不当成空正文。"""

    def fake_call(method_name, *args, allow_none=False, **kwargs):
        """
        输入：方法名与协议参数。
        输出：无；总是断开。
        用途：覆盖全部站点失败。
        边界：不访问网络。
        """
        raise RuntimeError(f"down:{method_name}")

    client = _client_for_stock("000559", 0)
    state = {"host": ("10.0.0.1", 7709)}
    pool = _attach_pinned_pool(
        client,
        fake_call,
        hosts=[("10.0.0.1", 7709), ("10.0.0.2", 7709)],
    )

    def get_active_host():
        """输入无。输出当前替身站。边界：不访问网络。"""
        host, port = state["host"]
        return f"{host}:{port}"

    def rotate():
        """输入无。输出切到第二台站。边界：只改替身状态。"""
        state["host"] = ("10.0.0.2", 7709)
        return True

    pool.get_active_host.side_effect = get_active_host
    pool.rotate_thread_host.side_effect = rotate

    try:
        UnifiedTdxClient.get_company_info_content(
            client, code="000559", return_df=False
        )
    except RuntimeError as exc:
        assert "down:get_company_info_category" in str(exc)
    else:
        raise AssertionError("全部站点失败时应抛出异常")


def test_fetch_company_info_parallel_payload_forwards_category(monkeypatch):
    """输入：带 category 的并行请求；输出：worker payload 含同一 category，非整表默认全分类。"""
    from zsdtdx import parallel_fetcher as pf

    captured: list[dict] = []

    class _FakeFuture:
        def __init__(self, payload):
            self._payload = payload

        def result(self):
            return {
                "rows": [
                    {"code": "600000", "category": "公司概况", "content": "x"},
                    {"code": "000001", "category": "公司概况", "content": "y"},
                ],
                "errors": [],
            }

    class _FakeExecutor:
        def submit(self, fn, payload):
            captured.append(dict(payload))
            return _FakeFuture(payload)

    class _FakeFetcher:
        auto_prewarm_on_async = False
        company_info_codes_per_chunk = 40
        company_info_stock_inproc_workers = 3
        company_info_category_workers = 3
        company_info_max_inflight_multiplier = 2
        num_processes = 2
        config = {"output": {"return_df_default": False}}

    monkeypatch.setattr(pf, "get_fetcher", lambda: _FakeFetcher())
    monkeypatch.setattr(pf, "_get_global_process_pool", lambda n: _FakeExecutor())
    monkeypatch.setattr(
        pf,
        "wait",
        lambda futs, return_when=None: (set(futs), set()),
    )

    rows = pf.fetch_company_info_parallel(
        codes=["600000", "000001"],
        category=["公司概况"],
        auto_prewarm=False,
    )
    assert captured
    assert all(p.get("category") == ["公司概况"] for p in captured)
    assert {r["category"] for r in rows} == {"公司概况"}
