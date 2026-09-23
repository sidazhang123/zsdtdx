# -*- coding: utf-8 -*-
"""
模块：`test_named_hq_file.py`。

职责：
1. 离线验收 0x02C5 / 0x06B9 组包与回包解析对齐银河抓包。
2. 验收 infoharbor 行解析与 get_etf_code_name_map 名称合并。

边界：
1. 不访问网络。
"""

import struct

from zsdtdx.parser.get_report_file import (
    NAMED_FILE_CHUNK_SIZE,
    GetReportFile,
    GetReportFileMeta,
    parse_named_file_chunk_body,
    parse_named_file_meta_body,
)
from zsdtdx.unified_client import UnifiedTdxClient


def test_meta_pack_matches_capture():
    """输入：文件名。输出：断言 52 字节头与 40 字节文件名。用途：对齐 0x02C5。边界：离线。"""
    parser = GetReportFileMeta(client=None)
    parser.setParams("infoharbor_ex.name")
    pkg = bytes(parser.send_pkg)
    assert len(pkg) == 52
    assert pkg[:12] == bytes.fromhex("0c 04 18 69 00 01 2a 00 2a 00 c5 02")
    assert pkg[12:].split(b"\x00", 1)[0] == b"infoharbor_ex.name"
    assert pkg[12 + 18 :] == b"\x00" * 22


def test_chunk_pack_matches_capture_offsets():
    """输入：偏移 0/30000。输出：断言 320 字节与页长 30000。用途：对齐 0x06B9。边界：离线。"""
    parser = GetReportFile(client=None)
    parser.setParams("infoharbor_ex.name", 0)
    pkg0 = bytes(parser.send_pkg)
    assert len(pkg0) == 320
    assert pkg0[:12] == bytes.fromhex("0c 05 18 6a 00 01 36 01 36 01 b9 06")
    offset, size = struct.unpack_from("<II", pkg0, 12)
    assert offset == 0
    assert size == NAMED_FILE_CHUNK_SIZE
    assert pkg0[20:].split(b"\x00", 1)[0] == b"infoharbor_ex.name"

    parser.setParams("infoharbor_ex.name", 30000)
    pkg1 = bytes(parser.send_pkg)
    offset1, size1 = struct.unpack_from("<II", pkg1, 12)
    assert offset1 == 30000
    assert size1 == NAMED_FILE_CHUNK_SIZE


def test_parse_meta_filesize_and_checksum():
    """输入：抓包风格元数据正文。输出：filesize=82629。用途：解析 0x02C5。边界：离线。"""
    body = struct.pack("<I", 82629) + b"012e51d9b1cb687430dcb4e577f71a2c6d\x00"
    parsed = parse_named_file_meta_body(body)
    assert parsed["filesize"] == 82629
    assert parsed["checksum"] == "012e51d9b1cb687430dcb4e577f71a2c6d"
    assert parse_named_file_meta_body(b"")["filesize"] == 0


def test_parse_chunk_body_truncates_to_chunksize():
    """输入：4 字节长度 + 正文。输出：按 chunksize 截取。用途：解析 0x06B9。边界：空页。"""
    payload = "0|159105|恒生生物科技ETF易方达\r\n".encode("gbk")
    body = struct.pack("<I", len(payload)) + payload + b"\x00\x00"
    parsed = parse_named_file_chunk_body(body)
    assert parsed["chunksize"] == len(payload)
    assert parsed["chunkdata"] == payload
    assert parse_named_file_chunk_body(struct.pack("<I", 0))["chunksize"] == 0


def _etf_client() -> UnifiedTdxClient:
    """输入：无。输出：带远程名称记录的 mock 客户端。用途：离线单测。边界：无真实连接。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.market_rules = {
        "stock_prefix_sz": ["000", "001", "002", "003", "300"],
        "stock_prefix_sh": ["600", "601", "603", "605"],
        "etf_name_drop_substr": ["债", "货币", "增强", "红利", "现金流"],
        "etf_name_remote_file": "infoharbor_ex.name",
    }
    client.pagination = {"named_file_chunk_size": 30000}
    client._etf_name_records = [
        {"market": 0, "code": "159915", "name": "创业板ETF易方达"},
        {"market": 1, "code": "510300", "name": "沪深300ETF华泰柏瑞"},
        {"market": 0, "code": "159105", "name": "恒生生物科技ETF易方达"},
        {"market": 0, "code": "161725", "name": "白酒LOF"},
        {"market": 1, "code": "510050", "name": "上证50ETF华夏"},
        {"market": 0, "code": "159001", "name": "货币ETF"},
        {"market": 1, "code": "511010", "name": "国债ETF"},
        {"market": 0, "code": "159992", "name": "银华增强ETF"},
        {"market": 0, "code": "399306", "name": "国证ETF"},
        {"market": 0, "code": "000001", "name": "平安银行"},
        {"market": 2, "code": "920000", "name": "某北交ETF"},
        {"market": 0, "code": "159888", "name": "某主题ETF"},
    ]
    client._etf_board_records = [
        {"market": 0, "code": "159915"},
        {"market": 1, "code": "510300"},
        {"market": 0, "code": "159105"},
        {"market": 0, "code": "161725"},
        {"market": 1, "code": "510050"},
        {"market": 0, "code": "159001"},
        {"market": 1, "code": "511010"},
        {"market": 0, "code": "159992"},
        {"market": 0, "code": "399306"},
        {"market": 2, "code": "920000"},
        {"market": 0, "code": "158099"},
    ]
    client._etf_name_date = "2099-01-01"
    client._std_catalog_records = []
    client._catalog_cache_enabled = False

    def _skip_network(*args, **kwargs):
        """输入：任意。输出：无。用途：跳过网络下载。边界：仅占位。"""
        return None

    client._ensure_etf_name_catalog = _skip_network  # type: ignore[method-assign]
    client.ensure_code_catalog = _skip_network  # type: ignore[method-assign]
    return client


def test_parse_infoharbor_lines():
    """输入：GBK/CSV 原文。输出：名称两行、板块两行。用途：行解析。边界：缺列跳过。"""
    client = _etf_client()
    raw = (
        "0|159105|恒生生物科技ETF易方达\r\n"
        "0||空代码应跳过\r\n"
        "1|510050|上证50ETF华夏\n"
        "badline\n"
    ).encode("gbk")
    rows = client._parse_infoharbor_name_file(raw)
    assert rows[0]["code"] == "159105"
    assert rows[0]["name"] == "恒生生物科技ETF易方达"
    assert rows[1]["code"] == "510050"
    assert len(rows) == 2
    board = client._parse_etf_board_file(
        b"0,159105,HZ5366,27\r\n1,510050,000016,1\r\n9,123,x\n"
    )
    assert board == [
        {"market": 0, "code": "159105"},
        {"market": 1, "code": "510050"},
    ]


def test_get_etf_code_name_map_uses_full_remote_names():
    """输入：mock 完整名。输出：保留 ETF/LOF、剔除 drop、前缀正确。用途：主路径。边界：离线。"""
    client = _etf_client()
    result = client.get_etf_code_name_map(use_cache=True)
    assert result["sz.159915"] == "创业板ETF易方达"
    assert result["sh.510300"] == "沪深300ETF华泰柏瑞"
    assert result["sz.159105"] == "恒生生物科技ETF易方达"
    assert result["sz.161725"] == "白酒LOF"
    assert result["sh.510050"] == "上证50ETF华夏"
    assert result["sz.159888"] == "某主题ETF"
    assert "sz.158099" not in result
    assert "sz.159001" not in result
    assert "sh.511010" not in result
    assert "sz.159992" not in result
    assert "sz.399306" not in result
    assert "sz.000001" not in result
    assert "bj.920000" not in result


def test_a_share_std_unchanged_by_etf_rules():
    """输入：mock 客户端。输出：断言。用途：15/51 段仍非 A 股。边界：离线。"""
    client = _etf_client()
    assert client._is_a_share_std(0, "000001") is True
    assert client._is_a_share_std(0, "159915") is False
    assert client._is_a_share_std(1, "510300") is False


def test_ensure_etf_name_catalog_merges_board_files():
    """输入：mock 下载。输出：名称与板块并集。用途：ensure 下载路径。边界：离线、不去重失败。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.market_rules = {
        "etf_name_remote_file": "infoharbor_ex.name",
        "etf_board_remote_files": ["spec/specetfdata.txt", "spec/speclofdata.txt"],
    }
    client._etf_name_records = None
    client._etf_board_records = None
    client._etf_name_date = ""
    client._catalog_cache_enabled = False
    client._catalog_cache_dir = None
    files = {
        "infoharbor_ex.name": "0|159105|恒生生物科技ETF易方达\n".encode("gbk"),
        "spec/specetfdata.txt": b"0,159105,HZ5366,27\n1,510050,000016,1\n",
        "spec/speclofdata.txt": b"0,161725,x,1\n1,510050,dup,1\n",
        "zhb.zip": b"",
    }

    def _today() -> str:
        """输入：无。输出：固定日期。用途：命中当日缓存键。边界：离线。"""
        return "2099-01-01"

    def _download(filename: str) -> bytes:
        """输入：远程文件名。输出：mock 原文。用途：跳过 0x02C5/0x06B9。边界：未知文件空。"""
        return files.get(str(filename), b"")

    client._today_catalog_cache_date = _today  # type: ignore[method-assign]
    client._download_named_hq_file = _download  # type: ignore[method-assign]
    client._ensure_etf_name_catalog(refresh=True)
    assert client._etf_name_records[0]["code"] == "159105"
    board_keys = {(int(r["market"]), str(r["code"])) for r in client._etf_board_records}
    assert board_keys == {(0, "159105"), (1, "510050"), (0, "161725")}
    assert client._etf_name_date == "2099-01-01"


def test_merge_etf_name_records_prefer_ilong():
    """输入：harbor 与 ilong。输出：同码取 ilong，仅 ilong 也保留。用途：合并规则。边界：离线。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    harbor = [
        {"market": 0, "code": "159545", "name": "港股通红利低波ETF易方达"},
        {"market": 0, "code": "158061", "name": "创业板算力ETF天弘"},
    ]
    ilong = [
        {"market": 0, "code": "159545", "name": "恒生红利低波ETF易方达"},
        {"market": 0, "code": "158053", "name": "创业板算力ETF大成"},
    ]
    merged = client._merge_etf_name_records_prefer_ilong(harbor, ilong)
    by_code = {r["code"]: r["name"] for r in merged}
    assert by_code["159545"] == "恒生红利低波ETF易方达"
    assert by_code["158061"] == "创业板算力ETF天弘"
    assert by_code["158053"] == "创业板算力ETF大成"


def test_ensure_etf_name_catalog_merges_ilong_from_zhb_zip():
    """输入：mock zhb.zip 含 ilong。输出：同码覆盖且补缺。用途：冷启动并入 ilong。边界：离线。"""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "ilong.dat",
            "0|158011||创业板软件ETF国泰改\n0|158053||创业板算力ETF大成\n".encode(
                "gbk"
            ),
        )
    client = _ensure_client(
        {
            "infoharbor_ex.name": (
                "0|158011|创业板软件ETF国泰\n0|158061|创业板算力ETF天弘\n"
            ).encode("gbk"),
            "spec/specetfdata.txt": b"0,158053,x,1\n0,158061,x,1\n0,158011,x,1\n",
            "spec/speclofdata.txt": b"0,158061,x,1\n",
            "zhb.zip": buf.getvalue(),
        }
    )
    client.market_rules = {
        **client.market_rules,
        "etf_name_drop_substr": ["债", "货币"],
    }
    client._std_catalog_records = []
    client.ensure_code_catalog = lambda **kwargs: None  # type: ignore[method-assign]
    client._ensure_etf_name_catalog(refresh=True)
    by_code = {r["code"]: r["name"] for r in client._etf_name_records}
    assert by_code["158011"] == "创业板软件ETF国泰改"
    assert by_code["158053"] == "创业板算力ETF大成"
    assert by_code["158061"] == "创业板算力ETF天弘"
    result = client.get_etf_code_name_map(use_cache=True)
    assert result["sz.158053"] == "创业板算力ETF大成"
    assert result["sz.158011"] == "创业板软件ETF国泰改"


def test_get_etf_code_name_map_falls_back_to_std_short_name():
    """输入：板块有码、命名文件无名、std 有 16 字节短名。输出：收录短名。用途：版面补缺。边界：离线。"""
    client = _etf_client()
    client._etf_name_records = []
    client._etf_board_records = [
        {"market": 0, "code": "159915"},
        {"market": 1, "code": "501046"},
    ]
    client._std_catalog_records = [
        {"market": 0, "code": "159915", "name": "创业板ETF易方达"},
        {"market": 1, "code": "501046", "name": "财通福鑫定开混合"},
    ]
    result = client.get_etf_code_name_map(use_cache=True)
    assert result["sz.159915"] == "创业板ETF易方达"
    assert result["sh.501046"] == "财通福鑫定开混合"


def _ensure_client(files: dict[str, bytes]) -> UnifiedTdxClient:
    """输入 mock 文件。输出可跑 ensure 的客户端。用途：空列表/磁盘用例。边界：无网络。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.market_rules = {
        "etf_name_remote_file": "infoharbor_ex.name",
        "etf_board_remote_files": ["spec/specetfdata.txt", "spec/speclofdata.txt"],
    }
    client.pagination = {"named_file_chunk_size": 30000}
    client._etf_name_records = None
    client._etf_board_records = None
    client._etf_name_date = ""
    client._catalog_cache_enabled = False
    client._catalog_cache_dir = None
    client._today_catalog_cache_date = lambda: "2099-01-01"  # type: ignore[method-assign]
    client._download_named_hq_file = (  # type: ignore[method-assign]
        lambda filename: files.get(str(filename), b"")
    )
    return client


def test_ensure_etf_name_catalog_rejects_empty_board_file():
    """输入：LOF 板块空。输出：不写内存日期。用途：空列表不得当成当天成功。边界：离线。"""
    client = _ensure_client(
        {
            "infoharbor_ex.name": "0|159105|恒生生物科技ETF易方达\n".encode("gbk"),
            "spec/specetfdata.txt": b"0,159105,HZ5366,27\n",
            "spec/speclofdata.txt": b"",
        }
    )
    client._ensure_etf_name_catalog(refresh=True)
    assert client._etf_name_records is None
    assert client._etf_board_records is None
    assert client._etf_name_date == ""


def test_ensure_etf_name_catalog_rejects_empty_names():
    """输入：名称文件空。输出：不写内存日期。用途：校验失败重试。边界：离线。"""
    client = _ensure_client(
        {
            "infoharbor_ex.name": b"",
            "spec/specetfdata.txt": b"0,159105,HZ5366,27\n",
            "spec/speclofdata.txt": b"0,161725,x,1\n",
        }
    )
    client._ensure_etf_name_catalog(refresh=True)
    assert client._etf_name_records is None
    assert client._etf_name_date == ""


def test_ensure_etf_name_catalog_disk_hit_skips_download(tmp_path):
    """输入：当日 etf pickle。输出：不调用下载。用途：日级磁盘缓存。边界：离线。"""
    from zsdtdx.catalog_disk_cache import (
        KIND_ETF,
        catalog_cache_file_path,
        save_etf_catalog_cache,
    )

    save_etf_catalog_cache(
        catalog_cache_file_path(tmp_path, KIND_ETF),
        cache_date="2099-01-01",
        name_records=[{"market": 0, "code": "159105", "name": "恒生生物科技ETF易方达"}],
        board_records=[{"market": 0, "code": "159105"}],
    )
    downloads: list[str] = []
    client = _ensure_client({})
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = tmp_path
    client._download_named_hq_file = (  # type: ignore[method-assign]
        lambda filename: downloads.append(str(filename)) or b""
    )
    client._ensure_etf_name_catalog(refresh=False)
    assert downloads == []
    assert client._etf_name_records[0]["code"] == "159105"
    assert client._etf_board_records[0]["code"] == "159105"
    assert client._etf_name_date == "2099-01-01"


def test_ensure_etf_refresh_ignores_disk_and_redownloads(tmp_path):
    """输入：磁盘已有当日缓存但 refresh=True。输出：仍下载并覆盖。用途：use_cache=False。边界：离线。"""
    from zsdtdx.catalog_disk_cache import (
        KIND_ETF,
        catalog_cache_file_path,
        save_etf_catalog_cache,
    )

    save_etf_catalog_cache(
        catalog_cache_file_path(tmp_path, KIND_ETF),
        cache_date="2099-01-01",
        name_records=[{"market": 0, "code": "159105", "name": "旧名"}],
        board_records=[{"market": 0, "code": "159105"}],
    )
    files = {
        "infoharbor_ex.name": "0|159105|恒生生物科技ETF易方达\n".encode("gbk"),
        "spec/specetfdata.txt": b"0,159105,HZ5366,27\n",
        "spec/speclofdata.txt": b"0,161725,x,1\n",
    }
    downloads: list[str] = []
    client = _ensure_client(files)
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = tmp_path
    inner = client._download_named_hq_file

    def _download(filename: str) -> bytes:
        """输入文件名。输出 mock 正文。用途：计数下载。边界：转调 files。"""
        downloads.append(str(filename))
        return inner(filename)

    client._download_named_hq_file = _download  # type: ignore[method-assign]
    client._ensure_etf_name_catalog(refresh=True)
    assert downloads
    assert client._etf_name_records[0]["name"] == "恒生生物科技ETF易方达"
    assert {row["code"] for row in client._etf_board_records} == {"159105", "161725"}


def test_ensure_etf_download_persists_disk_cache(tmp_path):
    """输入：无当日 pickle。输出：下载后写出 etf_code_name.pkl。用途：冷启动落盘。边界：离线。"""
    from zsdtdx.catalog_disk_cache import (
        KIND_ETF,
        catalog_cache_file_path,
        load_etf_catalog_cache,
    )

    files = {
        "infoharbor_ex.name": "0|159105|恒生生物科技ETF易方达\n".encode("gbk"),
        "spec/specetfdata.txt": b"0,159105,HZ5366,27\n",
        "spec/speclofdata.txt": b"0,161725,x,1\n",
    }
    client = _ensure_client(files)
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = tmp_path
    client._ensure_etf_name_catalog(refresh=False)
    loaded = load_etf_catalog_cache(
        catalog_cache_file_path(tmp_path, KIND_ETF),
        expected_cache_date="2099-01-01",
    )
    assert loaded is not None
    _date, names, boards = loaded
    assert names[0]["code"] == "159105"
    assert {row["code"] for row in boards} == {"159105", "161725"}


def test_ensure_etf_empty_board_does_not_persist(tmp_path):
    """输入：LOF 板块空且缓存开启。输出：不写 pickle。用途：空快照不得落盘。边界：离线。"""
    from zsdtdx.catalog_disk_cache import KIND_ETF, catalog_cache_file_path

    client = _ensure_client(
        {
            "infoharbor_ex.name": "0|159105|恒生生物科技ETF易方达\n".encode("gbk"),
            "spec/specetfdata.txt": b"0,159105,HZ5366,27\n",
            "spec/speclofdata.txt": b"",
        }
    )
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = tmp_path
    client._ensure_etf_name_catalog(refresh=True)
    assert client._etf_name_records is None
    assert not catalog_cache_file_path(tmp_path, KIND_ETF).exists()


def test_download_named_hq_file_uses_get_report_file_by_size():
    """输入：连接池 mock。输出：只调 by_size。用途：合并下载循环。边界：离线。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.pagination = {"named_file_chunk_size": 30000}
    calls: list[str] = []

    class _Pool:
        """输入：无。输出：记录方法名。用途：断言唯一下载入口。边界：固定返回。"""

        def call(self, method_name, *args, **kwargs):
            """输入方法名。输出假文件。用途：替身。边界：不发网络。"""
            calls.append(str(method_name))
            return b"abc"

    client.std_pool = _Pool()
    assert client._download_named_hq_file("infoharbor_ex.name") == b"abc"
    assert calls == ["get_report_file_by_size"]


def test_get_report_file_by_size_pages_and_slices():
    """输入：两页 mock。输出：按 chunksize 截取后拼接。用途：HQ 唯一翻页循环。边界：离线。"""
    from zsdtdx.hq import TdxHq_API

    api = TdxHq_API.__new__(TdxHq_API)
    pages = {
        0: {"chunksize": 3, "chunkdata": b"abcXXXX"},
        3: {"chunksize": 2, "chunkdata": b"de"},
    }
    meta_calls: list[str] = []
    offsets: list[int] = []

    def _meta(filename):
        """输入文件名。输出假元数据。用途：0x02C5 替身。边界：固定 5 字节。"""
        meta_calls.append(str(filename))
        return {"filesize": 5, "checksum": "x"}

    def _page(filename, offset, chunk_size=None):
        """输入偏移。输出假页。用途：0x06B9 替身。边界：按 offset 取页。"""
        offsets.append(int(offset))
        return pages[int(offset)]

    api.get_report_file_meta = _meta  # type: ignore[method-assign]
    api.get_report_file = _page  # type: ignore[method-assign]
    raw = api.get_report_file_by_size("infoharbor_ex.name", 0, None, 30000)
    assert bytes(raw) == b"abcde"
    assert meta_calls == ["infoharbor_ex.name"]
    assert offsets == [0, 3]
