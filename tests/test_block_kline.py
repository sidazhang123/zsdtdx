"""离线验收板块指数目录、6 小时文件缓存，以及 0x0523 组包与任务出口。"""

import io
import struct
import time
import zipfile

import pytest

from zsdtdx.cache.block_file_cache import (
    BLOCK_FILE_TTL_SECONDS,
    block_file_cache_path,
    load_fresh_block_files,
    save_block_files,
)
from zsdtdx.engine.parallel_fetcher import (
    _build_symbol_kline_payload,
    _normalize_block_task_payload,
)
from zsdtdx.parser.block_index_catalog import parse_block_index_catalog
from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page
from zsdtdx.parser.get_block_bars import GetBlockBarsCmd
from zsdtdx.parser.get_security_bars import pack_standard_kline_request
from zsdtdx.simple_api import get_block_kline
from zsdtdx.engine.unified_client import UnifiedTdxClient


def _encode_price(val: int) -> bytes:
    """输入有符号整数价，输出通达信变长价字节。用途：构造绝对价单页夹具。"""
    sign = val < 0
    v = abs(int(val))
    out = bytearray()
    first = v & 0x3F
    v >>= 6
    if sign:
        first |= 0x40
    if v:
        first |= 0x80
    out.append(first)
    while v:
        byte = v & 0x7F
        v >>= 7
        if v:
            byte |= 0x80
        out.append(byte)
    return bytes(out)


def _pack_block_abs_daily_page(
    bars: list[tuple[int, int, int, int, int, int, int, int]],
) -> bytes:
    """
    输入多根 (yyyymmdd, open, close, high, low, vol_u32, amt_u32, up, down) 毫单位价。
    输出带涨跌家数的日线绝对价单页。
    用途：对照抓包 0x0523 布局做离线解码验收。
    边界：vol/amt 直接写 4 字节原码，不经 get_volume 编码。
    """
    body = bytearray()
    body += struct.pack("<H", len(bars))
    for ymd, o, c, h, low, vol_u32, amt_u32, up, down in bars:
        body += struct.pack("<I", ymd)
        body += _encode_price(o)
        body += _encode_price(c)
        body += _encode_price(h)
        body += _encode_price(low)
        body += struct.pack("<II", vol_u32, amt_u32)
        body += struct.pack("<HH", up, down)
    return bytes(body)


def _zhb(zs_lines, zs3_lines) -> bytes:
    """输入两张表的文本行，输出含 tdxzs.cfg 与 tdxzs3.cfg 的 zip。"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("tdxzs.cfg", "\n".join(zs_lines).encode("gbk"))
        archive.writestr("tdxzs3.cfg", "\n".join(zs3_lines).encode("gbk"))
    return buffer.getvalue()


def _files() -> dict:
    """输入无，输出最小三文件字典。用途：目录与缓存用例。边界：原文非空即可。"""
    return {
        "infoharbor_block.dat": b"block",
        "tdxhy.cfg": b"hy",
        "zhb.zip": _zhb(
            [
                "汽车拆解|880744|4|2|0|汽车拆解",
                "昨日涨停|880863|5|2|0|昨日涨停",
                "轮动趋势|880081|5|2|0|轮动趋势",
                "高分红股|880526|5|2|0|高分红股",
                "黑龙江|880201|3|1|0|1",
                "煤炭|880301|2|1|0|T0101",
            ],
            [
                "精细化工|881479|12|1|0|X4006",
                "煤炭开采|881002|12|1|1|X010101",
                "能源|881000|12|1|0|X40",
            ],
        ),
    }


def test_block_catalog_keeps_all_board_subset():
    """输入最小三文件。输出概念、风格、地区、研究中类，并去掉统计风格和 T 行业。"""
    catalog = parse_block_index_catalog(_files())
    assert catalog.names == ["汽车拆解", "高分红股", "黑龙江", "精细化工"]
    assert catalog.by_name["汽车拆解"]["code"] == "880744"
    assert catalog.by_name["汽车拆解"]["market"] == 1
    assert "昨日涨停" not in catalog.by_name
    assert "煤炭" not in catalog.by_name
    assert "煤炭开采" not in catalog.by_name


def test_block_file_cache_expires_at_six_hours(tmp_path):
    """输入写入后的快照。输出 6 小时内可读取，满 6 小时视为过期。"""
    files = _files()
    save_block_files(files, fetched_at=1_000.0, cache_dir=tmp_path)
    fresh = load_fresh_block_files(
        now=1_000.0 + BLOCK_FILE_TTL_SECONDS - 1, cache_dir=tmp_path
    )
    expired = load_fresh_block_files(
        now=1_000.0 + BLOCK_FILE_TTL_SECONDS, cache_dir=tmp_path
    )
    assert fresh is not None
    assert fresh["files"]["zhb.zip"] == files["zhb.zip"]
    assert expired is None


def test_block_file_cache_rejects_future_and_nan_timestamps(tmp_path):
    """输入未来时间与 NaN。输出均不得被当作永不过期的有效缓存。"""
    files = _files()
    save_block_files(files, fetched_at=2_000.0, cache_dir=tmp_path)
    assert load_fresh_block_files(now=1_000.0, cache_dir=tmp_path) is None
    assert not block_file_cache_path(tmp_path).exists()
    with pytest.raises(ValueError, match="时间戳非法"):
        save_block_files(files, fetched_at=float("nan"), cache_dir=tmp_path)


def _block_cache_client(tmp_path):
    """构造只含板块缓存流程所需字段的离线客户端。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = tmp_path
    client._record_failure = lambda *args: None
    return client


def test_semantically_broken_block_cache_is_rebuilt(tmp_path):
    """输入结构完整但无法解析目录的快照。输出删除坏缓存并用新下载重建。"""
    broken = {name: b"broken" for name in _files()}
    save_block_files(broken, cache_dir=tmp_path)
    client = _block_cache_client(tmp_path)
    downloads: list[int] = []

    def download():
        downloads.append(1)
        return _files()

    client._download_block_named_files = download
    names = client.get_block_names()
    assert "汽车拆解" in names
    assert downloads == [1]
    loaded = load_fresh_block_files(cache_dir=tmp_path)
    assert loaded is not None
    assert loaded["files"]["zhb.zip"] == _files()["zhb.zip"]


def test_block_cache_uses_configured_path_and_memory_after_write_failure(
    monkeypatch, tmp_path
):
    """输入自定义目录且磁盘写失败。输出下载结果仍在进程内复用，不重复拉取。"""
    client = _block_cache_client(tmp_path)
    downloads: list[int] = []

    def download():
        downloads.append(1)
        return _files()

    client._download_block_named_files = download
    monkeypatch.setattr(
        "zsdtdx.engine.unified_client.save_block_files",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("磁盘只读")),
    )
    first = client.get_block_names()
    second = client.get_block_names()
    assert first == second
    assert downloads == [1]
    assert client._block_file_cache_dir() == tmp_path


def test_stock_concepts_reads_existing_fresh_block_snapshot(tmp_path):
    """输入已存在的新鲜共享快照。输出股票板块接口不得再次下载同一组三文件。"""
    save_block_files(_files(), fetched_at=time.time(), cache_dir=tmp_path)
    client = _block_cache_client(tmp_path)
    client.get_stock_code_name_map = lambda: {}
    client._download_named_hq_file = lambda name: (_ for _ in ()).throw(
        AssertionError(f"不应重新下载 {name}")
    )
    result = client.get_stock_concepts()
    assert result == {"names": [], "map": {}}


def test_block_kline_packet_matches_index_layout_except_command():
    """输入同一分页参数。输出 54 字节，且仅命令号从 0x052D 换成 0x0523。"""
    index_pkg = pack_standard_kline_request(4, 1, "880744", 0, 420, qfq=False)
    block_pkg = bytes(
        pack_standard_kline_request(4, 1, "880744", 0, 420, qfq=False, command=0x0523)
    )
    cmd = GetBlockBarsCmd(client=None)
    cmd.setParams(4, 1, "880744", 0, 420)
    assert bytes(cmd.send_pkg) == block_pkg
    assert len(block_pkg) == 54
    assert struct.unpack_from("<H", block_pkg, 10)[0] == 0x0523
    assert struct.unpack_from("<H", index_pkg, 10)[0] == 0x052D
    assert block_pkg[:10] == bytes(index_pkg[:10])
    assert block_pkg[12:] == bytes(index_pkg[12:])


def test_block_bars_parse_absolute_ohlc_not_differential():
    """
    输入抓包同构的两根日线绝对价页（取自 880744 量级）。
    输出 OHLC 自洽且不沿差分链发散；差分解必然 open 落在区间外。
    """
    # 数值取自今日抓包首两根：绝对价约 909~936，差分误解会变成 ~1800+。
    body = _pack_block_abs_daily_page(
        [
            (20250107, 909180, 930640, 930950, 906870, 0, 0, 19, 5),
            (20250108, 925050, 932050, 936170, 905930, 0, 0, 9, 15),
        ]
    )
    abs_rows = parse_diff_encoded_kline_page(
        body, 4, with_index_counts=True, absolute_ohlc=True
    )
    diff_rows = parse_diff_encoded_kline_page(
        body, 4, with_index_counts=True, absolute_ohlc=False
    )
    cmd = GetBlockBarsCmd(client=None)
    cmd.setParams(4, 1, "880744", 0, 2)
    cmd_rows = cmd.parseResponse(body)

    assert len(abs_rows) == 2
    assert abs_rows[0]["datetime"] == "2025-01-07 15:00:00"
    assert abs_rows[0]["open"] == 909.18
    assert abs_rows[0]["close"] == 930.64
    assert abs_rows[0]["high"] == 930.95
    assert abs_rows[0]["low"] == 906.87
    assert abs_rows[0]["up_count"] == 19
    assert abs_rows[1]["open"] == 925.05
    assert abs_rows[1]["high"] == 936.17
    # GetBlockBarsCmd 必须走绝对价，与 abs_rows 一致。
    assert cmd_rows[0]["open"] == abs_rows[0]["open"]
    assert cmd_rows[1]["close"] == abs_rows[1]["close"]
    # 差分误解：第二根 open 会跳到数千量级且不自洽。
    assert diff_rows[1]["open"] > 2000
    assert diff_rows[0]["high"] > diff_rows[0]["open"] + 500


def test_block_payload_uses_block_name_without_code():
    """输入带板块标记的指数任务。输出 task/rows 使用 block_name，且不含代码。"""
    payload = _build_symbol_kline_payload(
        "block_name",
        task={
            "block_name": "汽车拆解",
            "freq": "d",
            "start_time": "2026-09-01 09:30:00",
            "end_time": "2026-09-29 16:00:00",
        },
        rows=[
            {
                "block_name": "汽车拆解",
                "freq": "d",
                "open": 1.0,
                "close": 2.0,
                "high": 3.0,
                "low": 0.5,
                "volume": 10,
                "amount": 20,
                "datetime": "2026-09-29 15:00:00",
            }
        ],
    )
    assert payload["task"]["block_name"] == "汽车拆解"
    assert "index_name" not in payload["task"]
    assert "code" not in payload["rows"][0]
    assert payload["rows"][0]["block_name"] == "汽车拆解"
    kept = _normalize_block_task_payload(
        {
            "block_name": "汽车拆解",
            "freq": "d",
            "start_time": "2026-09-01 09:30:00",
            "end_time": "2026-09-29 16:00:00",
            "_index_route_source": "std",
            "_index_route_market": 1,
            "_index_route_code": "880744",
            "_index_route_name": "汽车拆解",
        }
    )
    assert kept["block_name"] == "汽车拆解"
    assert "index_name" not in kept
    assert kept["_index_route_code"] == "880744"


def test_get_block_kline_attaches_internal_route(monkeypatch):
    """输入板块名称任务。输出抓取器收到 0x0523 标记和内部代码。"""
    captured = {}

    class _Fetcher:
        def fetch_block_tasks_sync(self, **kwargs):
            captured["tasks"] = kwargs["tasks"]
            return []

        def fetch_block_tasks_async(self, **kwargs):
            raise AssertionError("sync 不应走 async")

    class _Client:
        def resolve_block_index_route(self, block_name):
            assert block_name == "汽车拆解"
            return {"name": block_name, "code": "880744", "market": 1, "kind": "GN"}

        def prepare_block_kline_tasks(self, tasks):
            return UnifiedTdxClient.prepare_block_kline_tasks(self, tasks)

    monkeypatch.setattr(
        "zsdtdx.engine.parallel_fetcher.get_fetcher", lambda: _Fetcher()
    )
    monkeypatch.setattr(
        "zsdtdx.biz.block_kline._ensure_active_config_ready", lambda **kwargs: None
    )
    monkeypatch.setattr(
        "zsdtdx.biz.block_kline.call_with_main_client",
        lambda fn, **kwargs: fn(_Client()),
    )
    result = get_block_kline(
        task=[
            {
                "block_name": "汽车拆解",
                "freq": "d",
                "start_time": "2026-09-29",
                "end_time": "2026-09-29",
            }
        ],
        mode="sync",
    )
    assert result == []
    task = captured["tasks"][0]
    assert task["block_name"] == "汽车拆解"
    assert "index_name" not in task
    assert "_block_kline" not in task
    assert task["_index_route_code"] == "880744"
    assert task["_index_route_market"] == 1
