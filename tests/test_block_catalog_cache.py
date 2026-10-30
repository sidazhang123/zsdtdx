"""完整股票—板块—分类6小时缓存的来源依赖、到期、损坏、共享与并发回归。"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import io
import pickle
import time
import zipfile

import pytest

from zsdtdx.cache.block_catalog_cache import (
    block_catalog_cache_path,
    build_block_catalog_snapshot,
    load_block_catalog_snapshot,
    save_block_catalog_snapshot,
)
from zsdtdx.cache.block_file_cache import BLOCK_FILE_TTL_SECONDS, save_block_files
from zsdtdx.engine.unified_client import UnifiedTdxClient
from zsdtdx.kline_task import BlockKlineTask


def _files():
    """两套同名行业含不同股票，额外含无代码的指数成分。"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("tdxzs.cfg", "元器件|880492|2|1|0|T1204\n".encode("gbk"))
        archive.writestr("tdxzs3.cfg", "元器件|881333|12|1|0|X4004\n".encode("gbk"))
        archive.writestr("spblock.dat", "#补充板块\n0000001\n".encode("gbk"))
    return {
        "infoharbor_block.dat": "#ZS_独立指数,1,,\n0#000001,\n".encode("gbk"),
        "tdxhy.cfg": "0|000001|T1204|||\n1|600000||||X400401\n".encode("gbk"),
        "zhb.zip": buffer.getvalue(),
    }


def _client(directory):
    """构造不会真实联网的客户端。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = directory
    client._record_failure = lambda *args, **kwargs: None
    client.get_stock_code_name_map = lambda: {
        "sz.000001": "平安银行",
        "sh.600000": "浦发银行",
    }
    client._download_named_hq_file = lambda name: _files()[name]
    return client


def test_disk_snapshot_expires_with_source_not_build_time(tmp_path):
    """原文已缓存5小时才建图，图只再有效1小时；跨午夜不失效。"""
    last = datetime(2026, 10, 30, 23, 59, 50).timestamp()
    snapshot = build_block_catalog_snapshot(
        _files(), {"sz.000001": "平安银行"}, now=last + 5 * 3600, source_fetched_at=last
    )
    save_block_catalog_snapshot(snapshot, tmp_path)
    assert (
        load_block_catalog_snapshot(tmp_path, now=last + BLOCK_FILE_TTL_SECONDS - 1)
        is not None
    )
    assert (
        load_block_catalog_snapshot(tmp_path, now=last + BLOCK_FILE_TTL_SECONDS) is None
    )
    assert load_block_catalog_snapshot(tmp_path, now=last - 1) is None
    assert (
        load_block_catalog_snapshot(tmp_path, now=last + 1, name_scope="不同范围")
        is None
    )


@pytest.mark.parametrize(
    "change", ["version", "identity", "relation", "kind", "empty_names"]
)
def test_corrupt_snapshot_rejected(tmp_path, change):
    """结构损坏、标记错位和空股票码表不能成为有效缓存。"""
    snapshot = build_block_catalog_snapshot(_files(), {"sz.000001": "平安银行"})
    if change == "version":
        snapshot["version"] = -1
    elif change == "identity":
        snapshot["records"][0]["identity"] = ("概念", "错位")
    elif change == "relation":
        snapshot["stock_concepts"]["map"]["平安银行"] = [("概念", "不存在")]
    elif change == "kind":
        snapshot["records"][0]["kind"] = "DQ"
    else:
        snapshot["code_name"] = {}
    block_catalog_cache_path(tmp_path).write_bytes(pickle.dumps(snapshot))
    assert load_block_catalog_snapshot(tmp_path) is None


def test_all_three_interfaces_share_disk_graph_without_sources(tmp_path):
    """首次落盘完整图，另一客户端的目录/归属/路由都只读同一文件。"""
    first = _client(tmp_path)
    names = first.get_block_names()
    concepts = first.get_stock_concepts()
    assert concepts["map"]["平安银行"] == [
        ("指数成分", "独立指数"),
        ("补充成分", "补充板块"),
        ("通达信基础行业", "元器件"),
    ]
    assert concepts["map"]["浦发银行"] == [("研究行业中类", "元器件")]
    path = block_catalog_cache_path(tmp_path)
    original = path.read_bytes()
    second = _client(tmp_path)
    second._download_named_hq_file = lambda *a: pytest.fail(
        "映射缓存命中后不能下载源文件"
    )
    second.get_stock_code_name_map = lambda: pytest.fail("映射缓存命中后不能重新拉码表")
    assert second.get_block_names() == names
    assert second.get_stock_concepts() == concepts
    tasks = second.prepare_block_kline_tasks(
        [
            BlockKlineTask(i, "d", "2026-10-01", "2026-10-30").to_dict()
            for i in [("通达信基础行业", "元器件"), ("研究行业中类", "元器件")]
        ]
    )
    assert [task["_index_route_code"] for task in tasks] == ["880492", "881333"]
    assert path.read_bytes() == original
    # 返回对象修改不会污染共享图。
    concepts["map"].clear()
    assert second.get_stock_concepts()["map"]


def test_memory_and_disk_expired_graph_refresh_names(tmp_path):
    """同一客户端不能继续保留已超过6小时的旧名字或旧归属。"""
    previous = time.time() - 86400
    stale = build_block_catalog_snapshot(
        _files(), {"sz.000001": "昨天名称"}, now=previous, name_scope="{}"
    )
    save_block_catalog_snapshot(stale, tmp_path)
    client = _client(tmp_path)
    client._block_catalog_snapshot = stale
    current = client.get_stock_concepts()
    assert "平安银行" in current["map"]
    assert "昨天名称" not in current["map"]
    assert (
        load_block_catalog_snapshot(tmp_path, name_scope="{}")["cache_date"]
        == datetime.now().date().isoformat()
    )


def test_partial_files_return_uncached_classified_membership(tmp_path):
    """缺少来源可以给股票归属部分结果，但不得落成完整映射缓存。"""
    client = _client(tmp_path)
    client._download_named_hq_file = (
        lambda n: _files()[n] if n == "infoharbor_block.dat" else b""
    )
    payload = client.get_stock_concepts()
    assert payload["map"]["平安银行"] == [("指数成分", "独立指数")]
    assert not block_catalog_cache_path(tmp_path).exists()


def test_yesterday_raw_files_within_six_hours_reused(monkeypatch, tmp_path):
    """跨午夜但原始文件仍有效时优先复用，不因自然日变化重复下载。"""
    now = datetime.now().replace(hour=0, minute=1, second=0, microsecond=0).timestamp()
    monkeypatch.setattr(time, "time", lambda: now)
    previous = datetime.fromtimestamp(now).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(seconds=1)
    # 模拟午夜前的原始文件且尚无统一图。
    save_block_files(_files(), fetched_at=previous.timestamp(), cache_dir=tmp_path)
    import os
    from zsdtdx.cache.block_file_cache import block_file_cache_path

    os.utime(block_file_cache_path(tmp_path), (now, now))
    client = _client(tmp_path)
    downloaded = []

    def download(name):
        downloaded.append(name)
        return _files()[name]

    client._download_named_hq_file = download
    client.get_block_names()
    assert downloaded == []
    assert client._block_catalog_snapshot["source_fetched_at"] == previous.timestamp()
    assert (
        load_block_catalog_snapshot(tmp_path, name_scope="{}")["cache_date"]
        == datetime.now().date().isoformat()
    )


def test_parallel_clients_only_build_once_and_write_failure_keeps_memory(
    monkeypatch, tmp_path
):
    """多个客户端冷启动只下载一次；映射缓存写失败时同客户端仍复用有效期内内存图。"""
    clients = [_client(tmp_path) for _ in range(4)]
    calls = []
    for client in clients:
        client.get_stock_code_name_map = lambda: (
            calls.append(1) or {"sz.000001": "平安银行"}
        )
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda c: c.get_block_names(), clients))
    assert all(result == results[0] for result in results)
    assert len(calls) == 1
    path = block_catalog_cache_path(tmp_path)
    path.unlink()
    client = _client(tmp_path)
    monkeypatch.setattr(
        "zsdtdx.engine.unified_client.save_block_catalog_snapshot",
        lambda *args: (_ for _ in ()).throw(OSError("只读")),
    )
    names = client.get_block_names()
    client.get_stock_code_name_map = lambda: pytest.fail("有效期内内存图应复用")
    assert client.get_block_names() == names
    assert not path.exists()


def test_source_expiry_refreshes_files_even_when_graph_built_recently(
    monkeypatch, tmp_path
):
    """映射虽然刚重建，依赖原文一旦到期仍需先下载原文再建图。"""
    current = [time.time()]
    monkeypatch.setattr(time, "time", lambda: current[0])
    start = current[0]
    save_block_files(_files(), fetched_at=start, cache_dir=tmp_path)
    import os
    from zsdtdx.cache.block_file_cache import block_file_cache_path

    os.utime(block_file_cache_path(tmp_path), (start - 1, start - 1))
    client = _client(tmp_path)
    downloads = []
    client._download_named_hq_file = lambda n: (downloads.append(n) or _files()[n])
    first = client.get_block_names()
    current[0] = start + 5 * 3600
    block_catalog_cache_path(tmp_path).unlink()
    client._block_catalog_snapshot = None
    assert client.get_block_names() == first
    assert downloads == []
    assert client._block_catalog_snapshot["source_fetched_at"] == start
    current[0] = start + BLOCK_FILE_TTL_SECONDS
    assert client.get_block_names() == first
    assert set(downloads) == set(_files())
    assert client._block_catalog_snapshot["source_fetched_at"] == current[0]


@pytest.mark.parametrize("advance", [0, 3600])
def test_external_raw_refresh_invalidates_memory_and_disk_graph(
    monkeypatch, tmp_path, advance
):
    """其它调用更新原始缓存后，当前客户端不能继续返回旧图，也不重复下载。"""
    current = [time.time()]
    monkeypatch.setattr(time, "time", lambda: current[0])
    client = _client(tmp_path)
    assert ("指数成分", "独立指数") in client.get_block_names()
    before = client._block_catalog_snapshot
    current[0] += advance
    changed = _files()
    changed["infoharbor_block.dat"] = "#ZS_刷新指数,1,,\n0#000001,\n".encode("gbk")
    save_block_files(changed, fetched_at=current[0], cache_dir=tmp_path)
    import os
    from zsdtdx.cache.block_file_cache import block_file_cache_path

    os.utime(block_file_cache_path(tmp_path), (current[0] - 1, current[0] - 1))
    client._download_named_hq_file = lambda *a: pytest.fail("有效新原文应直接复用")
    names = client.get_block_names()
    assert ("指数成分", "刷新指数") in names
    assert ("指数成分", "独立指数") not in names
    assert client._block_catalog_snapshot["source_hashes"] != before["source_hashes"]
    assert ("指数成分", "刷新指数") in client.get_stock_concepts()["map"]["平安银行"]


def test_expired_sources_refresh_failure_does_not_return_old_graph(
    monkeypatch, tmp_path
):
    """依赖过期且下载失败时不能把旧映射作为成功数据继续返回。"""
    current = [time.time()]
    monkeypatch.setattr(time, "time", lambda: current[0])
    client = _client(tmp_path)
    assert client.get_block_names()
    current[0] += BLOCK_FILE_TTL_SECONDS
    client._download_named_hq_file = lambda *a: b""
    with pytest.raises(ValueError, match="下载失败"):
        client.get_block_names()
    assert client.get_stock_concepts() == {"names": [], "map": {}}


def test_source_expiring_during_build_is_refreshed(monkeypatch, tmp_path):
    """建图跨过来源到期边界时重新取得原文，不交付刚生成但已过期的映射。"""
    current = [time.time()]
    monkeypatch.setattr(time, "time", lambda: current[0])
    start = current[0]
    save_block_files(
        _files(), fetched_at=start - BLOCK_FILE_TTL_SECONDS + 0.5, cache_dir=tmp_path
    )
    import os
    from zsdtdx.cache.block_file_cache import block_file_cache_path

    os.utime(block_file_cache_path(tmp_path), (start - 1, start - 1))
    original = build_block_catalog_snapshot
    builds = []

    def build(*a, **kw):
        result = original(*a, **kw)
        builds.append(1)
        if len(builds) == 1:
            current[0] += 1
        return result

    monkeypatch.setattr(
        "zsdtdx.engine.unified_client.build_block_catalog_snapshot", build
    )
    client = _client(tmp_path)
    downloads = []
    client._download_named_hq_file = lambda n: (downloads.append(n) or _files()[n])
    assert client.get_block_names()
    assert set(downloads) == set(_files())
    assert client._block_catalog_snapshot["source_fetched_at"] == current[0]
