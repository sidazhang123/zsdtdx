"""
模块：`test_stock_concept_regions.py`。

职责：
1. 验证股票地区归属、缓存复用、来源刷新与查询失败重试。
2. 验证地区不进入板块目录与 K 线路由。

边界：
1. 使用真实缓存和客户端业务路径，仅替换码表、命名文件与财务查询，不联网。
"""

from concurrent.futures import ThreadPoolExecutor
import io
import pickle
import zipfile

import pytest

from zsdtdx.cache.block_catalog_cache import (
    block_catalog_cache_path,
    load_block_catalog_snapshot,
)
from zsdtdx.cache.block_file_cache import save_block_files
from zsdtdx.engine.unified_client import UnifiedTdxClient
from zsdtdx.parser.stock_regions import merge_stock_regions


def _files(region="深圳板块"):
    """输出含地区目录、概念和行业的三文件；空成分地区不应出现在股票归属。"""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "tdxzs.cfg",
            (
                "银行|880471|2|1|0|T0101\n"
                f"{region}|880203|3|1|0|18\n"
                "上海板块|880200|3|1|0|1\n"
                "空地区|880201|3|1|0|99\n"
            ).encode("gbk"),
        )
        archive.writestr("tdxzs3.cfg", b"header")
    return {
        "infoharbor_block.dat": "#GN_深圳板块,1,880999,\n0#000001,\n".encode("gbk"),
        "tdxhy.cfg": b"0|000001|T0101|||\n",
        "zhb.zip": stream.getvalue(),
    }


def _client(directory, files=None):
    """输出离线客户端，覆盖沪深北、港股通、零码和未知地区；财务配置刻意超过协议上限。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = directory
    client.config = {"stock_stat": {"finance_batch_size": 1000, "finance_workers": 20}}
    client.failures = []
    client._record_failure = lambda *args: client.failures.append(args)
    client.get_stock_code_name_map = lambda: {
        "sz.000001": "平安银行",
        "sh.600000": "浦发银行",
        "bj.920001": "北交股票",
        "sz.000002": "零码股票",
        "sz.000003": "未知地区股票",
        "hk.00700": "腾讯控股",
    }
    files = _files() if files is None else files
    client._download_named_hq_file = lambda name: files[name]
    client.finance_calls = []

    def fetch(codes, batch_size, workers):
        client.finance_calls.append((codes, batch_size, workers))
        values = {"000001": 18, "600000": 1, "920001": 18, "000002": 0, "000003": 999}
        return {code: {"_region_code": values[code]} for _, code in codes}

    client.get_finance_info_batch_map = fetch
    return client


def test_region_memberships_are_lazy_and_do_not_change_block_routes(tmp_path):
    """目录查询不拉财务；归属加入地区后目录、原分类及 K 线路由保持原有行为。"""
    client = _client(tmp_path)
    before = client.get_block_names()
    assert client.finance_calls == []
    payload = client.get_stock_concepts()
    assert payload["map"]["平安银行"] == [
        ("地区", "深圳板块"),
        ("概念", "深圳板块"),
        ("通达信基础行业", "银行"),
    ]
    assert payload["map"]["浦发银行"] == [("地区", "上海板块")]
    assert payload["map"]["北交股票"] == [("地区", "深圳板块")]
    assert "零码股票" not in payload["map"]
    assert "未知地区股票" not in payload["map"]
    assert "腾讯控股" not in payload["map"]
    assert ("地区", "空地区") not in payload["names"]
    assert payload["names"] == sorted(set(payload["names"]))
    codes, size, workers = client.finance_calls[0]
    assert set(codes) == {
        (0, "000001"),
        (1, "600000"),
        (2, "920001"),
        (0, "000002"),
        (0, "000003"),
    }
    assert (size, workers) == (100, 8)
    assert client.get_block_names() == before
    assert client.resolve_block_index_route(("概念", "深圳板块"))["code"] == "880999"
    with pytest.raises(ValueError, match="未知板块分类"):
        client.resolve_block_index_route(("地区", "深圳板块"))


def test_region_cache_reused_across_clients_and_mutation_isolated(tmp_path):
    """成功地区随完整缓存跨客户端复用，调用方修改结果不能污染后续归属。"""
    first = _client(tmp_path)
    expected = first.get_stock_concepts()
    payload = first.get_stock_concepts()
    payload["names"].clear()
    payload["map"]["浦发银行"].clear()
    assert first.get_stock_concepts() == expected
    assert len(first.finance_calls) == 1
    cold = _client(tmp_path)
    cold.get_stock_code_name_map = lambda: pytest.fail("不应刷新有效码表")
    cold._download_named_hq_file = lambda name: pytest.fail("不应重复下载原文")
    cold.get_finance_info_batch_map = lambda *args: pytest.fail("不应重复查询地区")
    assert cold.get_stock_concepts() == expected


@pytest.mark.parametrize("failure", ["network", "missing", "invalid"])
def test_failed_region_query_keeps_other_blocks_and_retries(tmp_path, failure):
    """网络失败、财务缺行和无效 DY 不记为地区缓存成功；下一调用仍查询。"""
    client = _client(tmp_path)
    original = client.get_finance_info_batch_map

    def broken(*args):
        if failure == "network":
            raise RuntimeError("财务断线")
        result = original(*args)
        if failure == "missing":
            result.pop("600000")
        else:
            result["600000"]["_region_code"] = None
        return result

    client.get_finance_info_batch_map = broken
    partial = client.get_stock_concepts()
    assert partial["map"]["平安银行"] == [
        ("概念", "深圳板块"),
        ("通达信基础行业", "银行"),
    ]
    assert all(kind != "地区" for kind, _ in partial["names"])
    assert (
        load_block_catalog_snapshot(tmp_path, name_scope="{}")["stock_regions"] is None
    )
    assert any(row[:2] == ("stock_concepts", "regions") for row in client.failures)
    client.get_finance_info_batch_map = original
    assert ("地区", "深圳板块") in client.get_stock_concepts()["names"]


def test_partial_named_files_still_return_regions_without_persisting(tmp_path):
    """概念文件缺失时地区与行业仍可返回；部分三文件结果不写完整缓存。"""
    client = _client(tmp_path, {**_files(), "infoharbor_block.dat": b""})
    payload = client.get_stock_concepts()
    assert payload["map"]["平安银行"] == [
        ("地区", "深圳板块"),
        ("通达信基础行业", "银行"),
    ]
    assert not block_catalog_cache_path(tmp_path).exists()


def test_source_refresh_rebuilds_region_memberships(tmp_path):
    """原文地区名更新立即失效旧地区缓存，不从财务查询时刻延长原文有效期。"""
    client = _client(tmp_path)
    assert ("地区", "深圳板块") in client.get_stock_concepts()["names"]
    save_block_files(_files("新深圳板块"), cache_dir=tmp_path)
    payload = client.get_stock_concepts()
    assert ("地区", "新深圳板块") in payload["names"]
    assert ("地区", "深圳板块") not in payload["names"]
    assert len(client.finance_calls) == 2


def test_parallel_clients_share_one_region_query(tmp_path):
    """独立客户端同时补齐地区时，通过磁盘锁复用成功结果。"""
    clients = [_client(tmp_path) for _ in range(3)]
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(
            executor.map(lambda client: client.get_stock_concepts(), clients)
        )
    assert results[0] == results[1] == results[2]
    assert sum(len(client.finance_calls) for client in clients) == 1


def test_corrupt_region_cache_is_rebuilt(tmp_path):
    """缓存中码表外代码或未知地区名称不被当作有效归属读取。"""
    client = _client(tmp_path)
    expected = client.get_stock_concepts()
    path = block_catalog_cache_path(tmp_path)
    snapshot = pickle.loads(path.read_bytes())
    snapshot["stock_regions"]["sz.999999"] = "伪造地区"
    path.write_bytes(pickle.dumps(snapshot))
    assert load_block_catalog_snapshot(tmp_path, name_scope="{}") is None
    assert _client(tmp_path).get_stock_concepts() == expected


def test_empty_region_mapping_is_a_successful_cache(tmp_path):
    """全部返回零地区码时，缓存成功空结果，避免每次重新查询财务。"""
    client = _client(tmp_path)
    calls = []

    def fetch(codes, *args):
        calls.append(codes)
        return {code: {"_region_code": 0} for _, code in codes}

    client.get_finance_info_batch_map = fetch
    payload = client.get_stock_concepts()
    assert client.get_stock_concepts() == payload
    assert len(calls) == 1
    assert load_block_catalog_snapshot(tmp_path, name_scope="{}")["stock_regions"] == {}


def test_same_stock_name_merges_region_memberships():
    """股票重名时取地区并集，已有同分类归属去重；原始返回容器不被修改。"""
    payload = {
        "names": [("地区", "上海板块")],
        "map": {"同名股票": [("地区", "上海板块")]},
    }
    result = merge_stock_regions(
        payload,
        {"sz.000001": "同名股票", "sh.600000": "同名股票"},
        {"sz.000001": "深圳板块", "sh.600000": "上海板块"},
    )
    assert result["map"]["同名股票"] == [("地区", "上海板块"), ("地区", "深圳板块")]
    assert payload["map"]["同名股票"] == [("地区", "上海板块")]
