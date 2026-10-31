"""股票归属契约回归：缓存/容错一致、同分类去重、跨分类隔离和可变返回隔离。"""

import io
import pickle
import zipfile

import pytest

from zsdtdx.cache.block_catalog_cache import (
    block_catalog_cache_path,
    load_block_catalog_snapshot,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient


def _files(text):
    """输入成分正文，输出有效三文件；目录只含无关板块，不替测试标题推断全称。"""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("tdxzs.cfg", "目录占位|880900|4|2|0|目录占位\n".encode("gbk"))
        archive.writestr("tdxzs3.cfg", b"header")
    return {
        "infoharbor_block.dat": text.encode("gbk"),
        "tdxhy.cfg": b"header",
        "zhb.zip": stream.getvalue(),
    }


def _client(directory, files):
    """输入独立目录和夹具，输出不访问网络的真实客户端方法宿主。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = directory
    client._record_failure = lambda *a, **kw: None
    client.get_stock_code_name_map = lambda: {
        "sz.000001": "甲股",
        "sz.000002": "乙股",
        "sh.600000": "丙股",
    }
    client._download_named_hq_file = lambda name: files[name]
    return client


@pytest.mark.parametrize(
    "text, expected",
    [
        (
            "#GN_甲名字,1,880999,\n0#000001,\n#GN_乙名字,1,880999,\n0#000002,\n",
            {"甲股": [("概念", "甲名字")], "乙股": [("概念", "乙名字")]},
        ),
        (
            "#GN_甲名字,1,880999,\n0#000001,\n#GN_甲名字,1,880998,\n0#000002,\n#GN_乙名字,1,880999,\n1#600000,\n",
            {
                "甲股": [("概念", "甲名字")],
                "乙股": [("概念", "甲名字")],
                "丙股": [("概念", "乙名字")],
            },
        ),
        (
            "#GN_中药,2,880999,\n0#000001,0#000002,\n#GN_中成药,2,880998,\n0#000001,0#000002,\n",
            {name: [("概念", "中成药"), ("概念", "中药")] for name in ["甲股", "乙股"]},
        ),
        (
            "#GN_同名,1,880999,\n0#000001,\n#FG_同名,1,880998,\n0#000001,\n",
            {"甲股": [("概念", "同名"), ("风格", "同名")]},
        ),
        (
            "#GN_正常概念,1,880999,\n0#000001,\n#XX_未知分类,1,880998,\n0#000002,\n",
            {"甲股": [("概念", "正常概念")]},
        ),
    ],
)
def test_complete_partial_and_disk_reload_have_same_memberships(
    tmp_path, text, expected
):
    """无关文件缺失不改变有效归属；缓存重载仍保持原文选名和原去重语义。"""
    files = _files(text)
    complete = _client(tmp_path / "full", files)
    payload = complete.get_stock_concepts()
    assert payload["map"] == expected
    assert payload["names"] == sorted(
        {i for values in expected.values() for i in values}
    )
    partial = _client(tmp_path / "partial", {**files, "tdxhy.cfg": b""})
    assert partial.get_stock_concepts() == payload
    assert not block_catalog_cache_path(tmp_path / "partial").exists()
    cold = _client(tmp_path / "full", files)
    cold.get_stock_code_name_map = lambda: pytest.fail("有效缓存不应再拉码表")
    cold._download_named_hq_file = lambda *a: pytest.fail("有效原文应复用")
    assert cold.get_stock_concepts() == payload
    assert ("概念", "目录占位") in cold.get_block_names()
    if "中成药" in text:
        assert ("概念", "中药") in cold.get_block_names()
        assert ("概念", "中成药") in cold.get_block_names()


def test_mutating_result_does_not_modify_shared_graph(tmp_path):
    """仅复制可变容器仍能隔离名称列表、归属列表和字典，叶子二元组保持不可变。"""
    files = _files("#GN_正常概念,1,880999,\n0#000001,\n")
    client = _client(tmp_path, files)
    payload = client.get_stock_concepts()
    original = block_catalog_cache_path(tmp_path).read_bytes()
    payload["names"].clear()
    payload["map"]["甲股"].append(("概念", "注入"))
    payload["map"]["不存在"] = []
    again = client.get_stock_concepts()
    assert again == {
        "names": [("概念", "正常概念")],
        "map": {"甲股": [("概念", "正常概念")]},
    }
    assert block_catalog_cache_path(tmp_path).read_bytes() == original


def test_empty_code_names_not_fetched_twice_or_cached(tmp_path):
    """已经取得但为空的码表不再次获取，也不把部分归属写成完整缓存。"""
    files = _files("#GN_正常概念,1,880999,\n0#000001,\n")
    client = _client(tmp_path, files)
    calls = []
    client.get_stock_code_name_map = lambda: (calls.append(1) or {})
    assert client.get_stock_concepts()["map"] == {}
    assert len(calls) == 1
    assert client._block_pending_code_name is None
    assert client._block_partial_named_files == {}
    assert not block_catalog_cache_path(tmp_path).exists()


def test_old_derived_schema_not_used_after_semantic_fix(tmp_path):
    """旧v2没有规范化归属块，不能在修复后继续命中旧结果。"""
    client = _client(tmp_path, _files("#GN_正常概念,1,880999,\n0#000001,\n"))
    client.get_stock_concepts()
    path = block_catalog_cache_path(tmp_path)
    snapshot = pickle.loads(path.read_bytes())
    snapshot["version"] = 2
    path.write_bytes(pickle.dumps(snapshot))
    assert load_block_catalog_snapshot(tmp_path, name_scope="{}") is None
