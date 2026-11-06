"""
模块：`test_region_block_kline.py`。

职责：验证地区 K 线全部周期与别名的路由和缓存重载，不改变六类公开目录。
边界：只替换文件下载与码表，不访问网络；真实协议验收另由手工脚本执行。
"""

import io
import zipfile

import pytest

from zsdtdx.cache.block_catalog_cache import (
    build_block_catalog_snapshot,
    load_block_catalog_snapshot,
    save_block_catalog_snapshot,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient
from zsdtdx.kline_task import BlockKlineTask
from zsdtdx.parser.block_index_catalog import (
    parse_block_index_catalog,
    published_block_names,
)
from zsdtdx.util.helper import TASK_FREQ_MAP


def _files(extra=""):
    """输入可选额外地区行，输出有效三文件；同名概念与地区分别拥有指数代码。"""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "tdxzs.cfg", ("深圳板块|880203|3|1|0|18\n" + extra).encode("gbk")
        )
        archive.writestr("tdxzs3.cfg", b"header")
    return {
        "infoharbor_block.dat": "#GN_深圳板块,1,880999,\n0#000001,\n".encode("gbk"),
        "tdxhy.cfg": b"header",
        "zhb.zip": stream.getvalue(),
    }


@pytest.mark.parametrize("freq", list(TASK_FREQ_MAP))
def test_region_all_frequency_tasks_route_without_finance(freq):
    """输入地区的任一周期或别名，输出归一化0x0523路由；不拉股票财务解析指数。"""
    catalog = parse_block_index_catalog(_files())
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._load_block_index_catalog = lambda: catalog
    task = BlockKlineTask(
        ["地区", "深圳板块"], freq, "2025-01-01", "2026-11-06"
    ).to_dict()
    result = client.prepare_block_kline_tasks([task])[0]
    assert result["block_name"] == ("地区", "深圳板块")
    assert result["freq"] == TASK_FREQ_MAP[freq]
    assert result["_index_route_code"] == "880203"
    assert result["_index_route_market"] == 1
    assert result["_index_route_source"] == "std"
    assert result["_block_no_kline"] == ""
    assert result["start_time"] == "2025-01-01 09:30:00"
    assert result["end_time"] == "2026-11-06 16:00:00"
    assert client.resolve_block_index_route(("概念", "深圳板块"))["code"] == "880999"
    assert client.get_block_names() == [("概念", "深圳板块")]


def test_region_route_survives_disk_reload(tmp_path):
    """输入完整含地区映射缓存，重载后仍能解析地区但不额外生成股票地区归属。"""
    snapshot = build_block_catalog_snapshot(_files(), {"sz.000001": "平安银行"})
    save_block_catalog_snapshot(snapshot, tmp_path)
    loaded = load_block_catalog_snapshot(tmp_path)
    assert loaded is not None
    catalog = parse_block_index_catalog(_files())
    from zsdtdx.parser.block_index_catalog import BlockIndexCatalog

    restored = BlockIndexCatalog(loaded["records"])
    assert restored.by_name[("地区", "深圳板块")]["code"] == "880203"
    assert published_block_names(restored) == published_block_names(catalog)
    assert loaded["stock_regions"] is None
    assert all(category != "地区" for category, _ in loaded["stock_concepts"]["names"])


def test_ambiguous_region_code_is_not_guessed():
    """输入同分类同名不同代码，输出不可确定路由；未知地区仍明确报错。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    catalog = parse_block_index_catalog(_files("深圳板块|880204|3|1|0|19\n"))
    client._load_block_index_catalog = lambda: catalog
    route = client.resolve_block_index_route(("地区", "深圳板块"))
    assert route["code"] == ""
    assert route["empty_reason"] == "ambiguous_code"
    with pytest.raises(ValueError, match="未知板块"):
        client.resolve_block_index_route(("地区", "不存在地区"))
