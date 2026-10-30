"""六类板块身份、同名路由、空行情与队列交付的离线回归。"""

import io
import queue
import zipfile
from types import SimpleNamespace

import numpy as np
import pytest

from zsdtdx.block_identity import BLOCK_KIND_NAMES, normalize_block_identity
from zsdtdx.engine.columnar_kline import fetch_chunk_columns, materialize_task_columns
from zsdtdx.engine.parallel_fetcher import ParallelKlineFetcher
from zsdtdx.engine.unified_client import UnifiedTdxClient
from zsdtdx.kline_task import BlockKlineTask
from zsdtdx.parser.block_index_catalog import (
    BlockIndexCatalog,
    parse_block_index_catalog,
)
from zsdtdx.parser.kline_columns import KlineColumns


def _files():
    """包含同名双行业、空概念、无代码指数、无股票补充段以及统计风格。"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(
            "tdxzs.cfg",
            "元器件|880492|2|1|0|T1204\n历史新高|880640|5|2|0|历史新高\n空概念|880900|4|2|0|空概念\n".encode(
                "gbk"
            ),
        )
        archive.writestr("tdxzs3.cfg", "元器件|881333|12|1|0|X4004\n".encode("gbk"))
        archive.writestr("spblock.dat", "#独立补充\n#空概念\n".encode("gbk"))
    return {
        "infoharbor_block.dat": "#ZS_独立指数,0,,\n".encode("gbk"),
        "tdxhy.cfg": b"hy",
        "zhb.zip": buf.getvalue(),
    }


def test_all_six_categories_and_same_name_routes():
    """目录保留无成分/无代码，双行业同名分别路由；SP不冒充概念分类。"""
    catalog = parse_block_index_catalog(_files())
    assert {r["kind"] for r in catalog.by_name.values()} == set(BLOCK_KIND_NAMES)
    assert catalog.by_name[("通达信基础行业", "元器件")]["code"] == "880492"
    assert catalog.by_name[("研究行业中类", "元器件")]["code"] == "881333"
    assert ("风格", "历史新高") in catalog.names
    assert catalog.by_name[("指数成分", "独立指数")]["empty_reason"] == "no_code"
    assert catalog.by_name[("补充成分", "空概念")]["kind"] == "SP"
    assert catalog.by_name[("补充成分", "空概念")]["code"] == "880900"
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._load_block_index_catalog = lambda: catalog
    tasks = client.prepare_block_kline_tasks(
        [
            BlockKlineTask(identity, "d", "2026-10-01", "2026-10-30").to_dict()
            for identity in [("通达信基础行业", "元器件"), ("研究行业中类", "元器件")]
        ]
    )
    assert [t["_index_route_code"] for t in tasks] == ["880492", "881333"]


@pytest.mark.parametrize(
    "value", ["元器件", ("未知", "元器件"), ("概念", ""), ("概念",), (1, "元器件")]
)
def test_invalid_identity_rejected(value):
    """不再接受裸名称，不通过字符串化掩盖错误输入。"""
    with pytest.raises(ValueError):
        BlockKlineTask(value, "d", "2026-10-01", "2026-10-30").to_dict()


def test_json_list_identity_and_code_collision():
    """JSON二元素数组规范为tuple，同分类同名多代码不静默选择。"""
    assert normalize_block_identity(["概念", "汽车拆解"]) == ("概念", "汽车拆解")
    catalog = BlockIndexCatalog(
        [{"name": "同名", "kind": "GN", "code": code} for code in ["880001", "880002"]]
    )
    assert catalog.names == [("概念", "同名")]
    assert catalog.by_name[catalog.names[0]]["empty_reason"] == "ambiguous_code"


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_no_code_returns_successful_empty_without_connecting(monkeypatch, mode):
    """缺代码在调度前交付空行情，异步成功计数与done也应正确。"""
    from zsdtdx import engine

    fetcher = ParallelKlineFetcher()
    fetcher.num_threads = 1
    monkeypatch.setattr(fetcher, "_ensure_async_prewarm", lambda **kw: None)
    monkeypatch.setattr(
        engine.unified_client,
        "_ensure_availability_hosts_cache",
        lambda *a, **kw: pytest.fail("无代码不应触发连接探测"),
    )
    task = {
        **BlockKlineTask(
            ("指数成分", "独立指数"), "d", "2026-10-01", "2026-10-30"
        ).to_dict(),
        "_block_no_kline": "no_code",
    }
    if mode == "sync":
        results = fetcher.fetch_block_tasks_sync(tasks=[task])
    else:
        events = queue.Queue()
        job = fetcher.fetch_block_tasks_async(tasks=[task], queue=events)
        assert job.result(timeout=3) == {
            "total_tasks": 1,
            "success_tasks": 1,
            "failed_tasks": 0,
        }
        results = [events.get(timeout=1)]
        assert events.get(timeout=1)["event"] == "done"
    assert results[0]["error"] is None
    assert results[0]["rows"] == []
    assert results[0]["task"]["block_name"] == task["block_name"]


def test_empty_server_page_success_and_transport_error_preserved(monkeypatch):
    """服务器合法空页返回成功；连接异常不能伪装为空数据。"""
    import zsdtdx.engine.columnar_kline as module

    task = BlockKlineTask(("概念", "空概念"), "d", "2026-10-01", "2026-10-30").to_dict()
    client = SimpleNamespace(
        _freq_to_category=lambda f: 4,
        _to_datetime_no_df=lambda s: __import__("datetime").datetime.fromisoformat(s),
        output_cfg={},
    )
    monkeypatch.setattr(module, "_resolve_route", lambda *a: ("880900", {}))
    monkeypatch.setattr(
        module,
        "_page",
        lambda *a: (KlineColumns({"_ts": np.array([], dtype=np.int64)}), 800),
    )
    result = fetch_chunk_columns(client, [task], "block")
    assert result["results"][0]["error"] is None
    assert materialize_task_columns(client, result["results"][0]) == []
    monkeypatch.setattr(
        module, "_page", lambda *a: (_ for _ in ()).throw(ConnectionError("连接失败"))
    )
    with pytest.raises(ConnectionError):
        fetch_chunk_columns(client, [task], "block")
