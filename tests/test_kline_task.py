"""离线验收 K 线任务类聚合与包入口导出。"""

from __future__ import annotations

import zsdtdx
from zsdtdx.kline_task import (
    BlockKlineTask,
    IndexKlineTask,
    KlineTask,
    StockKlineTask,
)


def test_task_classes_live_in_kline_task_module():
    """输入无。输出三类任务均继承 KlineTask 且定义在 kline_task。边界：不从抓取模块再定义类。"""
    assert issubclass(StockKlineTask, KlineTask)
    assert issubclass(IndexKlineTask, KlineTask)
    assert issubclass(BlockKlineTask, KlineTask)
    assert StockKlineTask.__module__ == "zsdtdx.kline_task"
    assert IndexKlineTask.__module__ == "zsdtdx.kline_task"
    assert BlockKlineTask.__module__ == "zsdtdx.kline_task"


def test_package_exports_task_classes_not_supported_markets():
    """输入包入口。输出可导入三类任务，且不再对外导出 get_supported_markets。"""
    assert zsdtdx.StockKlineTask is StockKlineTask
    assert zsdtdx.IndexKlineTask is IndexKlineTask
    assert zsdtdx.BlockKlineTask is BlockKlineTask
    assert "get_supported_markets" not in zsdtdx.__all__
    assert not hasattr(zsdtdx, "get_supported_markets")


def test_params_holds_domain_constants_moved_from_call_sites():
    """输入 TDXParams。输出港股通名、tdxzs3、日期默认时刻与期货品种表均已集中。"""
    from zsdtdx.params import TDXParams

    assert TDXParams.HK_EX_MARKET_NAME == "港股通"
    assert TDXParams.TDXZS3_ZIP_MEMBER == "tdxzs3.cfg"
    assert TDXParams.STOCK_DATE_ONLY_START_TIME == "09:30:00"
    assert TDXParams.STOCK_DATE_ONLY_END_TIME == "16:00:00"
    assert TDXParams.FUTURE_DATE_ONLY_START_TIME == "09:00:00"
    assert TDXParams.FUTURE_DATE_ONLY_END_TIME == "15:00:00"


def test_stock_task_to_dict_normalizes_date_only_window():
    """输入仅日期窗口。输出补齐 09:30:00 / 16:00:00，freq 规范为小写协议值。"""
    payload = StockKlineTask(
        code="600000",
        freq="D",
        start_time="2026-02-13",
        end_time="2026-02-13",
    ).to_dict()
    assert payload == {
        "code": "600000",
        "freq": "d",
        "start_time": "2026-02-13 09:30:00",
        "end_time": "2026-02-13 16:00:00",
    }


def test_block_and_index_task_symbol_fields():
    """输入指数与板块任务。输出各自名称字段进入字典。边界：字段名互不混用。"""
    index_payload = IndexKlineTask(
        index_name="上证指数",
        freq="d",
        start_time="2026-02-13",
        end_time="2026-02-13",
    ).to_dict()
    block_payload = BlockKlineTask(
        block_name="汽车拆解",
        freq="d",
        start_time="2026-02-13",
        end_time="2026-02-13",
    ).to_dict()
    assert index_payload["index_name"] == "上证指数"
    assert "code" not in index_payload
    assert block_payload["block_name"] == "汽车拆解"
    assert "index_name" not in block_payload
