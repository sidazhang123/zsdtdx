"""离线验收 simple_api K 线契约：新增 qfq 关键字，默认前复权，task 结构不变。"""

import inspect
from unittest.mock import MagicMock, patch

from zsdtdx.simple_api import get_future_kline, get_index_kline, get_stock_kline


_STOCK_TASK = {
    "code": "600000",
    "freq": "d",
    "start_time": "2026-02-13",
    "end_time": "2026-02-13",
}
_INDEX_TASK = {
    "index_name": "上证指数",
    "freq": "d",
    "start_time": "2026-02-13",
    "end_time": "2026-02-13",
}


def test_get_stock_kline_qfq_is_last_param_default_true():
    """输入：函数签名；输出：qfq 在末尾且默认 True，原位置参数顺序不变。"""
    params = inspect.signature(get_stock_kline).parameters
    assert list(params) == [
        "task",
        "queue",
        "preprocessor_operator",
        "mode",
        "qfq",
    ]
    assert params["qfq"].default is True
    assert params["qfq"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD


def test_get_index_kline_has_no_qfq():
    """输入：函数签名；输出：指数入口不暴露 qfq。"""
    params = inspect.signature(get_index_kline).parameters
    assert "qfq" not in params
    assert list(params)[:4] == ["task", "queue", "preprocessor_operator", "mode"]


def test_get_future_kline_has_no_qfq():
    """输入：函数签名；输出：期货入口不暴露 qfq。"""
    params = inspect.signature(get_future_kline).parameters
    assert "qfq" not in params


def test_get_stock_kline_sync_forwards_default_qfq():
    """输入：不传 qfq；输出：fetcher 收到 qfq=True。"""
    with (
        patch("zsdtdx.simple_api._ensure_active_config_ready"),
        patch("zsdtdx.simple_api.get_fetcher") as get_fetcher,
    ):
        fetcher = MagicMock()
        fetcher.fetch_stock_tasks_sync.return_value = []
        get_fetcher.return_value = fetcher
        get_stock_kline(task=[dict(_STOCK_TASK)], mode="sync")
        kwargs = fetcher.fetch_stock_tasks_sync.call_args.kwargs
        assert kwargs["qfq"] is True
        task0 = kwargs["tasks"][0]
        assert set(task0) == {"code", "freq", "start_time", "end_time"}


def test_get_stock_kline_sync_forwards_qfq_false():
    """输入：qfq=False；输出：fetcher 收到 False。"""
    with (
        patch("zsdtdx.simple_api._ensure_active_config_ready"),
        patch("zsdtdx.simple_api.get_fetcher") as get_fetcher,
    ):
        fetcher = MagicMock()
        fetcher.fetch_stock_tasks_sync.return_value = []
        get_fetcher.return_value = fetcher
        get_stock_kline(task=[dict(_STOCK_TASK)], mode="sync", qfq=False)
        assert fetcher.fetch_stock_tasks_sync.call_args.kwargs["qfq"] is False


def test_get_index_kline_sync_does_not_forward_qfq():
    """输入：指数 sync；输出：fetcher 调用不含 qfq。"""
    fake_client = MagicMock()
    fake_client.resolve_index_name.return_value = {
        "source": "std",
        "market": 1,
        "code": "000001",
        "name": "上证指数",
    }

    def _run_with_client(fn, **_kwargs):
        return fn(fake_client)

    with (
        patch("zsdtdx.simple_api._ensure_active_config_ready"),
        patch("zsdtdx.simple_api.get_fetcher") as get_fetcher,
        patch("zsdtdx.simple_api._call_with_client", side_effect=_run_with_client),
    ):
        fetcher = MagicMock()
        fetcher.fetch_index_tasks_sync.return_value = []
        get_fetcher.return_value = fetcher
        get_index_kline(task=[dict(_INDEX_TASK)], mode="sync")
        kwargs = fetcher.fetch_index_tasks_sync.call_args.kwargs
        assert "qfq" not in kwargs


def test_std_page_fetch_honors_qfq_false():
    """输入：_fetch_kline_page_rows_no_df(qfq=False)；输出：pool.call 带 qfq=False。"""
    from zsdtdx.unified_client import UnifiedTdxClient

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    mock_pool = MagicMock()
    mock_pool.call.return_value = []
    client.std_pool = mock_pool
    client._fetch_kline_page_rows_no_df(
        source="std",
        market=1,
        code="600000",
        category=4,
        start=0,
        page_size=420,
        qfq=False,
    )
    assert mock_pool.call.call_args.kwargs.get("qfq") is False


def test_ex_page_fetch_honors_qfq():
    """输入：扩展行情港股页抓取；输出：pool.call 带对应 qfq。"""
    from zsdtdx.unified_client import UnifiedTdxClient

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    mock_pool = MagicMock()
    mock_pool.call.return_value = []
    client.ex_pool = mock_pool
    client._fetch_kline_page_rows_no_df(
        source="ex",
        market=31,
        code="00700",
        category=4,
        start=0,
        page_size=700,
        qfq=True,
    )
    assert mock_pool.call.call_args.kwargs.get("qfq") is True
    client._fetch_kline_page_rows_no_df(
        source="ex",
        market=31,
        code="00700",
        category=4,
        start=0,
        page_size=700,
        qfq=False,
    )
    assert mock_pool.call.call_args.kwargs.get("qfq") is False
