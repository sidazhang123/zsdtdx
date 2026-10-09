"""港股 K 线负成交额归零回归。"""

import datetime as dt

import pandas as pd

from zsdtdx.engine.unified_client import UnifiedTdxClient


def _normalizer_client() -> UnifiedTdxClient:
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._normalize_freq = lambda freq: str(freq)
    client._stock_code_with_prefix_no_df = lambda **kwargs: str(kwargs["code"])
    client._stock_code_with_prefix = lambda **kwargs: str(kwargs["code"])
    client._filter_placeholder_ohlc_equal_rows = lambda frame: frame
    return client


def test_hk_task_rows_clamp_negative_amount_without_changing_volume():
    """港股 task 路径仅归零负成交额，成交量保持服务端原值。"""
    client = _normalizer_client()
    bar_time = dt.datetime(2026, 9, 15, 10, 30)
    rows = [
        {
            "open": 5.32,
            "high": 5.39,
            "low": 5.32,
            "close": 5.34,
            "trade": 40,
            "amount": -5138,
            "datetime": "2026-09-15 10:30:00",
            "_ts": int(bar_time.timestamp()),
        }
    ]

    result = client._normalize_stock_kline_rows(
        rows=rows,
        source="ex",
        market=31,
        code="00045",
        freq="15",
        start_dt=dt.datetime(2026, 9, 14),
        end_dt=dt.datetime(2026, 9, 18, 23, 59, 59),
    )

    assert result[0]["amount"] == 0
    assert result[0]["volume"] == 40


def test_hk_dataframe_path_clamps_negative_amount_without_changing_volume():
    """港股 DataFrame 兼容路径与 task 路径保持相同契约。"""
    client = _normalizer_client()
    frame = pd.DataFrame(
        [
            {
                "open": 0.31,
                "high": 0.31,
                "low": 0.31,
                "close": 0.31,
                "trade": 25,
                "amount": -63718,
                "datetime": "2026-09-17 10:15:00",
            }
        ]
    )

    result = client._normalize_stock_kline_fields(
        df=frame,
        source="ex",
        market=31,
        code="00410",
        freq="15",
    )

    assert result.iloc[0]["amount"] == 0
    assert result.iloc[0]["volume"] == 25


def test_standard_stock_amount_is_not_clamped():
    """负值归零只作用于扩展行情股票，不改变标准行情语义。"""
    client = _normalizer_client()
    frame = pd.DataFrame(
        [
            {
                "open": 10,
                "high": 10,
                "low": 10,
                "close": 10,
                "vol": 5,
                "amount": -1,
                "datetime": "2026-09-17 10:15:00",
            }
        ]
    )

    result = client._normalize_stock_kline_fields(
        df=frame,
        source="std",
        market=1,
        code="600000",
        freq="15",
    )

    assert result.iloc[0]["amount"] == -1
    assert result.iloc[0]["volume"] == 5
