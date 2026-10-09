"""
模块：`biz/future_kline.py`。

职责：
1. 标准化期货代码与周期后，交给并行抓取器的 DataFrame 批处理入口。

边界：
1. 无独立 FutureKlineTask；入参仍为 codes/freq/start_time/end_time。
2. 不解析扩展行情包，不管理连接池。
3. 不依赖 `simple_api`。
"""

from __future__ import annotations

from typing import Any, List, Optional, Union

import pandas as pd

from zsdtdx.biz._client_context import call_with_main_client
from zsdtdx.util.helper import normalize_future_time_window


def fetch_future_kline(
    codes: Optional[Any] = None,
    freq: Union[str, List[str], None] = None,
    start_time: Any = None,
    end_time: Any = None,
) -> pd.DataFrame:
    """
    输入期货代码与周期，输出合并后的 K 线 DataFrame。

    输入：codes/freq/start_time/end_time 与 get_future_kline 相同。
    输出：字段含 code/freq/open/close/high/low/settlement_price/volume/datetime。
    用途：标准化后走 ParallelKlineFetcher.fetch_future_kline。
    边界条件：
    1. freq 为空列表或类型非法时抛 ValueError。
    2. codes 标准化后为空时返回空 DataFrame。
    3. codes 为 None 时拉全量商品期货列表。
    """
    if freq is None:
        freq_list: List[Any] = ["d"]
    elif isinstance(freq, str):
        freq_list = [freq]
    elif not isinstance(freq, (list, tuple)):
        raise ValueError("freq 必须是字符串或列表，如 'd' 或 ['d', '60']")
    elif len(freq) == 0:
        raise ValueError("freq 列表不能为空")
    else:
        freq_list = list(freq)

    if codes is None:
        codes = call_with_main_client(
            lambda client: list(
                client.get_all_future_list(return_df=True)["code"].tolist()
            ),
            caller_name="get_future_kline",
        )
    elif isinstance(codes, str):
        codes = [codes]
    else:
        codes = list(codes)

    if len(codes) == 0:
        return pd.DataFrame()

    from zsdtdx.engine.parallel_fetcher import get_fetcher

    fetcher = get_fetcher()
    normalized_start_time = str(start_time) if start_time is not None else None
    normalized_end_time = str(end_time) if end_time is not None else None
    if start_time is not None and end_time is not None:
        normalized_start_time, normalized_end_time = normalize_future_time_window(
            start_time, end_time
        )

    return fetcher.fetch_future_kline(
        codes=codes,
        freqs=list(freq_list),
        start_time=normalized_start_time,
        end_time=normalized_end_time,
    )
