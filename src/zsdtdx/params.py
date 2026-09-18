"""
模块：`params.py`。

职责：
1. 定义通达信协议相关的市场、K 线周期等常量。
2. 供 parser 与客户端在组包时引用。

边界：
1. 仅包含常量类 `TDXParams`，不含运行时逻辑。
"""

# coding=utf-8


class TDXParams:
    # 市场

    MARKET_SZ = 0  # 深圳
    MARKET_SH = 1  # 上海
    MARKET_BJ = 2  # 北京

    # K线种类
    # K 线种类
    # 0 -   5 分钟K 线
    # 1 -   15 分钟K 线
    # 2 -   30 分钟K 线
    # 3 -   1 小时K 线
    # 4 -   日K 线
    # 5 -   周K 线
    # 6 -   月K 线
    # 7 -   1 分钟
    # 8 -   1 分钟K 线
    # 9 -   日K 线
    # 10 -  季K 线
    # 11 -  年K 线

    KLINE_TYPE_5MIN = 0
    KLINE_TYPE_15MIN = 1
    KLINE_TYPE_30MIN = 2
    KLINE_TYPE_1HOUR = 3
    KLINE_TYPE_DAILY = 4
    KLINE_TYPE_WEEKLY = 5
    KLINE_TYPE_MONTHLY = 6
    # 1 分钟 category=7；8 为同义别名。
    KLINE_TYPE_EXHQ_1MIN = 7
    KLINE_TYPE_1MIN = 8
    # 日线 category=4；9 为同义别名。
    KLINE_TYPE_RI_K = 9
    KLINE_TYPE_3MONTH = 10
    KLINE_TYPE_YEARLY = 11

    # 标准行情 K 线单页上限 800
    MAX_KLINE_COUNT = 800
    # 扩展行情 K 线单页上限 700
    MAX_EXTENDED_KLINE_COUNT = 700
    # 标准行情码表 0x044D 单页条数
    MAX_SECURITY_LIST_COUNT = 1600
    # 标准行情码表单条记录字节数
    SECURITY_LIST_RECORD_SIZE = 37
