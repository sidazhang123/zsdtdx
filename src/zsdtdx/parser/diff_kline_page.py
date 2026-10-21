"""
模块：`parser/diff_kline_page.py`。

职责：
1. 标准行情 K 线单页解码（个股/指数差分；板块指数绝对价，字段布局同构）。
2. 解码后调用 helper 向量化格式化，作为 socket 层唯一数值刻度出口。

边界：
1. 处理 get_security_bars / get_index_bars / get_block_bars 同构回包，不承担分页。
2. 时间：本层从二进制解析年月日时分，写入 datetime 为 `YYYY-MM-DD HH:MM:SS`（秒位固定 `:00`）与 `_ts`；不向 dict 返回分列字段。
3. socket 层以上（unified_client 等）只消费 datetime/_ts，不再解析 year/month/day/hour/minute。
4. `absolute_ohlc=True` 时 OHLC 四字段各自 `/1000` 为绝对价（0x0523 板块指数）；默认 False 为差分（0x052D）。
"""

# coding=utf-8

from __future__ import annotations


def parse_diff_encoded_kline_page(
    body_buf,
    category: int,
    *,
    with_index_counts: bool = False,
    absolute_ohlc: bool = False,
    volume_scale: float = 1.0,
    ieee_volume: bool = True,
):
    """输入标准页及格式，输出行列表或上下文指定的列页；坏包抛错。

    ``volume_scale`` 只用于 0x052D 已取证的服务端周期单位归一化；
    0x0523 板块等未验收协议保持默认 1，不在共用解码器内猜测业务类型。
    """
    from zsdtdx.parser.kline_columns import columnar_decode, parse_standard_columns

    page = parse_standard_columns(
        body_buf,
        category,
        with_index_counts=with_index_counts,
        absolute_ohlc=absolute_ohlc,
        volume_scale=volume_scale,
        ieee_volume=ieee_volume,
    )
    return page if columnar_decode.get() else page.raw_rows()


def standard_kline_volume_scale(category: int, *, is_index: bool) -> float:
    """返回标准 0x052D 成交量到最小成交单位的倍率。"""

    period = int(category)
    if period in {5, 6}:
        # 银河周/月线成交量以手返回；公开 K 线统一还原为日线相同的最小单位。
        return 100.0
    if is_index and period in {0, 1, 2, 3}:
        # 标准指数 5/15/30/60 分钟量比最小单位放大 100 倍。
        return 0.01
    return 1.0
