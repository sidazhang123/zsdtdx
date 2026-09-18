"""
模块：`exhq.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 13 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

from zsdtdx.base_socket_client import BaseSocketClient, update_last_ack_time
from zsdtdx.params import TDXParams
from zsdtdx.parser.ex_get_history_instrument_bars_range import (
    GetHistoryInstrumentBarsRange,
)
from zsdtdx.parser.ex_get_history_minute_time_data import GetHistoryMinuteTimeData
from zsdtdx.parser.ex_get_history_transaction_data import GetHistoryTransactionData
from zsdtdx.parser.ex_get_instrument_bars import GetInstrumentBars
from zsdtdx.parser.ex_get_instrument_count import GetInstrumentCount
from zsdtdx.parser.ex_get_instrument_info import GetInstrumentInfo
from zsdtdx.parser.ex_get_instrument_quote import GetInstrumentQuote
from zsdtdx.parser.ex_get_instrument_quote_list import GetInstrumentQuoteList
from zsdtdx.parser.ex_get_markets import GetMarkets
from zsdtdx.parser.ex_get_minute_time_data import GetMinuteTimeData
from zsdtdx.parser.ex_get_transaction_data import GetTransactionData
from zsdtdx.parser.ex_setup_commands import ExSetupCmd1


class TdxExHq_API(BaseSocketClient):
    def setup(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setup` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        ExSetupCmd1(self.client).call_api()

    # API LIST

    @update_last_ack_time
    def get_markets(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_markets` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetMarkets(self.client)
        return cmd.call_api()

    @update_last_ack_time
    def get_instrument_count(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_instrument_count` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetInstrumentCount(self.client)
        return cmd.call_api()

    @update_last_ack_time
    def get_instrument_quote(self, market, code):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_instrument_quote` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetInstrumentQuote(self.client)
        cmd.setParams(market, code)
        return cmd.call_api()

    @update_last_ack_time
    def get_instrument_bars(
        self,
        category,
        market,
        code,
        start=0,
        count=TDXParams.MAX_EXTENDED_KLINE_COUNT,
        qfq=False,
    ):
        """
        输入：
        1. category: K 线周期。
        2. market/code: 扩展市场与代码。
        3. start: 分页偏移，0 为最新窗口。
        4. count: 本页条数，默认 700（服务端硬上限）。
        5. qfq: 港股前复权；True extra=1，False extra=0。期货忽略该字段。
        输出：
        1. 本页 K 线 dict 列表。
        用途：
        1. 发送 0xD808/0xD908 请求并解析回包。
        边界条件：
        1. 请求 count>700 时服务端仍最多返回 700 条。
        2. 默认 qfq=False（extra=0）；港股由 get_stock_kline 传入 True/False。
        """
        cmd = GetInstrumentBars(self.client)
        cmd.setParams(category, market, code, start=start, count=count, qfq=qfq)
        return cmd.call_api()

    @update_last_ack_time
    def get_minute_time_data(self, market, code):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_minute_time_data` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetMinuteTimeData(self.client)
        cmd.setParams(market, code)
        return cmd.call_api()

    @update_last_ack_time
    def get_history_minute_time_data(self, market, code, date):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. date: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_history_minute_time_data` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetHistoryMinuteTimeData(self.client)
        cmd.setParams(market, code, date=date)
        return cmd.call_api()

    @update_last_ack_time
    def get_transaction_data(self, market, code, start=0, count=1800):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. start: 输入参数，约束以协议定义与函数实现为准。
        4. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_transaction_data` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetTransactionData(self.client)
        cmd.setParams(market, code, start=start, count=count)
        return cmd.call_api()

    @update_last_ack_time
    def get_history_transaction_data(self, market, code, date, start=0, count=1800):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. date: 输入参数，约束以协议定义与函数实现为准。
        4. start: 输入参数，约束以协议定义与函数实现为准。
        5. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_history_transaction_data` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetHistoryTransactionData(self.client)
        cmd.setParams(market, code, date, start=start, count=count)
        return cmd.call_api()

    @update_last_ack_time
    def get_history_instrument_bars_range(self, market, code, start, end):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. start: 输入参数，约束以协议定义与函数实现为准。
        4. end: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_history_instrument_bars_range` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetHistoryInstrumentBarsRange(self.client)
        cmd.setParams(market, code, start, end)
        return cmd.call_api()

    @update_last_ack_time
    def get_instrument_info(self, start, count=None):
        """
        输入：
        1. start: 本页起始下标。
        2. count: 保留参数，服务端按自身页长返回。
        输出：
        1. 本页合约记录列表（market/code/name）。
        用途：
        1. 拉取扩展行情码表单页，供港股、期货与扩展指数目录分页。
        边界条件：
        1. 仅单页；完整目录需由调用方按返回条数累加 start，直到空页。
        """
        cmd = GetInstrumentInfo(self.client)
        cmd.setParams(start, count)
        return cmd.call_api()

    @update_last_ack_time
    def get_instrument_quote_list(self, market, category, start=0, count=80):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. category: 输入参数，约束以协议定义与函数实现为准。
        3. start: 输入参数，约束以协议定义与函数实现为准。
        4. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_instrument_quote_list` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetInstrumentQuoteList(self.client)
        cmd.setParams(market, category, start, count)
        return cmd.call_api()
