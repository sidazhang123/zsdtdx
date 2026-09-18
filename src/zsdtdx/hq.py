"""
模块：`hq.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 20 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

from collections import OrderedDict

import pandas as pd

from zsdtdx.base_socket_client import BaseSocketClient, update_last_ack_time
from zsdtdx.params import TDXParams
from zsdtdx.parser.get_company_info_category import GetCompanyInfoCategory
from zsdtdx.parser.get_company_info_content import GetCompanyInfoContent
from zsdtdx.parser.get_finance_info import GetFinanceInfo
from zsdtdx.parser.get_history_minute_time_data import GetHistoryMinuteTimeData
from zsdtdx.parser.get_history_transaction_data import GetHistoryTransactionData
from zsdtdx.parser.get_index_bars import GetIndexBarsCmd
from zsdtdx.parser.get_minute_time_data import GetMinuteTimeData
from zsdtdx.parser.get_report_file import GetReportFile
from zsdtdx.parser.get_security_bars import GetSecurityBarsCmd
from zsdtdx.parser.get_security_count import GetSecurityCountCmd
from zsdtdx.parser.get_security_list import GetSecurityList
from zsdtdx.parser.get_security_quotes import GetSecurityQuotesCmd
from zsdtdx.parser.get_transaction_data import GetTransactionData
from zsdtdx.parser.get_xdxr_info import GetXdXrInfo
from zsdtdx.parser.setup_commands import SetupCmd1, SetupCmd2, SetupCmd3


class TdxHq_API(BaseSocketClient):
    def setup(self):
        """
        输入：无。
        输出：无。
        用途：连接成功后发送三条标准行情握手。
        边界：任一条握手失败时由 `connect()` 断开连接并抛错，不会继续发行情请求。
        """
        # 292 字节首包 → 13 字节第二包 → tdxlevel 身份包。
        SetupCmd1(self.client).call_api()
        SetupCmd2(self.client).call_api()
        SetupCmd3(self.client).call_api()

    def get_markets(self):
        """
        返回标准行情支持的市场定义。

        输入:
            无。
        输出:
            list[OrderedDict]: 标准行情市场列表。
        用途:
            供统一封装层发现标准行情的市场能力。
        边界条件:
            该方法不发起网络请求，固定返回深圳、上海、北京三个市场。
        """
        return [
            OrderedDict(
                [
                    ("market", TDXParams.MARKET_SZ),
                    ("category", 1),
                    ("name", "深圳"),
                    ("short_name", "SZ"),
                ]
            ),
            OrderedDict(
                [
                    ("market", TDXParams.MARKET_SH),
                    ("category", 1),
                    ("name", "上海"),
                    ("short_name", "SH"),
                ]
            ),
            OrderedDict(
                [
                    ("market", TDXParams.MARKET_BJ),
                    ("category", 1),
                    ("name", "北京"),
                    ("short_name", "BJ"),
                ]
            ),
        ]

    # API List

    # Notice：，如果一个股票当天停牌，那天的K线还是能取到，成交量为0
    @update_last_ack_time
    def get_security_bars(self, category, market, code, start, count, qfq=True):
        """
        输入：
        1. category: K 线周期；1 分钟=7，日线=4。
        2. market: 市场号（0 深圳 / 1 上海 / 2 北京）。
        3. code: 证券代码。
        4. start: 分页偏移，0 表示从最新一段起取。
        5. count: 本页条数，服务端硬上限 800。
        6. qfq: True 请求前复权，False 请求不复权。
        输出：
        1. 已解析的 K 线 dict 列表；失败时为 `None`。
        用途：
        1. 发送 54 字节 0x052D 个股 K 线请求并解析回包。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetSecurityBarsCmd(self.client, lock=self.lock)
        cmd.setParams(category, market, code, start, count, qfq=qfq)
        return cmd.call_api()

    @update_last_ack_time
    def get_index_bars(self, category, market, code, start, count):
        """
        输入：
        1. category: K 线周期；1 分钟=7，日线=4。
        2. market: 市场号（0 深圳 / 1 上海 / 2 北京）。
        3. code: 指数代码。
        4. start: 分页偏移，0 表示从最新一段起取。
        5. count: 本页条数，服务端硬上限 800。
        输出：
        1. 已解析的指数 K 线 dict 列表；失败时为 `None`。
        用途：
        1. 发送 54 字节 0x052D 指数 K 线请求并解析回包。
        边界条件：
        1. 指数无复权语义，请求 reserved0 固定为 0。
        2. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetIndexBarsCmd(self.client, lock=self.lock)
        cmd.setParams(category, market, code, start, count)
        return cmd.call_api()

    @update_last_ack_time
    def get_security_quotes(self, all_stock, code=None):
        """
        支持三种形式的参数
        get_security_quotes(market, code )
        get_security_quotes((market, code))
        get_security_quotes([(market1, code1), (market2, code2)] )
        :param all_stock （market, code) 的数组
        :param code{optional} code to query
        :return:
        """

        if code is not None:
            all_stock = [(all_stock, code)]
        elif (
            (isinstance(all_stock, list) or isinstance(all_stock, tuple))
            and len(all_stock) == 2
            and type(all_stock[0]) is int
        ):
            all_stock = [all_stock]

        cmd = GetSecurityQuotesCmd(self.client, lock=self.lock)
        cmd.setParams(all_stock)
        return cmd.call_api()

    @update_last_ack_time
    def get_security_count(self, market):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_security_count` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetSecurityCountCmd(self.client, lock=self.lock)
        cmd.setParams(market)
        return cmd.call_api()

    @update_last_ack_time
    def get_security_list(self, market, start, count=None):
        """
        输入：
        1. market: 标准市场号（0 深圳 / 1 上海 / 2 北京）。
        2. start: 本页起始下标。
        3. count: 本页条数，默认 1600。
        输出：
        1. 本页证券记录列表；无数据时为空列表或 `None`（由底层 allow_none 决定）。
        用途：
        1. 拉取标准行情码表单页，供深沪京股票与 HQ 指数目录分页。
        边界条件：
        1. 仅单页；完整市场需由调用方按返回条数累加 start，直到不足一页。
        """
        cmd = GetSecurityList(self.client, lock=self.lock)
        cmd.setParams(market, start, count)
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
        cmd = GetMinuteTimeData(self.client, lock=self.lock)
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
        cmd = GetHistoryMinuteTimeData(self.client, lock=self.lock)
        cmd.setParams(market, code, date)
        return cmd.call_api()

    @update_last_ack_time
    def get_transaction_data(self, market, code, start, count):
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
        cmd = GetTransactionData(self.client, lock=self.lock)
        cmd.setParams(market, code, start, count)
        return cmd.call_api()

    @update_last_ack_time
    def get_history_transaction_data(self, market, code, start, count, date):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. start: 输入参数，约束以协议定义与函数实现为准。
        4. count: 输入参数，约束以协议定义与函数实现为准。
        5. date: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_history_transaction_data` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetHistoryTransactionData(self.client, lock=self.lock)
        cmd.setParams(market, code, start, count, date)
        return cmd.call_api()

    @update_last_ack_time
    def get_company_info_category(self, market, code):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_company_info_category` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetCompanyInfoCategory(self.client, lock=self.lock)
        cmd.setParams(market, code)
        return cmd.call_api()

    @update_last_ack_time
    def get_company_info_content(
        self, market, code, filename, start, length, category_index
    ):
        """
        输入：
        1. market: 标准市场编号。
        2. code: 股票代码。
        3. filename: 目录返回的文件名。
        4. start: 本页文件内偏移。
        5. length: 从 start 起的剩余总字节。
        6. category_index: 目录记录下标。
        输出：
        1. 本页 GBK 正文；失败由底层抛错或返回空。
        用途：
        1. 发送一页公司信息正文请求。
        边界条件：
        1. 单页服务端上限 30720 字节；完整正文需由上层按剩余 length 继续请求。
        """
        cmd = GetCompanyInfoContent(self.client, lock=self.lock)
        cmd.setParams(market, code, filename, start, length, category_index)
        return cmd.call_api()

    @update_last_ack_time
    def get_xdxr_info(self, market, code):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_xdxr_info` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetXdXrInfo(self.client, lock=self.lock)
        cmd.setParams(market, code)
        return cmd.call_api()

    @update_last_ack_time
    def get_finance_info(self, market, code):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_finance_info` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetFinanceInfo(self.client, lock=self.lock)
        cmd.setParams(market, code)
        return cmd.call_api()

    @update_last_ack_time
    def get_report_file(self, filename, offset):
        """
        输入：
        1. filename: 输入参数，约束以协议定义与函数实现为准。
        2. offset: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_report_file` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = GetReportFile(self.client, lock=self.lock)
        cmd.setParams(filename, offset)
        return cmd.call_api()

    def get_report_file_by_size(self, filename, filesize=0, reporthook=None):
        """
        Download file from proxy server

        :param filename the filename to download
        :param filesize the filesize to download , if you do not known the actually filesize, leave this value 0
        """
        filecontent = bytearray()
        current_downloaded_size = 0
        get_zero_length_package_times = 0
        while current_downloaded_size < filesize or filesize == 0:
            response = self.get_report_file(filename, current_downloaded_size)
            if not response or not isinstance(response, dict):
                break
            if response["chunksize"] > 0:
                current_downloaded_size = (
                    current_downloaded_size + response["chunksize"]
                )
                filecontent.extend(response["chunkdata"])
                if reporthook is not None:
                    reporthook(current_downloaded_size, filesize)
            else:
                get_zero_length_package_times = get_zero_length_package_times + 1
                if filesize == 0:
                    break
                elif get_zero_length_package_times > 2:
                    break

        return filecontent

    def get_k_data(self, code, start_date, end_date):
        # 具体详情参见 https://github.com/rainx/zsdtdx/issues/5
        # 具体详情参见 https://github.com/rainx/zsdtdx/issues/21
        """
        输入：
        1. code: 输入参数，约束以协议定义与函数实现为准。
        2. start_date: 输入参数，约束以协议定义与函数实现为准。
        3. end_date: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_k_data` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """

        def __select_market_code(code):
            """
            输入：
            1. code: 输入参数，约束以协议定义与函数实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `__select_market_code` 对应的协议处理、数据解析或调用适配逻辑。
            边界条件：
            1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
            """
            code = str(code)
            if code[0] in ["5", "6", "9"] or code[:3] in [
                "009",
                "126",
                "110",
                "201",
                "202",
                "203",
                "204",
            ]:
                return 1
            return 0

        # https://github.com/rainx/zsdtdx/issues/33
        # 0 - 深圳， 1 - 上海

        data = pd.concat(
            [
                self.to_df(
                    self.get_security_bars(
                        TDXParams.KLINE_TYPE_DAILY,
                        __select_market_code(code),
                        code,
                        (9 - i) * TDXParams.MAX_KLINE_COUNT,
                        TDXParams.MAX_KLINE_COUNT,
                    )
                )
                for i in range(10)
            ],
            axis=0,
        )

        data = (
            data.assign(date=data["datetime"].apply(lambda x: str(x)[0:10]))
            .assign(code=str(code))
            .set_index("date", drop=False, inplace=False)
            .drop(["datetime"], axis=1)[start_date:end_date]
        )
        return data.assign(date=data["date"].apply(lambda x: str(x)[0:10]))
