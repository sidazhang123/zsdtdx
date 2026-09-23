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
    # 扩展行情码表短页结束阈值
    EXTENDED_INSTRUMENT_INFO_PAGE_SIZE = 800
    # K 线向更早历史翻页的次数上限
    MAX_KLINE_PAGES = 400
    # 公司信息正文单页字节，服务端上限 30720
    COMPANY_INFO_CHUNK_SIZE = 30720
    # 标准行情命名文件 0x06B9 单页字节
    NAMED_FILE_CHUNK_SIZE = 30000
    # 标准行情码表单条 37 字节（名称字段 16 字节 GBK）
    SECURITY_LIST_RECORD_SIZE = 37

    # 北京股票代码前缀（标准行情 market=2）
    BEIJING_CODE_PREFIXES = ("92",)
    # 深圳 / 上海 A 股代码前缀
    STOCK_PREFIX_SZ = ("000", "001", "002", "003", "300", "301", "302")
    STOCK_PREFIX_SH = ("600", "601", "603", "605", "688", "689")
    # 纳入商品期货的扩展市场名称
    FUTURE_MARKET_NAMES = ("郑州商品", "大连商品", "上海期货", "广州期货")
    # 场内 ETF/LOF 远程名称文件与板块成分文件
    ETF_NAME_REMOTE_FILE = "infoharbor_ex.name"
    ETF_BOARD_REMOTE_FILES = ("spec/specetfdata.txt", "spec/speclofdata.txt")
    # 标准行情命名板块文件：概念/风格/指数，板块名对股票代码
    INFOHARBOR_BLOCK_REMOTE_FILE = "infoharbor_block.dat"
    # 股票到通达信行业码、研究行业码
    TDXHY_REMOTE_FILE = "tdxhy.cfg"
    # 行业板块名称表打在该 zip 内，成员为 tdxzs.cfg；长名补表成员为 ilong.dat
    ZHB_ZIP_REMOTE_FILE = "zhb.zip"
    TDXZS_ZIP_MEMBER = "tdxzs.cfg"
    ILONG_ZIP_MEMBER = "ilong.dat"
