"""zsdtdx 最外层简明封装入口。

职责：
1. 对外暴露 get_* 风格 API，隐藏连接池、市场路由、分页与并行调度细节。
2. 统一 task 输入（StockKlineTask / IndexKlineTask / BlockKlineTask 或等价 dict）并完成时间窗口标准化。
3. 提供同步/异步两种股票/指数/板块指数 K 线任务获取模式。
4. 暴露并行进程池生命周期管理入口（prewarm/restart/destroy）。

数据契约：
1. 股票/指数/板块任务仅传日期时，start/end 分别补齐为 09:30:00 / 16:00:00；期货为 09:00:00 / 15:00:00。
2. K 线 rows 的 datetime 统一为 YYYY-MM-DD HH:MM:SS，秒位固定 :00。

调用约定：
1. 程序启动阶段先调用 `set_config_path()` 设置配置路径；后续各接口无需重复传参。
   用户 YAML 可为不完整：以包内默认为底深合并覆盖同名键，丢弃未知字段，列表整段替换。
2. 若未调用 `set_config_path()`，首次调用相关接口会打印提醒并回退到包内默认配置。
3. `get_stock_kline` / `get_index_kline` / `get_block_kline` 的 sync/async 不要求前置 `with get_client()`；
   抓取连接由并行层进程内常驻 client 管理，不复用 with 内主进程 client。
   指数/板块在解析名称路由时若无 with 会临时建连。同流程若还要调码表等主进程 API，可包在 with 内。
4. `mode="async"` 的 K 线抓取在 worker 子进程内执行；`mode="sync"` 在主进程 inproc 调度，但仍用抓取器常驻 client。
5. `get_future_kline` 走 DataFrame 批处理：进程数 > 1 时 worker 并行（不占用主进程连接），
   ≤ 1 时串行并可复用 with 内连接；建议 with 以便同块调用其它主进程 API。
6. async 模式通过 `job.queue` 消费到 `event="done"`。
7. 冷启动可手动 `prewarm_parallel_fetcher()`；异常恢复 `restart_parallel_fetcher()`；退出前 `destroy_parallel_fetcher()`。
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

from zsdtdx.biz._client_context import call_with_main_client
from zsdtdx.biz.block_kline import fetch_block_kline
from zsdtdx.biz.company_info import fetch_company_info
from zsdtdx.biz.future_kline import fetch_future_kline
from zsdtdx.biz.index_kline import fetch_index_kline
from zsdtdx.biz.stock_kline import fetch_stock_kline
from zsdtdx.biz.stock_stat import fetch_stock_stat
from zsdtdx.engine.parallel_fetcher import (
    StockKlineJob,
    get_fetcher,
)
from zsdtdx.engine.parallel_fetcher import (
    destroy_parallel_fetcher as _destroy_parallel_fetcher,
)
from zsdtdx.engine.parallel_fetcher import (
    force_restart_parallel_fetcher as _force_restart_parallel_fetcher,
)
from zsdtdx.engine.parallel_fetcher import (
    prewarm_parallel_fetcher as _prewarm_parallel_fetcher,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient
from zsdtdx.kline_task import BlockKlineTask, IndexKlineTask, StockKlineTask
from zsdtdx.util.helper import (
    _apply_active_config_path,
    _ensure_active_config_ready,
)


# ---------- 配置与客户端 ----------
def set_config_path(config_path: str, async_background_probe: bool = True) -> str:
    """
    设置 simple_api 全局配置路径（主进程与并行 worker 统一生效）。

    输入:
    - config_path: 配置文件路径（建议在程序启动阶段调用一次）。
      可为不完整 YAML：以包内默认 `config.yaml` 为底深合并覆盖同名键，
      丢弃内置不存在的字段；列表字段整段替换。
    - async_background_probe: 缓存不可用时是否在后台线程预热 TCP 可用地址缓存。

    输出:
    - 解析后的绝对配置路径字符串（指向用户文件；内存中生效的是合并后的配置）。

    调用示例:
    ```python
    from zsdtdx import set_config_path

    set_config_path(r"D:\\configs\\zsdtdx.yaml")
    ```

    边界条件:
    - 配置非法时会直接抛出异常，调用方可在启动阶段尽早失败。
    - async_background_probe=True 时函数立即返回，探测在后台完成。
    """
    return _apply_active_config_path(
        config_path=config_path,
        async_background_probe=async_background_probe,
    )


def get_client(
    separate_instance: bool = False,
) -> UnifiedTdxClient:
    """获取客户端实例。

    输入:
    - separate_instance: 是否强制新建独立客户端实例；为 True 时忽略当前 with 上下文并创建新实例。

    输出:
    - UnifiedTdxClient 实例。

    调用示例:
    ```python
    # 一个 with 中复用同一 client，避免重复建连。
    with get_client():
        stock_map = get_stock_code_name()
        board = get_stock_stat()
    ```

    返回示例:
    ```python
    <zsdtdx.engine.unified_client.UnifiedTdxClient object at 0x...>
    ```

    作用边界:
    - 该 client 仅管理“当前主进程”上下文中的连接生命周期（进入 with 预连接，退出 with 自动 close）。
    - `get_stock_kline` / `get_index_kline` / `get_block_kline` 的 sync/async 抓取连接由并行层
      进程内常驻 client 管理，不与此处 with 返回的主进程 client 共用；with 结束也不会关掉它们。
    - 市场列表请用 `with get_client() as client: client.get_supported_markets(...)`，不再提供独立 `get_supported_markets` 封装。

    边界条件:
    - 若用户未显式调用 `set_config_path()`，会回退到默认配置并打印一次提醒。
    - 若在 with 上下文中且 `separate_instance=False`，会优先返回当前上下文客户端。
    - outside-with 直接调用会返回独立客户端实例，建议配合 with 或显式调用 close()。
    """
    active_context_client = UnifiedTdxClient.get_active_context_client()
    if active_context_client is not None and not separate_instance:
        return active_context_client

    new_path = _ensure_active_config_ready(caller_name="get_client")
    return UnifiedTdxClient(config_path=new_path)


# ---------- 码表与目录 ----------
def get_stock_code_name() -> Dict[str, str]:
    """获取统一股票代码名称字典。

    调用前置约定:
    - 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接。

    输入:
    - 无缓存开关：自动复用有效缓存，缺失或过期时重建。
      本函数属于全量代码接口，返回范围由
      `config.yaml.stock_scope.defaults_when_codes_none.get_stock_code_name` 控制
      （包内默认 `szsh+bj`；可增配 `hk` 港股通，五位代码，不含香港主板）。

    输出:
    - 返回 `Dict[str, str]`：key 为带市场前缀的股票代码（`sh./sz./bj./hk.`），
      value 为股票名称；`hk.` 为港股通标的；不返回纯数字代码。

    调用示例:
    ```python
    with get_client():
        stock_map = get_stock_code_name()
    ```

    返回示例:
    ```json
    {"sh.600000": "浦发银行", "sz.000001": "平安银行"}
    ```
    """
    return call_with_main_client(
        lambda client: client.get_stock_code_name_map(),
        caller_name="get_stock_code_name",
    )


def get_stock_concepts() -> Dict[str, Any]:
    """获取股票所属板块（成分归属，不是板块指数名单）。

    调用前置约定:
    - 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接。
    - 与 `get_block_names` 共用三份命名文件，但语义不同：本接口回答「股票属于哪些板块」；
      可请求 K 线的板块指数名请用 `get_block_names`。

    输入:
    - 无。标准行情主机取配置 `hosts.standard`。
      `infoharbor_block.dat` 提供概念、风格、指数；
      `tdxhy.cfg` 提供股票的通达信行业码；
      `zhb.zip` 内的 `tdxzs.cfg` 把行业码换成钢铁、煤炭、水泥等行业板块名。

    输出:
    - `{names, map}`：names 为有成分的板块名（概念、风格、指数、基础行业），去重按字排序；
      map 为股票名称 -> 所属板块名列表（去重按字排序）。
    - 股票名称来自 `get_stock_code_name` 当日码表；缺失或非当日时先按该路径更新。
    - 码表中没有名称的代码不进入 map。本接口自身不写盘；共享板块缓存有效时直接复用。

    调用示例:
    ```python
    with get_client():
        concepts = get_stock_concepts()
    ```

    返回示例:
    ```json
    {"names": ["5G概念", "芯片"], "map": {"中信特钢": ["5G概念"]}}
    ```
    """
    return call_with_main_client(
        lambda client: client.get_stock_concepts(),
        caller_name="get_stock_concepts",
    )


def get_etf_code_name() -> Dict[str, str]:
    """获取场内 ETF/LOF（本语境统称 etf）代码名称字典。

    调用前置约定:
    - 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接。

    输入:
    - 无缓存开关：自动复用当日 `catalog_cache/etf_code_name.pkl`，缺失或过期时重建。
      成分来自 `spec/specetfdata.txt`/`spec/speclofdata.txt`；名称优先
      `infoharbor_ex.name` 与 `zhb.zip`/`ilong.dat` 合并（同码以 ilong 为准），
      缺名回退 std 码表版面短名。另纳入名称文件中命中 etf/lof 的代码；
      排除深指 `399*` 与 `market_rules.etf_name_drop_substr`。
      不读银河安装目录；无当日缓存才下载；任一侧解析为空则不写盘。

    输出:
    - `Dict[str, str]`：`sz.`/`sh.` 前缀代码 -> 名称；不扩宽 `get_stock_code_name` 口径。

    调用示例:
    ```python
    with get_client():
        etf_map = get_etf_code_name()
    ```

    返回示例:
    ```json
    {"sz.159915": "创业板ETF易方达", "sh.510050": "上证50ETF华夏", "sz.159105": "恒生生物科技ETF易方达"}
    ```
    """
    return call_with_main_client(
        lambda client: client.get_etf_code_name_map(),
        caller_name="get_etf_code_name",
    )


def get_block_names() -> List[str]:
    """获取可请求 K 线的「全部板块」指数名称列表。

    调用前置约定:
    - 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接。
    - 与 `get_block_kline` 共用三份命名文件的 6 小时本地缓存。
    - 与 `get_stock_concepts` 文件同源，但本接口是板块指数名单，不是股票归属。

    输入:
    - 无。文件为 `infoharbor_block.dat`、`tdxhy.cfg`、`zhb.zip`（目录来自 zip 内 tdxzs/tdxzs3）。

    输出:
    - 名称列表：概念、非统计风格、地区、研究行业中类；不含指数代码。
      可直接作为 `get_block_kline` 的 `block_name`；历史区间没有行情时返回空 rows，不视为报错。

    调用示例:
    ```python
    with get_client():
        names = get_block_names()
    ```
    """
    return call_with_main_client(
        lambda client: client.get_block_names(),
        caller_name="get_block_names",
    )


def get_all_future_list(return_df: Optional[bool] = None):
    """获取统一商品期货列表（郑州/大连/上海/广州）。

    调用前置约定:
    - 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接。

    输入:
    - return_df: 是否返回 DataFrame；None 时跟随 `output.return_df_default`（包内默认 True）。
    - 无缓存开关：自动复用有效缓存，缺失或过期时重建。

    调用示例:
    ```python
    with get_client():
        future_df = get_all_future_list(return_df=True)
    ```

    返回示例:
    ```json
    [{"code": "CU2603", "name": "沪铜2603", "market_name": "上海期货", "source": "ex"}]
    ```
    """
    return call_with_main_client(
        lambda client: client.get_all_future_list(return_df=return_df),
        caller_name="get_all_future_list",
    )


# ---------- 股票 ----------
def get_stock_kline(
    task: List[Any],
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[
        Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ] = None,
    mode: str = "async",
    qfq: bool = True,
) -> Any:
    """获取股票 K 线任务结果（任务化输入，支持同步/异步与队列实时回传）。

    调用前置约定:
    - sync/async 均不要求前置 `with get_client()`；可选 with 以同块调用其它主进程 API。
    - 抓取连接由并行层进程内常驻 client 管理，不复用 with 内主进程 client。
    - `mode="async"`：在 worker 子进程内抓取；`mode="sync"`：主进程 inproc 调度。
    - `preprocessor_operator`：可选钩子 `f(payload)->dict|None`；返回 None 或空 dict 时丢弃该条；
      OHLC/成交额/成交量默认刻度由协议解析层完成。
    - `qfq`：True 前复权（默认），False 不复权；A 股与港股共用。期货请用 `get_future_kline`。

    时间窗口:
    - start_time/end_time 支持 str/date/datetime；仅日期时补齐 start=09:30:00、end=16:00:00。

    K 线 datetime 输出:
    - rows 中 `datetime` 为 `YYYY-MM-DD HH:MM:SS`，秒位固定 `:00`。

    调用示例（写法一：with + sync + 其它接口）:
    ```python
    import queue as py_queue
    from zsdtdx import StockKlineTask, get_client, get_stock_kline, get_stock_stat

    with get_client():
        board = get_stock_stat()
        q = py_queue.Queue()
        result = get_stock_kline(
            task=[
                StockKlineTask(code="600000", freq="d", start_time="2026-02-13", end_time="2026-02-13"),
                {"code": "000001", "freq": "60", "start_time": "2026-02-13", "end_time": "2026-02-14"},
            ],
            queue=q,
            mode="sync",
        )
    ```

    调用示例（写法二：async + 进程池生命周期）:
    ```python
    from zsdtdx import destroy_parallel_fetcher, get_stock_kline, restart_parallel_fetcher

    job = get_stock_kline(
        task=[{"code": "600000", "freq": "d", "start_time": "2026-02-13", "end_time": "2026-02-13"}],
        mode="async",
    )
    try:
        while True:
            event = job.queue.get(timeout=20)
            if event.get("event") == "done":
                break
        job.result()
    except Exception:
        restart_parallel_fetcher(prewarm=True)
        raise
    finally:
        destroy_parallel_fetcher()
    ```

    连接生命周期:
    - sync：主进程 inproc 调度，连接为抓取器进程内常驻 client；with 结束只关主进程 with client，不关该常驻连接。
    - async：worker 子进程内按任务侧懒建连接；进程池生命周期见 prewarm/restart/destroy。

    返回:
    - mode="sync": `list[task_payload]`（传 queue 时同时推送 data/done）。
    - mode="async": `StockKlineJob`（从 `job.queue` 消费到 done）。
    - task_payload: `{"event":"data","task":{...},"rows":[...],"error":str|None,"worker_pid":int}`。
      示例 rows 元素: `{"code":"sh.600000","freq":"d","open":10.07,"close":10.06,"high":10.25,"low":10.03,"volume":105771232,"amount":1072786048,"datetime":"2026-02-02 15:00:00"}`。
    - done: `{"event":"done","total_tasks":...,"success_tasks":...,"failed_tasks":...}`。
    """
    return fetch_stock_kline(
        task=task,
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        mode=mode,
        qfq=qfq,
    )


def get_stock_stat() -> pd.DataFrame:
    """获取全市场股票统计宽表。

    调用前置约定:
    - 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接。

    输入:
    - 无。分页与财务批次由 `config.yaml` 的 `stock_stat` 控制
      （`page_size`/`finance_batch_size`/`finance_workers`）。
      行情 0x054B；财务/股本 0x0010；多日涨幅与估值等来自 zhb/tdxstat + tdxhy。
      市值/市净/市销/市现用现价（现价非正则回退昨收）。
      市盈率(TTM|静)、股息率、多日涨幅与统计基准日按 zhb/tdxstat 文件原值返回。

    输出:
    - 一行一只 A 股股票；中文表头。金额万元、量手、股本万股；不含指数/港股；无 codes。
    - 返回列说明见 `STOCK_STAT_COLUMN_LABELS` 及 README「字段说明」。
      涨跌额=现价-昨收；涨幅(%)=(现价-昨收)/昨收*100；
      振幅(%)=(最高-最低)/昨收*100；总量(手)=当日成交量；现量(手)=最近分笔量；
      内盘(手)/外盘(手)=主动卖/买；内外比=内盘/外盘；换手(%)=总量/流通股本；
      流通市值/总市值=对应股本×现价；市净率/市销率/市现率=现价÷每股净资、总市值÷营业收入、现价÷每股现金流；
      财报更新日期=财务数据更新日期；净利润=归母；税后利润含少数股东损益。

    调用示例:
    ```python
    with get_client():
        df = get_stock_stat()
    ```

    返回示例（列节选）:
    ```text
    代码    现价  昨收  ...  市盈率(TTM)  5日涨幅(%)  行业
    600000  9.98  9.90  ...  5.2         1.23        银行
    ```
    """
    return fetch_stock_stat()


def get_company_info(
    codes: List[str],
    category: Optional[List[str]] = None,
    mode: str = "async",
    queue: Optional[Any] = None,
    return_df: Optional[bool] = None,
):
    """获取股票公司信息（默认并行；可选顺序）。

    调用前置约定:
    - `mode="sync"`：请先进入 `with get_client():`，主进程顺序拉取。
    - `mode="async"`（默认）：走进程池并行，不依赖主进程 with 连接；
      退出前建议 `destroy_parallel_fetcher()`。

    输入:
    - codes: 股票代码列表（一只也须包在 list 里，如 `["600000"]`）。
    - category: 中文分类名列表；两种 mode 共用同一过滤语义——
      传入则只拉这些分类；为 None/空则拉全部分类。
    - mode: `async`（默认）或 `sync`（无论 codes 长短均顺序跑）。
    - queue: 可选事件队列（需 `put()`）。sync 可边拉边 put；async 不传则自动创建。
    - return_df: 仅对 `mode="sync"` 生效；True 转 DataFrame，False 返回 list[dict]；
      None 跟随 `output.return_df_default`。中间传递始终为 list[dict]，不用 DataFrame。

    返回:
    - mode="sync": `list[dict]` 或 DataFrame（由 return_df 决定）。
    - mode="async": `StockKlineJob`；从 `job.queue` 消费直到 `event="done"`；
      `job.result()` 为全部行的 list[dict]。

    队列事件:
    - data: `{"event":"data","code":"...","rows":[...],"error":str|None}`
    - done: `{"event":"done","total_codes":N,"success_codes":N,"failed_codes":N}`

    调用示例:
    ```python
    # 默认 async：消费队列
    job = get_company_info(
        codes=["600000", "000001"],
        category=["最新提示", "公司概况"],
    )
    while True:
        event = job.queue.get()
        if event.get("event") == "done":
            break
    destroy_parallel_fetcher()

    # sync：最终可选 DataFrame
    with get_client():
        info_df = get_company_info(
            codes=["689009"],
            category=["最新提示", "公司概况"],
            mode="sync",
            return_df=True,
        )
    ```
    """
    return fetch_company_info(
        codes=codes,
        category=category,
        mode=mode,
        queue=queue,
        return_df=return_df,
    )


# ---------- 指数 ----------
def get_index_kline(
    task: Optional[List[Any]] = None,
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[
        Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ] = None,
    mode: str = "async",
) -> Any:
    """
    获取指数 K 线任务结果（按指数名称输入，支持 sync/async）。

    调用前置约定:
    - sync/async 均不要求前置 `with get_client()`；无 with 时名称路由解析会临时建连。
    - 抓取连接由并行层常驻 client 管理（sync 主进程 inproc；async worker 子进程）。
    - sync 走主进程 inproc chunk；async 走进程池 worker。

    输入:
    - task: dict 或 `IndexKlineTask`，字段 `index_name/freq/start_time/end_time`。
      async 且 `None`/空列表时自动展开（全量指数 × 日线 × 近 7 天）；sync 必须非空。
    - queue: 可选，需 `put()`。
    - preprocessor_operator: `f(payload)->dict|None`；None 或空 dict 丢弃该条。
    - mode: `"sync"` / `"async"`。
    - 无 `qfq`（指数无复权）。

    时间窗口 / datetime:
    - 与 get_stock_kline 相同（仅日期补齐 09:30:00 / 16:00:00；秒位 `:00`）。

    返回:
    - sync: `list[task_payload]`；async: `StockKlineJob`。
    """
    return fetch_index_kline(
        task=task,
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        mode=mode,
    )


# ---------- 板块指数 ----------
def get_block_kline(
    task: List[Any],
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[
        Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ] = None,
    mode: str = "async",
) -> Any:
    """获取板块指数 K 线（按板块名称，支持 sync/async）。

    调用前置约定:
    - sync/async 均不要求前置 `with get_client()`；无 with 时名称解析会临时建连。
    - 抓取连接由并行层常驻 client 管理（与 get_stock_kline 相同）。
    - 板块名与 `get_block_names` 共用 6 小时文件缓存；必须显式非空 task（不会自动拉全量）。

    输入:
    - task: dict 或 `BlockKlineTask`，字段 `block_name/freq/start_time/end_time`；
      `block_name` 取自 `get_block_names`。
    - queue / preprocessor_operator / mode：与 get_index_kline 相同（空 dict 亦丢弃）。
    - 无复权参数；底层命令为板块指数 0x0523；回包 OHLC 为绝对价（非 0x052D 差分）。

    时间窗口 / 返回:
    - 时间补齐与 datetime 契约同 get_index_kline。
    - sync: `list[task_payload]`；async: `StockKlineJob`。
    - rows 含 `block_name/freq/open/close/high/low/volume/amount/datetime`。
    """
    return fetch_block_kline(
        task=task,
        queue=queue,
        preprocessor_operator=preprocessor_operator,
        mode=mode,
    )


# ---------- 期货 ----------
def get_future_kline(
    codes: Optional[Any] = None,
    freq: Union[str, List[str]] = None,
    start_time: Any = None,
    end_time: Any = None,
) -> pd.DataFrame:
    """获取商品期货 K 线（多周期批处理，返回合并 DataFrame）。

    调用前置约定:
    - 与 `get_stock_kline` 的 task 路径不同：走 `fetch_stock` DataFrame 批处理。
    - 进程数 > 1 时 worker 并行（不占用主进程连接）；≤ 1 时串行并可复用 with 内连接。
    - 建议 `with get_client():` 以便同块调用其它主进程 API。
    - 批处理总超时 300 秒、单 future 超时 600 秒，写死不读 YAML。

    输入:
    - codes: str/list/tuple/set；纯品种按码表「主连」补全（如 `AL`→`ALL8`）；空则全量商品期货；
      带合约月或 `L7/L8/L9` 原样查询。无主连时抛错。
    - freq: str 或列表，如 `"d"` / `["d","60"]`；支持 d/w/m/60min/30min/15min/5min 与 60/30/15/5。
    - start_time/end_time: 闭区间；仅日期时补齐 09:00:00 / 15:00:00。
    - 无 `qfq`。

    调用示例:
    ```python
    from zsdtdx import get_client, get_future_kline

    with get_client():
        df = get_future_kline(
            codes=["CU", "AL"],
            freq=["d", "60"],
            start_time="2026-02-01",
            end_time="2026-02-13",
        )
    ```

    返回:
    - DataFrame 字段: code, freq, open, close, high, low, settlement_price, volume, datetime。

    并行提示:
    - 进程数 = max(2, int(物理核心数 × `parallel.process_count_core_multiplier`))；>1 默认并行。
    - 大批量任务建议自行分批，避免内存过高；无成交数据可能返回空行。
    """
    return fetch_future_kline(
        codes=codes,
        freq=freq,
        start_time=start_time,
        end_time=end_time,
    )


def get_future_latest_price(codes: Optional[Any] = None) -> Dict[str, Optional[float]]:
    """获取商品期货实时最新价字典。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - codes: 可选期货代码列表；纯品种代码按码表主连合约补全；为空时拉取全部商品期货。

    调用示例:
    - 1个code:
    ```python
    with get_client():
        one = get_future_latest_price("CU2603")
    ```
    - 2个code:
    ```python
    with get_client():
        two = get_future_latest_price(["AL", "CU2603"])
    ```
    - 全部code:
    ```python
    with get_client():
        all_prices = get_future_latest_price()
    ```

    返回示例:
    ```json
    {"ALL8": 23610.0, "CU2603": 102330.0}
    ```

    边界条件:
    - 服务端现价非正时回退昨收；两者均为空或非正时返回 None，并写入运行时失败明细。
    """
    return call_with_main_client(
        lambda client: client.get_future_latest_price(codes=codes),
        caller_name="get_future_latest_price",
    )


# ---------- 并行进程池生命周期 ----------
def prewarm_parallel_fetcher() -> Dict[str, Any]:
    """
    手动预热 async 并行抓取进程池。

    预热固定开启：默认不要求全部 worker 成功，超时 60 秒，最多 3 轮（写死，不读 YAML）。

    输出:
    - 预热摘要字典：`ready_processes` 表示已启动进程数，
      `connection_ready_workers` 表示已建立行情连接的进程数；预热阶段通常为 0。
      `warmed_workers` 为兼容旧调用方保留，等价于 `ready_processes`。

    什么时候调用:
    - 服务启动或压测前，希望把 async 冷启动成本前移。
    - 一般不必手动调用：首次 async 任务会自动预热。

    边界条件:
    - 仅拉起 worker 进程，不建立 std/ex 行情连接；业务连接按任务路由懒建。
    - 仅当 `require_all_workers=True` 且预热不足时抛 RuntimeError；默认 False，不足只记入摘要。
    """
    _ensure_active_config_ready(caller_name="prewarm_parallel_fetcher")
    fetcher = get_fetcher()
    # 预热超时与轮次是抓取器常数，不是 YAML 项。
    require_all_workers = getattr(fetcher, "auto_prewarm_require_all_workers", False)
    timeout_seconds = getattr(fetcher, "auto_prewarm_timeout_seconds", 60.0)
    max_rounds = getattr(fetcher, "auto_prewarm_max_rounds", 3)
    # target_workers 没有对应的 config 参数，使用 None（让内部决定）
    target_workers = None
    return _prewarm_parallel_fetcher(
        require_all_workers=bool(require_all_workers),
        timeout_seconds=float(timeout_seconds),
        max_rounds=int(max_rounds),
        target_workers=target_workers,
    )


def restart_parallel_fetcher(
    prewarm: Optional[bool] = None,
    prewarm_timeout_seconds: Optional[float] = None,
    max_rounds: Optional[int] = None,
) -> Dict[str, Any]:
    """
    强制重启 async 并行抓取进程池（终止旧 worker 并按需预热）。

    输入:
    - prewarm: 重启后是否立即预热新池；None 时默认 True。
    - prewarm_timeout_seconds: 重启后预热总超时（秒）；None 时用抓取器常数 60。
    - max_rounds: 重启后预热轮次上限；None 时用抓取器常数 3。

    输出:
    - 重启摘要字典（旧 pid、终止结果、预热摘要、耗时等）。

    什么时候调用:
    - 出现连续 timeout/连接异常，怀疑 worker 状态异常时。
    - 需要快速回收并重建 worker 连接状态时。

    边界条件:
    - 即便旧池不存在也会返回摘要，不抛错。
    - 未传预热参数时使用抓取器上的固定预热常数。
    """
    _ensure_active_config_ready(caller_name="restart_parallel_fetcher")
    fetcher = get_fetcher()
    resolved_prewarm = True if prewarm is None else bool(prewarm)
    resolved_timeout = (
        float(prewarm_timeout_seconds)
        if prewarm_timeout_seconds is not None
        else float(getattr(fetcher, "auto_prewarm_timeout_seconds", 60.0))
    )
    resolved_max_rounds = (
        int(max_rounds)
        if max_rounds is not None
        else int(getattr(fetcher, "auto_prewarm_max_rounds", 3))
    )
    return _force_restart_parallel_fetcher(
        prewarm=resolved_prewarm,
        prewarm_timeout_seconds=resolved_timeout,
        max_rounds=resolved_max_rounds,
    )


def destroy_parallel_fetcher() -> Dict[str, Any]:
    """
    销毁 async 并行抓取进程池并释放 worker 资源。

    输入:
    - 无显式输入参数。

    输出:
    - 销毁摘要字典（是否存在旧池、旧 worker 数、销毁后版本号、耗时）。

    什么时候调用:
    - 长驻服务优雅停机前，主动释放 worker 与连接资源。
    - 短脚本结束前，避免保留并行进程池到解释器退出阶段。

    边界条件:
    - 进程池不存在时安全返回，不抛错。
    """
    return _destroy_parallel_fetcher()


# ---------- 运行时 introspection ----------
def get_runtime_failures() -> pd.DataFrame:
    """获取运行期失败/无数据明细。

    调用前置约定:
    - 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接。

    调用示例:
    ```python
    with get_client():
        failures = get_runtime_failures()
    ```

    返回示例:
    ```json
    [{"task": "stock_kline", "code": "999999", "freq": "d", "reason": "code_not_found"}]
    ```
    """
    return call_with_main_client(
        lambda client: client.get_failures_df(),
        caller_name="get_runtime_failures",
    )


def get_runtime_metadata() -> Dict[str, Any]:
    """获取运行元数据快照。

    调用前置约定:
    - 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接。

    调用示例:
    ```python
    with get_client():
        meta = get_runtime_metadata()
    ```

    返回示例:
    ```json
    {"config_path": "<auto>", "std_active_host": "120.76.1.198:7709"}
    ```
    """
    return call_with_main_client(
        lambda client: client.get_runtime_metadata(),
        caller_name="get_runtime_metadata",
    )


__all__ = [
    "StockKlineTask",
    "IndexKlineTask",
    "BlockKlineTask",
    "StockKlineJob",
    "set_config_path",
    "get_client",
    "get_stock_code_name",
    "get_stock_concepts",
    "get_etf_code_name",
    "get_block_names",
    "get_all_future_list",
    "get_stock_kline",
    "get_stock_stat",
    "get_company_info",
    "get_index_kline",
    "get_block_kline",
    "get_future_kline",
    "get_future_latest_price",
    "prewarm_parallel_fetcher",
    "restart_parallel_fetcher",
    "destroy_parallel_fetcher",
    "get_runtime_failures",
    "get_runtime_metadata",
]
