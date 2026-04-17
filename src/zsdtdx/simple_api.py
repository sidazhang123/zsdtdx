"""zsdtdx 最外层简明封装入口。

职责：
1. 对外暴露 get_* 风格 API，隐藏连接池、市场路由、分页与并行调度细节。
2. 统一 task 输入格式（code/freq/start_time/end_time）并完成时间窗口标准化。
3. 提供同步/异步两种股票/指数 K 线任务获取模式。
4. 暴露并行进程池生命周期管理入口（prewarm/restart/destroy）。

调用约定：
1. 程序启动阶段先调用 `set_config_path()` 设置配置路径；后续各接口无需重复传参。
2. 若未调用 `set_config_path()`，首次调用相关接口会打印提醒并回退到包内默认配置。
3. 进入 `with get_client():` 后，可在同一个 with 块连续调用多个“主进程 API”（如 `get_supported_markets`、`get_stock_latest_price`、`get_stock_kline(mode="sync")`）复用主进程连接。
4. `get_stock_kline(mode="async")` 的数据抓取在并行 worker 进程内执行，worker 会独立创建并复用自己的连接，不复用 `with get_client()` 的主进程连接。
5. async 模式通过 `job.queue` 持续消费结果，直到收到 `event="done"`。
6. 对 async 冷启动敏感场景可在启动阶段手动调用 `prewarm_parallel_fetcher()`；异常恢复可调用 `restart_parallel_fetcher()`；进程退出前可调用 `destroy_parallel_fetcher()`。
"""

from __future__ import annotations

import datetime as dt
import queue as std_queue
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

from zsdtdx.helper import (
    _apply_active_config_path,
    _ensure_active_config_ready,
    _DEFAULT_CONFIG_PATH,
    call_with_client as _call_with_client,
    normalize_future_time_window as _normalize_future_time_window,
    normalize_task_input as _normalize_task_input,
    normalize_task_time_window as _normalize_task_time_window,
    parse_task_datetime as _parse_task_datetime,
)
from zsdtdx.parallel_fetcher import (
    StockKlineJob,
    destroy_parallel_fetcher as _destroy_parallel_fetcher,
    force_restart_parallel_fetcher as _force_restart_parallel_fetcher,
    get_fetcher,
    prewarm_parallel_fetcher as _prewarm_parallel_fetcher,
    set_active_config_path,
)
from zsdtdx.unified_client import UnifiedTdxClient

_TASK_FREQ_MAP: Dict[str, str] = {
    "d": "d",
    "w": "w",
    "m": "m",
    "60": "60",
    "60min": "60",
    "30": "30",
    "30min": "30",
    "15": "15",
    "15min": "15",
    "5": "5",
    "5min": "5",
}

@dataclass
class StockKlineTask:
    """
    股票 K 线任务模板。

    输入：
    1. code: 股票代码。
    2. freq: 周期，支持 d/w/m/60/30/15/5 及同义写法。
    3. start_time/end_time: 时间窗口。
    输出：
    1. 通过 `to_dict()` 输出标准化 task 字典。
    用途：
    1. 为 get_stock_kline 提供显式、可校验的任务对象。
    边界条件：
    1. 四个核心字段任一为空均会校验失败。
    """

    code: Any
    freq: Any
    start_time: Any
    end_time: Any

    def validate(self) -> None:
        """
        校验任务字段合法性。

        输入：
        1. 当前对象的 code/freq/start_time/end_time。
        输出：
        1. 无返回值；校验失败抛出 ValueError。
        用途：
        1. 在任务进入并行链路前尽早暴露输入问题。
        边界条件：
        1. freq 不在支持集合内时直接抛错。
        """
        code = str(self.code or "").strip()
        freq = str(self.freq or "").strip().lower()
        start_time = str(self.start_time or "").strip()
        end_time = str(self.end_time or "").strip()
        if code == "":
            raise ValueError("task.code 不能为空")
        if freq == "":
            raise ValueError("task.freq 不能为空")
        if start_time == "":
            raise ValueError("task.start_time 不能为空")
        if end_time == "":
            raise ValueError("task.end_time 不能为空")
        if freq not in _TASK_FREQ_MAP:
            raise ValueError(f"不支持的频率: {self.freq}")
        _parse_task_datetime(start_time)
        _parse_task_datetime(end_time)

    def to_dict(self) -> Dict[str, str]:
        """
        输出标准化任务字典。

        输入：
        1. 当前任务对象。
        输出：
        1. `{"code","freq","start_time","end_time"}` 标准字典。
        用途：
        1. 作为并行抓取器统一输入。
        边界条件：
        1. 会先执行 validate；失败时抛错。
        """
        self.validate()
        normalized_start, normalized_end = _normalize_task_time_window(self.start_time, self.end_time)
        return {
            "code": str(self.code).strip(),
            "freq": _TASK_FREQ_MAP[str(self.freq).strip().lower()],
            "start_time": normalized_start,
            "end_time": normalized_end,
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "StockKlineTask":
        """
        从字典构造任务对象并执行校验。

        输入:
        1. raw: 包含 code/freq/start_time/end_time 的字典。
        输出:
        1. `StockKlineTask` 实例。
        用途:
        1. 兼容 dict 任务输入并统一校验行为。
        边界条件:
        1. raw 不是 dict 时抛 ValueError。
        """
        if not isinstance(raw, dict):
            raise ValueError("task 元素必须是 dict 或 StockKlineTask")
        task = cls(
            code=raw.get("code"),
            freq=raw.get("freq"),
            start_time=raw.get("start_time"),
            end_time=raw.get("end_time"),
        )
        task.validate()
        return task


@dataclass
class IndexKlineTask:
    """
    指数 K 线任务模板。

    输入：
    1. index_name: 指数中文名称（会走配置映射/别名解析）。
    2. freq: 周期，与 `_TASK_FREQ_MAP` 一致（含 d/w/m/60/30/15/5 及同义写法）。
    3. start_time/end_time: 时间窗口。
    输出：
    1. 通过 `to_dict()` 输出标准化 task 字典。
    用途：
    1. 为 get_index_kline 提供显式、可校验的任务对象。
    边界条件：
    1. 四个核心字段任一为空均会校验失败。
    """

    index_name: Any
    freq: Any
    start_time: Any
    end_time: Any

    def validate(self) -> None:
        """
        校验任务字段合法性。

        输入：
        1. 当前对象的 index_name/freq/start_time/end_time。
        输出：
        1. 无返回值；校验失败抛出 ValueError。
        用途：
        1. 在指数任务进入执行链路前尽早暴露输入问题。
        边界条件：
        1. freq 不在支持集合内时直接抛错。
        """
        index_name = str(self.index_name or "").strip()
        freq = str(self.freq or "").strip().lower()
        start_time = str(self.start_time or "").strip()
        end_time = str(self.end_time or "").strip()
        if index_name == "":
            raise ValueError("task.index_name 不能为空")
        if freq == "":
            raise ValueError("task.freq 不能为空")
        if start_time == "":
            raise ValueError("task.start_time 不能为空")
        if end_time == "":
            raise ValueError("task.end_time 不能为空")
        if freq not in _TASK_FREQ_MAP:
            raise ValueError(f"不支持的频率: {self.freq}")
        _parse_task_datetime(start_time)
        _parse_task_datetime(end_time)

    def to_dict(self) -> Dict[str, str]:
        """
        输出标准化任务字典。

        输入：
        1. 当前任务对象。
        输出：
        1. `{\"index_name\",\"freq\",\"start_time\",\"end_time\"}` 标准字典。
        用途：
        1. 作为指数任务执行器统一输入。
        边界条件：
        1. 会先执行 validate；失败时抛错。
        """
        self.validate()
        normalized_start, normalized_end = _normalize_task_time_window(self.start_time, self.end_time)
        return {
            "index_name": str(self.index_name).strip(),
            "freq": _TASK_FREQ_MAP[str(self.freq).strip().lower()],
            "start_time": normalized_start,
            "end_time": normalized_end,
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "IndexKlineTask":
        """
        从字典构造任务对象并执行校验。

        输入：
        1. raw: 包含 index_name/freq/start_time/end_time 的字典。
        输出：
        1. `IndexKlineTask` 实例。
        用途：
        1. 兼容 dict 任务输入并统一校验行为。
        边界条件：
        1. raw 不是 dict 时抛 ValueError。
        """
        if not isinstance(raw, dict):
            raise ValueError("task 元素必须是 dict 或 IndexKlineTask")
        task = cls(
            index_name=raw.get("index_name"),
            freq=raw.get("freq"),
            start_time=raw.get("start_time"),
            end_time=raw.get("end_time"),
        )
        task.validate()
        return task


def set_config_path(config_path: str) -> str:
    """
    设置 simple_api 全局配置路径（主进程与并行 worker 统一生效）。

    输入:
    - config_path: 配置文件路径（建议在程序启动阶段调用一次）。

    输出:
    - 解析后的绝对配置路径字符串。

    调用示例:
    ```python
    from zsdtdx import set_config_path

    set_config_path(r"D:\\configs\\zsdtdx.yaml")
    ```

    边界条件:
    - 配置非法时会直接抛出异常，调用方可在启动阶段尽早失败。
    """
    return _apply_active_config_path(config_path=config_path)


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
        markets = get_supported_markets(return_df=True)
        stock_map = get_stock_code_name()
        prices = get_stock_latest_price(["600000", "000001"])
    ```

    返回示例:
    ```python
    <zsdtdx.unified_client.UnifiedTdxClient object at 0x...>
    ```

    作用边界:
    - 该 client 仅管理“当前主进程”上下文中的连接生命周期（进入 with 预连接，退出 with 自动 close）。
    - `get_stock_kline(mode="async")` 的 worker 连接由并行抓取器在 worker 进程内独立维护，不与此处返回的主进程 client 共用连接对象。

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


def get_supported_markets(return_df: Optional[bool] = None):
    """获取标准+扩展行情支持的市场列表。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    调用示例:
    ```python
    with get_client():
        df = get_supported_markets(return_df=True)
    ```

    返回示例:
    ```json
    [{"market": 0, "name": "深圳", "source": "std"}, {"market": 1, "name": "上海", "source": "std"}]
    ```
    """
    return _call_with_client(
        lambda client: client.get_supported_markets(return_df=return_df),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )


def get_stock_code_name(use_cache: bool = True) -> Dict[str, str]:
    """获取统一股票代码名称字典。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - use_cache: 是否使用股票缓存；为 False 时强制刷新股票缓存。
      本函数属于全量代码接口，返回范围由
      `config.yaml.stock_scope.defaults_when_codes_none.get_stock_code_name` 控制（默认 `szsh`）。

    输出:
    - 返回 `Dict[str, str]`：key 为带市场前缀的股票代码（`sh./sz./bj./hk.`），
      value 为股票名称；不返回纯数字代码。

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
    return _call_with_client(
        lambda client: client.get_stock_code_name_map(use_cache=use_cache),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )


def get_all_future_list(return_df: Optional[bool] = None, use_cache: bool = True):
    """获取统一商品期货列表（郑州/大连/上海/广州）。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - use_cache: 是否使用期货清单缓存；为 False 时强制刷新期货清单缓存。

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
    return _call_with_client(
        lambda client: client.get_all_future_list(return_df=return_df, use_cache=use_cache),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )


def get_stock_kline(
    task: List[Any],
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    mode: str = "async",
) -> Any:
    """获取股票 K 线任务结果（任务化输入，支持同步/异步与队列实时回传）。

    调用前置约定:
    - `mode="sync"`：可进入 `with get_client():` 在主进程复用连接并统一资源释放。
    - `mode="async"`：使用多进程内的独立连接，不依赖get_client()。

    调用示例（写法一：with 主进程上下文 + sync + 其它接口）:
    ```python
    import queue as py_queue
    from zsdtdx import (
        StockKlineTask,
        get_client,
        get_stock_kline,
        get_stock_latest_price,
        get_supported_markets,
    )
    
    with get_client():
        markets = get_supported_markets(return_df=True)
        prices = get_stock_latest_price(["600000", "000001"])
        # sync 模式可传入队列，边产出边消费；也可不传，待所有结果归集后返回。
        q = py_queue.Queue()
        result = get_stock_kline(
            task=[
                # 使用任务对象写法，字段校验更明确
                StockKlineTask(code="600000", freq="d", start_time="2026-02-13", end_time="2026-02-13"),
                # 也支持 dict 写法
                {"code": "000001", "freq": "60", "start_time": "2026-02-13", "end_time": "2026-02-14"},
            ],
            queue=q,
            mode="sync",
        )
        print(len(markets), prices)
        # result 为完整 payload 列表；q 中也会收到相同 data 事件和最终 done 事件
        print(result)
    ```

    调用示例（写法二：async 独立进程池调用 + prewarm/restart/destroy）:
    ```python
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_stock_kline,
        prewarm_parallel_fetcher,
        restart_parallel_fetcher,
    )

    prewarm_parallel_fetcher(require_all_workers=True, timeout_seconds=60, max_rounds=3)
    job = get_stock_kline(
        task=[{"code": "600000", "freq": "d", "start_time": "2026-02-13", "end_time": "2026-02-13"}],
        mode="async",
    )
    try:
        while True:
            # 实时读取 data 事件，直到 done
            event = job.queue.get(timeout=20)
            if event.get("event") == "done":
                break
            # event="data" 时可按 task/rows/error 增量处理
            print(event.get("task"), event.get("error"))
        # 等待后台任务完全结束并传播异常
        job.result()
    except Exception:
        # 任务执行链路出现持续异常时，可强制重启并重建 worker 连接
        restart_parallel_fetcher(prewarm=True, prewarm_timeout_seconds=60, max_rounds=3)
        raise
    finally:
        # 服务停机或脚本结束前主动销毁进程池
        destroy_parallel_fetcher()
    ```

    连接生命周期说明:
    - `mode="sync"`：主要使用主进程连接；with 结束会关闭主进程 client 连接。
    - `mode="async"`：主要使用 worker 连接；with 结束不会直接关闭 worker 连接，worker 连接由并行抓取器按进程池生命周期管理。

    返回:
    - mode="sync": 始终返回 `list[task_payload]`（无论是否传 queue）。
      - 未传 queue：结果仅在返回值中。
      - 传了 queue：结果既在返回值中，也会同步推送到 queue。
    - mode="async": 始终返回 `StockKlineJob`（无论是否传 queue）。
      - 未传 queue：可从自动创建的 `job.queue` 消费事件。
      - 传了 queue：可从传入的 queue（即 `job.queue`）消费事件。
    - task_payload 结构:
      `{"event":"data","task":{...},"rows":[...],"error":str|None,"worker_pid":int}`。
    - 队列最终会额外推送 done 事件:
      `{"event":"done","total_tasks":...,"success_tasks":...,"failed_tasks":...}`。
    """
    if queue is not None and not hasattr(queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")
    if preprocessor_operator is not None and not callable(preprocessor_operator):
        raise ValueError("preprocessor_operator 必须是可调用对象")

    _ensure_active_config_ready(caller_name="get_stock_kline")
    normalized_tasks = _normalize_task_input(task=task, task_cls=StockKlineTask)
    mode_key = str(mode or "async").strip().lower()

    fetcher = get_fetcher()
    if mode_key == "sync":
        return fetcher.fetch_stock_tasks_sync(
            tasks=normalized_tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
        )
    if mode_key == "async":
        async_queue = queue if queue is not None else std_queue.Queue()
        return fetcher.fetch_stock_tasks_async(
            tasks=normalized_tasks,
            queue=async_queue,
            preprocessor_operator=preprocessor_operator,
        )
    raise ValueError("mode 仅支持 'sync' 或 'async'")


def get_index_kline(
    task: Optional[List[Any]] = None,
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    mode: str = "async",
) -> Any:
    """
    获取指数 K 线任务结果（按指数名称输入，支持 sync/async）。

    调用前置约定:
    - `mode="sync"`：走 `ParallelFetcher` 的主进程 inproc chunk 路径；若当前已进入 `with get_client():`，其他主进程 API 仍可继续复用该上下文连接。
    - `mode="async"`：走 `ParallelFetcher` 的进程池 chunk 路径，worker 会独立创建并复用自己的连接，不依赖 with 上下文。

    输入:
    - task: 指数任务列表，元素支持 dict 或 `IndexKlineTask`，字段:
      `index_name/freq/start_time/end_time`。当传 `None` 或空列表时，会自动构建默认任务清单（全量指数目录，日线，近 7 天窗口）。
    - queue: 可选事件队列，需实现 `put()`。
    - preprocessor_operator: 可选事件预处理函数，返回 None 时丢弃该事件。
    - mode: `"sync"` 或 `"async"`。

    返回:
    - mode="sync": `list[task_payload]`。
    - mode="async": `StockKlineJob`，可从 `job.queue` 消费事件直到 done。
    """
    if queue is not None and not hasattr(queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")
    if preprocessor_operator is not None and not callable(preprocessor_operator):
        raise ValueError("preprocessor_operator 必须是可调用对象")

    _ensure_active_config_ready(caller_name="get_index_kline")
    raw_tasks: List[Any]
    if task is None or (isinstance(task, (list, tuple)) and len(task) == 0):
        end_dt = dt.datetime.now()
        start_dt = end_dt - dt.timedelta(days=7)
        start_text = start_dt.strftime("%Y-%m-%d")
        end_text = end_dt.strftime("%Y-%m-%d")

        def _build_default_index_tasks(client: UnifiedTdxClient) -> List[Dict[str, str]]:
            """
            基于运行时指数目录构建默认指数任务列表。

            输入：
            1. client: 统一客户端实例。
            输出：
            1. 默认任务字典列表。
            用途：
            1. 支持 get_index_kline 在 task 缺省场景下自动拉取指数数据。
            边界条件：
            1. 目录为空时抛 ValueError，提示调用方显式传 task 或检查连接。
            """
            records = client._discover_index_route_records(refresh=False)
            tasks_buffer: List[Dict[str, str]] = []
            seen_names: set[str] = set()
            for record in records:
                index_name = str(record.get("name", "")).strip()
                if index_name == "" or index_name in seen_names:
                    continue
                seen_names.add(index_name)
                tasks_buffer.append(
                    {
                        "index_name": index_name,
                        "freq": "d",
                        "start_time": start_text,
                        "end_time": end_text,
                    }
                )
            if not tasks_buffer:
                raise ValueError("默认指数任务构建失败：未发现可用指数目录")
            return tasks_buffer

        raw_tasks = _call_with_client(
            _build_default_index_tasks,
            get_active_context_client=UnifiedTdxClient.get_active_context_client,
            build_client=lambda: get_client(),
        )
    else:
        raw_tasks = list(task)

    normalized_tasks = _normalize_task_input(task=raw_tasks, task_cls=IndexKlineTask)

    def _attach_index_routes(
        client: UnifiedTdxClient,
        tasks_buffer: List[Dict[str, str]],
    ) -> List[Dict[str, Any]]:
        """
        在主进程预解析指数名称路由并写回任务字典。

        输入：
        1. client: 统一客户端实例。
        2. tasks_buffer: 标准化后的指数任务列表。
        输出：
        1. 附带 `_index_route_*` 字段的任务列表。
        用途：
        1. 将名称解析前置到主进程，降低 worker 侧重复目录扫描次数。
        边界条件：
        1. 名称未命中时保持原有行为，直接抛 ValueError。
        """
        enriched_tasks: List[Dict[str, Any]] = []
        for task_item in tasks_buffer:
            route = client.resolve_index_name(index_name=task_item.get("index_name", ""), refresh=False)
            task_copy: Dict[str, Any] = dict(task_item)
            task_copy["_index_route_source"] = str(route.get("source", "")).strip().lower()
            task_copy["_index_route_market"] = int(route.get("market", -1))
            task_copy["_index_route_code"] = str(route.get("code", "")).strip()
            task_copy["_index_route_name"] = str(route.get("name", "")).strip()
            enriched_tasks.append(task_copy)
        return enriched_tasks

    normalized_tasks = _call_with_client(
        lambda client: _attach_index_routes(client=client, tasks_buffer=normalized_tasks),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )
    mode_key = str(mode or "async").strip().lower()
    fetcher = get_fetcher()

    if mode_key == "sync":
        return fetcher.fetch_index_tasks_sync(
            tasks=normalized_tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
        )
    if mode_key == "async":
        async_queue = queue if queue is not None else std_queue.Queue()
        return fetcher.fetch_index_tasks_async(
            tasks=normalized_tasks,
            queue=async_queue,
            preprocessor_operator=preprocessor_operator,
        )
    raise ValueError("mode 仅支持 'sync' 或 'async'")


def prewarm_parallel_fetcher() -> Dict[str, Any]:
    """
    手动预热 async 并行抓取进程池与 worker 常驻连接。

    使用 config 中的 parallel.auto_prewarm_* 参数（config 来源：用户 set_config_path() 后使用用户提供的配置文件，如果没有调用则使用包内默认配置文件 config.yaml）。
    若 config 读取失败，则使用内部兜底值。

    输出:
    - 预热摘要字典（目标进程数、已预热进程数、pid 列表、耗时等）。

    什么时候调用:
    - 服务启动阶段：希望把 async 首次冷启动成本前移。
    - 压测/批跑前：希望先确认 worker 建连健康再开始任务。

    边界条件:
    - 默认从 config 读取参数；若 config 读取失败则使用兜底值。
    - 若预热不足会抛 RuntimeError。
    """
    _ensure_active_config_ready(caller_name="prewarm_parallel_fetcher")
    fetcher = get_fetcher()
    # 从 config 读取参数，兜底到内部默认值
    require_all_workers = getattr(fetcher, 'auto_prewarm_require_all_workers', True)
    timeout_seconds = getattr(fetcher, 'auto_prewarm_timeout_seconds', 60.0)
    max_rounds = getattr(fetcher, 'auto_prewarm_max_rounds', 3)
    spread_standard_hosts = getattr(fetcher, 'auto_prewarm_spread_standard_hosts', False)
    # target_workers 没有对应的 config 参数，使用 None（让内部决定）
    target_workers = None
    return _prewarm_parallel_fetcher(
        require_all_workers=bool(require_all_workers),
        timeout_seconds=float(timeout_seconds),
        max_rounds=int(max_rounds),
        target_workers=target_workers,
        spread_standard_hosts=bool(spread_standard_hosts),
    )


def restart_parallel_fetcher(
    prewarm: bool = True,
    prewarm_timeout_seconds: float = 60.0,
    max_rounds: int = 3,
) -> Dict[str, Any]:
    """
    强制重启 async 并行抓取进程池（终止旧 worker 并按需预热）。

    输入:
    - prewarm: 重启后是否立即预热新池。
    - prewarm_timeout_seconds: 重启后预热总超时（秒）。
    - max_rounds: 重启后预热轮次上限。

    输出:
    - 重启摘要字典（旧 pid、终止结果、预热摘要、耗时等）。

    什么时候调用:
    - 出现连续 timeout/连接异常，怀疑 worker 状态异常时。
    - 需要快速回收并重建 worker 连接状态时。

    边界条件:
    - 即便旧池不存在也会返回摘要，不抛错。
    """
    _ensure_active_config_ready(caller_name="restart_parallel_fetcher")
    return _force_restart_parallel_fetcher(
        prewarm=bool(prewarm),
        prewarm_timeout_seconds=float(prewarm_timeout_seconds),
        max_rounds=int(max_rounds),
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


def get_future_kline(
    codes: Optional[Any] = None,
    freq: Union[str, List[str]] = None,
    start_time: Any = None,
    end_time: Any = None,
) -> pd.DataFrame:
    """获取商品期货 K 线（支持多周期并行获取，返回合并后的 DataFrame）。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - codes: 期货代码，支持 str/list/tuple/set；代码不含数字会自动补 `L8`（如 `AL` -> `ALL8`）。
      为空时获取全部商品期货。
    - freq: 周期，支持 str 或列表，如 `"d"` / `["d", "60", "30"]`。
      支持周期: d/w/m/60min/30min/15min/5min 与 60/30/15/5。
    - start_time/end_time: 支持字符串/date/datetime，底层过滤按闭区间 `[start_time, end_time]` 执行。
      若传入不带时分秒的日期字符串，自动补齐为 start=09:00:00、end=15:00:00。
      例如 `2026-02-13` 等价于 `start_time="2026-02-13 09:00:00"`、`end_time="2026-02-13 15:00:00"`。
    调用示例:
    ```python
    from zsdtdx import get_client, get_future_kline
    
    with get_client():
        # 获取多个期货、多个周期的数据，返回一个合并 DataFrame
        df = get_future_kline(
            codes=["CU", "AL"], 
            freq=["d", "60"],
            start_time="2026-02-01", 
            end_time="2026-02-13"
        )
        # df 已按统一字段规范输出，可直接过滤 code/freq 继续处理
        print(df)
    ```

    返回:
    - pd.DataFrame: 包含所有获取的数据，字段:
      code, freq, open, close, high, low, settlement_price, volume, datetime

    并行模式说明:
    - 入口策略: 进程数 > 1 时默认并行；进程数不足时自动串行
    - 进程数: 自动计算 = int(CPU物理核心数 × process_count_core_multiplier)，至少2个进程
      （`process_count_core_multiplier` 位于 `config.yaml.parallel`，默认 1.5）
    - 全局进程池: 首次并行调用时创建（约2-3秒开销），后续调用复用，程序退出时统一关闭
    - 建议分批: 100个期货×5周期=500任务/批

    注意事项:
    - 返回的是单个合并后的 DataFrame，不是迭代器
    - 大规模数据获取(5000+任务)建议手动分批，避免内存占用过高
    - 部分期货可能因无交易数据返回空
    """
    # 标准化 freq
    if freq is None:
        freq = ["d"]
    elif isinstance(freq, str):
        freq = [freq]
    elif not isinstance(freq, (list, tuple)):
        raise ValueError("freq 必须是字符串或列表，如 'd' 或 ['d', '60']")
    elif len(freq) == 0:
        raise ValueError("freq 列表不能为空")
    
    # 标准化 codes
    if codes is None:
        with get_client() as client:
            codes = list(client.get_all_future_list(return_df=True)["code"].tolist())
    elif isinstance(codes, str):
        codes = [codes]
    else:
        codes = list(codes)
    
    if len(codes) == 0:
        return pd.DataFrame()
    
    # 使用并行获取器
    fetcher = get_fetcher()

    normalized_start_time = str(start_time) if start_time is not None else None
    normalized_end_time = str(end_time) if end_time is not None else None
    if start_time is not None and end_time is not None:
        normalized_start_time, normalized_end_time = _normalize_future_time_window(start_time, end_time)

    return fetcher.fetch_stock(
        codes=codes,
        freqs=list(freq),
        start_time=normalized_start_time,
        end_time=normalized_end_time
    )


def get_company_info(code: str, category: Optional[List[str]] = None, return_df: Optional[bool] = None):
    """获取单只股票公司信息。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - code: 股票代码。
    - category: 中文分类名列表；为空时返回全部分类。

    调用示例:
    ```python
    with get_client():
        info_df = get_company_info(code="689009", category=["最新提示", "公司概况"], return_df=True)
    ```

    返回示例:
    ```json
    [{"code": "689009", "category": "公司概况", "content": "......"}]
    ```
    """
    return _call_with_client(
        lambda client: client.get_company_info_content(code=code, category=category, return_df=return_df),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )


def get_stock_latest_price(codes: Optional[Any] = None) -> Dict[str, Optional[float]]:
    """获取股票实时最新价字典。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - codes: 可选股票代码列表；支持 `sh./sz./bj./hk.` 前缀；
      为空时按 `config.yaml.stock_scope.defaults_when_codes_none.get_stock_latest_price`
      拉取默认范围全量股票；显式传入代码时不受该范围开关影响。

    调用示例:
    - 1个code:
    ```python
    with get_client():
        one = get_stock_latest_price("600000")
    ```
    - 2个code:
    ```python
    with get_client():
        two = get_stock_latest_price(["600000", "09988"])
    ```
    - 全部code:
    ```python
    with get_client():
        all_prices = get_stock_latest_price()
    ```

    返回示例:
    ```json
    {"600000": 9.98, "09988": 158.5}
    ```
    """
    return _call_with_client(
        lambda client: client.get_stock_latest_price(codes=codes),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )


def get_future_latest_price(codes: Optional[Any] = None) -> Dict[str, Optional[float]]:
    """获取商品期货实时最新价字典。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - codes: 可选期货代码列表；不含数字会自动补 `L8`；为空时拉取全部商品期货。

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
    """
    return _call_with_client(
        lambda client: client.get_future_latest_price(codes=codes),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )


def get_runtime_failures() -> pd.DataFrame:
    """获取运行期失败/无数据明细。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

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
    return _call_with_client(
        lambda client: client.get_failures_df(),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )


def get_runtime_metadata() -> Dict[str, Any]:
    """获取运行元数据快照。

    调用前置约定:
    - 请先进入 `with get_client():`；
      一个 with 块内可连续调用多个 `get_*` 函数。

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
    return _call_with_client(
        lambda client: client.get_runtime_metadata(),
        get_active_context_client=UnifiedTdxClient.get_active_context_client,
        build_client=lambda: get_client(),
    )


__all__ = [
    "StockKlineTask",
    "IndexKlineTask",
    "StockKlineJob",
    "set_config_path",
    "get_client",
    "get_supported_markets",
    "get_stock_code_name",
    "get_all_future_list",
    "get_stock_kline",
    "get_index_kline",
    "prewarm_parallel_fetcher",
    "restart_parallel_fetcher",
    "destroy_parallel_fetcher",
    "get_future_kline",
    "get_company_info",
    "get_stock_latest_price",
    "get_future_latest_price",
    "get_runtime_failures",
    "get_runtime_metadata",
]

