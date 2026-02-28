"""zsdtdx 最外层简明封装入口。

职责：
1. 对外暴露 get_* 风格 API，隐藏连接池、市场路由、分页与并行调度细节。
2. 统一 task 输入格式（code/freq/start_time/end_time）并完成时间窗口标准化。
3. 提供同步/异步两种股票 K 线任务获取模式。

调用约定（推荐）：
1. 先进入 `with get_client() as client:`。
2. 在同一个 with 块内连续调用多个 `get_*` 函数。
3. async 模式通过 `job.queue` 持续消费结果，直到收到 `event="done"`。
"""

from __future__ import annotations

import queue as std_queue
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import pandas as pd

from zsdtdx.parallel_fetcher import StockKlineJob, get_fetcher, set_active_config_path
from zsdtdx.wrapper.unified_client import UnifiedTdxClient

_DEFAULT_CONFIG_PATH = str(Path(__file__).resolve().with_name("config.yaml"))
_ACTIVE_CONFIG_PATH: Optional[str] = None
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


def _parse_task_datetime(raw_value: Any) -> tuple[datetime, str]:
    """
    解析任务时间字符串并返回 `(datetime, format_key)`。

    输入：
    1. raw_value: 原始时间值，支持 str/date/datetime。
    输出：
    1. `(parsed_datetime, format_key)`；format_key 用于后续按原风格回写字符串。
    用途：
    1. 统一 task 时间解析逻辑，避免各入口重复处理。
    边界条件：
    1. 空字符串或无法识别的时间格式会抛出 ValueError。
    """
    raw = str(raw_value or "").strip()
    if raw == "":
        raise ValueError("时间不能为空")

    format_pairs = [
        ("%Y-%m-%d %H:%M:%S", "ymd_hms_dash"),
        ("%Y-%m-%d %H:%M", "ymd_hm_dash"),
        ("%Y-%m-%d", "ymd_dash"),
        ("%Y/%m/%d %H:%M:%S", "ymd_hms_slash"),
        ("%Y/%m/%d %H:%M", "ymd_hm_slash"),
        ("%Y/%m/%d", "ymd_slash"),
    ]
    for fmt, key in format_pairs:
        try:
            return datetime.strptime(raw, fmt), key
        except Exception:
            continue

    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        raise ValueError(f"非法时间: {raw_value}") from None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed, "iso"


def _format_task_datetime(parsed: datetime, format_key: str) -> str:
    """
    按 format_key 将 datetime 回写为字符串。

    输入：
    1. parsed: 已解析时间。
    2. format_key: 原始格式标识（如 ymd_hms_dash、iso）。
    输出：
    1. 格式化后的时间字符串。
    用途：
    1. 在标准化时间后保持调用方原有格式风格。
    边界条件：
    1. 未知 format_key 会回退到 `%Y-%m-%d %H:%M:%S`。
    """
    if format_key == "ymd_hms_dash":
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    if format_key == "ymd_dash":
        return parsed.strftime("%Y-%m-%d")
    if format_key == "ymd_slash":
        return parsed.strftime("%Y/%m/%d")
    if format_key == "ymd_hm_dash":
        return parsed.strftime("%Y-%m-%d %H:%M")
    if format_key == "ymd_hm_slash":
        return parsed.strftime("%Y/%m/%d %H:%M")
    if format_key == "ymd_hms_slash":
        return parsed.strftime("%Y/%m/%d %H:%M:%S")
    if format_key == "iso":
        return parsed.isoformat(timespec="seconds")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_task_time_window(start_time: Any, end_time: Any) -> tuple[str, str]:
    """
    标准化股票任务时间窗口。

    输入：
    1. start_time/end_time: 原始时间值，支持 str/date/datetime。
    输出：
    1. 标准化后的 `(start_time, end_time)` 字符串元组。
    用途：
    1. 为股票 task 提供统一时间语义，避免日期型输入产生歧义。
    边界条件：
    1. 日期入参会映射为 start=09:30:00、end=16:00:00。
    """
    start_dt, start_fmt = _parse_task_datetime(start_time)
    end_dt, end_fmt = _parse_task_datetime(end_time)

    if start_fmt in {"ymd_dash", "ymd_slash"}:
        start_dt = start_dt.replace(hour=9, minute=30, second=0, microsecond=0)
        start_fmt = "ymd_hms_slash" if start_fmt == "ymd_slash" else "ymd_hms_dash"
    if end_fmt in {"ymd_dash", "ymd_slash"}:
        end_dt = end_dt.replace(hour=16, minute=0, second=0, microsecond=0)
        end_fmt = "ymd_hms_slash" if end_fmt == "ymd_slash" else "ymd_hms_dash"

    return _format_task_datetime(start_dt, start_fmt), _format_task_datetime(end_dt, end_fmt)


def _normalize_future_time_window(start_time: Any, end_time: Any) -> tuple[str, str]:
    """
    标准化期货任务时间窗口。

    输入：
    1. start_time/end_time: 原始时间值，支持 str/date/datetime。
    输出：
    1. 标准化后的 `(start_time, end_time)` 字符串元组。
    用途：
    1. 统一期货 K 线时间过滤口径。
    边界条件：
    1. 日期入参会映射为 start=09:00:00、end=15:00:00。
    """
    start_dt, start_fmt = _parse_task_datetime(start_time)
    end_dt, end_fmt = _parse_task_datetime(end_time)

    if start_fmt in {"ymd_dash", "ymd_slash"}:
        start_dt = start_dt.replace(hour=9, minute=0, second=0, microsecond=0)
        start_fmt = "ymd_hms_slash" if start_fmt == "ymd_slash" else "ymd_hms_dash"
    if end_fmt in {"ymd_dash", "ymd_slash"}:
        end_dt = end_dt.replace(hour=15, minute=0, second=0, microsecond=0)
        end_fmt = "ymd_hms_slash" if end_fmt == "ymd_slash" else "ymd_hms_dash"

    return _format_task_datetime(start_dt, start_fmt), _format_task_datetime(end_dt, end_fmt)


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

        输入：
        1. raw: 包含 code/freq/start_time/end_time 的字典。
        输出：
        1. `StockKlineTask` 实例。
        用途：
        1. 兼容 dict 任务输入并统一校验行为。
        边界条件：
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


def _normalize_task_input(task: List[Any]) -> List[Dict[str, str]]:
    """
    标准化 task 列表输入。

    输入：
    1. task: list/tuple，元素为 dict 或 StockKlineTask。
    输出：
    1. 标准化 task 字典列表。
    用途：
    1. 统一 get_stock_kline 的任务输入口径。
    边界条件：
    1. 空列表、非列表或非法元素会抛 ValueError。
    """
    if not isinstance(task, (list, tuple)):
        raise ValueError("task 必须是列表，元素为 dict 或 StockKlineTask")
    normalized: List[Dict[str, str]] = []
    for item in list(task):
        if isinstance(item, StockKlineTask):
            normalized.append(item.to_dict())
            continue
        normalized.append(StockKlineTask.from_dict(item).to_dict())
    if not normalized:
        raise ValueError("task 列表不能为空")
    return normalized


def _resolve_active_config_path(config_path: Optional[str]) -> str:
    """
    解析当前生效配置路径。

    输入：
    1. config_path: 可选显式路径。
    输出：
    1. 当前应使用的配置路径字符串。
    用途：
    1. 在 simple_api 层实现“显式优先、上下文复用、默认兜底”。
    边界条件：
    1. 未传参时按 `_ACTIVE_CONFIG_PATH -> 默认配置` 顺序回退。
    """
    requested = str(config_path or "").strip()
    if requested:
        return requested
    if _ACTIVE_CONFIG_PATH:
        return str(_ACTIVE_CONFIG_PATH)
    return _DEFAULT_CONFIG_PATH


def _borrow_client() -> tuple[UnifiedTdxClient, bool]:
    """
    获取可用客户端并返回是否需要自动关闭。

    输入：
    1. 无显式输入参数。
    输出：
    1. `(client, need_close)`。
    用途：
    1. 统一处理“上下文复用”和“临时实例自动释放”。
    边界条件：
    1. 有活动上下文时不会新建客户端。
    """
    context_client = UnifiedTdxClient.get_active_context_client()
    if context_client is not None:
        return context_client, False
    return get_client(), True


def _call_with_client(func: Callable[[UnifiedTdxClient], Any]) -> Any:
    """
    在统一客户端上下文中执行回调。

    输入：
    1. func: 以 `UnifiedTdxClient` 为参数的回调。
    输出：
    1. 回调执行结果。
    用途：
    1. 为非迭代接口提供统一资源管理。
    边界条件：
    1. 临时创建的客户端会在 finally 中关闭。
    """
    client, need_close = _borrow_client()
    try:
        return func(client)
    finally:
        if need_close:
            client.close()


def _iter_with_client(iterator_factory: Callable[[UnifiedTdxClient], Iterator[Any]]) -> Iterator[Any]:
    """
    在统一客户端上下文中创建迭代器。

    输入：
    1. iterator_factory: 以 client 为参数的迭代器工厂函数。
    输出：
    1. 可迭代对象。
    用途：
    1. 预留给需要流式返回的接口，统一处理资源释放。
    边界条件：
    1. 无活动上下文时，会包装生成器并在迭代结束后关闭 client。
    """
    client, need_close = _borrow_client()
    iterator = iterator_factory(client)
    if not need_close:
        return iterator

    def _gen():
        """生成器包装，确保迭代结束后关闭客户端。"""
        try:
            for item in iterator:
                yield item
        finally:
            client.close()

    return _gen()


def get_client(
    config_path: Optional[str] = None,
    separate_instance: bool = False,
) -> UnifiedTdxClient:
    """获取客户端实例。

    输入:
    - config_path: 配置文件路径；仅需初始化时设置一次。
    - separate_instance: 是否强制新建独立客户端实例；为 True 时忽略当前 with 上下文并创建新实例。

    输出:
    - UnifiedTdxClient 实例。

    调用示例:
    ```python
    # 推荐：一个 with 中复用同一 client，避免重复建连。
    with get_client() as client:
        markets = get_supported_markets(return_df=True)
        stock_map = get_stock_code_name()
        prices = get_stock_latest_price(["600000", "000001"])
    ```

    返回示例:
    ```python
    <zsdtdx.wrapper.unified_client.UnifiedTdxClient object at 0x...>
    ```

    边界条件:
    - 若在 with 上下文中且本次未显式传参，会优先返回当前上下文客户端。
    - outside-with 直接调用会返回独立客户端实例，建议配合 with 或显式调用 close()。
    """
    global _ACTIVE_CONFIG_PATH

    active_context_client = UnifiedTdxClient.get_active_context_client()
    if active_context_client is not None and not separate_instance and str(config_path or "").strip() == "":
        return active_context_client

    new_path = _resolve_active_config_path(config_path)
    _ACTIVE_CONFIG_PATH = new_path
    set_active_config_path(new_path)
    return UnifiedTdxClient(config_path=new_path)


def get_supported_markets(return_df: Optional[bool] = None):
    """获取标准+扩展行情支持的市场列表。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    调用示例:
    ```python
    with get_client() as client:
        df = get_supported_markets(return_df=True)
    ```

    返回示例:
    ```json
    [{"market": 0, "name": "深圳", "source": "std"}, {"market": 1, "name": "上海", "source": "std"}]
    ```
    """
    return _call_with_client(lambda client: client.get_supported_markets(return_df=return_df))


def get_stock_code_name(use_cache: bool = True) -> Dict[str, str]:
    """获取统一股票代码名称字典。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
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
    with get_client() as client:
        stock_map = get_stock_code_name()
    ```

    返回示例:
    ```json
    {"sh.600000": "浦发银行", "sz.000001": "平安银行"}
    ```
    """
    return _call_with_client(lambda client: client.get_stock_code_name_map(use_cache=use_cache))


def get_all_future_list(return_df: Optional[bool] = None, use_cache: bool = True):
    """获取统一商品期货列表（郑州/大连/上海/广州）。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - use_cache: 是否使用期货清单缓存；为 False 时强制刷新期货清单缓存。

    调用示例:
    ```python
    with get_client() as client:
        future_df = get_all_future_list(return_df=True)
    ```

    返回示例:
    ```json
    [{"code": "CU2603", "name": "沪铜2603", "market_name": "上海期货", "source": "ex"}]
    ```
    """
    return _call_with_client(lambda client: client.get_all_future_list(return_df=return_df, use_cache=use_cache))


def get_stock_kline(
    task: List[Any],
    queue: Optional[Any] = None,
    preprocessor_operator: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    mode: str = "async",
) -> Any:
    """获取股票 K 线任务结果（任务化输入，支持同步/异步与队列实时回传）。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - task: 任务列表，元素是 `StockKlineTask` 或 dict，模板字段:
      `{code, freq, start_time, end_time}`。
    - queue: 可选队列，需支持 `put()`；sync 模式可传入，async 模式不传时会自动创建并挂到返回 job.queue。
    - preprocessor_operator: 可选预处理函数，签名 `f(payload)->dict|None`；
      返回 None 或空 dict 时该条结果不入队也不进入返回值。
    - mode: `"sync"` 或 `"async"`，默认 `"async"`。sync 阻塞直到完成；async 立即返回句柄。

    调用示例（sync，阻塞直到完成）:
    ```python
    import queue as py_queue
    from zsdtdx import get_client, get_stock_kline, StockKlineTask
    
    with get_client() as client:
        # sync 模式可传入队列，边产出边消费（也可不传，仅用返回值）。
        q = py_queue.Queue()
        result = get_stock_kline(
            task=[
                # 使用任务对象写法（推荐，字段校验更明确）
                StockKlineTask(code="600000", freq="d", start_time="2026-02-13", end_time="2026-02-13"),
                # 也支持 dict 写法
                {"code": "000001", "freq": "60", "start_time": "2026-02-13", "end_time": "2026-02-14"},
            ],
            queue=q,
            mode="sync",
        )
        # result 为完整 payload 列表；q 中也会收到相同 data 事件和最终 done 事件
        print(result)
    ```

    调用示例（async，实时消费队列）:
    ```python
    from zsdtdx import get_client, get_stock_kline

    with get_client() as client:
        job = get_stock_kline(
            task=[{"code": "600000", "freq": "d", "start_time": "2026-02-13", "end_time": "2026-02-13"}],
            mode="async",
        )
        while True:
            # 实时读取 data 事件，直到 done
            event = job.queue.get(timeout=20)
            if event.get("event") == "done":
                break
            # event="data" 时可按 task/rows/error 增量处理
            print(event.get("task"), event.get("error"))
        # 等待后台任务完全结束并传播异常
        job.result()
    ```

    返回:
    - mode="sync": 返回 `list[task_payload]`。
    - mode="async": 返回 `StockKlineJob`，并可通过 `job.queue` 持续消费实时结果。
    - task_payload 结构:
      `{"event":"data","task":{...},"rows":[...],"error":str|None,"worker_pid":int}`。
    - 队列最终会额外推送 done 事件:
      `{"event":"done","total_tasks":...,"success_tasks":...,"failed_tasks":...}`。
    """
    if queue is not None and not hasattr(queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")
    if preprocessor_operator is not None and not callable(preprocessor_operator):
        raise ValueError("preprocessor_operator 必须是可调用对象")

    normalized_tasks = _normalize_task_input(task)
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


def get_future_kline(
    codes: Optional[Any] = None,
    freq: Union[str, List[str]] = None,
    start_time: Any = None,
    end_time: Any = None,
) -> pd.DataFrame:
    """获取商品期货 K 线（支持多周期并行获取，返回合并后的 DataFrame）。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
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
    
    with get_client() as client:
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
    - 进程数: 自动计算 = int(CPU物理核心数 × 1.5)，至少2个进程
    - 全局进程池: 首次并行调用时创建（约2-3秒开销），后续调用复用，程序退出时统一关闭
    - 推荐分批: 100个期货×5周期=500任务/批

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
    - 请先进入 `with get_client() as client:`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - code: 股票代码。
    - category: 中文分类名列表；为空时返回全部分类。

    调用示例:
    ```python
    with get_client() as client:
        info_df = get_company_info(code="689009", category=["最新提示", "公司概况"], return_df=True)
    ```

    返回示例:
    ```json
    [{"code": "689009", "category": "公司概况", "content": "......"}]
    ```
    """
    return _call_with_client(
        lambda client: client.get_company_info_content(code=code, category=category, return_df=return_df)
    )


def get_stock_latest_price(codes: Optional[Any] = None) -> Dict[str, Optional[float]]:
    """获取股票实时最新价字典。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - codes: 可选股票代码列表；支持 `sh./sz./bj./hk.` 前缀；
      为空时按 `config.yaml.stock_scope.defaults_when_codes_none.get_stock_latest_price`
      拉取默认范围全量股票；显式传入代码时不受该范围开关影响。

    调用示例:
    - 1个code:
    ```python
    with get_client() as client:
        one = get_stock_latest_price("600000")
    ```
    - 2个code:
    ```python
    with get_client() as client:
        two = get_stock_latest_price(["600000", "09988"])
    ```
    - 全部code:
    ```python
    with get_client() as client:
        all_prices = get_stock_latest_price()
    ```

    返回示例:
    ```json
    {"600000": 9.98, "09988": 158.5}
    ```
    """
    return _call_with_client(lambda client: client.get_stock_latest_price(codes=codes))


def get_future_latest_price(codes: Optional[Any] = None) -> Dict[str, Optional[float]]:
    """获取商品期货实时最新价字典。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    输入:
    - codes: 可选期货代码列表；不含数字会自动补 `L8`；为空时拉取全部商品期货。

    调用示例:
    - 1个code:
    ```python
    with get_client() as client:
        one = get_future_latest_price("CU2603")
    ```
    - 2个code:
    ```python
    with get_client() as client:
        two = get_future_latest_price(["AL", "CU2603"])
    ```
    - 全部code:
    ```python
    with get_client() as client:
        all_prices = get_future_latest_price()
    ```

    返回示例:
    ```json
    {"ALL8": 23610.0, "CU2603": 102330.0}
    ```
    """
    return _call_with_client(lambda client: client.get_future_latest_price(codes=codes))


def get_runtime_failures() -> pd.DataFrame:
    """获取运行期失败/无数据明细。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    调用示例:
    ```python
    with get_client() as client:
        failures = get_runtime_failures()
    ```

    返回示例:
    ```json
    [{"task": "stock_kline", "code": "999999", "freq": "d", "reason": "code_not_found"}]
    ```
    """
    return _call_with_client(lambda client: client.get_failures_df())


def get_runtime_metadata() -> Dict[str, Any]:
    """获取运行元数据快照。

    调用前置约定:
    - 请先进入 `with get_client() as client:`；
      一个 with 块内可连续调用多个 `get_*` 函数。

    调用示例:
    ```python
    with get_client() as client:
        meta = get_runtime_metadata()
    ```

    返回示例:
    ```json
    {"config_path": "<auto>", "std_active_host": "120.76.1.198:7709"}
    ```
    """
    return _call_with_client(lambda client: client.get_runtime_metadata())


__all__ = [
    "StockKlineTask",
    "StockKlineJob",
    "get_client",
    "get_supported_markets",
    "get_stock_code_name",
    "get_all_future_list",
    "get_stock_kline",
    "get_future_kline",
    "get_company_info",
    "get_stock_latest_price",
    "get_future_latest_price",
    "get_runtime_failures",
    "get_runtime_metadata",
]
