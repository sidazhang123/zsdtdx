"""
模块：`helper.py`。

职责：
1. 提供协议解析层共用的二进制解码与 K 线数值格式化工具。
2. K 线 OHLC/量能刻度仅在 socket 解析出口通过本模块函数完成（向量化批量为主）。
3. 聚合可复用的通用工具函数，供 parser 与上层 API 共享。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import struct
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Type

import six

# imported inside functions to avoid circular dependency
# from zsdtdx.parallel_fetcher import set_active_config_path
# from zsdtdx.unified_client import UnifiedTdxClient


#### XXX: 分析了一下，貌似是类似utf-8的编码方式保存有符号数字
def get_price(data, pos):
    """
    输入：
    1. data: 输入参数，约束以协议定义与函数实现为准。
    2. pos: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_price` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    pos_byte = 6
    bdata = indexbytes(data, pos)
    intdata = bdata & 0x3f
    if bdata & 0x40:
        sign = True
    else:
        sign = False

    if bdata & 0x80:
        while True:
            pos += 1
            bdata = indexbytes(data, pos)
            intdata += (bdata & 0x7f) << pos_byte
            pos_byte += 7

            if not (bdata & 0x80):
                break

    pos += 1

    if sign:
        intdata = -intdata

    return intdata, pos


# P1: pow2 预计算查找表，避免每次 get_volume 调用 pow(2.0, x)
_POW2_CACHE_MIN = -300
_POW2_CACHE_MAX = 600
_POW2_CACHE: dict = {}
for _e in range(_POW2_CACHE_MIN, _POW2_CACHE_MAX + 1):
    _POW2_CACHE[_e] = pow(2.0, _e)


def get_volume(ivol):
    """P1 优化：使用预计算查找表替代 pow(2.0, x) 调用。"""
    logpoint = ivol >> 24
    hleax = (ivol >> 16) & 0xff
    lheax = (ivol >> 8) & 0xff
    lleax = ivol & 0xff

    ecx = (logpoint << 1) - 0x7f
    edx = (logpoint << 1) - 0x86
    esi = (logpoint << 1) - 0x8e
    eax = (logpoint << 1) - 0x96

    abs_ecx = -ecx if ecx < 0 else ecx
    dbl_xmm6 = _POW2_CACHE[abs_ecx]
    if ecx < 0:
        dbl_xmm6 = 1.0 / dbl_xmm6

    hleax_hi = hleax & 0x80
    if hleax_hi:
        dbl_xmm0 = _POW2_CACHE[edx] * 128.0 + (hleax & 0x7f) * _POW2_CACHE[edx + 1]
    else:
        if edx >= 0:
            dbl_xmm0 = _POW2_CACHE[edx] * hleax
        else:
            dbl_xmm0 = (1.0 / _POW2_CACHE[-edx]) * hleax

    dbl_xmm3 = _POW2_CACHE[esi] * lheax
    dbl_xmm1 = _POW2_CACHE[eax] * lleax if eax >= 0 else (1.0 / _POW2_CACHE[-eax]) * lleax
    if hleax_hi:
        dbl_xmm3 *= 2.0
        dbl_xmm1 *= 2.0

    return dbl_xmm6 + dbl_xmm0 + dbl_xmm3 + dbl_xmm1


def format_socket_kline_page_inplace(
    opens: Any,
    closes: Any,
    highs: Any,
    lows: Any,
    volumes: Any,
    amounts: Any,
    count: int,
    *,
    settlement_prices: Any = None,
    positions: Any = None,
) -> None:
    """
    单页 K 线数组原地向量化格式化（socket 层唯一批量出口）。

    输入：
    1. opens/closes/highs/lows/volumes: 长度不少于 count 的 float64 numpy 数组。
    2. amounts: 可选；为 None 时跳过成交额 rint（期货等无 amount 场景）。
    3. count: 本页有效 bar 数。
    3. settlement_prices: 可选，期货结算价/price，与 OHLC 同为 2 位小数。
    4. positions: 可选，持仓量，与量能同为整数四舍五入。
    输出：
    1. 无；原地写入刻度（OHLC/结算价 `np.round(..., 2)`，量能/持仓 `np.rint`）。
    用途：
    1. parser 在组 dict 前一次性刻度，避免逐 bar Python round。
    边界条件：
    1. count<=0 时 no-op；调用方组 dict 时对量能/持仓字段使用 int(...)。
    """
    n = int(count)
    if n <= 0:
        return
    import numpy as np

    sl = slice(0, n)
    price_like = [opens, closes, highs, lows]
    if settlement_prices is not None:
        price_like.append(settlement_prices)
    for arr in price_like:
        np.round(arr[sl], 2, out=arr[sl])
    np.rint(volumes[sl], out=volumes[sl])
    if amounts is not None:
        np.rint(amounts[sl], out=amounts[sl])
    if positions is not None:
        np.rint(positions[sl], out=positions[sl])


def format_socket_bar_ohlc_2dp(value: Any) -> Any:
    """
    协议解析层标量格式：开/高/低/收保留两位小数。

    输入：
    1. value: 解析得到的原始数值或可转 float 的类型。
    输出：
    1. float，语义与 `format_socket_kline_page_inplace` 对 OHLC 一致。
    用途：
    1. 无法批量数组化的 parser 分支（如逐条 struct 解析）复用同一刻度。
    边界条件：
    1. None 返回 None；不可解析时原样返回。
    """
    if value is None:
        return None
    try:
        import numpy as np

        x = float(value)
    except (TypeError, ValueError):
        return value
    return float(np.round(np.float64(x), 2))


def format_socket_bar_amount_volume_int(value: Any) -> Any:
    """
    协议解析层标量格式：成交量、成交额为整数（四舍五入）。

    输入：
    1. value: 解析得到的原始数值或可转 float 的类型。
    输出：
    1. int；None 输入返回 None。
    用途：
    1. 与批量 `np.rint` 语义一致的标量出口。
    边界条件：
    1. 不可解析时原样返回。
    """
    if value is None:
        return None
    try:
        import numpy as np

        x = float(value)
    except (TypeError, ValueError):
        return value
    return int(np.rint(np.float64(x)))


def get_datetime(category, buffer, pos):
    """
    输入：
    1. category: 输入参数，约束以协议定义与函数实现为准。
    2. buffer: 输入参数，约束以协议定义与函数实现为准。
    3. pos: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_datetime` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    year = 0
    month = 0
    day = 0
    hour = 15
    minute = 0
    if category < 4 or category == 7 or category == 8:
        (zipday, tminutes) = struct.unpack("<HH", buffer[pos: pos + 4])
        year = (zipday >> 11) + 2004
        month = int((zipday % 2048) / 100)
        day = (zipday % 2048) % 100

        hour = int(tminutes / 60)
        minute = tminutes % 60
    else:
        (zipday,) = struct.unpack("<I", buffer[pos: pos + 4])

        year = int(zipday / 10000);
        month = int((zipday % 10000) / 100)
        day = zipday % 100

    pos += 4

    return year, month, day, hour, minute, pos


def get_time(buffer, pos):
    """
    输入：
    1. buffer: 输入参数，约束以协议定义与函数实现为准。
    2. pos: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_time` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    (tminutes, ) = struct.unpack("<H", buffer[pos: pos + 2])
    hour = int(tminutes / 60)
    minute = tminutes % 60
    pos += 2

    return hour, minute, pos

def indexbytes(data, pos):

    """
    输入：
    1. data: 输入参数，约束以协议定义与函数实现为准。
    2. pos: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `indexbytes` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    if six.PY2:
        if type(data) is bytearray:
            return data[pos]
        else:
            return six.indexbytes(data, pos)
    else:
        return data[pos]


def parse_task_datetime(raw_value: Any) -> tuple[datetime, str]:
    """
    解析任务时间字符串并返回 `(datetime, format_key)`。

    输入：
    1. raw_value: 原始时间值，支持 str/date/datetime。
    输出：
    1. `(parsed_datetime, format_key)`；format_key 用于后续按原风格回写字符串。
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


def format_task_datetime(parsed: datetime, format_key: str) -> str:
    """
    按 format_key 将 datetime 回写为字符串。

    输入：
    1. parsed: 已解析时间。
    2. format_key: 原始格式标识（如 ymd_hms_dash、iso）。
    输出：
    1. 格式化后的时间字符串。
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


def normalize_task_time_window(start_time: Any, end_time: Any) -> tuple[str, str]:
    """
    标准化股票任务时间窗口。

    输入：
    1. start_time/end_time: 原始时间值，支持 str/date/datetime。
    输出：
    1. 标准化后的 `(start_time, end_time)` 字符串元组。
    边界条件：
    1. 日期入参会映射为 start=09:30:00、end=16:00:00。
    """
    start_dt, start_fmt = parse_task_datetime(start_time)
    end_dt, end_fmt = parse_task_datetime(end_time)

    if start_fmt in {"ymd_dash", "ymd_slash"}:
        start_dt = start_dt.replace(hour=9, minute=30, second=0, microsecond=0)
        start_fmt = "ymd_hms_slash" if start_fmt == "ymd_slash" else "ymd_hms_dash"
    if end_fmt in {"ymd_dash", "ymd_slash"}:
        end_dt = end_dt.replace(hour=16, minute=0, second=0, microsecond=0)
        end_fmt = "ymd_hms_slash" if end_fmt == "ymd_slash" else "ymd_hms_dash"

    return format_task_datetime(start_dt, start_fmt), format_task_datetime(end_dt, end_fmt)


def normalize_future_time_window(start_time: Any, end_time: Any) -> tuple[str, str]:
    """
    标准化期货任务时间窗口。

    输入：
    1. start_time/end_time: 原始时间值，支持 str/date/datetime。
    输出：
    1. 标准化后的 `(start_time, end_time)` 字符串元组。
    边界条件：
    1. 日期入参会映射为 start=09:00:00、end=15:00:00。
    """
    start_dt, start_fmt = parse_task_datetime(start_time)
    end_dt, end_fmt = parse_task_datetime(end_time)

    if start_fmt in {"ymd_dash", "ymd_slash"}:
        start_dt = start_dt.replace(hour=9, minute=0, second=0, microsecond=0)
        start_fmt = "ymd_hms_slash" if start_fmt == "ymd_slash" else "ymd_hms_dash"
    if end_fmt in {"ymd_dash", "ymd_slash"}:
        end_dt = end_dt.replace(hour=15, minute=0, second=0, microsecond=0)
        end_fmt = "ymd_hms_slash" if end_fmt == "ymd_slash" else "ymd_hms_dash"

    return format_task_datetime(start_dt, start_fmt), format_task_datetime(end_dt, end_fmt)


def normalize_task_input(task: Sequence[Any], task_cls: Type[Any]) -> List[Dict[str, str]]:
    """
    标准化 task 列表输入。

    输入：
    1. task: list/tuple，元素为 dict 或 task_cls 实例。
    2. task_cls: 任务类，需实现 `to_dict()` 与 `from_dict()`。
    输出：
    1. 标准化 task 字典列表。
    边界条件：
    1. 空列表、非列表或非法元素会抛 ValueError。
    """
    if not isinstance(task, (list, tuple)):
        raise ValueError(f"task 必须是列表，元素为 dict 或 {task_cls.__name__}")
    normalized: List[Dict[str, str]] = []
    for item in list(task):
        if isinstance(item, task_cls):
            normalized.append(item.to_dict())
            continue
        normalized.append(task_cls.from_dict(item).to_dict())
    if not normalized:
        raise ValueError("task 列表不能为空")
    return normalized


def resolve_active_config_path(
    config_path: Optional[str],
    active_config_path: Optional[str],
    default_config_path: str,
) -> str:
    """
    解析当前生效配置路径。

    输入：
    1. config_path: 可选显式路径。
    2. active_config_path: 当前活动配置路径。
    3. default_config_path: 默认配置路径。
    输出：
    1. 应使用的配置路径字符串。
    """
    requested = str(config_path or "").strip()
    if requested:
        return requested
    if active_config_path:
        return str(active_config_path)
    return str(default_config_path)


def borrow_client(
    get_active_context_client: Callable[[], Any],
    build_client: Callable[[], Any],
) -> tuple[Any, bool]:
    """
    获取活动客户端并返回是否需要自动关闭。

    输入：
    1. get_active_context_client: 获取上下文客户端的回调。
    2. build_client: 新建客户端回调。
    输出：
    1. `(client, need_close)`。
    """
    context_client = get_active_context_client()
    if context_client is not None:
        return context_client, False
    return build_client(), True


def call_with_client(
    func: Callable[[Any], Any],
    get_active_context_client: Callable[[], Any],
    build_client: Callable[[], Any],
) -> Any:
    """
    在统一客户端上下文中执行回调。

    输入：
    1. func: 以客户端为参数的回调。
    2. get_active_context_client: 获取上下文客户端的回调。
    3. build_client: 新建客户端回调。
    输出：
    1. 回调执行结果。
    边界条件：
    1. 临时创建的客户端会在 finally 中关闭。
    """
    client, need_close = borrow_client(
        get_active_context_client=get_active_context_client,
        build_client=build_client,
    )
    try:
        return func(client)
    finally:
        if need_close:
            client.close()


# Global variables for config management
_DEFAULT_CONFIG_PATH = str(Path(__file__).resolve().with_name("config.yaml"))
_ACTIVE_CONFIG_PATH: Optional[str] = None
_DEFAULT_CONFIG_NOTICE_PRINTED: bool = False


def _apply_active_config_path(config_path: str) -> str:
    """
    解析并激活 simple_api 的全局配置路径。

    输入:
    - config_path: 待激活配置路径（支持相对/绝对路径）。

    输出:
    - 解析后的绝对配置路径字符串。

    边界条件:
    - 配置路径不存在、YAML 非法或 hosts 配置无效时会抛出异常。
    """
    global _ACTIVE_CONFIG_PATH

    requested = str(config_path or "").strip()
    if requested == "":
        raise ValueError("config_path 不能为空")

    # import here to avoid circular imports
    from zsdtdx.unified_client import UnifiedTdxClient
    from zsdtdx.parallel_fetcher import set_active_config_path

    client = UnifiedTdxClient(config_path=requested)
    resolved_path = str(client.config_path)
    try:
        client.close()
    except Exception:
        pass

    _ACTIVE_CONFIG_PATH = resolved_path
    set_active_config_path(resolved_path)
    return resolved_path


def _ensure_active_config_ready(caller_name: str) -> str:
    """
    确保 simple_api 全局配置已就绪。

    输入:
    - caller_name: 触发方名称，用于默认配置提醒文本。

    输出:
    - 当前生效配置路径字符串。

    边界条件:
    - 若用户未显式设置配置，会自动回退到包内默认配置并打印一次提醒。
    """
    global _DEFAULT_CONFIG_NOTICE_PRINTED

    if _ACTIVE_CONFIG_PATH:
        # avoid circular import
        from zsdtdx.parallel_fetcher import set_active_config_path
        set_active_config_path(_ACTIVE_CONFIG_PATH)
        return str(_ACTIVE_CONFIG_PATH)

    default_path = _apply_active_config_path(config_path=_DEFAULT_CONFIG_PATH)
    if not _DEFAULT_CONFIG_NOTICE_PRINTED:
        print(
            f"[zsdtdx.simple_api] {caller_name} 未先调用 set_config_path()，"
            f"当前使用默认配置: {default_path}"
        )
        _DEFAULT_CONFIG_NOTICE_PRINTED = True
    return default_path
