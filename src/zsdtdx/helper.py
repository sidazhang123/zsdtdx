"""
模块：`helper.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 聚合可复用的通用工具函数，供上层 API 与底层协议模块共享。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import struct
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Type

import six


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

            if bdata & 0x80:
                pass
            else:
                break

    pos += 1

    if sign:
        intdata = -intdata

    return intdata, pos


def get_volume(ivol):
    """
    输入：
    1. ivol: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_volume` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    logpoint = ivol >> (8 * 3)
    hleax = (ivol >> (8 * 2)) & 0xff;  # [2]
    lheax = (ivol >> 8) & 0xff;  # [1]
    lleax = ivol & 0xff;  # [0]

    dwEcx = logpoint * 2 - 0x7f;
    dwEdx = logpoint * 2 - 0x86;
    dwEsi = logpoint * 2 - 0x8e;
    dwEax = logpoint * 2 - 0x96;
    if dwEcx < 0:
        tmpEax = - dwEcx
    else:
        tmpEax = dwEcx

    dbl_xmm6 = 0.0
    dbl_xmm6 = pow(2.0, tmpEax)
    if dwEcx < 0:
        dbl_xmm6 = 1.0 / dbl_xmm6

    dbl_xmm4 = 0
    if hleax > 0x80:
        tmpdbl_xmm3 = 0.0
        dwtmpeax = dwEdx + 1
        tmpdbl_xmm3 = pow(2.0, dwtmpeax)
        dbl_xmm0 = pow(2.0, dwEdx) * 128.0
        dbl_xmm0 += (hleax & 0x7f) * tmpdbl_xmm3
        dbl_xmm4 = dbl_xmm0

    else:
        dbl_xmm0 = 0.0
        if dwEdx >= 0:
            dbl_xmm0 = pow(2.0, dwEdx) * hleax
        else:
            dbl_xmm0 = (1 / pow(2.0, dwEdx)) * hleax
        dbl_xmm4 = dbl_xmm0

    dbl_xmm3 = pow(2.0, dwEsi) * lheax
    dbl_xmm1 = pow(2.0, dwEax) * lleax
    if hleax & 0x80:
        dbl_xmm3 *= 2.0
        dbl_xmm1 *= 2.0

    dbl_ret = dbl_xmm6 + dbl_xmm4 + dbl_xmm3 + dbl_xmm1
    return dbl_ret


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
        raise ValueError("task 必须是列表，元素为 dict 或 StockKlineTask")
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
