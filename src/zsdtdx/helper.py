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

# imported inside functions to avoid circular dependency
# from zsdtdx.parallel_fetcher import set_active_config_path
# from zsdtdx.unified_client import UnifiedTdxClient


def get_price(data, pos):
    """
    从协议缓冲区解析变长有符号价格（类似 UTF-8 的多字节延续编码）。

    输入：
    1. data: bytes/bytearray 缓冲区。
    2. pos: 起始下标。
    输出：
    1. `(price, next_pos)`：解析出的有符号整数与下一读取位置。
    用途：
    1. K 线等行情字段的增量价格解码。
    边界条件：
    1. 缓冲区越界由调用方保证；符号位与延续位按通达信协议约定解析。
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


# pow2 预计算查找表：覆盖 get_volume 中实际访问的全部 exponent 范围。
# 仅在"已知与 pytdx 数学等价"的分支使用；与 pytdx 行为不一致的分支沿用 pow(2.0,x)。
_POW2_CACHE_MIN = -300
_POW2_CACHE_MAX = 600
_POW2_CACHE: dict = {}
for _e in range(_POW2_CACHE_MIN, _POW2_CACHE_MAX + 1):
    _POW2_CACHE[_e] = pow(2.0, _e)


def get_volume(ivol):
    """
    通达信量能/成交额 IEEE-754-like 解码。

    输入：
    1. ivol: 4 字节无符号整数（从协议体直接 unpack 出）。
    输出：
    1. float，量能/成交额数值。
    用途：
    1. K 线 vol 与 amount 的协议出口解码；输出会被 `format_socket_kline_page_inplace`
       做 `np.rint` 整数化（项目内 schema 设计）。
    边界条件：
    1. **行为必须与 pytdx 原版按位等价**——包括 pytdx 在 `dwEdx < 0` 时
       `1 / pow(2.0, dwEdx) = pow(2.0, -dwEdx)`（与"取倒数"惯例方向相反）这一历史保留特性，
       否则会让量价偏差几百倍。
    2. `if hleax > 0x80` 是严格大于；`hleax == 0x80` 必须走 else 分支。
    """
    logpoint = ivol >> 24
    hleax = (ivol >> 16) & 0xff
    lheax = (ivol >> 8) & 0xff
    lleax = ivol & 0xff

    ecx = (logpoint << 1) - 0x7f
    edx = (logpoint << 1) - 0x86
    esi = (logpoint << 1) - 0x8e
    eax = (logpoint << 1) - 0x96

    # dbl_xmm6 = pow(2.0, |ecx|)；ecx<0 时取倒数（与 pytdx 一致：abs(ecx) 后再倒）。
    abs_ecx = -ecx if ecx < 0 else ecx
    dbl_xmm6 = _POW2_CACHE[abs_ecx]
    if ecx < 0:
        dbl_xmm6 = 1.0 / dbl_xmm6

    # hleax 分支：严格 > 0x80 走大分支（pytdx 行为，hleax == 0x80 走 else）。
    if hleax > 0x80:
        # pytdx: pow(2, edx) * 128 + (hleax&0x7f) * pow(2, edx+1)；edx 可正可负，缓存覆盖。
        dbl_xmm0 = _POW2_CACHE[edx] * 128.0 + (hleax & 0x7f) * _POW2_CACHE[edx + 1]
    else:
        if edx >= 0:
            dbl_xmm0 = _POW2_CACHE[edx] * hleax
        else:
            # pytdx 历史行为：dwEdx<0 时用 `1/pow(2.0, dwEdx) = pow(2.0, -dwEdx)`。
            # 这一分支与"取倒数"方向相反，但必须保持以与 pytdx 按位等价。
            dbl_xmm0 = _POW2_CACHE[-edx] * hleax

    # 末两段 pow 累加：pytdx 在所有分支都直接 pow(2.0, esi)/pow(2.0, eax)，不取倒数。
    dbl_xmm3 = _POW2_CACHE[esi] * lheax
    dbl_xmm1 = _POW2_CACHE[eax] * lleax
    # `if hleax & 0x80`：包含 0x80-0xFF（与 pytdx 一致，与上方 `> 0x80` 不冲突）。
    if hleax & 0x80:
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
    读取缓冲区指定下标的单字节（0–255）。

    输入：
    1. data: bytes 或 bytearray。
    2. pos: 下标。
    输出：
    1. int，单字节无符号值。
    用途：
    1. `get_price` 等解码路径的统一字节访问。
    边界条件：
    1. 要求 Python 3；越界由调用方保证。
    """
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


def _validate_config_or_raise(cfg: Dict[str, Any], resolved_path: str) -> None:
    """
    校验配置文件结构（不触发 TCP 探测或 UnifiedTdxClient 建连）。

    输入：
    1. cfg: 已加载 YAML 字典。
    2. resolved_path: 解析后的绝对路径（用于错误信息）。
    输出：无。
    边界条件：
    1. standard hosts 缺失或无效时抛出 ValueError。
    2. extended 配置存在但无效时抛出 ValueError。
    """
    from zsdtdx.unified_client import normalize_hosts_entries

    if not isinstance(cfg, dict):
        raise ValueError(f"配置无效（非字典）: {resolved_path}")

    hosts_cfg = cfg.get("hosts", {}) or {}
    std_raw = hosts_cfg.get("standard", [])
    if not std_raw:
        raise ValueError(f"配置缺少 hosts.standard: {resolved_path}")
    normalize_hosts_entries(std_raw)

    ex_raw = hosts_cfg.get("extended", []) or []
    if ex_raw:
        normalize_hosts_entries(ex_raw)


def _apply_active_config_path(
    config_path: str,
    *,
    async_background_probe: bool = True,
) -> str:
    """
    解析并激活 simple_api 的全局配置路径，并按需预热 TCP 可用地址缓存。

    输入：
    1. config_path: 待激活配置路径（支持相对/绝对路径）。
    2. async_background_probe: True 时后台线程调用 `_ensure_availability_hosts_cache`。
    输出：
    1. 解析后的绝对配置路径字符串。
    边界条件：
    1. 路径不存在、YAML 非法或 hosts 无效时抛出异常。
    2. 校验阶段不构造 UnifiedTdxClient，避免同步探测破坏异步语义。
    """
    global _ACTIVE_CONFIG_PATH

    import threading

    import yaml
    from zsdtdx.parallel_fetcher import set_active_config_path
    from zsdtdx.unified_client import (
        _cache_usable_for_cfg,
        _ensure_availability_hosts_cache,
        _resolve_zsdtdx_config_path,
    )

    requested = str(config_path or "").strip()
    if requested == "":
        raise ValueError("config_path 不能为空")

    resolved = _resolve_zsdtdx_config_path(requested)
    resolved_str = str(resolved.resolve())
    with open(resolved, "r", encoding="utf-8") as fp:
        cfg = yaml.safe_load(fp) or {}

    _validate_config_or_raise(cfg, resolved_str)

    _ACTIVE_CONFIG_PATH = resolved_str
    set_active_config_path(resolved_str)

    if not _cache_usable_for_cfg(cfg):
        if async_background_probe:

            def _background_ensure() -> None:
                try:
                    _ensure_availability_hosts_cache(
                        config_path=resolved_str,
                        cfg=cfg,
                    )
                except Exception as exc:
                    from zsdtdx.log import log

                    log.error(
                        "[set_config_path] 后台 TCP 可用地址探测失败: %s",
                        exc,
                    )

            threading.Thread(
                target=_background_ensure,
                name="zsdtdx-tcp-probe",
                daemon=True,
            ).start()
        else:
            try:
                _ensure_availability_hosts_cache(config_path=resolved_str, cfg=cfg)
            except Exception as exc:
                from zsdtdx.log import log

                log.error(
                    "[set_config_path] 同步 TCP 可用地址探测失败: %s",
                    exc,
                )

    return resolved_str


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
