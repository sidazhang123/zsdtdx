"""
模块：`util/log.py`。

职责：
1. 配置并复用 zsdtdx 包级 logger。
2. 使用 YAML 中唯一的全局阈值过滤代码内固定级别的日志。

边界：
1. 仅管理 `ZSDTDX` logger，不接管第三方库日志。
2. 配置尚未加载时使用 INFO，加载 YAML 后原地更新 logger 与 handler。
"""

from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "ERROR": logging.ERROR,
    "OFF": logging.CRITICAL + 100,
}
_DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

log = logging.getLogger("ZSDTDX")
log.propagate = False

_handler: Optional[logging.Handler] = None


def _parse_level(value: Any, default: int) -> int:
    """输入日志级别，输出 logging 数值；仅允许 DEBUG/INFO/ERROR/OFF。"""
    if value is None or str(value).strip() == "":
        return int(default)
    key = str(value or "").strip().upper()
    if key not in _LEVELS:
        allowed = "/".join(_LEVELS)
        raise ValueError(f"logging.level 仅支持 {allowed}，当前值: {value!r}")
    return int(_LEVELS[key])


def configure_logging(config: Optional[Mapping[str, Any]] = None) -> None:
    """
    输入完整配置或 logging 子配置，原地更新包级日志策略。

    `level` 是唯一的输出控制项，仅支持 DEBUG/INFO/ERROR/OFF。
    每条日志的实际级别由调用处固定，不允许通过 YAML 重新分类。
    """
    global _handler, DEBUG, LOGLEVEL

    raw: Mapping[str, Any] = config or {}
    nested = raw.get("logging") if isinstance(raw, Mapping) else None
    logging_cfg = nested if isinstance(nested, Mapping) else raw

    output_level = _parse_level(logging_cfg.get("level"), logging.INFO)
    formatter_text = str(logging_cfg.get("format") or _DEFAULT_FORMAT)

    if _handler is None:
        _handler = logging.StreamHandler()
        _handler._zsdtdx_handler = True  # type: ignore[attr-defined]
        log.addHandler(_handler)

    _handler.setLevel(output_level)
    _handler.setFormatter(logging.Formatter(formatter_text))
    log.setLevel(output_level)
    LOGLEVEL = output_level
    DEBUG = log.isEnabledFor(logging.DEBUG)


def resolve_event_level(level: str) -> int:
    """输入代码中写死的级别名，输出对应 logging 数值。"""
    key = str(level or "info").strip().lower()
    return _parse_level(key, logging.INFO)


def is_event_enabled(level: str) -> bool:
    """返回事件是否会经过当前 logger 阈值输出；OFF 永远返回 False。"""
    numeric = resolve_event_level(level)
    return numeric <= logging.CRITICAL and log.isEnabledFor(numeric)


def event_level_name(level: str) -> str:
    """输入事件名，输出回调可消费的标准小写日志级别名。"""
    numeric = resolve_event_level(level)
    if numeric > logging.CRITICAL:
        return "off"
    return str(logging.getLevelName(numeric)).lower()


def emit_log(level: str, message: str, *args: Any) -> None:
    """按代码固定级别输出日志；低于 YAML 全局阈值时直接跳过。"""
    numeric = resolve_event_level(level)
    if numeric > logging.CRITICAL or not log.isEnabledFor(numeric):
        return
    log.log(numeric, message, *args)


# 随后 `_load_merged_zsdtdx_config` 会应用 YAML 中的全局阈值。
DEBUG = False
LOGLEVEL = logging.INFO
configure_logging({})
