"""
模块：`log.py`。

职责：
1. 配置 zsdtdx 包级日志记录器与默认级别。
2. 通过环境变量 `TDX_DEBUG` 控制是否输出 DEBUG 日志。

边界：
1. 仅初始化根 logger `ZSDTDX`，不接管第三方库日志。
"""

# coding=utf-8

import logging
import os

_DEBUG_FLAG = os.getenv("TDX_DEBUG", "").strip().lower()
DEBUG = _DEBUG_FLAG in ("1", "true", "yes", "on")
LOGLEVEL = logging.DEBUG if DEBUG else logging.INFO

log = logging.getLogger("ZSDTDX")

log.setLevel(LOGLEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOGLEVEL)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
log.addHandler(ch)
