"""指数名称路由的落盘缓存（pickle）。

用途：
1. 在 pip 安装场景下将指数目录与名称->路由映射持久化到用户可写目录。
2. 通过原子替换写入与加载后校验，降低文件损坏导致崩溃的概率。

边界：
1. 不负责网络或业务解析，仅做序列化/反序列化与路径解析。
2. 多进程并发写可能产生竞态；当前以“最后一次写入为准”，不引入额外依赖锁。
"""

from __future__ import annotations

import hashlib
import json
import os
import pickle
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 与磁盘格式绑定；升级结构时递增并做兼容分支。
_INDEX_ROUTE_CACHE_FORMAT_VERSION = 1


def default_zsdtdx_user_cache_dir() -> Path:
    """
    返回 zsdtdx 默认用户缓存根目录（pip install 后仍可写）。

    输入：
    1. 无显式输入参数。
    输出：
    1. 绝对路径 Path。
    用途：
    1. Windows 使用 LOCALAPPDATA；非 Windows 使用 XDG_CACHE_HOME 或 ~/.cache。
    边界条件：
    1. 目录可能尚不存在，调用方负责 mkdir。
    """
    if os.name == "nt":
        local = os.environ.get("LOCALAPPDATA", "").strip()
        if local:
            return Path(local) / "zsdtdx" / "cache"
        return Path.home() / "AppData" / "Local" / "zsdtdx" / "cache"
    xdg = os.environ.get("XDG_CACHE_HOME", "").strip()
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "zsdtdx"


def fingerprint_index_kline_config(index_kline_cfg: Any) -> str:
    """
    对 index_kline 配置段做稳定指纹，用于缓存失效判断。

    输入：
    1. index_kline_cfg: 配置中的 index_kline 字典（或可被 json 序列化的结构）。
    输出：
    1. sha256 十六进制字符串。
    用途：
    1. 用户修改别名、prefer_ex_markets 等后自动丢弃旧缓存。
    边界条件：
    1. 非 dict 时按空 dict 处理。
    """
    if not isinstance(index_kline_cfg, dict):
        payload: Dict[str, Any] = {}
    else:
        payload = index_kline_cfg
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _is_valid_route_record(obj: Any) -> bool:
    """校验单条路由记录结构是否合法。"""
    if not isinstance(obj, dict):
        return False
    name = str(obj.get("name", "")).strip()
    source = str(obj.get("source", "")).strip().lower()
    code = str(obj.get("code", "")).strip()
    try:
        market = int(obj.get("market", -1))
    except Exception:
        return False
    if name == "" or source not in {"std", "ex"} or code == "" or market < 0:
        return False
    if "market_name" in obj and obj["market_name"] is not None:
        if not isinstance(obj["market_name"], str):
            return False
    return True


def validate_cache_payload(obj: Any) -> Optional[Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]]:
    """
    校验反序列化后的缓存根对象。

    输入：
    1. obj: pickle 加载结果。
    输出：
    1. (name_route, catalog) 或 None（表示不可用）。
    用途：
    1. 避免损坏或恶意 pickle 污染内存。
    边界条件：
    1. 结构不符时返回 None。
    """
    if not isinstance(obj, dict):
        return None
    if int(obj.get("format_version", -1)) != _INDEX_ROUTE_CACHE_FORMAT_VERSION:
        return None
    fp = str(obj.get("fingerprint", "")).strip()
    if fp == "":
        return None
    name_route = obj.get("name_route")
    catalog = obj.get("catalog")
    if not isinstance(name_route, dict) or not isinstance(catalog, list):
        return None
    clean_nr: Dict[str, Dict[str, Any]] = {}
    for key, val in name_route.items():
        if not isinstance(key, str) or not str(key).strip():
            return None
        if not _is_valid_route_record(val):
            return None
        clean_nr[str(key).strip()] = dict(val)
    clean_cat: List[Dict[str, Any]] = []
    for item in catalog:
        if not _is_valid_route_record(item):
            return None
        clean_cat.append(dict(item))
    return clean_nr, clean_cat


def load_index_route_cache(path: Path, expected_fingerprint: str) -> Optional[Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]]:
    """
    从磁盘加载指数路由缓存。

    输入：
    1. path: 缓存文件路径。
    2. expected_fingerprint: 当前配置指纹；不一致则视为未命中并尝试删除损坏/过期文件。
    输出：
    1. (name_route, catalog) 或 None。
    用途：
    1. 进程启动时预热内存缓存。
    边界条件：
    1. 文件不存在返回 None；校验失败会删除该文件（若可删）。
    """
    path = Path(path)
    if not path.is_file():
        return None
    try:
        with open(path, "rb") as fp:
            raw = fp.read()
        if len(raw) < 8:
            raise ValueError("too_short")
        obj = pickle.loads(raw)
    except Exception:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    if not isinstance(obj, dict):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    fp_disk = str(obj.get("fingerprint", "")).strip()
    if fp_disk != str(expected_fingerprint or "").strip():
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    validated = validate_cache_payload(obj)
    if validated is None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    return validated


def save_index_route_cache(
    path: Path,
    *,
    fingerprint: str,
    name_route: Dict[str, Dict[str, Any]],
    catalog: List[Dict[str, Any]],
) -> None:
    """
    将指数路由缓存原子写入磁盘。

    输入：
    1. path: 目标文件路径。
    2. fingerprint: 配置指纹。
    3. name_route: 名称键 -> 路由。
    4. catalog: 指数目录列表。
    输出：
    1. 无；失败抛 OSError/IOError 等。
    用途：
    1. 目录更新或解析到新路由后持久化。
    边界条件：
    1. 先写临时文件再 os.replace，降低半截文件风险。
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "format_version": _INDEX_ROUTE_CACHE_FORMAT_VERSION,
        "fingerprint": str(fingerprint),
        "name_route": dict(name_route),
        "catalog": list(catalog),
    }
    data = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    fd: Optional[int] = None
    tmp_path: Optional[Path] = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            dir=str(path.parent),
            prefix=".zsdtdx_index_route_",
            suffix=".tmp",
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "wb") as tmp_fp:
            fd = None
            tmp_fp.write(data)
            tmp_fp.flush()
            os.fsync(tmp_fp.fileno())
        os.replace(str(tmp_path), str(path))
    except Exception:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise
