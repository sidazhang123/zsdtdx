"""统一封装 zsdtdx 的高层客户端，屏蔽分页、市场和连接细节。"""

from __future__ import annotations

import datetime as dt
import ipaddress
import json
import math
import re
import socket as _socket_mod
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Tuple

import pandas as pd
import yaml

from zsdtdx.exhq import TdxExHq_API
from zsdtdx.hq import TdxHq_API
from zsdtdx.index_route_disk_cache import (
    fingerprint_index_kline_config,
    load_index_route_cache,
    resolve_index_route_cache_file_path,
    save_index_route_cache,
)
from zsdtdx.log import log

_PACKAGE_DIR = Path(__file__).resolve().parent
_DEFAULT_CONFIG_PATH = _PACKAGE_DIR / "config.yaml"
_DEFAULT_MAX_KLINE_PAGES = 400


def _resolve_zsdtdx_config_path(config_path: Optional[str]) -> Path:
    """
    解析 zsdtdx 配置文件绝对路径。

    输入：
    1. config_path: 用户配置路径；None 或空串时使用包内默认 config.yaml。
    输出：
    1. 存在的配置文件 Path。
    用途：
    1. 供 TCP 探测刷新与 UnifiedTdxClient 共用同一套路径规则。
    边界条件：
    1. 文件不存在时抛出 FileNotFoundError。
    """
    if config_path is None or str(config_path).strip() == "":
        candidate = _DEFAULT_CONFIG_PATH
        if candidate.exists():
            return candidate.resolve()
        raise FileNotFoundError(f"默认配置文件不存在: {candidate}")

    requested = Path(str(config_path))
    if requested.is_absolute():
        if requested.exists():
            return requested.resolve()
        raise FileNotFoundError(f"配置文件不存在: {requested}")

    cwd_candidate = requested
    if cwd_candidate.exists():
        return cwd_candidate.resolve()

    package_candidate = _PACKAGE_DIR / requested
    if package_candidate.exists():
        return package_candidate.resolve()

    raise FileNotFoundError(
        f"配置文件不存在: {requested}；已尝试 {cwd_candidate.resolve()} 与 {package_candidate.resolve()}"
    )


def normalize_hosts_entries(hosts: List[Any]) -> List[Tuple[str, int]]:
    """
    将 YAML 中的 hosts 段规范为 (ip, port) 列表。

    输入：
    1. hosts: 字符串 "ip:port"、二元组或字典等混合列表。
    输出：
    1. 校验通过的 (host, port) 列表。
    用途：
    1. UnifiedTdxClient 与 TCP 探测指纹共用同一规范化逻辑。
    边界条件：
    1. 全部无效时抛出 ValueError。
    """
    normalized: List[Tuple[str, int]] = []
    for item in hosts or []:
        ip = None
        port = None
        if isinstance(item, str) and ":" in item:
            ip, p = item.rsplit(":", 1)
            port = int(p)
        elif isinstance(item, (tuple, list)) and len(item) == 2:
            ip, port = str(item[0]), int(item[1])
        elif isinstance(item, dict):
            ip = str(item.get("ip", ""))
            port = int(item.get("port", 0))

        if not ip or not port:
            continue
        try:
            ipaddress.ip_address(ip)
        except Exception:
            continue
        if not (1 <= int(port) <= 65535):
            continue
        normalized.append((ip, int(port)))

    if not normalized:
        raise ValueError("配置中的 host 全部无效")
    return normalized


def compute_hosts_fingerprint(
    std_hosts: List[Tuple[str, int]],
    ex_hosts: List[Tuple[str, int]],
) -> Tuple[Tuple[Tuple[str, int], ...], Tuple[Tuple[str, int], ...]]:
    """
    计算 standard / extended 地址集合指纹（与 YAML 列表顺序无关）。

    输入：
    1. std_hosts / ex_hosts: 已规范化的 (host, port) 列表。
    输出：
    1. 二元组 (standard 排序元组, extended 排序元组)，用于判断集合是否变化。
    用途：
    1. 仅在地址集合变化时重新执行 TCP 全池探测。
    边界条件：
    1. host 均视为 strip 后的字符串再比较。
    """
    std_key = tuple(sorted((str(h).strip(), int(p)) for h, p in std_hosts))
    ex_key = tuple(sorted((str(h).strip(), int(p)) for h, p in ex_hosts))
    return (std_key, ex_key)


def _tcp_probe_and_trim_available_hosts(
    hosts: List[Tuple[str, int]],
    timeout: float,
    fallback_hosts: List[Tuple[str, int]],
    pool_label: str = "",
) -> List[Tuple[str, int]]:
    """
    对地址池做 TCP 探测后裁剪：剔除不可达；可达按延迟升序；若可达数≥2 再去掉最慢 1 个；若结果为空则回退 fallback 顺序。

    输入：
    1. hosts: 待探测的 (host, port) 列表。
    2. timeout: 单地址探测超时（秒）。
    3. fallback_hosts: 裁剪为空时的回退列表（通常为配置原始顺序）。
    4. pool_label: 日志用池名前缀。
    输出：
    1. 裁剪后的可用地址列表。
    用途：
    1. 主进程刷新缓存与 PersistentFailoverPool 首次探测共用。
    边界条件：
    1. hosts 为空时返回 fallback 副本。
    """
    label = str(pool_label or "").strip()
    prefix = f"[{label}] " if label else ""
    if not hosts:
        return list(fallback_hosts)
    if len(hosts) == 1:
        h, p = hosts[0]
        _ht, lat = _tcp_probe_one(h, p, timeout)
        if lat is not None:
            return [hosts[0]]
        log.warning(f"{prefix}[TCP Probe] 单节点不可达，回退配置顺序")
        return list(fallback_hosts)

    workers = min(len(hosts), 32)
    results: List[Tuple[Tuple[str, int], Optional[float]]] = []
    with _ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_tcp_probe_one, h, p, timeout): (h, p) for h, p in hosts
        }
        for future in futures:
            try:
                results.append(future.result(timeout=timeout + 1.0))
            except Exception:
                host_tuple = futures[future]
                results.append((host_tuple, None))

    reachable = [(ht, lat) for ht, lat in results if lat is not None]
    unreachable = [(ht, lat) for ht, lat in results if lat is None]
    reachable.sort(key=lambda x: x[1])

    total = len(results)
    ok_count = len(reachable)
    if ok_count > 0:
        top3 = reachable[:3]
        top3_str = ", ".join(f"{h}:{p} ({lat:.1f}ms)" for (h, p), lat in top3)
        log.info(f"{prefix}[TCP Probe] {ok_count}/{total} 可达, 最快: {top3_str}")
    else:
        log.warning(f"{prefix}[TCP Probe] 0/{total} 可达, 全部超时")
    if unreachable:
        fail_str = ", ".join(f"{h}:{p}" for (h, p), _ in unreachable)
        log.info(f"{prefix}[TCP Probe] 不可达已剔除: {fail_str}")

    trimmed = [ht for ht, _ in reachable]
    if len(trimmed) >= 2:
        dropped = trimmed[-1]
        trimmed = trimmed[:-1]
        log.info(f"{prefix}[TCP Probe] 去掉最慢节点: {dropped[0]}:{dropped[1]}")
    if not trimmed:
        log.warning(
            f"{prefix}[TCP Probe] 裁剪后为空，回退配置原始顺序 ({len(fallback_hosts)} 项)"
        )
        return list(fallback_hosts)
    return trimmed


# ---------------------------------------------------------------------------
# TCP 延迟探测：进程内可用地址缓存（主进程全量列表；worker 经种子化写入槽位列表）
# ---------------------------------------------------------------------------
_probe_result_cache: Dict[str, List[Tuple[str, int]]] = {}
_probe_result_cache_lock = threading.Lock()
_last_tcp_probe_hosts_fingerprint: Optional[
    Tuple[Tuple[Tuple[str, int], ...], Tuple[Tuple[str, int], ...]]
] = None
_ensure_availability_lock = threading.Lock()
_ensure_inflight_event = threading.Event()
_ensure_inflight_event.set()


def get_probe_result_cache() -> Dict[str, List[Tuple[str, int]]]:
    """
    返回当前进程 TCP 探测排序结果快照。

    输入：无。
    输出：含 standard/extended 列表副本的字典。
    边界条件：未写入过时可能缺少 key 或为空列表。
    """
    with _probe_result_cache_lock:
        return {k: list(v) for k, v in _probe_result_cache.items()}


def _normalize_hosts_from_cfg(
    cfg: Dict[str, Any],
) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
    """
    从配置字典解析 standard / extended 地址列表。

    输入：
    1. cfg: 已加载 YAML 字典。
    输出：
    1. (std_hosts, ex_hosts)；extended 配置为空时 ex_hosts 为 []。
    边界条件：
    1. standard 无效或为空时抛出 ValueError。
    """
    hosts_cfg = cfg.get("hosts", {}) or {}
    std_hosts = normalize_hosts_entries(hosts_cfg.get("standard", []))
    ex_raw = hosts_cfg.get("extended", []) or []
    if not ex_raw:
        ex_hosts: List[Tuple[str, int]] = []
    else:
        try:
            ex_hosts = normalize_hosts_entries(ex_raw)
        except ValueError:
            ex_hosts = []
    return std_hosts, ex_hosts


def _cache_usable_for_cfg(cfg: Dict[str, Any]) -> bool:
    """
    判断当前进程内 `_probe_result_cache` 是否可用于该配置。

    输入：
    1. cfg: 已加载 YAML 字典。
    输出：
    1. True 表示缓存非空且指纹与 cfg 中 hosts 一致。
    边界条件：
    1. 配置侧为空的池不要求缓存项；指纹不一致视为不可用。
    """
    std_hosts, ex_hosts = _normalize_hosts_from_cfg(cfg)
    fp_new = compute_hosts_fingerprint(std_hosts, ex_hosts)
    with _probe_result_cache_lock:
        if _last_tcp_probe_hosts_fingerprint != fp_new:
            return False
        if std_hosts and not _probe_result_cache.get("standard"):
            return False
        if ex_hosts and not _probe_result_cache.get("extended"):
            return False
        return True


def _probe_trim_and_write_availability_cache(
    std_hosts: List[Tuple[str, int]],
    ex_hosts: List[Tuple[str, int]],
    probe_timeout: float,
) -> Dict[str, List[Tuple[str, int]]]:
    """
    对 std/ex 执行 TCP 探测裁剪并写入 `_probe_result_cache`。

    输入：
    1. std_hosts / ex_hosts: 配置侧规范化地址。
    2. probe_timeout: 单地址探测超时（秒）。
    输出：
    1. 写入后的 standard/extended 列表字典。
    边界条件：
    1. 某池配置为空则跳过探测，缓存中不写该 key。
    """
    global _last_tcp_probe_hosts_fingerprint

    std_trimmed: List[Tuple[str, int]] = []
    ex_trimmed: List[Tuple[str, int]] = []
    if std_hosts:
        std_trimmed = _tcp_probe_and_trim_available_hosts(
            std_hosts, probe_timeout, std_hosts, "standard"
        )
    if ex_hosts:
        ex_trimmed = _tcp_probe_and_trim_available_hosts(
            ex_hosts, probe_timeout, ex_hosts, "extended"
        )
    fp_new = compute_hosts_fingerprint(std_hosts, ex_hosts)
    with _probe_result_cache_lock:
        if std_trimmed:
            _probe_result_cache["standard"] = list(std_trimmed)
        elif not std_hosts:
            _probe_result_cache.pop("standard", None)
        if ex_trimmed:
            _probe_result_cache["extended"] = list(ex_trimmed)
        elif not ex_hosts:
            _probe_result_cache.pop("extended", None)
        _last_tcp_probe_hosts_fingerprint = fp_new
    return {
        "standard": list(std_trimmed),
        "extended": list(ex_trimmed),
    }


def _load_cfg_for_ensure(
    config_path: Optional[str] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Path, Dict[str, Any]]:
    """
    为 ensure 解析配置路径并加载 cfg。

    输入：
    1. config_path: YAML 路径；cfg 非空时可仅用于解析路径。
    2. cfg: 已加载配置；非空时不再读盘。
    输出：
    1. (resolved_path, cfg_dict)。
    边界条件：
    1. cfg 与 config_path 均为空时使用包内默认 config.yaml。
    """
    if cfg is not None:
        if config_path is not None and str(config_path).strip():
            resolved = _resolve_zsdtdx_config_path(config_path)
        else:
            resolved = _DEFAULT_CONFIG_PATH.resolve()
        return resolved, cfg
    resolved = _resolve_zsdtdx_config_path(config_path)
    with open(resolved, "r", encoding="utf-8") as fp:
        loaded = yaml.safe_load(fp) or {}
    return resolved, loaded


def _ensure_availability_hosts_cache(
    config_path: Optional[str] = None,
    *,
    cfg: Optional[Dict[str, Any]] = None,
    force: bool = False,
    wait_timeout: float = 120.0,
) -> Dict[str, Any]:
    """
    检查可用地址缓存；不可用时从配置读取 hosts 并探测写入缓存。

    输入：
    1. config_path: 配置文件路径。
    2. cfg: 已加载 YAML；优先使用以避免重复读盘。
    3. force: True 时强制重新探测。
    4. wait_timeout: single-flight 等待进行中的探测完成的超时（秒）。
    输出：
    1. 摘要字典（skipped、config_path、各级数量等）。
    用途：
    1. 进程内唯一触发 TCP 探测并写入 `_probe_result_cache` 的入口。
    边界条件：
    1. 并发调用时仅一次真实探测，其余等待完成后返回 skipped/结果。
    """
    resolved, loaded_cfg = _load_cfg_for_ensure(config_path=config_path, cfg=cfg)

    if not force and _cache_usable_for_cfg(loaded_cfg):
        return {
            "skipped": True,
            "config_path": str(resolved),
            "reason": "cache_usable",
        }

    if not _ensure_inflight_event.is_set():
        if not _ensure_inflight_event.wait(timeout=max(1.0, float(wait_timeout))):
            raise TimeoutError("等待可用地址缓存探测完成超时")
        if not force and _cache_usable_for_cfg(loaded_cfg):
            return {
                "skipped": True,
                "config_path": str(resolved),
                "reason": "cache_usable_after_wait",
            }

    with _ensure_availability_lock:
        if not force and _cache_usable_for_cfg(loaded_cfg):
            return {
                "skipped": True,
                "config_path": str(resolved),
                "reason": "cache_usable",
            }
        if not _ensure_inflight_event.is_set():
            _ensure_inflight_event.wait(timeout=max(1.0, float(wait_timeout)))
            if not force and _cache_usable_for_cfg(loaded_cfg):
                return {
                    "skipped": True,
                    "config_path": str(resolved),
                    "reason": "cache_usable_after_wait",
                }
        _ensure_inflight_event.clear()

    try:
        pool_cfg = loaded_cfg.get("pool", {}) or {}
        probe_timeout = float(pool_cfg.get("probe_timeout", 0.8))
        std_hosts, ex_hosts = _normalize_hosts_from_cfg(loaded_cfg)
        trimmed = _probe_trim_and_write_availability_cache(
            std_hosts, ex_hosts, probe_timeout
        )
        return {
            "skipped": False,
            "config_path": str(resolved),
            "standard_count": len(trimmed.get("standard") or []),
            "extended_count": len(trimmed.get("extended") or []),
        }
    finally:
        _ensure_inflight_event.set()


def _presorted_hosts_valid(presorted: Optional[Dict[str, Any]]) -> bool:
    """
    判断 presorted_hosts 是否可作为建连用的有效快照。

    输入：
    1. presorted: 含 standard/extended 的字典。
    输出：
    1. True 表示至少一侧有非空列表。
    边界条件：
    1. 空列表或缺 key 视为无效。
    """
    if not isinstance(presorted, dict) or not presorted:
        return False
    std_list = presorted.get("standard")
    ex_list = presorted.get("extended")
    if isinstance(std_list, list) and len(std_list) > 0:
        return True
    if isinstance(ex_list, list) and len(ex_list) > 0:
        return True
    return False


def resolve_presorted_hosts_for_connection(
    config_path: Optional[str] = None,
    *,
    cfg: Optional[Dict[str, Any]] = None,
    presorted_hosts: Optional[Dict[str, List[Tuple[str, int]]]] = None,
    sync_if_missing: bool = True,
) -> Dict[str, List[Tuple[str, int]]]:
    """
    解析建连用的 standard/extended 地址列表。

    输入：
    1. config_path / cfg: 配置来源。
    2. presorted_hosts: worker 槽位列表等调用方已有快照。
    3. sync_if_missing: True 时缓存不可用则调用 ensure；False 时不可用则抛错。
    输出：
    1. 含 standard/extended 列表的字典（副本）。
    用途：
    1. UnifiedTdxClient 与 worker 建连前统一取址。
    边界条件：
    1. worker 应传入有效 presorted_hosts 且 sync_if_missing=False，避免子进程探测。
    """
    if _presorted_hosts_valid(presorted_hosts):
        presorted_map = dict(presorted_hosts or {})
        return {
            "standard": list(presorted_map.get("standard") or []),
            "extended": list(presorted_map.get("extended") or []),
        }

    if cfg is None and config_path is not None:
        _, cfg = _load_cfg_for_ensure(config_path=config_path)
    elif cfg is None:
        _, cfg = _load_cfg_for_ensure(config_path=None)

    if _cache_usable_for_cfg(cfg):
        cache = get_probe_result_cache()
        return {
            "standard": list(cache.get("standard") or []),
            "extended": list(cache.get("extended") or []),
        }

    if not sync_if_missing:
        raise RuntimeError(
            "可用地址缓存不可用且 sync_if_missing=False，"
            "请先在主进程调用 set_config_path 或 _ensure_availability_hosts_cache"
        )

    _ensure_availability_hosts_cache(config_path=config_path, cfg=cfg)
    cache = get_probe_result_cache()
    return {
        "standard": list(cache.get("standard") or []),
        "extended": list(cache.get("extended") or []),
    }


def _seed_probe_result_cache_from_snapshot(
    snapshot: Dict[str, List[Tuple[str, int]]],
    fingerprint: Tuple[Tuple[Tuple[str, int], ...], Tuple[Tuple[str, int], ...]],
) -> None:
    """
    将快照写入当前进程 `_probe_result_cache`（供 worker 子进程使用）。

    输入：
    1. snapshot: 槽位旋转后的 standard/extended 列表。
    2. fingerprint: 与主进程一致的 hosts 指纹。
    输出：无。
    边界条件：
    1. 不触发网络探测；仅内存写入。
    """
    global _last_tcp_probe_hosts_fingerprint

    with _probe_result_cache_lock:
        _probe_result_cache.clear()
        std_list = snapshot.get("standard")
        ex_list = snapshot.get("extended")
        if isinstance(std_list, list) and std_list:
            _probe_result_cache["standard"] = list(std_list)
        if isinstance(ex_list, list) and ex_list:
            _probe_result_cache["extended"] = list(ex_list)
        _last_tcp_probe_hosts_fingerprint = fingerprint


def rotate_hosts_list(
    hosts: List[Tuple[str, int]],
    start_index: int,
) -> List[Tuple[str, int]]:
    """
    对地址列表做循环旋转，用于 worker 槽位起始下标分配。

    输入：
    1. hosts: 全量探测排序列表。
    2. start_index: 起始下标（可大于 len-1，自动取模）。
    输出：
    1. 旋转后的新列表。
    边界条件：
    1. hosts 为空时返回 []。
    """
    if not hosts:
        return []
    k = int(start_index) % len(hosts)
    return list(hosts[k:]) + list(hosts[:k])


def _tcp_probe_one(
    host: str, port: int, timeout: float
) -> Tuple[Tuple[str, int], Optional[float]]:
    """对单个 host 做 TCP Connect 探测，返回 (host_tuple, latency_ms) 或 (host_tuple, None)。"""
    sock = _socket_mod.socket(_socket_mod.AF_INET, _socket_mod.SOCK_STREAM)
    sock.settimeout(timeout)
    t0 = time.monotonic()
    try:
        sock.connect((host, port))
        latency = (time.monotonic() - t0) * 1000.0
        return ((host, port), latency)
    except Exception:
        return ((host, port), None)
    finally:
        try:
            sock.close()
        except Exception:
            pass


class PersistentFailoverPool:
    """连接池：P0改造，线程本地连接，消除锁竞争。"""

    def __init__(
        self,
        name: str,
        api_cls,
        hosts: List[Tuple[str, int]],
        connect_timeout: float,
        api_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        输入连接参数，输出连接池实例。

        输入：
        1. hosts: 建连用的 (host, port) 列表（须来自缓存或槽位分配，已排序）。
        输出：
        1. PersistentFailoverPool 实例。
        边界条件：
        1. hosts 为空时抛出 ValueError。
        2. 单次 call 单 host 最多 3 次、多 host 最多 6 次请求（见 call 方法注释），无 YAML 重试参数。
        """
        if not hosts:
            raise ValueError(f"{name} host 列表为空")
        self.name = name
        self.api_cls = api_cls
        self.hosts = list(hosts)
        self.connect_timeout = float(connect_timeout)
        self.api_kwargs = api_kwargs or {}

        # P0改造：线程本地存储
        self._local = threading.local()
        self._thread_registry_lock = threading.Lock()
        self._thread_registry: Dict[int, Dict[str, Any]] = {}

        # 全局统计和hosts需要锁保护
        self._stats_lock = threading.Lock()
        self._global_stats = {
            "total_calls": 0,
            "retries": 0,
            "rotations": 0,
            "connect_failures": 0,
            "none_as_end": 0,
            "same_conn_retries": 0,
            "same_host_reconnects": 0,
        }
        self._used_hosts_lock = threading.Lock()
        self._global_used_hosts = set()

    def _prune_dead_thread_data(self):
        """
        清理已退出线程的连接数据并关闭残留连接。

        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 避免 thread-local 所在线程结束后遗留未关闭 socket/heartbeat。
        边界条件：
        1. 清理过程中的断开异常会被忽略，确保调用链稳定。
        """
        stale_data_list: List[Dict[str, Any]] = []
        with self._thread_registry_lock:
            stale_idents: List[int] = []
            for ident, entry in list(self._thread_registry.items()):
                thread_ref = entry.get("thread_ref")
                thread_obj = thread_ref() if callable(thread_ref) else None
                if thread_obj is not None and bool(thread_obj.is_alive()):
                    continue
                stale_idents.append(int(ident))

            for ident in stale_idents:
                entry = self._thread_registry.pop(int(ident), None)
                if not isinstance(entry, dict):
                    continue
                data = entry.get("data")
                if isinstance(data, dict):
                    stale_data_list.append(data)

        for data in stale_data_list:
            self._disconnect_api(data.get("api"))
            data["api"] = None
            data["active_index"] = -1

    def cleanup_dead_thread_connections(self):
        """
        对外暴露的死线程连接清理入口。

        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 允许上层在批次边界主动回收已退出线程遗留的连接。
        边界条件：
        1. 重复调用安全；无死线程时无副作用。
        """
        self._prune_dead_thread_data()

    def _get_thread_data(self):
        """获取当前线程的连接数据。"""
        self._prune_dead_thread_data()
        data = getattr(self._local, "data", None)
        if not isinstance(data, dict):
            data = {
                "api": None,
                "active_index": -1,
                "socket_read_deadline": None,
                "socket_read_timeout_override": None,
            }
            self._local.data = data
        else:
            data.setdefault("socket_read_deadline", None)
            data.setdefault("socket_read_timeout_override", None)

        current_thread = threading.current_thread()
        thread_ident = int(current_thread.ident or 0)
        if thread_ident <= 0:
            thread_ident = int(id(current_thread))
        with self._thread_registry_lock:
            entry = self._thread_registry.get(thread_ident)
            if not isinstance(entry, dict) or entry.get("data") is not data:
                self._thread_registry[thread_ident] = {
                    "data": data,
                    "thread_ref": weakref.ref(current_thread),
                }
        return data

    def _disconnect_api(self, api_obj):
        """输入 API 对象，输出无；用于安全断开；断开异常会忽略。"""
        if api_obj is None:
            return
        try:
            api_obj.disconnect()
        except Exception:
            pass

    def _apply_thread_socket_read_timeout(
        self, data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        将当前线程保存的 socket 读超时预算应用到活跃连接。

        输入：
        1. data: 可选线程本地连接状态；为空时读取当前线程状态。
        输出：无。
        用途：
        1. 让 chunk deadline 覆盖首次建连、重连、rotate 后的新 socket 以及每次底层请求。
        边界条件：
        1. deadline 已过期时抛 TimeoutError；无活跃 socket 时仅保留线程状态。
        """
        thread_data = data if isinstance(data, dict) else self._get_thread_data()
        deadline = thread_data.get("socket_read_deadline")
        override = thread_data.get("socket_read_timeout_override")
        if deadline is None and override is None:
            return

        if deadline is not None:
            remaining = float(deadline) - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("chunk attempt timeout")
            timeout_seconds = max(0.1, remaining)
        else:
            timeout_seconds = max(0.1, float(override))

        api_obj = thread_data.get("api")
        if api_obj is None:
            return
        sock_client = getattr(api_obj, "client", None)
        if sock_client is None:
            return
        try:
            sock_client.settimeout(timeout_seconds)
        except Exception:
            pass

    def _connect_to_index(self, index: int) -> bool:
        """输入 host 索引，输出连接结果；用于切换连接；连接失败返回 False。"""
        host, port = self.hosts[index]
        api = self.api_cls(**self.api_kwargs)
        try:
            ok = api.connect(host, port, time_out=self.connect_timeout)
            if not ok:
                with self._stats_lock:
                    self._global_stats["connect_failures"] += 1
                self._disconnect_api(api)
                return False
        except Exception:
            with self._stats_lock:
                self._global_stats["connect_failures"] += 1
            self._disconnect_api(api)
            return False

        thread_data = self._get_thread_data()
        old_api = thread_data["api"]
        old_active_index = thread_data["active_index"]
        thread_data["api"] = api
        thread_data["active_index"] = index
        try:
            self._apply_thread_socket_read_timeout(thread_data)
        except TimeoutError:
            thread_data["api"] = old_api
            thread_data["active_index"] = old_active_index
            self._disconnect_api(api)
            raise
        with self._used_hosts_lock:
            self._global_used_hosts.add(f"{host}:{port}")
        self._disconnect_api(old_api)
        return True

    def _ensure_connected(self) -> bool:
        """
        输入无，输出连接是否可用；按 hosts 下标 0→n 顺序尝试建连。

        输入：无。
        输出：
        1. True 表示至少一个 host 连接成功。
        边界条件：
        1. 全部失败返回 False；不在池内做 TCP 探测。
        """
        thread_data = self._get_thread_data()
        if thread_data["api"] is not None and thread_data["active_index"] >= 0:
            return True
        for index in range(len(self.hosts)):
            if self._connect_to_index(index):
                return True
        return False

    def ensure_connected(self) -> bool:
        """输入无，输出连接是否可用；用于外部预连接；失败返回 False。"""
        return self._ensure_connected()

    def set_thread_socket_read_timeout(self, timeout_seconds: float) -> None:
        """
        设置当前线程 socket 读超时预算。

        输入：
        1. timeout_seconds: 读超时秒数。
        输出：
        1. 无返回值。
        用途：
        1. chunk 单次抓取尝试超时时，使当前与后续新建 socket 都在该次剩余预算内结束。
        边界条件：
        1. 无活跃连接时记录线程级预算，后续首次建连会继承该预算。
        """
        data = self._get_thread_data()
        timeout_value = max(0.1, float(timeout_seconds))
        data["socket_read_timeout_override"] = timeout_value
        data["socket_read_deadline"] = time.monotonic() + timeout_value
        self._apply_thread_socket_read_timeout(data)

    def restore_thread_socket_read_timeout(self) -> None:
        """
        D3 优化：将当前线程活跃连接的 socket 读超时还原为 pool 默认。

        输入：1. 无显式输入参数。
        输出：1. 无返回值。
        用途：
        1. chunk 结束 finally 分支调用，避免短超时残留影响心跳/后续 chunk 读路径。
        边界条件：
        1. 无活跃连接时静默跳过；pool 没有显式 default 时使用 BaseSocketClient.CONNECT_TIMEOUT(5)。
        """
        data = self._get_thread_data()
        data["socket_read_deadline"] = None
        data["socket_read_timeout_override"] = None
        api_obj = data.get("api")
        if api_obj is None:
            return
        sock_client = getattr(api_obj, "client", None)
        if sock_client is None:
            return
        # 还原默认值：从 pool 实例属性读取（如有），否则使用 socket 模块默认 None（阻塞）。
        default_timeout = getattr(self, "_default_socket_read_timeout", None)
        try:
            sock_client.settimeout(default_timeout)
        except Exception:
            pass

    def reset_thread_connection(self, reconnect: bool = True) -> bool:
        """
        重置当前线程连接并按需重连。

        输入：
        1. reconnect: 是否在重置后立即尝试重连。
        输出：
        1. bool，True 表示重置（及可选重连）成功。
        用途：
        1. 在“当前线程连接已失效”场景下，仅修复当前线程连接，避免影响其他线程。
        边界条件：
        1. 仅作用于线程本地连接；不会关闭其他线程中的活跃连接。
        """
        thread_data = self._get_thread_data()
        old_api = thread_data["api"]
        thread_data["api"] = None
        thread_data["active_index"] = -1
        self._disconnect_api(old_api)
        if not bool(reconnect):
            return True
        try:
            return bool(self._ensure_connected())
        except Exception:
            return False

    def _rotate(self) -> bool:
        """输入无，输出轮换结果；用于失败切换；无可用 host 返回 False。"""
        with self._stats_lock:
            self._global_stats["rotations"] += 1
        if not self.hosts:
            return False
        thread_data = self._get_thread_data()
        start = (
            0
            if thread_data["active_index"] < 0
            else (thread_data["active_index"] + 1) % len(self.hosts)
        )
        for offset in range(len(self.hosts)):
            idx = (start + offset) % len(self.hosts)
            if self._connect_to_index(idx):
                return True
        return False

    def _reconnect_active_host(self) -> bool:
        """输入无，输出重连结果；用于同 host 重连；未激活时返回 False。"""
        thread_data = self._get_thread_data()
        if thread_data["active_index"] < 0:
            return False
        with self._stats_lock:
            self._global_stats["same_host_reconnects"] += 1
        return self._connect_to_index(thread_data["active_index"])

    def _attempt_call_step(
        self,
        method_name: str,
        allow_none: bool,
        *args: Any,
        **kwargs: Any,
    ) -> Tuple[Any, Optional[Exception], bool]:
        """
        执行一次底层 API 请求。

        输入：
        1. method_name: API 方法名。
        2. allow_none: 是否允许 None 作为合法返回值。
        输出：
        1. (result, exception, needs_retry) 三元组。
        用途：
        1. 供 call 单 host 三步流水线逐步调用。
        边界条件：
        1. 抛错或（allow_none 且返回 None）视为 needs_retry=True。
        2. 空列表等非 None 结果视为成功。
        """
        thread_data = self._get_thread_data()
        api = thread_data.get("api")
        if api is None:
            return None, RuntimeError(f"{self.name} 无可用连接"), True
        try:
            self._apply_thread_socket_read_timeout(thread_data)
            result = getattr(api, method_name)(*args, **kwargs)
        except Exception as exc:
            return None, exc, True
        if result is None:
            if allow_none:
                return None, None, True
            return None, RuntimeError(f"{self.name}.{method_name} 返回 None"), True
        return result, None, False

    def _run_host_call_ladder(
        self,
        method_name: str,
        allow_none: bool,
        *args: Any,
        **kwargs: Any,
    ) -> Tuple[Any, Optional[Exception], bool]:
        """
        在当前 host 上执行固定三步：请求 → 同连接再请求 → 同 host 重连后再请求。

        输入：
        1. method_name: API 方法名。
        2. allow_none: 是否允许 None 作为合法返回值。
        输出：
        1. (result, last_exception, succeeded) 三元组。
        用途：
        1. 供 call 在当前 host 与 rotate 后的 host 各执行一轮。
        边界条件：
        1. 单轮最多 3 次底层请求；成功时 succeeded=True 且 result 有效。
        """
        last_exception: Optional[Exception] = None

        def _remember_failure(exc: Optional[Exception]) -> None:
            nonlocal last_exception
            if exc is not None:
                last_exception = exc

        result, exc, needs_retry = self._attempt_call_step(
            method_name, allow_none, *args, **kwargs
        )
        if not needs_retry:
            return result, None, True
        _remember_failure(exc)

        with self._stats_lock:
            self._global_stats["same_conn_retries"] += 1
            self._global_stats["retries"] += 1
        result, exc, needs_retry = self._attempt_call_step(
            method_name, allow_none, *args, **kwargs
        )
        if not needs_retry:
            return result, None, True
        _remember_failure(exc)

        if self._reconnect_active_host():
            with self._stats_lock:
                self._global_stats["retries"] += 1
            result, exc, needs_retry = self._attempt_call_step(
                method_name, allow_none, *args, **kwargs
            )
            if not needs_retry:
                return result, None, True
            _remember_failure(exc)
        else:
            with self._stats_lock:
                self._global_stats["retries"] += 1

        return None, last_exception, False

    def call(
        self,
        method_name: str,
        *args,
        allow_none: bool = False,
        **kwargs,
    ):
        """
        输入方法与参数，输出调用结果；固定 host 内三步 + 可选换 host 再走三步。

        单 host 流水线（无 sleep，最多 3 次请求）：
        1. 当前连接请求 1 次；
        2. 失败则同连接再请求 1 次；
        3. 仍失败则同 host 重连后再请求 1 次。

        多 host：上述三步失败后 rotate 一次，在新 host 上重复同样三步（最多再 3 次）。

        失败定义：抛错，或 allow_none=True 时返回 None。
        最坏情况：单 host 3 次请求；多 host 6 次请求后返回失败。
        """

        with self._stats_lock:
            self._global_stats["total_calls"] += 1
        if not self._ensure_connected():
            raise RuntimeError(f"{self.name} 无可用连接")

        result, last_exception, succeeded = self._run_host_call_ladder(
            method_name, allow_none, *args, **kwargs
        )
        if succeeded:
            return result

        if len(self.hosts) > 1 and self._rotate():
            result, rotate_exc, succeeded = self._run_host_call_ladder(
                method_name, allow_none, *args, **kwargs
            )
            if succeeded:
                return result
            if rotate_exc is not None:
                last_exception = rotate_exc

        if allow_none and last_exception is None:
            with self._stats_lock:
                self._global_stats["none_as_end"] += 1
            return None
        raise RuntimeError(f"{self.name}.{method_name} 调用失败: {last_exception}")

    def get_active_host(self) -> str:
        """输入无，输出活跃 host；用于报告；未连接返回空字符串。"""
        thread_data = self._get_thread_data()
        if thread_data["active_index"] < 0:
            return ""
        host, port = self.hosts[thread_data["active_index"]]
        return f"{host}:{port}"

    def get_used_hosts(self) -> List[str]:
        """输入无，输出已用 host 列表；用于追踪；未使用时返回空列表。"""
        with self._used_hosts_lock:
            return sorted(self._global_used_hosts)

    @property
    def stats(self):
        """输入无，输出统计信息；用于报告；返回副本。"""
        with self._stats_lock:
            return self._global_stats.copy()

    def close(self):
        """
        输入无，输出无；用于释放连接；重复调用安全。

        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 关闭该池在所有线程中建立过的连接，防止跨线程残留 socket。
        边界条件：
        1. 连接关闭异常会被忽略；关闭后线程再次调用会自动重建连接状态。
        """
        self._prune_dead_thread_data()
        tracked_data: List[Dict[str, Any]] = []
        with self._thread_registry_lock:
            for entry in self._thread_registry.values():
                if not isinstance(entry, dict):
                    continue
                data = entry.get("data")
                if isinstance(data, dict):
                    tracked_data.append(data)
            self._thread_registry.clear()

        current_data = getattr(self._local, "data", None)
        if isinstance(current_data, dict):
            tracked_data.append(current_data)

        seen: set[int] = set()
        for data in tracked_data:
            token = int(id(data))
            if token in seen:
                continue
            seen.add(token)
            self._disconnect_api(data.get("api"))
            data["api"] = None
            data["active_index"] = -1


class ChunkLocalRowCache:
    """单进程内的 chunk 级行缓存：每次 chunk 入口新建分区，chunk 出口释放。

    设计要点：
    1. **无锁**：移除原 `_pool_lock` 与 `threading.local` 租约；同进程同 chunk 内的访问天然串行（async 路径
       下每个 chunk 在 `asyncio.to_thread` worker 内独立运行；sync 路径单线程顺序执行）。
    2. **无 free 池**：移除分区软复用，避免跨 chunk 复用 list 槽位的复杂性；分区由 Python GC 在 chunk 结束后回收。
    3. **API 兼容**：保留 `acquire_partition` / `soft_delete` / `get_partition` 调用契约，外部调用零改动。
    4. **命中率统计**：保留 partition 的 `_partition_key/_n/oldest_dt/newest_dt/page_start/fetched_pages`
       字段集与 SharedChunkCache 完全一致，外层调用方逻辑透明迁移。

    - cache_rows：有序 list[(dt_key, row)]，按时间升序追加；_n 为有效下标（chunk 内单调递增）。
    - acquire_partition：返回该 chunk 的私有分区；同一 chunk 内重复 acquire 等价（返回同一对象）。
    - chunk 内：首次分页填充为写分区（merge 增 _n）；后续 task 命中缓存为读分区（range(_n) 过滤）。
    - soft_delete：清除分区引用，让 GC 回收；不再有内存复用语义。
    """

    def __init__(self):
        # 当前 chunk 持有的分区（同进程内最多 1 个；async 路径每个协程独立 to_thread 不会重叠）。
        # 用 dict 表存 (partition_key -> partition) 以支持理论上的并发；实际只会有 1 个 entry。
        self._partitions: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _new_partition(partition_key: str) -> Dict[str, Any]:
        """构造一个空分区，字段与原 SharedChunkCache._new_partition 一致以保持上层兼容。"""
        return {
            "cache_rows": [],
            "_n": 0,
            "_partition_key": partition_key,
            "oldest_dt": None,
            "newest_dt": None,
            "page_start": 0,
            "fetched_pages": 0,
        }

    def acquire_partition(self, code: str, freq: str) -> Dict[str, Any]:
        """输入 code/freq，输出 chunk 私有分区。

        输入：
        1. code/freq: 分区 key 维度。
        输出：
        1. 全新或已有的分区 dict（_n 归零、oldest/newest/page_start/fetched_pages 归零）。
        用途：
        1. chunk 入口处调用一次；chunk 内只读/写该对象。
        边界条件：
        1. 同 chunk 内对同 (code,freq) 重复 acquire 返回同一分区（幂等）。
        2. 跨 chunk 之间不复用内存：上一 chunk 的 list 在 soft_delete 后被丢弃。
        """
        partition_key = f"{code}|{freq}"
        existing = self._partitions.get(partition_key)
        if existing is not None:
            # 同 chunk 内幂等：复用现有分区但归零游标，保持与 SharedChunkCache.acquire 相同语义。
            existing["_n"] = 0
            existing["oldest_dt"] = None
            existing["newest_dt"] = None
            existing["page_start"] = 0
            existing["fetched_pages"] = 0
            return existing
        partition = self._new_partition(partition_key)
        self._partitions[partition_key] = partition
        return partition

    def get_partition(self, code: str, freq: str) -> Dict[str, Any]:
        """兼容别名；等价于 acquire_partition。"""
        return self.acquire_partition(code, freq)

    def soft_delete(self, code: str, freq: str) -> None:
        """chunk 结束：从内部表中移除分区引用，等待 GC 回收。

        输入：code/freq；输出：无。
        边界：
        1. 重复调用安全（key 不存在时静默 no-op）。
        2. 与 SharedChunkCache.soft_delete 不同：不再"归还 free 池"。
        """
        partition_key = f"{code}|{freq}"
        self._partitions.pop(partition_key, None)


# 兼容别名：旧代码与外部测试可能仍引用 SharedChunkCache。
SharedChunkCache = ChunkLocalRowCache


class UnifiedTdxClient:
    """统一行情高层客户端，封装市场路由、分页和连接池。"""

    PERIOD_MAP = {
        "5min": 0,
        "5": 0,
        "15min": 1,
        "15": 1,
        "30min": 2,
        "30": 2,
        "60min": 3,
        "60": 3,
        "d": 4,
        "w": 5,
        "m": 6,
    }
    FREQ_MAP = {
        "d": "d",
        "w": "w",
        "60min": "60",
        "60": "60",
        "30min": "30",
        "30": "30",
        "15min": "15",
        "15": "15",
        "5min": "5",
        "5": "5",
        "m": "m",
    }
    STOCK_SCOPE_TOKENS = {"szsh", "bj", "hk"}
    _CONTEXT_STACK: List["UnifiedTdxClient"] = []

    def __init__(
        self,
        config_path: Optional[str] = None,
        presorted_hosts: Optional[Dict[str, List[Tuple[str, int]]]] = None,
        worker_client: bool = False,
    ):
        """输入配置路径，输出客户端实例；用于统一初始化；路径为空时默认包内配置。"""
        resolved_config_path = self._resolve_config_path(config_path)
        self.config_path = str(resolved_config_path)
        self.config = self._load_config(resolved_config_path)

        self.pagination = self.config.get("pagination", {})
        self.output_cfg = self.config.get("output", {})
        self.market_rules = self.config.get("market_rules", {})
        self.stock_scope_cfg = self.config.get("stock_scope", {}) or {}
        self.stock_scope_defaults = (
            self.stock_scope_cfg.get("defaults_when_codes_none", {}) or {}
        )
        self.index_kline_cfg = self.config.get("index_kline", {}) or {}
        self.index_kline_aliases_cfg = self.index_kline_cfg.get("aliases", {}) or {}
        self.index_kline_lookup_cfg = self.index_kline_cfg.get("lookup", {}) or {}
        self._index_kline_fingerprint = fingerprint_index_kline_config(
            self.index_kline_cfg
        )
        # 动态发现的指数目录快照（全量候选列表），可 refresh 重建。
        self._index_catalog_records: List[Dict[str, Any]] = []
        self._index_name_route_map: Dict[str, Dict[str, Any]] = {}
        self.index_kline_route_cache_cfg = (
            self.index_kline_cfg.get("route_cache", {}) or {}
        )
        self._index_route_cache_enabled = bool(
            self.index_kline_route_cache_cfg.get("enabled", True)
        )
        self._index_route_cache_refresh_granularity = (
            str(self.index_kline_route_cache_cfg.get("refresh_granularity", "day"))
            .strip()
            .lower()
            or "day"
        )
        self._index_route_cache_date = dt.datetime.now().date().isoformat()
        self._index_route_cache_path: Optional[Path] = None
        if (
            self._index_route_cache_enabled
            and self._index_route_cache_refresh_granularity == "day"
        ):
            self._index_route_cache_path = self._resolve_index_route_cache_path()
            if self._index_route_cache_path is None:
                self._index_route_cache_enabled = False
        self.client_cfg = self.config.get("client", {})
        self.preconnect_on_enter = bool(
            self.client_cfg.get("preconnect_on_enter", True)
        )

        pool_cfg = self.config.get("pool", {})
        connect_timeout = float(pool_cfg.get("connect_timeout", 1.5))
        api_kwargs = {
            "multithread": True,
            "heartbeat": bool(pool_cfg.get("heartbeat", True)),
            "raise_exception": False,
        }

        resolved_hosts = resolve_presorted_hosts_for_connection(
            config_path=str(resolved_config_path),
            cfg=self.config,
            presorted_hosts=presorted_hosts,
            sync_if_missing=not bool(worker_client),
        )
        std_list = list(resolved_hosts.get("standard") or [])
        ex_list = list(resolved_hosts.get("extended") or [])
        if not std_list:
            std_list = self._normalize_hosts(
                self.config.get("hosts", {}).get("standard", [])
            )
        if not ex_list:
            ex_raw = self.config.get("hosts", {}).get("extended", []) or []
            if ex_raw:
                ex_list = self._normalize_hosts(ex_raw)

        self.std_pool = PersistentFailoverPool(
            "standard",
            TdxHq_API,
            std_list,
            connect_timeout,
            api_kwargs=api_kwargs,
        )
        self.ex_pool = PersistentFailoverPool(
            "extended",
            TdxExHq_API,
            ex_list if ex_list else std_list[:1],
            connect_timeout,
            api_kwargs=api_kwargs,
        )

        self._markets_df = None
        self._ex_market_name_map = {}
        self._stock_df = None
        self._future_df = None
        self._stock_route = {}
        self._future_route = {}
        self._instrument_cache = None
        self._runtime_failures: List[Dict[str, Any]] = []
        self._entered_client = None
        self._worker_client_flag = bool(worker_client)
        self._presorted_hosts_snapshot: Dict[str, List[Tuple[str, int]]] = {
            "standard": list(self.std_pool.hosts),
            "extended": list(self.ex_pool.hosts),
        }
        # chunk 级 2D 分区缓存（(code|index_name, freq) → 分区）；股票/指数共用
        self._shared_chunk_cache = SharedChunkCache()

    def _resolve_config_path(self, config_path: Optional[str]) -> Path:
        """输入配置路径，输出可读取绝对路径；支持相对/绝对路径；不存在时抛错。"""
        return _resolve_zsdtdx_config_path(config_path)

    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        """输入配置路径，输出配置字典；用于参数集中管理；文件缺失会抛错。"""
        with open(config_path, "r", encoding="utf-8") as fp:
            return yaml.safe_load(fp) or {}

    def _normalize_hosts(self, hosts: List[Any]) -> List[Tuple[str, int]]:
        """输入 host 配置，输出合法 host 列表；用于剔除脏地址；全无效会抛错。"""
        return normalize_hosts_entries(hosts)

    def _default_return_df(self, return_df: Optional[bool]) -> bool:
        """输入返回开关，输出是否 DataFrame；用于统一默认值；None 时走配置。"""
        if return_df is None:
            return bool(self.output_cfg.get("return_df_default", True))
        return bool(return_df)

    def _freq_to_category(self, freq: str) -> int:
        """输入周期字符串，输出 category；用于隐藏数字常量；非法周期抛错。"""
        p = str(freq).strip().lower()
        if p not in self.PERIOD_MAP:
            raise ValueError(f"不支持的频率: {freq}")
        return self.PERIOD_MAP[p]

    def _pool_call_allow_none(
        self,
        pool: PersistentFailoverPool,
        method_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        K 线/最新价专用 pool 调用：allow_none=True，语义与 pool.call 相同。

        输入：
        1. pool: 标准或扩展连接池。
        2. method_name: API 方法名。
        3. *args/**kwargs: 透传参数。
        输出：
        1. API 返回值；重试耗尽后仍为 None 则返回 None。
        用途：
        1. 集中 K 线/报价等 allow_none 调用点，避免漏写 allow_none=True。
        边界条件：
        1. 固定 allow_none=True；重试逻辑仅在 pool.call 内实现一次。
        """
        return pool.call(method_name, *args, allow_none=True, **kwargs)

    def _to_datetime(self, value: Any) -> pd.Timestamp:
        """输入时间值，输出 Timestamp；用于统一过滤；非法时间抛错。"""
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            raise ValueError(f"非法时间: {value}")
        return ts

    def _is_date_only_input(self, value: Any) -> bool:
        """判断输入是否为“仅日期”语义（不含时分秒）；datetime 一律返回 False。"""
        if isinstance(value, dt.datetime):
            return False
        if isinstance(value, dt.date):
            return True
        raw = str(value or "").strip()
        if raw == "":
            return False
        return re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", raw) is not None

    def _normalize_future_time_window(
        self, start_time: Any, end_time: Any
    ) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """标准化期货时间窗口；日期入参默认映射为 start=09:00:00、end=15:00:00。"""
        start_ts = self._to_datetime(start_time)
        end_ts = self._to_datetime(end_time)

        if self._is_date_only_input(start_time):
            start_ts = start_ts.replace(hour=9, minute=0, second=0, microsecond=0)
        if self._is_date_only_input(end_time):
            end_ts = end_ts.replace(hour=15, minute=0, second=0, microsecond=0)
        return start_ts, end_ts

    def _to_datetime_no_df(self, value: Any) -> dt.datetime:
        """输入时间值，输出 datetime；用于无 DataFrame 链路；非法时间抛错。"""

        def _strip_tz(parsed: dt.datetime) -> dt.datetime:
            """将时区时间统一转为 UTC-naive，避免 aware/naive 比较异常。"""
            if parsed.tzinfo is None:
                return parsed
            return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)

        if isinstance(value, dt.datetime):
            return _strip_tz(value)
        if isinstance(value, dt.date):
            return dt.datetime.combine(value, dt.time.min)

        raw = str(value or "").strip()
        if raw == "":
            raise ValueError(f"非法时间: {value}")

        normalized = raw.replace("Z", "+00:00")
        try:
            return _strip_tz(dt.datetime.fromisoformat(normalized))
        except Exception:
            pass

        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ]
        for fmt in formats:
            try:
                return _strip_tz(dt.datetime.strptime(raw, fmt))
            except Exception:
                continue
        raise ValueError(f"非法时间: {value}")

    def _safe_float_no_df(self, value: Any) -> Optional[float]:
        """输入任意值，输出浮点数或 None；用于无 DataFrame 数值清洗；非法值返回 None。"""
        try:
            parsed = float(value)
        except Exception:
            return None
        if math.isnan(parsed):
            return None
        return parsed

    def _normalize_task_dict_no_df(self, task: Dict[str, Any]) -> Dict[str, str]:
        """输入任务字典，输出标准化任务；用于无 DataFrame 链路入口校验；缺字段抛错。"""
        if not isinstance(task, dict):
            raise ValueError("task 必须是 dict")
        code = str(task.get("code", "")).strip()
        freq = str(task.get("freq", "")).strip().lower()
        start_time = str(task.get("start_time", "")).strip()
        end_time = str(task.get("end_time", "")).strip()
        if code == "":
            raise ValueError("task.code 不能为空")
        if freq == "":
            raise ValueError("task.freq 不能为空")
        if start_time == "":
            raise ValueError("task.start_time 不能为空")
        if end_time == "":
            raise ValueError("task.end_time 不能为空")
        self._freq_to_category(freq)
        return {
            "code": code,
            "freq": self._normalize_freq(freq),
            "start_time": start_time,
            "end_time": end_time,
        }

    def _normalize_index_task_payload_no_df(
        self, task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """输入指数任务字典，输出标准化任务；用于无 DataFrame 指数 chunk 校验；缺字段或时间非法抛错。"""
        if not isinstance(task, dict):
            raise ValueError("task 必须是 dict")
        index_name = str(task.get("index_name", "")).strip()
        freq = str(task.get("freq", "")).strip().lower()
        start_time = str(task.get("start_time", "")).strip()
        end_time = str(task.get("end_time", "")).strip()
        if index_name == "":
            raise ValueError("task.index_name 不能为空")
        if freq == "":
            raise ValueError("task.freq 不能为空")
        if start_time == "":
            raise ValueError("task.start_time 不能为空")
        if end_time == "":
            raise ValueError("task.end_time 不能为空")

        self._freq_to_category(freq)
        start_dt = self._to_datetime_no_df(start_time)
        end_dt = self._to_datetime_no_df(end_time)
        if start_dt > end_dt:
            raise ValueError("start_time 不能晚于 end_time")

        normalized: Dict[str, Any] = {
            "index_name": index_name,
            "freq": self._normalize_freq(freq),
            "start_time": start_time,
            "end_time": end_time,
        }

        pre_route_source = str(task.get("_index_route_source", "")).strip().lower()
        pre_route_code = str(task.get("_index_route_code", "")).strip()
        pre_route_name = str(task.get("_index_route_name", "")).strip()
        try:
            pre_route_market = int(task.get("_index_route_market", -1))
        except Exception:
            pre_route_market = -1
        if (
            pre_route_source in {"std", "ex"}
            and pre_route_code != ""
            and pre_route_market >= 0
        ):
            normalized["_index_route_source"] = pre_route_source
            normalized["_index_route_code"] = pre_route_code
            normalized["_index_route_market"] = pre_route_market
            if pre_route_name != "":
                normalized["_index_route_name"] = pre_route_name

        return normalized

    def _fetch_index_rows_for_task_with_route_no_df(
        self,
        normalized_task: Dict[str, Any],
        route: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """输入标准化任务和路由，输出指数K线；用于无 DataFrame 任务执行；同请求内路由发现最多 refresh 一次。"""
        index_name = str(normalized_task.get("index_name", "")).strip()
        freq = str(normalized_task.get("freq", "")).strip().lower()
        start_dt = self._to_datetime_no_df(normalized_task.get("start_time"))
        end_dt = self._to_datetime_no_df(normalized_task.get("end_time"))
        if start_dt > end_dt:
            raise ValueError("start_time 不能晚于 end_time")
        category = self._freq_to_category(freq)

        def _fetch_route_rows(
            current_route: Dict[str, Any],
        ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
            source = str(current_route.get("source", "std")).strip().lower()
            market = int(current_route.get("market", -1))
            code = str(current_route.get("code", "")).strip()
            if market < 0 or code == "":
                raise ValueError(f"指数路由配置非法: {current_route}")
            page_size = int(
                self.pagination.get(
                    "standard_kline_page_size"
                    if source == "std"
                    else "extended_kline_page_size",
                    800 if source == "std" else 700,
                )
            )

            def _fetch_index_page(start: int) -> Any:
                if source == "std":
                    return self._pool_call_allow_none(
                        self.std_pool,
                        "get_index_bars",
                        int(category),
                        int(market),
                        str(code),
                        int(start),
                        int(page_size),
                    )
                return self._pool_call_allow_none(
                    self.ex_pool,
                    "get_instrument_bars",
                    int(category),
                    int(market),
                    str(code),
                    int(start),
                    int(page_size),
                )

            rows = self._paginate_kline_pages(
                page_size=page_size,
                start_boundary=start_dt,
                boundary_mode="no_df",
                fetch_page=_fetch_index_page,
            )
            return {"source": source, "market": market, "code": code}, rows

        resolved_route = dict(route)
        route_refreshed = False
        try:
            effective_route, raw_rows = _fetch_route_rows(resolved_route)
        except Exception:
            if not route_refreshed:
                resolved_route = self.resolve_index_name(
                    index_name=index_name, refresh=True
                )
                route_refreshed = True
                effective_route, raw_rows = _fetch_route_rows(resolved_route)
            else:
                raise
        if not raw_rows and not route_refreshed:
            resolved_route = self.resolve_index_name(
                index_name=index_name, refresh=True
            )
            route_refreshed = True
            effective_route, raw_rows = _fetch_route_rows(resolved_route)

        return self._normalize_index_kline_rows(
            rows=raw_rows,
            index_name=str(resolved_route.get("name", index_name)),
            route_code=str(effective_route.get("code", "")),
            freq=freq,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    def _ensure_ex_market_name_map_no_df(self):
        """输入无，输出无；用于无 DataFrame 路径补充扩展市场映射；失败时保持现状。"""
        if self._ex_market_name_map:
            return
        try:
            rows = self.ex_pool.call("get_markets", allow_none=True)
        except Exception:
            return
        mapping: Dict[int, str] = {}
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            try:
                market = int(row.get("market", -1))
            except Exception:
                market = -1
            if market < 0:
                continue
            mapping[market] = str(row.get("name", "")).strip()
        if mapping:
            self._ex_market_name_map.update(mapping)

    def _get_ex_market_name_no_df(self, market: int) -> str:
        """输入扩展市场号，输出市场名；用于无 DataFrame 路径；未命中返回空串。"""
        self._ensure_ex_market_name_map_no_df()
        return self._ex_market_name_map.get(int(market), "")

    def _is_hk_market_ex_no_df(self, market: int) -> bool:
        """输入扩展市场号，输出是否港股市场；用于无 DataFrame 路径；未配置时按默认港股通。"""
        target_market_names = set(
            self.market_rules.get("include_hk_market_names", ["港股通"])
        )
        market_name = self._get_ex_market_name_no_df(int(market))
        return market_name in target_market_names

    def _is_hk_stock_ex_no_df(self, market: int, code: str) -> bool:
        """输入扩展市场与代码，输出是否港股；用于无 DataFrame 路由；固定按5位数字过滤。"""
        if not self._is_hk_market_ex_no_df(int(market)):
            return False
        raw_code = str(code).strip()
        if not raw_code.isdigit():
            return False
        return len(raw_code) == 5

    def _stock_code_with_prefix_no_df(self, source: str, market: int, code: str) -> str:
        """输入来源市场与代码，输出前缀代码；用于无 DataFrame 输出；未知来源回退原代码。"""
        c = str(code).strip()
        s = str(source).strip().lower()
        m = int(market)
        if s == "std":
            return f"sz.{c}" if m == 0 else f"sh.{c}"
        if s == "ex":
            if m == 44:
                return f"bj.{c}"
            if self._is_hk_market_ex_no_df(m):
                return f"hk.{c}"
        return c

    def _ensure_stock_route_cache_no_df(self, refresh: bool = False):
        """输入刷新开关，输出无；用于无 DataFrame 股票路由；分页空页视为结束。"""
        if self._stock_route and not refresh:
            return

        security_page = int(
            self.pagination.get("standard_security_list_page_size", 800)
        )
        route: Dict[str, Dict[str, Any]] = {}

        for market in [0, 1]:
            start = 0
            while True:
                page = self.std_pool.call(
                    "get_security_list", market, start, allow_none=True
                )
                if not page:
                    break
                for item in page:
                    code = str(item.get("code", "")).strip()
                    if not code or not self._is_a_share_std(market, code):
                        continue
                    if code in route:
                        continue
                    route[code] = {
                        "code": code,
                        "name": str(item.get("name", "")).strip(),
                        "market": int(market),
                        "market_name": "深圳" if market == 0 else "上海",
                        "source": "std",
                        "asset_type": "stock",
                    }
                start += len(page)
                if len(page) < security_page:
                    break

        for item in self._fetch_all_instrument_info(refresh=refresh):
            market = int(item.get("market", -1))
            code = str(item.get("code", "")).strip()
            is_beijing_stock = market == 44 and self._is_beijing_stock_ex(code)
            is_hk_stock = self._is_hk_stock_ex_no_df(market, code)
            if not is_beijing_stock and not is_hk_stock:
                continue
            if code in route:
                continue
            route[code] = {
                "code": code,
                "name": str(item.get("name", "")).strip(),
                "market": market,
                "market_name": self._get_ex_market_name_no_df(market),
                "source": "ex",
                "asset_type": "stock",
            }

        self._stock_route = route

    def _resolve_stock_route_no_df(
        self, code: str, freq: str
    ) -> Tuple[str, Dict[str, Any]]:
        """输入代码和频率，输出标准化代码与路由；用于无 DataFrame 查询；未找到抛错。"""
        normalized_code = self._normalize_stock_query_code(code)
        self._ensure_stock_route_cache_no_df(refresh=False)
        route = self._stock_route.get(str(normalized_code))
        if route is None:
            self._ensure_stock_route_cache_no_df(refresh=True)
            route = self._stock_route.get(str(normalized_code))
        if route is None:
            self._record_failure(
                "stock_kline", str(code), "code_not_found", "route_missing", freq
            )
            raise ValueError(f"股票代码未找到: {code}")
        return normalized_code, route

    def _raw_kline_row_volume(self, row: Dict[str, Any]) -> Optional[float]:
        """原始 K 线行成交量：优先 vol，其次 trade，再次 volume。"""
        vol_v = self._safe_float_no_df(row.get("vol"))
        if vol_v is not None:
            return vol_v
        trade_v = self._safe_float_no_df(row.get("trade"))
        if trade_v is not None:
            return trade_v
        return self._safe_float_no_df(row.get("volume"))

    def _is_placeholder_raw_kline_row(self, row: Dict[str, Any]) -> bool:
        """
        输入：socket 原始 K 线行。
        输出：True 表示 o=h=l=c 且成交量/额近零的占位条，取 bar 时应跳过。

        实现说明：OHLC 在 parser 层经 `np.round(..., 2)` 原地刻度后位级稳定，
        因此此处 `==` 浮点等比较不会因 IEEE-754 微差出现假阴性；
        若上游改动 OHLC 精度策略需同步评估此判定。
        """
        if not bool(self.output_cfg.get("filter_suspended_placeholder_bar", True)):
            return False
        if not isinstance(row, dict):
            return False
        eps = float(self.output_cfg.get("suspended_placeholder_eps", 1e-20))
        open_v = self._safe_float_no_df(row.get("open"))
        close_v = self._safe_float_no_df(row.get("close"))
        high_v = self._safe_float_no_df(row.get("high"))
        low_v = self._safe_float_no_df(row.get("low"))
        if None in (open_v, close_v, high_v, low_v):
            return False
        if not (open_v == close_v == high_v == low_v):
            return False
        volume_v = self._raw_kline_row_volume(row)
        amount_v = self._safe_float_no_df(row.get("amount"))
        vol_tiny = volume_v is None or abs(volume_v) <= eps
        amount_tiny = amount_v is None or abs(amount_v) <= eps
        return vol_tiny and amount_tiny

    def _filter_placeholder_raw_kline_rows(
        self, rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """逐条剔除 o=h=l=c 且量额近零的占位 bar；不丢弃同页其它正常 bar。"""
        if not rows:
            return []
        return [row for row in rows if not self._is_placeholder_raw_kline_row(row)]

    def _append_raw_kline_page_rows(
        self,
        rows: List[Dict[str, Any]],
        raw_page: List[Dict[str, Any]],
    ) -> int:
        """
        将一页 socket 原始 bar 逐条写入 rows（跳过占位条）。

        输出：原始页长度，供 start+=len(raw_page) 分页偏移；与是否含占位条无关。
        """
        raw_page = list(raw_page or [])
        for row in raw_page:
            if isinstance(row, dict) and not self._is_placeholder_raw_kline_row(row):
                rows.append(row)
        return len(raw_page)

    def _paginate_kline_pages(
        self,
        *,
        page_size: int,
        start_boundary: Any,
        boundary_mode: Literal["no_df", "pandas"],
        fetch_page: Callable[[int], Any],
        rows: Optional[List[Dict[str, Any]]] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        K 线向更早历史翻页的统一循环。

        输入：
        1. page_size: 单页条数上限。
        2. start_boundary: 左时间边界（`datetime` 或 `pd.Timestamp`）。
        3. boundary_mode: `no_df` 用 `_to_datetime_no_df`；`pandas` 用 `pd.to_datetime`。
        4. fetch_page: `(start) -> page | None`，由调用方封装 pool/API/参数差异。
        5. rows: 可选已有列表；为 None 时新建。
        6. max_pages: 最大页数；None 时读 `pagination.max_kline_pages`。
        输出：
        1. 追加后的原始 K 线 dict 列表（未做 normalize/DataFrame）。
        边界条件：
        1. 空页、达左边界、末页不足 `page_size` 时结束；`fetch_page` 差异由调用方保证。
        """
        effective_max_pages = int(
            max_pages
            if max_pages is not None
            else self.pagination.get("max_kline_pages", _DEFAULT_MAX_KLINE_PAGES)
        )
        out_rows: List[Dict[str, Any]] = list(rows) if rows is not None else []
        start = 0
        for _ in range(effective_max_pages):
            page = fetch_page(start)
            if not page:
                break
            raw_page = list(page)
            self._append_raw_kline_page_rows(out_rows, raw_page)
            oldest_raw = (
                raw_page[0].get("datetime") if isinstance(raw_page[0], dict) else None
            )
            if boundary_mode == "pandas":
                oldest_ts = pd.to_datetime(oldest_raw, errors="coerce")
                if not pd.isna(oldest_ts) and oldest_ts <= start_boundary:
                    break
            else:
                try:
                    oldest_dt = self._to_datetime_no_df(oldest_raw)
                except Exception:
                    oldest_dt = None
                if oldest_dt is not None and oldest_dt <= start_boundary:
                    break
            if len(raw_page) < page_size:
                break
            start += len(raw_page)
        return out_rows

    def _dt_key_for_raw_kline_row(self, row: Dict[str, Any]) -> Optional[str]:
        """输入原始 K 线行，输出与 `_normalize_stock_kline_rows` 一致的 datetime 文本键；非法返回 None。"""
        try:
            parsed_dt = self._to_datetime_no_df(row.get("datetime"))
        except Exception:
            return None
        return parsed_dt.strftime("%Y-%m-%d %H:%M:%S")

    def _normalize_stock_kline_rows(
        self,
        rows: List[Dict[str, Any]],
        source: str,
        market: int,
        code: str,
        freq: str,
        start_dt: dt.datetime,
        end_dt: dt.datetime,
    ) -> List[Dict[str, Any]]:
        """P2: numpy 向量化时间过滤/排序；仅使用 socket 层 datetime/_ts，不解析 year/month/day/hour/minute。"""
        if not rows:
            return []
        import numpy as np

        n = len(rows)
        normalized_freq = self._normalize_freq(freq)
        prefixed_code = self._stock_code_with_prefix_no_df(
            source=source, market=market, code=code
        )

        timestamps = np.empty(n, dtype=np.int64)
        opens = np.empty(n, dtype=np.float64)
        closes = np.empty(n, dtype=np.float64)
        highs = np.empty(n, dtype=np.float64)
        lows = np.empty(n, dtype=np.float64)
        volumes = np.empty(n, dtype=np.float64)
        amounts = np.empty(n, dtype=np.float64)
        datetime_texts = np.empty(n, dtype=object)
        valid_mask = np.ones(n, dtype=bool)

        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                valid_mask[i] = False
                continue
            ts = row.get("_ts")
            dt_raw = row.get("datetime")
            if ts is not None:
                timestamps[i] = int(ts)
                datetime_texts[i] = str(dt_raw).strip() if dt_raw else None
            else:
                try:
                    parsed_dt = self._to_datetime_no_df(dt_raw)
                    timestamps[i] = int(parsed_dt.timestamp())
                    datetime_texts[i] = parsed_dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    valid_mask[i] = False
                    continue
            opens[i] = float(row.get("open") or 0)
            closes[i] = float(row.get("close") or 0)
            highs[i] = float(row.get("high") or 0)
            lows[i] = float(row.get("low") or 0)
            vol_v = float(row.get("vol") or 0)
            trade_v = float(row.get("trade") or 0)
            volumes[i] = vol_v if vol_v else trade_v
            amounts[i] = float(row.get("amount") or 0)

        if not valid_mask.any():
            return []

        ts_start = int(start_dt.timestamp())
        ts_end = int(end_dt.timestamp())
        valid_mask &= (timestamps >= ts_start) & (timestamps <= ts_end)
        if not valid_mask.any():
            return []

        idx = np.where(valid_mask)[0]
        timestamps = timestamps[idx]
        opens = opens[idx]
        closes = closes[idx]
        highs = highs[idx]
        lows = lows[idx]
        volumes = volumes[idx]
        amounts = amounts[idx]
        datetime_texts = datetime_texts[idx]

        _, unique_idx = np.unique(timestamps, return_index=True)
        sort_order = np.argsort(timestamps[unique_idx])
        final_idx = unique_idx[sort_order]

        timestamps = timestamps[final_idx]
        opens = opens[final_idx]
        closes = closes[final_idx]
        highs = highs[final_idx]
        lows = lows[final_idx]
        volumes = volumes[final_idx]
        amounts = amounts[final_idx]
        datetime_texts = datetime_texts[final_idx]

        out = []
        for i in range(len(timestamps)):
            ts = int(timestamps[i])
            dt_str = datetime_texts[i]
            if not dt_str:
                # parser 已写入 YYYY-MM-DD HH:MM:SS（秒位固定 :00）；此分支仅在 _ts 存在而 datetime 缺失时触发，
                # 强制按同一 schema 生成，避免与 socket 层输出不一致。
                dt_str = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            out.append(
                {
                    "code": prefixed_code,
                    "freq": normalized_freq,
                    "open": float(opens[i]),
                    "close": float(closes[i]),
                    "high": float(highs[i]),
                    "low": float(lows[i]),
                    "volume": int(volumes[i]),
                    "amount": int(amounts[i]),
                    "datetime": dt_str,
                }
            )
        return out

    def _index_route_fingerprint(self, route: Dict[str, Any]) -> str:
        """输入路由字典，输出 source:market:code 指纹；用于 chunk 分区失效判断。"""
        source = str(route.get("source", "")).strip().lower()
        try:
            market = int(route.get("market", -1))
        except Exception:
            market = -1
        code = str(route.get("code", "")).strip()
        return f"{source}:{market}:{code}"

    def _reset_index_chunk_partition(self, partition: Dict[str, Any]) -> None:
        """
        输入：chunk 分区字典。
        输出：无；原地清空指数分页缓存状态。
        用途：路由变更或强制失效时重置分区。
        边界条件：不释放 cache_rows 底层列表，仅归零 _n 与游标。
        """
        partition["_n"] = 0
        partition["oldest_dt"] = None
        partition["newest_dt"] = None
        partition["page_start"] = 0
        partition["fetched_pages"] = 0
        partition["_route_fp"] = ""

    def _ensure_index_chunk_route_valid(
        self, partition: Dict[str, Any], route: Dict[str, Any]
    ) -> None:
        """
        输入：chunk 分区与当前路由。
        输出：无；若路由指纹变化则清空分区缓存。
        用途：避免混用旧 market/code 的 bar。
        边界条件：首次写入 _route_fp 时不重置。
        """
        fp = self._index_route_fingerprint(route)
        old_fp = str(partition.get("_route_fp", "") or "").strip()
        if old_fp != "" and old_fp != fp:
            self._reset_index_chunk_partition(partition)
        partition["_route_fp"] = fp

    def _normalize_index_kline_rows(
        self,
        rows: List[Dict[str, Any]],
        index_name: str,
        route_code: str,
        freq: str,
        start_dt: dt.datetime,
        end_dt: dt.datetime,
    ) -> List[Dict[str, Any]]:
        """
        输入：原始指数 K 线行、名称/代码/频率与时间窗。
        输出：标准化指数 K 线 dict 列表。
        用途：chunk 窗口过滤阶段批量归一化；仅使用 socket 层 datetime/_ts，不解析年月日时分分列。
        边界条件：无有效行时返回 []。
        """
        if not rows:
            return []
        import numpy as np

        n = len(rows)
        normalized_freq = self._normalize_freq(freq)
        route_name = str(index_name).strip()
        route_code_text = str(route_code).strip()

        timestamps = np.empty(n, dtype=np.int64)
        opens = np.empty(n, dtype=np.float64)
        closes = np.empty(n, dtype=np.float64)
        highs = np.empty(n, dtype=np.float64)
        lows = np.empty(n, dtype=np.float64)
        volumes = np.empty(n, dtype=np.float64)
        amounts = np.empty(n, dtype=np.float64)
        datetime_texts = np.empty(n, dtype=object)
        valid_mask = np.ones(n, dtype=bool)

        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                valid_mask[i] = False
                continue
            ts = row.get("_ts")
            dt_raw = row.get("datetime")
            if ts is not None:
                timestamps[i] = int(ts)
                datetime_texts[i] = str(dt_raw).strip() if dt_raw else None
            else:
                try:
                    parsed_dt = self._to_datetime_no_df(dt_raw)
                    timestamps[i] = int(parsed_dt.timestamp())
                    datetime_texts[i] = parsed_dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    valid_mask[i] = False
                    continue
            open_v = self._safe_float_no_df(row.get("open"))
            close_v = self._safe_float_no_df(row.get("close"))
            high_v = self._safe_float_no_df(row.get("high"))
            low_v = self._safe_float_no_df(row.get("low"))
            if open_v is None or close_v is None or high_v is None or low_v is None:
                valid_mask[i] = False
                continue
            opens[i] = open_v
            closes[i] = close_v
            highs[i] = high_v
            lows[i] = low_v
            vol_v = self._safe_float_no_df(row.get("vol"))
            trade_v = self._safe_float_no_df(row.get("trade"))
            volumes[i] = (
                vol_v
                if vol_v is not None
                else (trade_v if trade_v is not None else 0.0)
            )
            amount_v = self._safe_float_no_df(row.get("amount"))
            amounts[i] = amount_v if amount_v is not None else 0.0

        if not valid_mask.any():
            return []

        ts_start = int(start_dt.timestamp())
        ts_end = int(end_dt.timestamp())
        valid_mask &= (timestamps >= ts_start) & (timestamps <= ts_end)
        if not valid_mask.any():
            return []

        idx = np.where(valid_mask)[0]
        timestamps = timestamps[idx]
        opens = opens[idx]
        closes = closes[idx]
        highs = highs[idx]
        lows = lows[idx]
        volumes = volumes[idx]
        amounts = amounts[idx]
        datetime_texts = datetime_texts[idx]

        _, unique_idx = np.unique(timestamps, return_index=True)
        sort_order = np.argsort(timestamps[unique_idx])
        final_idx = unique_idx[sort_order]

        timestamps = timestamps[final_idx]
        opens = opens[final_idx]
        closes = closes[final_idx]
        highs = highs[final_idx]
        lows = lows[final_idx]
        volumes = volumes[final_idx]
        amounts = amounts[final_idx]
        datetime_texts = datetime_texts[final_idx]

        out: List[Dict[str, Any]] = []
        for i in range(len(timestamps)):
            ts = int(timestamps[i])
            dt_str = datetime_texts[i]
            if not dt_str:
                # parser 已写入 YYYY-MM-DD HH:MM:SS（秒位固定 :00）；fallback 强制按同一 schema 生成。
                dt_str = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            out.append(
                {
                    "index_name": route_name,
                    "code": route_code_text,
                    "freq": normalized_freq,
                    "open": float(opens[i]),
                    "close": float(closes[i]),
                    "high": float(highs[i]),
                    "low": float(lows[i]),
                    "volume": int(volumes[i]),
                    "amount": int(amounts[i]),
                    "datetime": dt_str,
                }
            )
        return out

    def _fetch_std_kline_rows(
        self,
        market: int,
        code: str,
        category: int,
        freq: str,
        start_dt: dt.datetime,
        end_dt: dt.datetime,
    ) -> List[Dict[str, Any]]:
        """输入标准行情参数，输出标准化K线列表；用于无 DataFrame 路径；空页即结束。"""
        page_size = int(self.pagination.get("standard_kline_page_size", 800))

        def _fetch_page(start: int) -> Any:
            return self._pool_call_allow_none(
                self.std_pool,
                "get_security_bars",
                category,
                int(market),
                str(code),
                int(start),
                int(page_size),
            )

        rows = self._paginate_kline_pages(
            page_size=page_size,
            start_boundary=start_dt,
            boundary_mode="no_df",
            fetch_page=_fetch_page,
        )
        return self._normalize_stock_kline_rows(
            rows=rows,
            source="std",
            market=int(market),
            code=str(code),
            freq=freq,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    def _fetch_ex_kline_rows(
        self,
        market: int,
        code: str,
        category: int,
        freq: str,
        start_dt: dt.datetime,
        end_dt: dt.datetime,
    ) -> List[Dict[str, Any]]:
        """输入扩展行情参数，输出标准化K线列表；用于无 DataFrame 路径；空页即结束。"""
        page_size = int(self.pagination.get("extended_kline_page_size", 700))

        def _fetch_page(start: int) -> Any:
            return self._pool_call_allow_none(
                self.ex_pool,
                "get_instrument_bars",
                category,
                int(market),
                str(code),
                int(start),
                int(page_size),
            )

        rows = self._paginate_kline_pages(
            page_size=page_size,
            start_boundary=start_dt,
            boundary_mode="no_df",
            fetch_page=_fetch_page,
        )
        return self._normalize_stock_kline_rows(
            rows=rows,
            source="ex",
            market=int(market),
            code=str(code),
            freq=freq,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    def _fetch_kline_page_rows_no_df(
        self,
        *,
        source: str,
        market: int,
        code: str,
        category: int,
        start: int,
        page_size: int,
    ) -> List[Dict[str, Any]]:
        """
        按分页偏移抓取单页原始 K 线。

        输入：
        1. source: 数据源标识（std/ex）。
        2. market/code/category: 查询定位参数。
        3. start/page_size: 分页偏移与页大小。
        输出：
        1. 原始行字典列表。
        用途：
        1. 为 chunk 级缓存路径提供统一分页抓取入口，并统计网络页调用次数。
        分页语义（socket 实测，见 tests/manual/probe_kline_pagination_direction.py）：
        1. start=0 从「当前最新」一段 bar 起取；start+=len(page) 向更早历史翻页（下一页整体更旧）。
        2. 单页内时间为升序：page[0] 为本页最老，page[-1] 为本页最新；停止条件用 page[0] 与任务 start_time 比较。
        边界条件：
        1. source 非法时抛 ValueError；空页返回空列表。
        """
        source_key = str(source).strip().lower()
        if source_key == "std":
            rows = self._pool_call_allow_none(
                self.std_pool,
                "get_security_bars",
                int(category),
                int(market),
                str(code),
                int(start),
                int(page_size),
            )
            return list(rows or [])
        if source_key == "ex":
            rows = self._pool_call_allow_none(
                self.ex_pool,
                "get_instrument_bars",
                int(category),
                int(market),
                str(code),
                int(start),
                int(page_size),
            )
            return list(rows or [])
        raise ValueError(f"不支持的 source: {source}")

    def _fetch_index_kline_page_rows_no_df(
        self,
        *,
        route: Dict[str, Any],
        category: int,
        start: int,
        page_size: int,
    ) -> List[Dict[str, Any]]:
        """
        按分页偏移抓取单页指数原始 K 线。

        输入：
        1. route: 指数路由（source/market/code）。
        2. category/start/page_size: TDX 分页参数。
        输出：
        1. 原始行字典列表。
        用途：
        1. 为指数 chunk 级缓存提供统一分页入口。
        边界条件：
        1. 路由非法时抛 ValueError；空页返回 []。
        """
        source_key = str(route.get("source", "std")).strip().lower()
        market = int(route.get("market", -1))
        code = str(route.get("code", "")).strip()
        if market < 0 or code == "":
            raise ValueError(f"指数路由配置非法: {route}")
        if source_key == "std":
            rows = self._pool_call_allow_none(
                self.std_pool,
                "get_index_bars",
                int(category),
                int(market),
                str(code),
                int(start),
                int(page_size),
            )
            return list(rows or [])
        if source_key == "ex":
            rows = self._pool_call_allow_none(
                self.ex_pool,
                "get_instrument_bars",
                int(category),
                int(market),
                str(code),
                int(start),
                int(page_size),
            )
            return list(rows or [])
        raise ValueError(f"不支持的 source: {source_key}")

    def _merge_chunk_cache_page_rows(
        self,
        *,
        page_rows: List[Dict[str, Any]],
        cache_rows: list,
        _n_ref: Optional[list] = None,  # [_n] — mutable int via single-element list
    ) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
        """Append page rows to cache_rows, bump _n. Return (page_oldest_dt, page_newest_dt).

        TDX 分页按 offset 不重叠；页内时间为升序（page[0] 最老、page[-1] 最新）。
        start 增大表示向更早历史翻页（见 tests/manual/probe_kline_pagination_direction.py 实测）。
        Overwrites at cache_rows[_n] if within existing allocation, else .append().
        """
        oldest_dt: Optional[dt.datetime] = None
        newest_dt: Optional[dt.datetime] = None
        _n = _n_ref[0] if _n_ref else len(cache_rows)
        for row in page_rows or []:
            if not isinstance(row, dict):
                continue
            if self._is_placeholder_raw_kline_row(row):
                continue
            try:
                parsed_dt = self._to_datetime_no_df(row.get("datetime"))
            except Exception:
                continue
            item = (parsed_dt, dict(row))
            if _n < len(cache_rows):
                cache_rows[_n] = item
            else:
                cache_rows.append(item)
            _n += 1
            if oldest_dt is None or parsed_dt < oldest_dt:
                oldest_dt = parsed_dt
            if newest_dt is None or parsed_dt > newest_dt:
                newest_dt = parsed_dt
        if _n_ref:
            _n_ref[0] = _n
        return (oldest_dt, newest_dt)

    def _fetch_rows_for_task_with_route_no_df(
        self,
        *,
        normalized_task: Dict[str, str],
        normalized_code: str,
        route: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        输入已标准化任务和已解析路由，输出任务K线列表；用于 chunk 执行复用路由结果。

        输入：
        1. normalized_task: 标准化任务字典。
        2. normalized_code: 标准化股票代码。
        3. route: 已解析路由字典。
        输出：
        1. 标准化后的K线字典列表。
        用途：
        1. 避免同 chunk 内重复解析 code 路由。
        边界条件：
        1. start_time 晚于 end_time 时抛 ValueError。
        """
        freq = str(normalized_task["freq"])
        category = self._freq_to_category(freq)
        start_dt = self._to_datetime_no_df(normalized_task["start_time"])
        end_dt = self._to_datetime_no_df(normalized_task["end_time"])
        if start_dt > end_dt:
            raise ValueError("start_time 不能晚于 end_time")
        if route["source"] == "std":
            return self._fetch_std_kline_rows(
                market=int(route["market"]),
                code=str(normalized_code),
                category=category,
                freq=freq,
                start_dt=start_dt,
                end_dt=end_dt,
            )
        return self._fetch_ex_kline_rows(
            market=int(route["market"]),
            code=str(normalized_code),
            category=category,
            freq=freq,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    def get_stock_kline_rows_for_chunk_tasks(
        self,
        tasks: List[Dict[str, Any]],
        enable_cache: bool,
    ) -> Dict[str, Any]:
        """
        执行同 `code+freq` 的 chunk 任务并返回逐任务结果。

        输入：
        1. tasks: 任务列表，元素至少包含 code/freq/start_time/end_time。
        2. enable_cache: 是否启用 chunk 级轻量缓存。
        输出：
        1. 结果字典：{"results":[{"task","rows","error"}...], "chunk_hit_tasks", "chunk_network_page_calls"}。
        用途：
        1. 在 chunk 内按 `start_time` 升序执行任务，尽量复用前序任务已拉取的原始 bar 缓存。
        边界条件：
        1. 若任务不属于同一 code+freq，直接抛 ValueError。
        2. `chunk_network_page_calls` 统计的是分页请求次数；发生分页时会累计多次。
        """
        raw_tasks = list(tasks or [])
        if not raw_tasks:
            return {"results": [], "chunk_hit_tasks": 0, "chunk_network_page_calls": 0}

        normalized_tasks: List[Dict[str, str]] = []
        for item in raw_tasks:
            normalized_tasks.append(self._normalize_task_dict_no_df(item))

        base_code = str(normalized_tasks[0]["code"]).strip()
        base_freq = str(normalized_tasks[0]["freq"]).strip()
        for task in normalized_tasks[1:]:
            if (
                str(task["code"]).strip() != base_code
                or str(task["freq"]).strip() != base_freq
            ):
                raise ValueError("chunk 任务必须属于同一 code+freq")

        ordered_tasks = sorted(
            list(enumerate(normalized_tasks)),
            key=lambda item: (
                self._to_datetime_no_df(item[1]["start_time"]),
                self._to_datetime_no_df(item[1]["end_time"]),
                int(item[0]),
            ),
        )
        task_items = [dict(item[1]) for item in ordered_tasks]

        normalized_code, route = self._resolve_stock_route_no_df(base_code, base_freq)
        if not bool(enable_cache):
            results: List[Dict[str, Any]] = []
            for task in task_items:
                try:
                    rows = self._fetch_rows_for_task_with_route_no_df(
                        normalized_task=task,
                        normalized_code=normalized_code,
                        route=route,
                    )
                except Exception as exc:
                    results.append({"task": task, "rows": [], "error": str(exc)[:200]})
                    continue
                if not rows:
                    results.append({"task": task, "rows": [], "error": "no_data"})
                else:
                    results.append({"task": task, "rows": rows, "error": None})
            return {
                "results": results,
                "chunk_hit_tasks": 0,
                "chunk_network_page_calls": 0,
            }

        category = self._freq_to_category(base_freq)
        source = str(route.get("source", "std")).strip().lower()
        page_size = int(
            self.pagination.get(
                "standard_kline_page_size"
                if source == "std"
                else "extended_kline_page_size",
                800 if source == "std" else 700,
            )
        )
        max_pages = int(
            self.pagination.get("max_kline_pages", _DEFAULT_MAX_KLINE_PAGES)
        )
        market = int(route.get("market", 0))

        chunk_hit_tasks = 0
        chunk_network_page_calls = 0
        results: List[Dict[str, Any]] = []
        normalized_bar_by_dt_key: Dict[str, Dict[str, Any]] = {}

        partition = self._shared_chunk_cache.acquire_partition(base_code, base_freq)
        try:
            cache_rows = partition["cache_rows"]
            _n_ref = [partition["_n"]]
            oldest_cached_dt = partition["oldest_dt"]
            newest_cached_dt = partition["newest_dt"]
            page_start = partition["page_start"]
            fetched_pages = partition["fetched_pages"]

            for task in task_items:
                try:
                    start_dt = self._to_datetime_no_df(task["start_time"])
                    end_dt = self._to_datetime_no_df(task["end_time"])
                except Exception as exc:
                    results.append({"task": task, "rows": [], "error": str(exc)[:200]})
                    continue

                if start_dt > end_dt:
                    results.append(
                        {
                            "task": task,
                            "rows": [],
                            "error": "start_time 不能晚于 end_time",
                        }
                    )
                    continue

                before_calls = int(chunk_network_page_calls)
                # 写分区：分页拉取并 merge 进租约分区（仅未命中缓存或需补新页时进入）
                while True:
                    covered = (
                        oldest_cached_dt is not None and oldest_cached_dt <= start_dt
                    )
                    stale = not self._chunk_cache_covers_end_dt(
                        newest_cached_dt, end_dt, base_freq
                    )
                    if covered and not stale:
                        break
                    if fetched_pages >= max_pages:
                        break
                    page_rows = self._fetch_kline_page_rows_no_df(
                        source=source,
                        market=market,
                        code=normalized_code,
                        category=category,
                        start=page_start,
                        page_size=page_size,
                    )
                    fetched_pages += 1
                    chunk_network_page_calls += 1
                    if not page_rows:
                        break
                    page_oldest, page_newest = self._merge_chunk_cache_page_rows(
                        page_rows=page_rows, cache_rows=cache_rows, _n_ref=_n_ref
                    )
                    if page_oldest is not None and (
                        oldest_cached_dt is None or page_oldest < oldest_cached_dt
                    ):
                        oldest_cached_dt = page_oldest
                    if page_newest is not None and (
                        newest_cached_dt is None or page_newest > newest_cached_dt
                    ):
                        newest_cached_dt = page_newest
                    if (
                        oldest_cached_dt is not None
                        and oldest_cached_dt <= start_dt
                        and not self._chunk_cache_covers_end_dt(
                            newest_cached_dt, end_dt, base_freq
                        )
                    ):
                        break
                    if len(page_rows) < page_size:
                        break
                    page_start += len(page_rows)

                # 读分区：仅扫描 [0,_n) 有效槽位，按任务时间窗过滤（不触碰 _n 之后残留槽位）
                filtered_raw: List[Dict[str, Any]] = []
                _n = _n_ref[0]
                for j in range(_n):
                    dt_key, row = cache_rows[j]
                    if not isinstance(row, dict):
                        continue
                    if dt_key < start_dt or dt_key > end_dt:
                        continue
                    filtered_raw.append(row)

                pending_raw = [
                    row
                    for row in filtered_raw
                    if self._dt_key_for_raw_kline_row(row)
                    not in normalized_bar_by_dt_key
                ]
                if pending_raw:
                    newly = self._normalize_stock_kline_rows(
                        rows=pending_raw,
                        source=source,
                        market=market,
                        code=str(normalized_code),
                        freq=base_freq,
                        start_dt=start_dt,
                        end_dt=end_dt,
                    )
                    for nr in newly:
                        dt_key = self._dt_key_for_raw_kline_row(
                            {"datetime": nr.get("datetime")}
                        )
                        if dt_key is not None:
                            normalized_bar_by_dt_key[dt_key] = nr

                window_keys: List[str] = []
                seen_window_k: set[str] = set()
                for row in filtered_raw:
                    k = self._dt_key_for_raw_kline_row(row)
                    if k is None or k in seen_window_k:
                        continue
                    seen_window_k.add(k)
                    window_keys.append(k)
                window_keys.sort()
                rows = [
                    normalized_bar_by_dt_key[k]
                    for k in window_keys
                    if k in normalized_bar_by_dt_key
                ]
                if int(chunk_network_page_calls) == int(before_calls):
                    chunk_hit_tasks += 1
                if not rows:
                    results.append({"task": task, "rows": [], "error": "no_data"})
                else:
                    results.append({"task": task, "rows": rows, "error": None})

            partition["_n"] = _n_ref[0]
            partition["oldest_dt"] = oldest_cached_dt
            partition["newest_dt"] = newest_cached_dt
            partition["page_start"] = page_start
            partition["fetched_pages"] = fetched_pages
        finally:
            self._shared_chunk_cache.soft_delete(base_code, base_freq)

        return {
            "results": results,
            "chunk_hit_tasks": int(chunk_hit_tasks),
            "chunk_network_page_calls": int(chunk_network_page_calls),
        }

    def get_index_kline_rows_for_chunk_tasks(
        self,
        tasks: List[Dict[str, Any]],
        enable_cache: bool,
    ) -> Dict[str, Any]:
        """
        执行同 `index_name+freq` 的指数 chunk 任务并返回逐任务结果。

        输入：
        1. tasks: 任务列表，元素至少包含 index_name/freq/start_time/end_time。
        2. enable_cache: 是否启用 chunk 级轻量缓存。
        输出：
        1. 结果字典：{"results":[{"task","rows","error"}...], "chunk_hit_tasks", "chunk_network_page_calls"}。
        用途：
        1. 在 chunk 内按 `start_time` 升序执行任务，尽量复用前序任务已拉取的指数原始 bar 缓存。
        边界条件：
        1. 若任务不属于同一 index_name+freq，直接抛 ValueError。
        2. `chunk_network_page_calls` 统计的是分页请求次数；发生分页时会累计多次。
        """
        raw_tasks = list(tasks or [])
        if not raw_tasks:
            return {"results": [], "chunk_hit_tasks": 0, "chunk_network_page_calls": 0}

        normalized_tasks: List[Dict[str, Any]] = []
        for item in raw_tasks:
            normalized_tasks.append(self._normalize_index_task_payload_no_df(item))

        base_index_name = str(normalized_tasks[0]["index_name"]).strip()
        base_freq = str(normalized_tasks[0]["freq"]).strip()
        for task in normalized_tasks[1:]:
            if (
                str(task["index_name"]).strip() != base_index_name
                or str(task["freq"]).strip() != base_freq
            ):
                raise ValueError("chunk 任务必须属于同一 index_name+freq")

        ordered_tasks = sorted(
            list(enumerate(normalized_tasks)),
            key=lambda item: (
                self._to_datetime_no_df(item[1]["start_time"]),
                self._to_datetime_no_df(item[1]["end_time"]),
                int(item[0]),
            ),
        )
        task_items = [dict(item[1]) for item in ordered_tasks]

        first_task = dict(task_items[0])
        pre_route_source = (
            str(first_task.get("_index_route_source", "")).strip().lower()
        )
        pre_route_code = str(first_task.get("_index_route_code", "")).strip()
        pre_route_name = str(first_task.get("_index_route_name", "")).strip()
        try:
            pre_route_market = int(first_task.get("_index_route_market", -1))
        except Exception:
            pre_route_market = -1
        if (
            pre_route_source in {"std", "ex"}
            and pre_route_code != ""
            and pre_route_market >= 0
        ):
            route = {
                "name": pre_route_name if pre_route_name != "" else base_index_name,
                "source": pre_route_source,
                "market": pre_route_market,
                "code": pre_route_code,
            }
        else:
            route = self.resolve_index_name(index_name=base_index_name, refresh=False)

        if not bool(enable_cache):
            results: List[Dict[str, Any]] = []
            for task in task_items:
                try:
                    rows = self._fetch_index_rows_for_task_with_route_no_df(
                        normalized_task=task,
                        route=route,
                    )
                except Exception as exc:
                    results.append({"task": task, "rows": [], "error": str(exc)[:200]})
                    continue
                if not rows:
                    results.append({"task": task, "rows": [], "error": "no_data"})
                else:
                    results.append({"task": task, "rows": rows, "error": None})
            return {
                "results": results,
                "chunk_hit_tasks": 0,
                "chunk_network_page_calls": 0,
            }

        category = self._freq_to_category(base_freq)
        source = str(route.get("source", "std")).strip().lower()
        route_code = str(route.get("code", "")).strip()
        route_name = str(route.get("name", base_index_name)).strip() or base_index_name
        page_size = int(
            self.pagination.get(
                "standard_kline_page_size"
                if source == "std"
                else "extended_kline_page_size",
                800 if source == "std" else 700,
            )
        )
        max_pages = int(
            self.pagination.get("max_kline_pages", _DEFAULT_MAX_KLINE_PAGES)
        )

        chunk_hit_tasks = 0
        chunk_network_page_calls = 0
        results: List[Dict[str, Any]] = []
        normalized_bar_by_dt_key: Dict[str, Dict[str, Any]] = {}

        partition = self._shared_chunk_cache.acquire_partition(
            base_index_name, base_freq
        )
        try:
            self._ensure_index_chunk_route_valid(partition, route)
            cache_rows = partition["cache_rows"]
            _n_ref = [partition["_n"]]
            oldest_cached_dt = partition["oldest_dt"]
            newest_cached_dt = partition["newest_dt"]
            page_start = partition["page_start"]
            fetched_pages = partition["fetched_pages"]
            route_refreshed = False

            for task in task_items:
                try:
                    start_dt = self._to_datetime_no_df(task["start_time"])
                    end_dt = self._to_datetime_no_df(task["end_time"])
                except Exception as exc:
                    results.append({"task": task, "rows": [], "error": str(exc)[:200]})
                    continue
                if start_dt > end_dt:
                    results.append(
                        {
                            "task": task,
                            "rows": [],
                            "error": "start_time 不能晚于 end_time",
                        }
                    )
                    continue

                before_calls = int(chunk_network_page_calls)
                # 写分区：分页拉取并 merge 进租约分区
                while True:
                    covered = (
                        oldest_cached_dt is not None and oldest_cached_dt <= start_dt
                    )
                    stale = not self._chunk_cache_covers_end_dt(
                        newest_cached_dt, end_dt, base_freq
                    )
                    if covered and not stale:
                        break
                    if fetched_pages >= max_pages:
                        break
                    try:
                        page_rows = self._fetch_index_kline_page_rows_no_df(
                            route=route,
                            category=category,
                            start=page_start,
                            page_size=page_size,
                        )
                    except Exception:
                        if not route_refreshed:
                            route = self.resolve_index_name(
                                index_name=base_index_name, refresh=True
                            )
                            route_refreshed = True
                            self._ensure_index_chunk_route_valid(partition, route)
                        route_code = str(route.get("code", "")).strip()
                        route_name = (
                            str(route.get("name", base_index_name)).strip()
                            or base_index_name
                        )
                        page_rows = self._fetch_index_kline_page_rows_no_df(
                            route=route,
                            category=category,
                            start=page_start,
                            page_size=page_size,
                        )
                    fetched_pages += 1
                    chunk_network_page_calls += 1
                    if not page_rows:
                        break
                    page_oldest, page_newest = self._merge_chunk_cache_page_rows(
                        page_rows=page_rows,
                        cache_rows=cache_rows,
                        _n_ref=_n_ref,
                    )
                    if page_oldest is not None and (
                        oldest_cached_dt is None or page_oldest < oldest_cached_dt
                    ):
                        oldest_cached_dt = page_oldest
                    if page_newest is not None and (
                        newest_cached_dt is None or page_newest > newest_cached_dt
                    ):
                        newest_cached_dt = page_newest
                    if (
                        oldest_cached_dt is not None
                        and oldest_cached_dt <= start_dt
                        and not self._chunk_cache_covers_end_dt(
                            newest_cached_dt, end_dt, base_freq
                        )
                    ):
                        break
                    if (
                        oldest_cached_dt is not None
                        and oldest_cached_dt <= start_dt
                        and self._chunk_cache_covers_end_dt(
                            newest_cached_dt, end_dt, base_freq
                        )
                    ):
                        break
                    if len(page_rows) < page_size:
                        break
                    page_start += len(page_rows)

                # 读分区：仅扫描 [0,_n) 有效槽位，按任务时间窗过滤
                filtered_raw: List[Dict[str, Any]] = []
                _n = _n_ref[0]
                for j in range(_n):
                    dt_key, row = cache_rows[j]
                    if not isinstance(row, dict):
                        continue
                    if dt_key < start_dt or dt_key > end_dt:
                        continue
                    filtered_raw.append(row)

                pending_raw = [
                    row
                    for row in filtered_raw
                    if self._dt_key_for_raw_kline_row(row)
                    not in normalized_bar_by_dt_key
                ]
                if pending_raw:
                    newly = self._normalize_index_kline_rows(
                        rows=pending_raw,
                        index_name=route_name,
                        route_code=route_code,
                        freq=base_freq,
                        start_dt=start_dt,
                        end_dt=end_dt,
                    )
                    for nr in newly:
                        dt_key = self._dt_key_for_raw_kline_row(
                            {"datetime": nr.get("datetime")}
                        )
                        if dt_key is not None:
                            normalized_bar_by_dt_key[dt_key] = nr

                window_keys: List[str] = []
                seen_window_k: set[str] = set()
                for row in filtered_raw:
                    k = self._dt_key_for_raw_kline_row(row)
                    if k is None or k in seen_window_k:
                        continue
                    seen_window_k.add(k)
                    window_keys.append(k)
                window_keys.sort()
                task_rows = [
                    normalized_bar_by_dt_key[k]
                    for k in window_keys
                    if k in normalized_bar_by_dt_key
                ]
                if int(chunk_network_page_calls) == int(before_calls):
                    chunk_hit_tasks += 1
                if not task_rows:
                    results.append({"task": task, "rows": [], "error": "no_data"})
                else:
                    results.append({"task": task, "rows": task_rows, "error": None})

            partition["_n"] = _n_ref[0]
            partition["oldest_dt"] = oldest_cached_dt
            partition["newest_dt"] = newest_cached_dt
            partition["page_start"] = page_start
            partition["fetched_pages"] = fetched_pages
        finally:
            self._shared_chunk_cache.soft_delete(base_index_name, base_freq)

        return {
            "results": results,
            "chunk_hit_tasks": int(chunk_hit_tasks),
            "chunk_network_page_calls": int(chunk_network_page_calls),
        }

    def _record_failure(
        self, task: str, code: str, reason: str, detail: str, freq: str = ""
    ):
        """输入失败信息，输出无；用于失败报告；detail 为空会转空串。"""
        self._runtime_failures.append(
            {
                "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
                "task": task,
                "code": str(code),
                "freq": str(freq),
                "reason": str(reason),
                "detail": str(detail or ""),
            }
        )

    def _refresh_markets(self):
        """输入无，输出无；用于刷新市场缓存；网络波动时连接池自动兜底。"""
        std_rows = self.std_pool.call("get_markets")
        ex_rows = self.ex_pool.call("get_markets")
        std_df = pd.DataFrame(std_rows)
        ex_df = pd.DataFrame(ex_rows)
        std_df["source"] = "std"
        ex_df["source"] = "ex"
        self._markets_df = pd.concat([std_df, ex_df], ignore_index=True)
        self._ex_market_name_map = {}
        if not ex_df.empty and "market" in ex_df.columns:
            for rec in ex_df.to_dict(orient="records"):
                market_val = rec.get("market")
                if market_val is None or (
                    isinstance(market_val, float) and pd.isna(market_val)
                ):
                    continue
                name_val = rec.get("name", "")
                self._ex_market_name_map[int(market_val)] = (
                    ""
                    if (
                        name_val is None
                        or (isinstance(name_val, float) and pd.isna(name_val))
                    )
                    else str(name_val)
                )

    def get_supported_markets(self, return_df: Optional[bool] = None):
        """输入返回开关，输出市场列表；用于统一市场发现；首次会触发网络请求。"""
        if self._markets_df is None:
            self._refresh_markets()
        if self._default_return_df(return_df):
            return self._markets_df.copy()
        return self._markets_df.to_dict(orient="records")

    def _get_ex_market_name(self, market: int) -> str:
        """输入扩展市场号，输出市场名；用于补充输出字段；未命中返回空串。"""
        if not self._ex_market_name_map:
            self.get_supported_markets(return_df=True)
        return self._ex_market_name_map.get(int(market), "")

    def _is_a_share_std(self, market: int, code: str) -> bool:
        """输入标准市场与代码，输出是否 A 股；用于股票口径过滤；未配置前缀则不纳入。"""
        code = str(code)
        if market == 0:
            prefixes = self.market_rules.get("stock_prefix_sz", [])
        elif market == 1:
            prefixes = self.market_rules.get("stock_prefix_sh", [])
        else:
            return False
        return any(code.startswith(prefix) for prefix in prefixes)

    def _is_beijing_stock_ex(self, code: str) -> bool:
        """输入代码，输出是否北京股票；用于 92* 过滤；前缀为空则不纳入。"""
        prefixes = self.market_rules.get("include_beijing_prefixes", ["92"])
        code = str(code)
        return any(code.startswith(prefix) and len(code) == 6 for prefix in prefixes)

    def _is_hk_market_ex(self, market: int) -> bool:
        """输入扩展市场号，输出是否港股市场；用于港股识别；未配置市场名则默认仅港股通。"""
        target_market_names = set(
            self.market_rules.get("include_hk_market_names", ["港股通"])
        )
        market_name = self._get_ex_market_name(int(market))
        return market_name in target_market_names

    def _is_hk_stock_ex(self, market: int, code: str) -> bool:
        """输入扩展市场与代码，输出是否港股；用于股票路由；固定按5位数字代码过滤。"""
        if not self._is_hk_market_ex(int(market)):
            return False
        raw_code = str(code).strip()
        if not raw_code.isdigit():
            return False
        return len(raw_code) == 5

    def _stock_code_with_prefix(self, source: str, market: int, code: str) -> str:
        """输入来源市场与代码，输出带交易所前缀代码；用于统一股票输出；未知来源回退原代码。"""
        c = str(code).strip()
        s = str(source).strip().lower()
        m = int(market)
        if s == "std":
            return f"sz.{c}" if m == 0 else f"sh.{c}"
        if s == "ex":
            if m == 44:
                return f"bj.{c}"
            if self._is_hk_market_ex(m):
                return f"hk.{c}"
        return c

    def _stock_code_prefix_fallback_by_rule(self, code: str) -> str:
        """输入股票代码，输出规则兜底前缀代码；用于缺少市场信息时补齐；无匹配回退原代码。"""
        c = str(code).strip()
        if c == "":
            return ""
        if len(c) == 5:
            return f"hk.{c}"
        if len(c) == 6:
            first = c[0]
            if first in {"0", "3"}:
                return f"sz.{c}"
            if first == "6":
                return f"sh.{c}"
            if first == "9":
                return f"bj.{c}"
        return c

    def _normalize_freq(self, freq: str) -> str:
        """输入周期字符串，输出频率字段；用于输出标准化；未知周期回退原值。"""
        p = str(freq).strip().lower()
        return self.FREQ_MAP.get(p, p)

    def _chunk_cache_covers_end_dt(
        self,
        newest_cached_dt: Optional[dt.datetime],
        end_dt: dt.datetime,
        freq: str,
    ) -> bool:
        """
        输入：缓存最新 bar 时间、任务 end_time、频率。
        输出：True 表示缓存已覆盖 end_time 所需数据范围。
        用途：chunk 分页终止；日线/周线/月线按日历日比较，避免 15:00 bar 对 16:00 end 误判 stale。
        分钟/小时线按完整时刻比较：newest_cached_dt >= end_dt。
        边界条件：newest_cached_dt 为 None 时返回 False。
        """
        if newest_cached_dt is None:
            return False
        normalized_freq = self._normalize_freq(freq)
        if normalized_freq in {
            "d",
            "day",
            "daily",
            "w",
            "week",
            "weekly",
            "m",
            "month",
            "monthly",
        }:
            return newest_cached_dt.date() >= end_dt.date()
        return newest_cached_dt >= end_dt

    def _normalize_future_query_code(self, code: Any) -> str:
        """输入期货代码，输出标准化代码；用于容错查询；空代码会抛错。"""
        raw_code = str(code or "").strip().upper()
        if raw_code == "":
            raise ValueError("期货代码不能为空")
        if not any(ch.isdigit() for ch in raw_code):
            raw_code = f"{raw_code}L8"
        return raw_code

    def _normalize_stock_query_code(self, code: Any) -> str:
        """输入股票代码，输出标准化代码；用于容错查询；支持 sh./sz./bj./hk. 前缀。"""
        raw_code = str(code or "").strip()
        if raw_code == "":
            raise ValueError("股票代码不能为空")
        if "." in raw_code:
            raw_code = raw_code.split(".", 1)[1]
        return raw_code

    def _normalize_index_name_key(self, name: Any) -> str:
        """
        输入指数名称，输出用于匹配的标准化键。

        输入：
        1. name: 任意名称输入。
        输出：
        1. 标准化后的名称字符串。
        用途：
        1. 统一指数名称、别名与候选匹配键，支持未来扩展。
        边界条件：
        1. 空值会返回空串。
        """
        text = str(name or "").strip()
        if text == "":
            return ""
        if bool(self.index_kline_lookup_cfg.get("normalize_whitespace", True)):
            text = re.sub(r"\s+", "", text)
        return text

    def _index_route_priority(
        self, source: str, market: int, code: str, market_name: str
    ) -> int:
        """
        计算指数路由优先级（越小越优先）。

        输入：
        1. source/market/code/market_name: 候选路由信息。
        输出：
        1. 整数优先级。
        用途：
        1. 在同名候选中优先挑选“更像指数主序列”的路由。
        边界条件：
        1. 无法识别时返回较大值，作为低优先级候选。
        """
        source_key = str(source or "").strip().lower()
        code_text = str(code or "").strip()
        market_title = str(market_name or "").strip()
        if source_key == "std":
            if market in {0, 1} and (
                code_text.startswith("399") or code_text.startswith("000")
            ):
                return 10
            if market in {0, 1}:
                return 80
            return 30
        if source_key == "ex":
            if market_title in {"中证指数", "国证指数", "全球指数(静态)", "香港指数"}:
                return 40
            if "指数" in market_title:
                return 50
            return 90
        return 100

    def _resolve_index_route_cache_path(self) -> Optional[Path]:
        """
        解析指数路由磁盘缓存路径并校验可写性。

        输入：
        1. 无显式输入参数，内部读取 `index_kline.route_cache.path`。
        输出：
        1. 可写缓存文件路径；无可写位置时返回 None。
        用途：
        1. 兼容 pip install 后站点目录只读场景。
        边界条件：
        1. 配置路径不可写时自动回退默认路径和临时目录。
        """
        configured_path = self.index_kline_route_cache_cfg.get("path")
        return resolve_index_route_cache_file_path(config_path=configured_path)

    def _rebuild_index_name_route_map(
        self, records: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        输入目录记录列表，输出名称键到路由的最优映射。

        输入：
        1. records: 全量指数目录记录。
        输出：
        1. `{normalized_name: route}` 字典。
        用途：
        1. 加速 `resolve_index_name` 命中路径，避免反复扫描全量记录。
        边界条件：
        1. 同名冲突时按 `_index_route_priority` 选择优先级更高的候选。
        """
        route_map: Dict[str, Dict[str, Any]] = {}
        for record in records:
            route_name = str(record.get("name", "")).strip()
            if route_name == "":
                continue
            key = self._normalize_index_name_key(route_name)
            if key == "":
                continue
            route = dict(record)
            previous = route_map.get(key)
            if previous is None:
                route_map[key] = route
                continue
            prev_score = self._index_route_priority(
                source=str(previous.get("source", "")),
                market=int(previous.get("market", -1)),
                code=str(previous.get("code", "")),
                market_name=str(previous.get("market_name", "")),
            )
            cur_score = self._index_route_priority(
                source=str(route.get("source", "")),
                market=int(route.get("market", -1)),
                code=str(route.get("code", "")),
                market_name=str(route.get("market_name", "")),
            )
            if cur_score < prev_score:
                route_map[key] = route
        return route_map

    def _try_load_index_route_cache_from_disk(self) -> None:
        """
        从磁盘加载日级指数路由缓存并回填内存。

        输入：
        1. 无显式输入参数。
        输出：
        1. 无；命中时更新内存快照。
        用途：
        1. 在同日重启场景复用缓存，避免重复全市场目录扫描。
        边界条件：
        1. 缓存缺失、损坏、跨日或配置指纹变化时保持当前内存状态不变。
        """
        if (
            not self._index_route_cache_enabled
        ) or self._index_route_cache_path is None:
            return
        loaded = load_index_route_cache(
            self._index_route_cache_path,
            expected_fingerprint=self._index_kline_fingerprint,
            expected_cache_date=self._index_route_cache_date,
        )
        if loaded is None:
            return
        name_route, catalog, _ = loaded
        self._index_name_route_map = {
            str(k): dict(v) for k, v in dict(name_route).items()
        }
        self._index_catalog_records = [dict(item) for item in list(catalog)]

    def _ensure_index_route_cache_ready(self) -> List[Dict[str, Any]]:
        """
        确保指数路由缓存在运行时可用并返回目录快照。

        输入：
        1. 无显式输入参数。
        输出：
        1. 目录快照列表。
        用途：
        1. 保证 `resolve_index_name` 先读内存，内存缺失时再查磁盘，磁盘无效时立即重建。
        边界条件：
        1. 磁盘缓存不可用或无效时会触发全量重建；重建失败时返回可能为空的快照。
        """
        if self._index_catalog_records and self._index_name_route_map:
            return list(self._index_catalog_records)
        self._try_load_index_route_cache_from_disk()
        if self._index_catalog_records and self._index_name_route_map:
            return list(self._index_catalog_records)
        return self._discover_index_route_records(refresh=True)

    def _persist_index_route_cache_to_disk(self) -> None:
        """
        将当前索引路由内存快照持久化到磁盘。

        输入：
        1. 无显式输入参数。
        输出：
        1. 无。
        用途：
        1. 保存“当天首次重建结果”，供同日后续运行直接复用。
        边界条件：
        1. 写失败时自动降级禁用磁盘缓存，不影响主流程继续执行。
        """
        if (
            not self._index_route_cache_enabled
        ) or self._index_route_cache_path is None:
            return
        if not self._index_catalog_records or not self._index_name_route_map:
            return
        try:
            save_index_route_cache(
                self._index_route_cache_path,
                fingerprint=self._index_kline_fingerprint,
                name_route=self._index_name_route_map,
                catalog=self._index_catalog_records,
                cache_date=self._index_route_cache_date,
            )
        except Exception:
            self._index_route_cache_enabled = False

    def _discover_index_route_records(
        self, refresh: bool = False
    ) -> List[Dict[str, Any]]:
        """
        动态发现指数名称路由并缓存。

        输入：
        1. refresh: 是否强制刷新发现缓存。
        输出：
        1. `[{name, source, market, code, market_name}, ...]` 路由记录列表。
        用途：
        1. 在运行时通过标准/扩展行情清单动态定位指数名称对应路由。
        边界条件：
        1. 网络异常时返回当前已缓存结果（可能为空）。
        2. 扩展侧合约遍历复用 `_fetch_all_instrument_info`，避免与路由发现重复分页。
        """
        # refresh=False 时优先复用进程内快照，避免重复全市场扫描。
        if (not bool(refresh)) and self._index_catalog_records:
            return list(self._index_catalog_records)

        records: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()
        security_page = int(
            self.pagination.get("standard_security_list_page_size", 800)
        )
        index_name_markers = ("指数", "中证", "深证", "上证", "沪深", "创业板", "科创")

        for market in [0, 1]:
            start = 0
            while True:
                page = self.std_pool.call(
                    "get_security_list", market, start, allow_none=True
                )
                if not page:
                    break
                for item in page:
                    name = str(item.get("name", "")).strip()
                    code = str(item.get("code", "")).strip()
                    if name == "" or code == "":
                        continue
                    looks_like_std_index_code = (
                        int(market) == 1 and code.startswith("000")
                    ) or (int(market) == 0 and code.startswith("399"))
                    if (not any(marker in name for marker in index_name_markers)) and (
                        not looks_like_std_index_code
                    ):
                        continue
                    key = f"std|{int(market)}|{code}|{name}"
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    records.append(
                        {
                            "name": name,
                            "source": "std",
                            "market": int(market),
                            "code": code,
                            "market_name": "深圳" if int(market) == 0 else "上海",
                        }
                    )
                start += len(page)
                if len(page) < security_page:
                    break

        ex_index_markets = set(
            self.index_kline_cfg.get("prefer_ex_markets", [62, 102, 37, 27])
        )
        for item in self._fetch_all_instrument_info(refresh=bool(refresh)):
            name = str(item.get("name", "")).strip()
            code = str(item.get("code", "")).strip()
            try:
                market = int(item.get("market", -1))
            except Exception:
                market = -1
            if name == "" or code == "" or market < 0:
                continue
            market_name = str(self._get_ex_market_name(market) or "").strip()
            should_keep = (
                market in ex_index_markets
                or "指数" in market_name
                or any(marker in name for marker in index_name_markers)
            )
            if not should_keep:
                continue
            key = f"ex|{market}|{code}|{name}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            records.append(
                {
                    "name": name,
                    "source": "ex",
                    "market": int(market),
                    "code": code,
                    "market_name": market_name,
                }
            )

        self._index_catalog_records = list(records)
        self._index_name_route_map = self._rebuild_index_name_route_map(
            self._index_catalog_records
        )
        self._persist_index_route_cache_to_disk()
        return list(records)

    def resolve_index_name(
        self, index_name: Any, refresh: bool = False
    ) -> Dict[str, Any]:
        """
        解析指数名称并返回标准路由。

        输入：
        1. index_name: 用户输入指数名称。
        输出：
        1. `{name, source, market, code}` 路由字典。
        用途：
        1. 在指数 K 线查询前完成“精确匹配优先 + 候选提示”校验。
        边界条件：
        1. 未命中时抛 ValueError，错误文本中包含片段候选。
        """
        raw_name = str(index_name or "").strip()
        if raw_name == "":
            raise ValueError("index_name 不能为空")
        normalized_name = self._normalize_index_name_key(raw_name)

        alias_map: Dict[str, str] = {}
        if isinstance(self.index_kline_aliases_cfg, dict):
            for alias_raw, canonical_raw in self.index_kline_aliases_cfg.items():
                alias_key = self._normalize_index_name_key(alias_raw)
                canonical_name = str(canonical_raw or "").strip()
                if alias_key == "" or canonical_name == "":
                    continue
                alias_map[alias_key] = canonical_name

        canonical_input_name = alias_map.get(normalized_name, raw_name)
        canonical_key = self._normalize_index_name_key(canonical_input_name)

        if bool(refresh):
            records = self._discover_index_route_records(refresh=True)
        else:
            self._ensure_index_route_cache_ready()
            cached_route = self._index_name_route_map.get(canonical_key)
            if cached_route is not None:
                return dict(cached_route)
            records = list(self._index_catalog_records)

        if not records:
            raise ValueError("指数路由发现失败：未获取到可用指数清单")
        exact_matches: List[Dict[str, Any]] = []
        for record in records:
            if self._normalize_index_name_key(record["name"]) == canonical_key:
                exact_matches.append(dict(record))
        if exact_matches:
            sorted_matches = sorted(
                exact_matches,
                key=lambda item: self._index_route_priority(
                    source=str(item.get("source", "")),
                    market=int(item.get("market", -1)),
                    code=str(item.get("code", "")),
                    market_name=str(item.get("market_name", "")),
                ),
            )
            picked = dict(sorted_matches[0])
            if canonical_key != "":
                self._index_name_route_map[canonical_key] = dict(picked)
                self._persist_index_route_cache_to_disk()
            return picked

        max_candidates = int(
            self.index_kline_lookup_cfg.get("max_candidates", 10) or 10
        )
        max_candidates = max(1, max_candidates)
        candidates: List[str] = []
        for record in records:
            record_name = str(record["name"])
            record_key = self._normalize_index_name_key(record_name)
            if normalized_name in record_key or record_key in normalized_name:
                candidates.append(record_name)
        candidates = sorted(list(dict.fromkeys(candidates)))[:max_candidates]
        candidate_text = "、".join(candidates) if candidates else "无"
        raise ValueError(f"指数名称未找到: {raw_name}；可选候选: {candidate_text}")

    def _normalize_code_list(self, codes: Optional[Any]) -> Optional[List[str]]:
        """输入代码列表参数，输出去重后的字符串列表；用于统一处理；空输入返回 None。"""
        if codes is None:
            return None
        if isinstance(codes, str):
            items = [codes]
        elif isinstance(codes, (list, tuple, set)):
            items = list(codes)
        else:
            items = [codes]

        result: List[str] = []
        seen = set()
        for item in items:
            raw = str(item or "").strip()
            if raw == "" or raw in seen:
                continue
            seen.add(raw)
            result.append(raw)
        return result

    def _normalize_scope_value(self, raw: Any) -> set[str]:
        """输入范围配置值，输出标准化范围集合；用于全量股票范围控制；非法值会被忽略。"""
        if raw is None:
            return set()

        tokens: List[str] = []
        if isinstance(raw, str):
            tokens.extend(
                [
                    part.strip().lower()
                    for part in re.split(r"[,+\s]+", raw)
                    if part.strip()
                ]
            )
        elif isinstance(raw, (list, tuple, set)):
            for item in raw:
                if isinstance(item, str):
                    tokens.extend(
                        [
                            part.strip().lower()
                            for part in re.split(r"[,+\s]+", item)
                            if part.strip()
                        ]
                    )
                else:
                    value = str(item).strip().lower()
                    if value:
                        tokens.extend(
                            [
                                part.strip().lower()
                                for part in re.split(r"[,+\s]+", value)
                                if part.strip()
                            ]
                        )
        else:
            value = str(raw).strip().lower()
            if value:
                tokens.extend(
                    [
                        part.strip().lower()
                        for part in re.split(r"[,+\s]+", value)
                        if part.strip()
                    ]
                )

        return {token for token in tokens if token in self.STOCK_SCOPE_TOKENS}

    def _get_default_stock_scopes(self, api_name: str) -> set[str]:
        """输入接口名，输出全量股票默认范围；用于 codes=None 默认范围；空配置回退 szsh。"""
        raw = self.stock_scope_defaults.get(str(api_name), ["szsh"])
        scopes = self._normalize_scope_value(raw)
        if not scopes:
            return {"szsh"}
        return scopes

    def _route_scope(self, route: Dict[str, Any]) -> Optional[str]:
        """输入股票路由，输出范围标签；用于按市场过滤；无法识别时返回 None。"""
        if not isinstance(route, dict):
            return None
        source = str(route.get("source", "")).strip().lower()
        try:
            market = int(route.get("market", -1))
        except Exception:
            market = -1

        if source == "std":
            return "szsh"
        if source != "ex":
            return None
        if market == 44:
            return "bj"
        if self._is_hk_market_ex(market):
            return "hk"
        return None

    def _route_in_scopes(self, route: Dict[str, Any], scopes: set[str]) -> bool:
        """输入路由和范围集合，输出是否命中；用于统一过滤；空范围视为不命中。"""
        if not scopes:
            return False
        scope = self._route_scope(route)
        if scope is None:
            return False
        return scope in scopes

    def _get_default_scoped_stock_codes(self, api_name: str) -> List[str]:
        """输入接口名，输出默认范围内股票代码列表；用于全量模式；按代码排序返回。"""
        self.get_all_stock_list(return_df=True)
        scopes = self._get_default_stock_scopes(api_name)
        targets: List[str] = []
        for code in sorted(self._stock_route.keys()):
            route = self._stock_route.get(code)
            if route is None:
                continue
            if self._route_in_scopes(route, scopes):
                targets.append(code)
        return targets

    def _filter_stock_df_by_scopes(
        self, df: pd.DataFrame, scopes: set[str]
    ) -> pd.DataFrame:
        """输入股票清单和范围集合，输出过滤后清单；用于代码名称映射；空范围返回空表。"""
        if df is None or df.empty:
            return pd.DataFrame(
                columns=df.columns if isinstance(df, pd.DataFrame) else []
            )
        if not scopes:
            return df.iloc[0:0].copy().reset_index(drop=True)

        records = df.to_dict(orient="records")
        mask = [
            self._route_in_scopes(
                {
                    "source": rec.get("source", ""),
                    "market": rec.get("market", -1),
                    "code": rec.get("code", ""),
                },
                scopes,
            )
            for rec in records
        ]
        return df.loc[mask].copy().reset_index(drop=True)

    def _safe_float(self, value: Any) -> Optional[float]:
        """输入任意值，输出浮点数或 None；用于报价结果清洗；非法数值返回 None。"""
        try:
            parsed = float(value)
        except Exception:
            return None
        if pd.isna(parsed):
            return None
        return parsed

    def _fetch_all_instrument_info(self, refresh: bool = False) -> List[Dict[str, Any]]:
        """输入是否刷新，输出扩展合约全量；用于路由；None/空分页视为结束。"""
        if self._instrument_cache is not None and not refresh:
            return self._instrument_cache

        page_size = int(self.pagination.get("extended_instrument_info_page_size", 800))
        start = 0
        rows: List[Dict[str, Any]] = []

        while True:
            page = self.ex_pool.call(
                "get_instrument_info", start, page_size, allow_none=True
            )
            if not page:
                break
            rows.extend(page)
            start += len(page)
            if len(page) < page_size:
                break

        self._instrument_cache = rows
        return rows

    def get_all_stock_list(
        self, return_df: Optional[bool] = None, refresh: bool = False
    ):
        """输入返回与刷新开关，输出全股票清单；用于统一上深京；分页 None 作为结束。"""
        if self._stock_df is not None and not refresh:
            if self._default_return_df(return_df):
                return self._stock_df.copy()
            return self._stock_df.to_dict(orient="records")

        security_page = int(
            self.pagination.get("standard_security_list_page_size", 800)
        )
        records = []

        for market in [0, 1]:
            start = 0
            while True:
                page = self.std_pool.call(
                    "get_security_list", market, start, allow_none=True
                )
                if not page:
                    break
                for item in page:
                    code = str(item.get("code", "")).strip()
                    if not code or not self._is_a_share_std(market, code):
                        continue
                    records.append(
                        {
                            "code": code,
                            "name": str(item.get("name", "")).strip(),
                            "market": int(market),
                            "market_name": "深圳" if market == 0 else "上海",
                            "source": "std",
                            "asset_type": "stock",
                        }
                    )
                start += len(page)
                if len(page) < security_page:
                    break

        for item in self._fetch_all_instrument_info(refresh=refresh):
            market = int(item.get("market", -1))
            code = str(item.get("code", "")).strip()
            is_beijing_stock = market == 44 and self._is_beijing_stock_ex(code)
            is_hk_stock = self._is_hk_stock_ex(market, code)
            if not is_beijing_stock and not is_hk_stock:
                continue
            records.append(
                {
                    "code": code,
                    "name": str(item.get("name", "")).strip(),
                    "market": market,
                    "market_name": self._get_ex_market_name(market),
                    "source": "ex",
                    "asset_type": "stock",
                }
            )

        df = pd.DataFrame(records)
        if not df.empty:
            df = (
                df.drop_duplicates(subset=["code"], keep="first")
                .sort_values(by=["source", "market", "code"])
                .reset_index(drop=True)
            )
        self._stock_df = df
        if df.empty:
            self._stock_route = {}
        else:
            stock_records = df.to_dict(orient="records")
            self._stock_route = {
                str(rec["code"]): rec
                for rec in stock_records
                if rec.get("code") is not None
            }

        if self._default_return_df(return_df):
            return df.copy()
        return df.to_dict(orient="records")

    def get_stock_code_name_map(self, use_cache: bool = True) -> Dict[str, str]:
        """输入缓存开关，输出带市场前缀的股票代码名称字典；用于全量代码接口；按配置范围过滤。"""
        stock_df = self.get_all_stock_list(return_df=True, refresh=not bool(use_cache))
        scopes = self._get_default_stock_scopes("get_stock_code_name")
        scoped_df = self._filter_stock_df_by_scopes(stock_df, scopes)
        if scoped_df.empty:
            return {}

        result: Dict[str, str] = {}
        for rec in scoped_df.to_dict(orient="records"):
            code = str(rec.get("code", "")).strip()
            if code == "":
                continue
            source = str(rec.get("source", "")).strip().lower()
            try:
                market = int(rec.get("market", -1))
            except Exception:
                market = -1

            prefixed_code = code
            if source in {"std", "ex"} and market >= 0:
                try:
                    prefixed_code = self._stock_code_with_prefix(
                        source=source, market=market, code=code
                    )
                except Exception:
                    prefixed_code = code
            if prefixed_code == code:
                prefixed_code = self._stock_code_prefix_fallback_by_rule(code)
            if prefixed_code == "":
                continue
            result[prefixed_code] = str(rec.get("name", "")).strip()
        return result

    def get_all_future_list(
        self, return_df: Optional[bool] = None, use_cache: bool = True
    ):
        """输入返回与缓存开关，输出全商品期货；用于统一四大商品所；默认不含中金所。"""
        if self._future_df is not None and use_cache:
            if self._default_return_df(return_df):
                return self._future_df.copy()
            return self._future_df.to_dict(orient="records")

        target_market_names = set(self.market_rules.get("future_market_names", []))
        records = []
        for item in self._fetch_all_instrument_info(refresh=not bool(use_cache)):
            market = int(item.get("market", -1))
            market_name = self._get_ex_market_name(market)
            if market_name not in target_market_names:
                continue
            records.append(
                {
                    "code": str(item.get("code", "")).strip(),
                    "name": str(item.get("name", "")).strip(),
                    "market": market,
                    "market_name": market_name,
                    "source": "ex",
                    "asset_type": "future",
                }
            )

        df = pd.DataFrame(records)
        if not df.empty:
            df = (
                df.drop_duplicates(subset=["code"], keep="first")
                .sort_values(by=["market", "code"])
                .reset_index(drop=True)
            )
        self._future_df = df
        self._future_route = {}
        for row_dict in df.to_dict(orient="records"):
            code_raw = str(row_dict.get("code", "")).strip()
            if not code_raw:
                continue
            self._future_route[code_raw] = row_dict
            self._future_route[code_raw.upper()] = row_dict

        if self._default_return_df(return_df):
            return df.copy()
        return df.to_dict(orient="records")

    def get_stock_latest_price(
        self, codes: Optional[Any] = None
    ) -> Dict[str, Optional[float]]:
        """输入股票代码列表，输出最新价字典；用于实时报价；不传代码时按配置范围拉取全量股票。"""
        self.get_all_stock_list(return_df=True)
        query_codes = self._normalize_code_list(codes)

        if query_codes is None:
            targets = self._get_default_scoped_stock_codes("get_stock_latest_price")
        else:
            targets = []
            missing = []
            for raw_code in query_codes:
                try:
                    code = self._normalize_stock_query_code(raw_code)
                except Exception:
                    continue
                if code in self._stock_route:
                    targets.append(code)
                else:
                    missing.append(code)
            if missing:
                self.get_all_stock_list(return_df=True, refresh=True)
                for code in missing:
                    if code in self._stock_route:
                        targets.append(code)

        targets = list(dict.fromkeys(targets))
        result: Dict[str, Optional[float]] = {code: None for code in targets}
        if not targets:
            return result

        std_targets: List[Tuple[str, int, str]] = []
        ex_targets: List[Tuple[str, int, str]] = []
        for code in targets:
            route = self._stock_route.get(code)
            if route is None:
                self._record_failure(
                    "stock_latest_price", code, "code_not_found", "route_missing"
                )
                continue
            market = int(route.get("market", -1))
            source = str(route.get("source", "")).strip().lower()
            if source == "std":
                std_targets.append((code, market, code))
            elif source == "ex":
                ex_targets.append((code, market, code))
            else:
                self._record_failure(
                    "stock_latest_price", code, "unsupported_source", source
                )

        quote_batch_size = int(self.output_cfg.get("latest_quote_batch_size", 80))
        quote_batch_size = max(1, quote_batch_size)

        for start in range(0, len(std_targets), quote_batch_size):
            chunk = std_targets[start : start + quote_batch_size]
            req = [(int(market), str(code)) for _, market, code in chunk]
            pair_to_key = {(int(market), str(code)): key for key, market, code in chunk}
            resolved = set()
            try:
                rows = self._pool_call_allow_none(
                    self.std_pool,
                    "get_security_quotes",
                    req,
                )
            except Exception as exc:
                for key, _, _ in chunk:
                    self._record_failure(
                        "stock_latest_price", key, "exception", str(exc)
                    )
                rows = []

            for row in rows or []:
                try:
                    market = int(row.get("market", -1))
                except Exception:
                    market = -1
                code = str(row.get("code", "")).strip()
                key = pair_to_key.get((market, code))
                if key is None:
                    continue
                result[key] = self._safe_float(row.get("price"))
                resolved.add(key)

            for key, _, _ in chunk:
                if key not in resolved:
                    result.setdefault(key, None)

        for key, market, code in ex_targets:
            try:
                rows = self._pool_call_allow_none(
                    self.ex_pool,
                    "get_instrument_quote",
                    int(market),
                    str(code),
                )
            except Exception as exc:
                self._record_failure("stock_latest_price", key, "exception", str(exc))
                result[key] = None
                continue
            one = (rows or [None])[0]
            if one is None:
                result[key] = None
                continue
            result[key] = self._safe_float(one.get("price"))

        return result

    def get_future_latest_price(
        self, codes: Optional[Any] = None
    ) -> Dict[str, Optional[float]]:
        """输入期货代码列表，输出最新价字典；用于实时报价；不传代码时拉全量商品期货。"""
        self.get_all_future_list(return_df=True)
        query_codes = self._normalize_code_list(codes)

        if query_codes is None:
            targets = []
            if self._future_df is not None and not self._future_df.empty:
                targets = [
                    str(code).strip()
                    for code in self._future_df["code"].tolist()
                    if str(code).strip()
                ]
        else:
            targets = []
            missing = []
            for raw_code in query_codes:
                try:
                    code = self._normalize_future_query_code(raw_code)
                except Exception:
                    continue
                if code in self._future_route:
                    targets.append(code)
                else:
                    missing.append(code)
            if missing:
                self.get_all_future_list(return_df=True, use_cache=False)
                for code in missing:
                    if code in self._future_route:
                        targets.append(code)

        targets = list(dict.fromkeys(targets))
        result: Dict[str, Optional[float]] = {code: None for code in targets}
        if not targets:
            return result

        for code in targets:
            route = self._future_route.get(code)
            if route is None:
                self._record_failure(
                    "future_latest_price", code, "code_not_found", "route_missing"
                )
                result[code] = None
                continue
            market = int(route.get("market", -1))
            canonical_code = str(route.get("code", code)).strip()
            if canonical_code == "":
                canonical_code = code
            try:
                rows = self._pool_call_allow_none(
                    self.ex_pool,
                    "get_instrument_quote",
                    market,
                    canonical_code,
                )
            except Exception as exc:
                self._record_failure(
                    "future_latest_price", canonical_code, "exception", str(exc)
                )
                result[canonical_code] = None
                if canonical_code != code:
                    result.pop(code, None)
                continue

            one = (rows or [None])[0]
            price = None if one is None else self._safe_float(one.get("price"))
            result[canonical_code] = price
            if canonical_code != code:
                result.pop(code, None)

        return result

    def _kline_dataframe(
        self,
        rows: List[Dict[str, Any]],
        code: str,
        market: int,
        market_name: str,
        source: str,
        freq: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        """输入原始行和过滤条件，输出规范 K 线；用于统一格式；无 datetime 返回空表。"""
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "datetime" not in df.columns:
            return pd.DataFrame()
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df = df.dropna(subset=["datetime"])
        df = df[(df["datetime"] >= start_ts) & (df["datetime"] <= end_ts)]
        df = df.sort_values(by="datetime").drop_duplicates(
            subset=["datetime"], keep="last"
        )
        if df.empty:
            return pd.DataFrame()
        for col in ["open", "close", "high", "low"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = self._filter_placeholder_ohlc_equal_rows(df)
        if df.empty:
            return pd.DataFrame()
        df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
        df["code"] = str(code)
        df["market"] = int(market)
        df["market_name"] = str(market_name)
        df["source"] = str(source)
        df["freq"] = self._normalize_freq(freq)
        return df.reset_index(drop=True)

    def _fetch_std_kline(
        self,
        market: int,
        code: str,
        category: int,
        freq: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        """输入标准股票参数，输出区间 K 线；用于自动分页；None/空页即结束。"""
        page_size = int(self.pagination.get("standard_kline_page_size", 800))

        def _fetch_page(start: int) -> Any:
            return self._pool_call_allow_none(
                self.std_pool,
                "get_security_bars",
                category,
                int(market),
                str(code),
                int(start),
                int(page_size),
            )

        rows = self._paginate_kline_pages(
            page_size=page_size,
            start_boundary=start_ts,
            boundary_mode="pandas",
            fetch_page=_fetch_page,
        )
        market_name = "深圳" if int(market) == 0 else "上海"
        return self._kline_dataframe(
            rows, code, int(market), market_name, "std", freq, start_ts, end_ts
        )

    def _fetch_ex_kline(
        self,
        market: int,
        code: str,
        category: int,
        freq: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        """输入扩展参数，输出区间 K 线；用于自动分页；None/空页即结束。"""
        page_size = int(self.pagination.get("extended_kline_page_size", 700))

        def _fetch_page(start: int) -> Any:
            return self._pool_call_allow_none(
                self.ex_pool,
                "get_instrument_bars",
                category,
                int(market),
                str(code),
                int(start),
                int(page_size),
            )

        rows = self._paginate_kline_pages(
            page_size=page_size,
            start_boundary=start_ts,
            boundary_mode="pandas",
            fetch_page=_fetch_page,
        )
        market_name = self._get_ex_market_name(int(market))
        return self._kline_dataframe(
            rows, code, int(market), market_name, "ex", freq, start_ts, end_ts
        )

    def _normalize_stock_kline_fields(
        self,
        df: pd.DataFrame,
        source: str,
        market: int,
        code: str,
        freq: str,
    ) -> pd.DataFrame:
        """输入股票K线原始DataFrame，输出统一字段；用于输出对齐；缺失字段会补 None。"""
        base_cols = [
            "code",
            "freq",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "amount",
            "datetime",
        ]
        if df is None or df.empty:
            return pd.DataFrame(columns=base_cols)

        result = df.copy()
        for col in ["open", "close", "high", "low", "datetime", "amount"]:
            if col not in result.columns:
                result[col] = None

        if str(source) == "std":
            if "vol" in result.columns:
                result["volume"] = result["vol"]
            elif "trade" in result.columns:
                result["volume"] = result["trade"]
            else:
                result["volume"] = None
        else:
            # 股票 ex 口径下：trade 视作成交量并统一命名为 volume
            if "trade" in result.columns:
                result["volume"] = result["trade"]
            elif "vol" in result.columns:
                result["volume"] = result["vol"]
            else:
                result["volume"] = None

        result["code"] = self._stock_code_with_prefix(
            source=source, market=market, code=code
        )
        result["freq"] = self._normalize_freq(freq)
        result = result[base_cols].copy()
        for col in ["open", "close", "high", "low", "volume", "amount"]:
            result[col] = pd.to_numeric(result[col], errors="coerce")
        result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")
        result = (
            result.dropna(subset=["datetime"])
            .sort_values("datetime")
            .reset_index(drop=True)
        )
        result = self._filter_placeholder_ohlc_equal_rows(result)
        if result.empty:
            return pd.DataFrame(columns=base_cols)
        return result

    def _normalize_future_kline_fields(
        self, df: pd.DataFrame, code: str, freq: str
    ) -> pd.DataFrame:
        """输入期货K线原始DataFrame，输出统一字段；用于输出裁剪；缺失字段会补 None。"""
        base_cols = [
            "code",
            "freq",
            "open",
            "close",
            "high",
            "low",
            "settlement_price",
            "volume",
            "datetime",
        ]
        if df is None or df.empty:
            return pd.DataFrame(columns=base_cols)

        result = df.copy()
        for col in ["open", "close", "high", "low", "price", "trade", "datetime"]:
            if col not in result.columns:
                result[col] = None

        result["settlement_price"] = result["price"]
        result["volume"] = result["trade"]
        result["code"] = str(code).upper()
        result["freq"] = self._normalize_freq(freq)
        result = result[base_cols].copy()

        for col in ["open", "close", "high", "low", "settlement_price", "volume"]:
            result[col] = pd.to_numeric(result[col], errors="coerce")
        result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")
        result = (
            result.dropna(subset=["datetime"])
            .sort_values("datetime")
            .reset_index(drop=True)
        )
        return result

    def _filter_placeholder_ohlc_equal_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """输入K线DataFrame，输出过滤后的DataFrame；用于剔除占位条；空表直接返回。"""
        if df is None or df.empty:
            return df
        if not bool(self.output_cfg.get("filter_suspended_placeholder_bar", True)):
            return df

        required_cols = {"open", "close", "high", "low"}
        if not required_cols.issubset(set(df.columns)):
            return df

        result = df.copy()

        open_v = pd.to_numeric(result["open"], errors="coerce")
        close_v = pd.to_numeric(result["close"], errors="coerce")
        high_v = pd.to_numeric(result["high"], errors="coerce")
        low_v = pd.to_numeric(result["low"], errors="coerce")

        # 规则1：OHLC完全相等。
        equal_ohlc = (
            open_v.notna()
            & close_v.notna()
            & high_v.notna()
            & low_v.notna()
            & open_v.eq(close_v)
            & open_v.eq(high_v)
            & open_v.eq(low_v)
        )

        # 规则2：成交量和成交额为0或极小异常值。
        vol_col = None
        for name in ["volume", "vol", "trade"]:
            if name in result.columns:
                vol_col = name
                break
        if vol_col is None or "amount" not in result.columns:
            return result.reset_index(drop=True)

        eps = float(self.output_cfg.get("suspended_placeholder_eps", 1e-20))
        vol_v = pd.to_numeric(result[vol_col], errors="coerce").abs()
        amount_v = pd.to_numeric(result["amount"], errors="coerce").abs()
        tiny_turnover = vol_v.le(eps) & amount_v.le(eps)

        placeholder_mask = equal_ohlc & tiny_turnover

        if bool(placeholder_mask.any()):
            result = result.loc[~placeholder_mask].copy()
        return result.reset_index(drop=True)

    def _resolve_stock_codes(self, codes: Optional[Any], freq: str) -> List[str]:
        """输入股票代码参数，输出可查询代码列表；用于统一 code 解析；codes=None 时按配置范围取全量。"""
        self.get_all_stock_list(return_df=True)
        query_codes = self._normalize_code_list(codes)
        if query_codes is None:
            return self._get_default_scoped_stock_codes("get_stock_kline")

        targets: List[str] = []
        unresolved: List[str] = []
        for raw_code in query_codes:
            try:
                code = self._normalize_stock_query_code(raw_code)
            except Exception as exc:
                self._record_failure(
                    "stock_kline", str(raw_code), "invalid_code", str(exc), freq
                )
                continue
            if code in self._stock_route:
                targets.append(code)
            else:
                unresolved.append(code)

        if unresolved:
            self.get_all_stock_list(return_df=True, refresh=True)
            for code in unresolved:
                if code in self._stock_route:
                    targets.append(code)
                else:
                    self._record_failure(
                        "stock_kline", code, "code_not_found", "route_missing", freq
                    )
        return list(dict.fromkeys(targets))

    def _resolve_future_codes(self, codes: Optional[Any], freq: str) -> List[str]:
        """输入期货代码参数，输出可查询代码列表；用于统一 code 解析；无效代码会记录失败。"""
        self.get_all_future_list(return_df=True)
        query_codes = self._normalize_code_list(codes)
        if query_codes is None:
            if self._future_df is None or self._future_df.empty:
                return []
            return [
                str(code).strip()
                for code in self._future_df["code"].tolist()
                if str(code).strip()
            ]

        targets: List[str] = []
        unresolved: List[str] = []
        for raw_code in query_codes:
            try:
                code = self._normalize_future_query_code(raw_code)
            except Exception as exc:
                self._record_failure(
                    "future_kline", str(raw_code), "invalid_code", str(exc), freq
                )
                continue
            if code in self._future_route:
                targets.append(code)
            else:
                unresolved.append(code)

        if unresolved:
            self.get_all_future_list(return_df=True, use_cache=False)
            for code in unresolved:
                if code in self._future_route:
                    targets.append(code)
                else:
                    self._record_failure(
                        "future_kline", code, "code_not_found", "route_missing", freq
                    )
        return list(dict.fromkeys(targets))

    def _get_stock_kline_one(
        self,
        code: str,
        freq: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        """输入单只股票参数，输出单只股票K线；用于批量迭代内部复用；代码不存在时抛错。"""
        normalized_code = self._normalize_stock_query_code(code)
        category = self._freq_to_category(freq)
        self.get_all_stock_list(return_df=True)
        route = self._stock_route.get(str(normalized_code))
        if route is None:
            self.get_all_stock_list(return_df=True, refresh=True)
            route = self._stock_route.get(str(normalized_code))
        if route is None:
            raise ValueError(f"股票代码未找到: {code}")
        if route["source"] == "std":
            df = self._fetch_std_kline(
                int(route["market"]),
                str(normalized_code),
                category,
                freq,
                start_ts,
                end_ts,
            )
        else:
            df = self._fetch_ex_kline(
                int(route["market"]),
                str(normalized_code),
                category,
                freq,
                start_ts,
                end_ts,
            )
        return self._normalize_stock_kline_fields(
            df=df,
            source=str(route["source"]),
            market=int(route["market"]),
            code=str(normalized_code),
            freq=str(freq),
        )

    def _get_future_kline_one(
        self,
        code: str,
        freq: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        """输入单只期货参数，输出单只期货K线；用于批量迭代内部复用；代码不存在时抛错。"""
        normalized_code = self._normalize_future_query_code(code)
        category = self._freq_to_category(freq)
        self.get_all_future_list(return_df=True)
        route = self._future_route.get(normalized_code)
        if route is None:
            self.get_all_future_list(return_df=True, use_cache=False)
            route = self._future_route.get(normalized_code)
        if route is None:
            raise ValueError(f"期货代码未找到: {code}（标准化后: {normalized_code}）")
        canonical_code = (
            str(route.get("code", normalized_code)).strip() or normalized_code
        )
        df = self._fetch_ex_kline(
            int(route["market"]), canonical_code, category, freq, start_ts, end_ts
        )
        return self._normalize_future_kline_fields(df, code=canonical_code, freq=freq)

    def get_stock_kline(
        self,
        codes: Optional[Any] = None,
        freq: str = "d",
        start_time: Any = None,
        end_time: Any = None,
        batch_size: Optional[int] = None,
        return_df: Optional[bool] = None,
    ) -> Iterator[Any]:
        """输入股票代码集合与区间，输出分批迭代；用于统一单/多/全量；codes=None 时按配置范围取全量。"""
        if start_time is None or end_time is None:
            raise ValueError("start_time 和 end_time 不能为空")
        freq_str = str(freq or "d").strip().lower()
        self._freq_to_category(freq_str)
        start_ts = self._to_datetime(start_time)
        end_ts = self._to_datetime(end_time)
        if start_ts > end_ts:
            raise ValueError("start_time 不能晚于 end_time")

        targets = self._resolve_stock_codes(codes=codes, freq=freq_str)
        effective_batch = int(
            batch_size or self.output_cfg.get("default_batch_size", 100)
        )
        effective_batch = max(1, effective_batch)
        as_df = self._default_return_df(return_df)

        def _gen():
            """生成器：分批产生K线数据。"""
            batch_frames: List[pd.DataFrame] = []
            for code in targets:
                try:
                    df = self._get_stock_kline_one(
                        code=code, freq=freq_str, start_ts=start_ts, end_ts=end_ts
                    )
                except Exception as exc:
                    self._record_failure(
                        "stock_kline", code, "exception", str(exc), freq_str
                    )
                    continue
                if df.empty:
                    self._record_failure(
                        "stock_kline", code, "no_data", "empty_dataframe", freq_str
                    )
                    continue
                batch_frames.append(df)
                if len(batch_frames) >= effective_batch:
                    out_df = pd.concat(batch_frames, ignore_index=True)
                    batch_frames.clear()
                    yield out_df if as_df else out_df.to_dict(orient="records")
            if batch_frames:
                out_df = pd.concat(batch_frames, ignore_index=True)
                yield out_df if as_df else out_df.to_dict(orient="records")

        return _gen()

    def get_future_kline(
        self,
        codes: Optional[Any] = None,
        freq: str = "d",
        start_time: Any = None,
        end_time: Any = None,
        batch_size: Optional[int] = None,
        return_df: Optional[bool] = None,
    ) -> Iterator[Any]:
        """输入期货代码集合与区间，输出分批迭代；用于统一单/多/全量；无效代码会跳过。"""
        if start_time is None or end_time is None:
            raise ValueError("start_time 和 end_time 不能为空")
        freq_str = str(freq or "d").strip().lower()
        self._freq_to_category(freq_str)
        start_ts, end_ts = self._normalize_future_time_window(start_time, end_time)
        if start_ts > end_ts:
            raise ValueError("start_time 不能晚于 end_time")

        targets = self._resolve_future_codes(codes=codes, freq=freq_str)
        effective_batch = int(
            batch_size or self.output_cfg.get("default_batch_size", 100)
        )
        effective_batch = max(1, effective_batch)
        as_df = self._default_return_df(return_df)

        def _gen():
            """生成器：分批产生期货K线数据。"""
            batch_frames: List[pd.DataFrame] = []
            for code in targets:
                try:
                    df = self._get_future_kline_one(
                        code=code, freq=freq_str, start_ts=start_ts, end_ts=end_ts
                    )
                except Exception as exc:
                    self._record_failure(
                        "future_kline", code, "exception", str(exc), freq_str
                    )
                    continue
                if df.empty:
                    self._record_failure(
                        "future_kline", code, "no_data", "empty_dataframe", freq_str
                    )
                    continue
                batch_frames.append(df)
                if len(batch_frames) >= effective_batch:
                    out_df = pd.concat(batch_frames, ignore_index=True)
                    batch_frames.clear()
                    yield out_df if as_df else out_df.to_dict(orient="records")
            if batch_frames:
                out_df = pd.concat(batch_frames, ignore_index=True)
                yield out_df if as_df else out_df.to_dict(orient="records")

        return _gen()

    def _fetch_company_content(
        self,
        market: int,
        code: str,
        filename: str,
        start: int,
        length: int,
    ) -> Tuple[str, str]:
        """输入公司信息定位参数，输出全文和状态；用于自动分块；None/空片段提前结束。"""
        chunk_size = int(self.pagination.get("company_info_chunk_size", 30000))
        offset = 0
        chunks = []
        status = "success"

        while offset < int(length):
            ask = min(chunk_size, int(length) - offset)
            part = self.std_pool.call(
                "get_company_info_content",
                int(market),
                str(code),
                str(filename),
                int(start) + offset,
                ask,
                allow_none=True,
            )
            if part is None:
                status = "none_terminated"
                break
            if isinstance(part, (bytes, bytearray)):
                part = bytes(part).decode("gbk", "ignore")
            part = str(part)
            if part == "":
                status = "empty_terminated"
                break
            chunks.append(part)
            offset += ask

        return "".join(chunks), status

    def _normalize_category_name(self, name: Any) -> str:
        """输入分类名，输出标准化名称；用于分类匹配；会去除全部空白字符。"""
        return "".join(str(name).split())

    def _normalize_category_filter(self, category: Optional[Any]) -> Optional[set]:
        """输入分类参数，输出标准化分类集合；用于中文分类过滤；空输入返回 None 表示不过滤。"""
        if category is None:
            return None
        if isinstance(category, str):
            items = [category]
        elif isinstance(category, (list, tuple, set)):
            items = list(category)
        else:
            items = [category]

        normalized = {
            self._normalize_category_name(item) for item in items if str(item).strip()
        }
        if not normalized:
            return None
        return normalized

    def get_company_info_content(
        self,
        code: str,
        category: Optional[Any] = None,
        return_df: Optional[bool] = None,
    ):
        """输入股票代码与中文分类列表，输出分类正文；用于单股抓取；返回 code/category/content 三列。"""
        self.get_all_stock_list(return_df=True)
        route = self._stock_route.get(str(code))
        if route is None:
            self.get_all_stock_list(return_df=True, refresh=True)
            route = self._stock_route.get(str(code))
        if route is None:
            raise ValueError(f"股票代码未找到: {code}")

        # 公司信息接口仅标准行情支持；非标准市场直接返回空结果并记录失败。
        if str(route.get("source", "")) != "std":
            self._record_failure(
                "company_info", str(code), "unsupported", "non_std_market"
            )
            empty_df = pd.DataFrame(columns=["code", "category", "content"])
            if self._default_return_df(return_df):
                return empty_df
            return empty_df.to_dict(orient="records")

        market = int(route["market"])
        categories = self.std_pool.call(
            "get_company_info_category", market, str(code), allow_none=True
        )

        if not categories:
            empty_df = pd.DataFrame(columns=["code", "category", "content"])
            if self._default_return_df(return_df):
                return empty_df
            return empty_df.to_dict(orient="records")

        category_filter = self._normalize_category_filter(category)
        missing_category = set(category_filter) if category_filter else set()

        records: List[Dict[str, Any]] = []
        for cat in categories:
            cat_name = str(cat.get("name", "")).strip()
            cat_name_key = self._normalize_category_name(cat_name)
            if category_filter and cat_name_key not in category_filter:
                continue

            if cat_name_key in missing_category:
                missing_category.discard(cat_name_key)

            content, status = self._fetch_company_content(
                market=market,
                code=str(code),
                filename=str(cat.get("filename", "")),
                start=int(cat.get("start", 0)),
                length=int(cat.get("length", 0)),
            )
            if status != "success":
                self._record_failure(
                    "company_info",
                    str(code),
                    status,
                    f"category={cat_name}",
                )
            records.append(
                {"code": str(code), "category": cat_name, "content": content}
            )

        for miss_name in sorted(missing_category):
            self._record_failure(
                "company_info",
                str(code),
                "category_not_found",
                miss_name,
            )

        df = pd.DataFrame(records, columns=["code", "category", "content"])
        if self._default_return_df(return_df):
            return df
        return df.to_dict(orient="records")

    def get_failures_df(self) -> pd.DataFrame:
        """输入无，输出失败明细表；用于报告；无失败时返回空表头。"""
        if not self._runtime_failures:
            return pd.DataFrame(
                columns=["timestamp", "task", "code", "freq", "reason", "detail"]
            )
        return pd.DataFrame(self._runtime_failures)

    def get_runtime_metadata(self) -> Dict[str, Any]:
        """输入无，输出运行元数据；用于报告；未连接时 host 字段为空。"""
        std_stats = self.std_pool.stats
        ex_stats = self.ex_pool.stats
        return {
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "config_path": self.config_path,
            "std_active_host": self.std_pool.get_active_host(),
            "ex_active_host": self.ex_pool.get_active_host(),
            "std_used_hosts": json.dumps(
                self.std_pool.get_used_hosts(), ensure_ascii=False
            ),
            "ex_used_hosts": json.dumps(
                self.ex_pool.get_used_hosts(), ensure_ascii=False
            ),
            "std_pool_stats": json.dumps(std_stats, ensure_ascii=False),
            "ex_pool_stats": json.dumps(ex_stats, ensure_ascii=False),
            "std_same_conn_retries": int(std_stats.get("same_conn_retries", 0)),
            "ex_same_conn_retries": int(ex_stats.get("same_conn_retries", 0)),
            "std_same_host_reconnects": int(std_stats.get("same_host_reconnects", 0)),
            "ex_same_host_reconnects": int(ex_stats.get("same_host_reconnects", 0)),
            "std_rotations": int(std_stats.get("rotations", 0)),
            "ex_rotations": int(ex_stats.get("rotations", 0)),
        }

    def cleanup_dead_thread_connections(self):
        """
        输入无，输出无；用于回收已退出线程遗留连接；重复调用安全。

        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 在并发批次边界主动清理线程生命周期结束后未显式关闭的连接。
        边界条件：
        1. 任一连接池清理异常会向上抛出，由调用方决定是否吞掉异常。
        """
        self.std_pool.cleanup_dead_thread_connections()
        self.ex_pool.cleanup_dead_thread_connections()

    def close(self):
        """输入无，输出无；用于关闭连接池；重复调用安全。"""
        self.std_pool.close()
        self.ex_pool.close()

    def _warmup_connections(self):
        """输入无，输出无；用于进入with时预连接；任一源失败都会抛错。"""
        std_ok = self.std_pool.ensure_connected()
        ex_ok = self.ex_pool.ensure_connected()
        if not std_ok:
            raise RuntimeError("标准行情预连接失败")
        if not ex_ok:
            raise RuntimeError("扩展行情预连接失败")

    @classmethod
    def _push_context_client(cls, client: "UnifiedTdxClient"):
        """输入上下文客户端，输出无；用于维护上下文栈；支持嵌套with。"""
        cls._CONTEXT_STACK.append(client)

    @classmethod
    def _pop_context_client(cls, client: "UnifiedTdxClient"):
        """输入上下文客户端，输出无；用于退出with；异常时尽量清理。"""
        for index in range(len(cls._CONTEXT_STACK) - 1, -1, -1):
            if cls._CONTEXT_STACK[index] is client:
                del cls._CONTEXT_STACK[index]
                return

    @classmethod
    def get_active_context_client(cls) -> Optional["UnifiedTdxClient"]:
        """输入无，输出当前上下文客户端；用于simple_api优先复用with内连接；无上下文返回None。"""
        if not cls._CONTEXT_STACK:
            return None
        return cls._CONTEXT_STACK[-1]

    def __enter__(self):
        """
        输入无，输出上下文客户端；用于 with 管理；退出时仅关闭上下文实例。

        内层客户端复用本实例连接池已确定的地址顺序（含 worker presorted 快照），
        避免 worker 路径再次触发全池 TCP 探测。
        """
        context_client = UnifiedTdxClient(
            config_path=self.config_path,
            presorted_hosts=dict(self._presorted_hosts_snapshot),
            worker_client=self._worker_client_flag,
        )
        if context_client.preconnect_on_enter:
            context_client._warmup_connections()
        self._entered_client = context_client
        self._push_context_client(context_client)
        return context_client

    def __exit__(self, exc_type, exc_val, exc_tb):
        """输入异常上下文，输出False；用于with退出自动close；保持异常透传。"""
        context_client = self._entered_client
        if context_client is None:
            return False
        try:
            context_client.close()
        finally:
            self._pop_context_client(context_client)
            self._entered_client = None
        return False
