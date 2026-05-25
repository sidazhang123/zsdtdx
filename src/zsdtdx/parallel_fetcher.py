"""
并行数据获取模块：使用全局 ProcessPoolExecutor

职责：
1. 提供股票/期货K线数据的并行获取能力。
2. 使用全局 ProcessPoolExecutor 复用工作进程。
3. 兼容旧版 DataFrame 聚合接口与新版 task payload 接口。

边界：
1. 全局进程池在首次并行调用时创建，程序退出时统一关闭。
2. 旧接口返回 DataFrame；任务接口返回 list[dict] 并支持队列实时事件。
"""

import asyncio
import atexit
import copy
import json
import os
import random
import threading
import time
import warnings
from collections import defaultdict
from concurrent.futures import (
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError,
    as_completed,
)
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import yaml
from zsdtdx.unified_client import UnifiedTdxClient

try:
    import psutil
except ImportError:
    psutil = None

warnings.filterwarnings("ignore")


# =============================================================================
# 全局进程池管理
# =============================================================================
_global_process_pool: Optional[ProcessPoolExecutor] = None
_global_pool_lock = threading.Lock()
_global_pool_max_workers: int = 0
_global_pool_epoch: int = 0
_log_callback: Optional[Callable[[str, str, Optional[Dict[str, Any]]], None]] = None
# C5: 日志相关全局状态改为 RCU 风格 —— 读路径完全无锁直接读全局引用；
# 写路径在锁内整体替换不可变快照对象（dict 在 Python 中赋值原子，无需读路径锁）。
# 仅保留 _set_lock 防止两个并发 set_* 调用同时基于旧值衍生更新导致丢更新。
_log_callback_lock = threading.Lock()
_log_detail_options_lock = threading.Lock()
# 当前 detail options 快照（不可变契约：读路径不得修改返回值；写路径替换整个引用）。
_log_detail_options: Dict[str, Any] = {
    "compact_enabled": True,
    "sample_size": 8,
    "include_full_tasks_debug": False,
}
_worker_client_holder: Any = None
_worker_client_context: Any = None
_worker_client_pid: Optional[int] = None
_worker_client_lock = threading.Lock()

_worker_chunk_coroutine_warmed_slots: int = 0
_worker_chunk_coroutine_warmed_lock = threading.Lock()
_active_config_path: Optional[str] = None
_worker_sorted_hosts: Optional[Dict[str, list]] = None
_global_pool_slot_manager: Any = None
_global_pool_slot_counter: Any = None


def _build_worker_host_slot_assignments(
    snapshot: Dict[str, List[Any]],
    num_slots: int,
) -> List[Dict[str, List[Any]]]:
    """
    为每个 worker 槽位生成旋转后的建连地址列表。

    输入：
    1. snapshot: 主进程全量探测结果（standard/extended）。
    2. num_slots: worker 数量。
    输出：
    1. 长度为 num_slots 的列表，每项为 {"standard": [...], "extended": [...]}。
    边界条件：
    1. 每槽位对全量列表随机起始下标后循环旋转，不在 worker 内 shuffle。
    2. 某侧列表为空时该侧返回 []。
    """
    from zsdtdx.unified_client import rotate_hosts_list

    std_full = list(snapshot.get("standard") or [])
    ex_full = list(snapshot.get("extended") or [])
    slots = max(1, int(num_slots))
    assignments: List[Dict[str, List[Any]]] = []
    for _ in range(slots):
        std_start = random.randint(0, len(std_full) - 1) if std_full else 0
        ex_start = random.randint(0, len(ex_full) - 1) if ex_full else 0
        assignments.append(
            {
                "standard": rotate_hosts_list(std_full, std_start) if std_full else [],
                "extended": rotate_hosts_list(ex_full, ex_start) if ex_full else [],
            }
        )
    return assignments


def set_log_callback(
    callback: Optional[Callable[[str, str, Optional[Dict[str, Any]]], None]],
):
    """
    设置并行获取器日志回调（线程安全）。

    输入：
    1. callback: 新回调，签名为 (level, message, detail)；传 None 表示清空。
    输出：
    1. 返回旧回调，便于调用方在退出时恢复。
    """
    global _log_callback
    with _log_callback_lock:
        previous = _log_callback
        _log_callback = callback
        return previous


def _read_log_detail_options() -> Dict[str, Any]:
    """
    读取并行日志细节配置快照（RCU 风格无锁读路径）。

    输入：
    1. 无显式输入参数。
    输出：
    1. 返回当前快照的不可变只读视图（调用方约定：不得修改返回值）。
    用途：
    1. C5 优化：dict 全局引用赋值在 CPython 中原子，无需锁保护即可线程安全读取。
    边界条件：
    1. 调用方仅可读取，不可修改返回值；如需修改请用 set_log_detail_options 整体替换。
    """
    return _log_detail_options


def set_log_detail_options(
    *,
    compact_enabled: Optional[bool] = None,
    sample_size: Optional[int] = None,
    include_full_tasks_debug: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    设置并行日志明细输出策略（C5 RCU 风格：构造新 snapshot 整体替换）。

    输入：
    1. compact_enabled: 是否启用精简日志（仅计数+采样）。
    2. sample_size: 任务采样条数上限。
    3. include_full_tasks_debug: 是否在日志 detail 中包含全量 tasks。
    输出：
    1. 返回更新前配置（副本），用于调用方在退出时恢复。
    用途：
    1. 让维护链路按场景切换"默认精简/调试全量"日志策略。
    边界条件：
    1. sample_size 非法时回退默认值 8，并限制在 [1, 200]。
    2. 写路径串行化：用 _log_detail_options_lock 保证两个并发 set_* 不会因衍生丢更新。
    """
    global _log_detail_options
    with _log_detail_options_lock:
        previous = dict(_log_detail_options)
        # 基于当前快照构造新 dict，避免读路径在中间状态读到部分更新。
        new_snapshot = dict(_log_detail_options)
        if compact_enabled is not None:
            new_snapshot["compact_enabled"] = bool(compact_enabled)
        if sample_size is not None:
            try:
                parsed = int(sample_size)
            except Exception:
                parsed = 8
            new_snapshot["sample_size"] = max(1, min(200, parsed))
        if include_full_tasks_debug is not None:
            new_snapshot["include_full_tasks_debug"] = bool(include_full_tasks_debug)
        # 整体替换全局引用（CPython 赋值原子）。
        _log_detail_options = new_snapshot
        return previous


def _emit_log(
    level: str, message: str, detail: Optional[Dict[str, Any]] = None
) -> None:
    """
    统一输出并行获取日志：控制台 print + 可选回调。

    输入：
    1. level: 日志级别（info/warning/error）。
    2. message: 日志消息。
    3. detail: 可选结构化明细。
    输出：
    1. 无返回值。
    """
    text = str(message)
    if detail:
        try:
            text = f"{text} | {json.dumps(detail, ensure_ascii=False, default=str)}"
        except Exception:
            text = f"{text} | {detail}"
    callback = _log_callback
    if callback is None:
        print(text)
        return
    try:
        callback(str(level or "info"), str(message), detail)
    except Exception as exc:
        print(f"[Parallel Logger Error] {exc}")
        print(text)


def _snapshot_pool_epoch() -> int:
    """
    读取全局进程池版本号快照。

    输入：
    1. 无显式输入参数。
    输出：
    1. 当前进程池版本号（单调递增整数）。
    用途：
    1. 让 async 自动预热逻辑判断“进程池是否已重建，需要重新预热”。
    边界条件：
    1. 仅用于状态比较，不保证跨进程共享。
    """
    with _global_pool_lock:
        return int(_global_pool_epoch)


def _get_global_process_pool(max_workers: int) -> ProcessPoolExecutor:
    """
    获取全局进程池（单例复用）。

    输入：
    1. max_workers: 目标进程数。
    输出：
    1. `ProcessPoolExecutor` 实例。
    用途：
    1. 在多次并行调用间复用 worker 进程，避免重复冷启动。
    边界条件：
    1. 当 `max_workers` 变化时会先关闭旧池再创建新池。
    2. 建池前同步 ensure 全量缓存并分配每 worker 槽位旋转列表。
    """
    global _global_process_pool, _global_pool_max_workers, _global_pool_epoch
    global _global_pool_slot_manager, _global_pool_slot_counter

    with _global_pool_lock:
        if _global_process_pool is None or _global_pool_max_workers != max_workers:
            if _global_process_pool is not None:
                try:
                    _global_process_pool.shutdown(wait=False)
                except Exception as exc:
                    _emit_log("warning", f"[Parallel] 关闭旧进程池失败: {exc}")
                # C4: multiprocessing.Value 直接挂在共享内存上，无 Manager 子进程需要 shutdown。
                _global_pool_slot_manager = None
                _global_pool_slot_counter = None

            import multiprocessing as _mp

            from zsdtdx.unified_client import (
                _ensure_availability_hosts_cache,
                _normalize_hosts_from_cfg,
                compute_hosts_fingerprint,
                get_probe_result_cache,
            )

            try:
                _ensure_availability_hosts_cache(config_path=_active_config_path)
            except Exception as exc:
                _emit_log("error", f"[Parallel] TCP 可用地址缓存 ensure 失败: {exc}")

            snapshot = get_probe_result_cache()
            if not snapshot.get("standard") and not snapshot.get("extended"):
                _emit_log(
                    "warning",
                    "[Parallel] TCP 可用地址缓存为空，worker 建连可能失败",
                )

            fingerprint: Tuple[Any, ...] = ((), ())
            try:
                if _active_config_path:
                    with open(_active_config_path, "r", encoding="utf-8") as fp:
                        cfg_for_fp = yaml.safe_load(fp) or {}
                else:
                    pkg_cfg = Path(__file__).resolve().parent / "config.yaml"
                    with open(pkg_cfg, "r", encoding="utf-8") as fp:
                        cfg_for_fp = yaml.safe_load(fp) or {}
                std_hosts, ex_hosts = _normalize_hosts_from_cfg(cfg_for_fp)
                fingerprint = compute_hosts_fingerprint(std_hosts, ex_hosts)
            except Exception as exc:
                _emit_log("warning", f"[Parallel] 计算 hosts 指纹失败: {exc}")

            assignments = _build_worker_host_slot_assignments(snapshot, max_workers)
            # C4 优化：用 multiprocessing.Value（共享内存原子计数器）替代 Manager.Value。
            # Manager.Value 的实现是通过 Manager 守护子进程的代理，缺少 get_lock() API
            # 导致 _init_worker 内 `slot_counter.get_lock()` 抛 AttributeError 退到 slot=0，
            # 让所有 worker 都拿到同一份 slot_assignments，槽位轮转完全失效。改用
            # multiprocessing.Value('i', 0, lock=True) 后 get_lock() 行为正确、且不再有
            # Manager 守护进程的额外开销。
            _global_pool_slot_manager = None
            _global_pool_slot_counter = _mp.Value("i", 0, lock=True)

            _global_pool_max_workers = max_workers
            _global_process_pool = ProcessPoolExecutor(
                max_workers=max_workers,
                initializer=_init_worker,
                initargs=(
                    _active_config_path,
                    copy.deepcopy(assignments),
                    fingerprint,
                    _global_pool_slot_counter,
                ),
            )
            _global_pool_epoch += 1
            _emit_log("info", f"[Parallel] 创建全局进程池 (workers: {max_workers})")

        return _global_process_pool


def _shutdown_global_pool():
    """
    关闭全局进程池并更新版本号。

    输入：
    1. 无显式输入参数。
    输出：
    1. 无返回值。
    用途：
    1. 在进程退出时统一回收并行资源。
    边界条件：
    1. 未创建进程池时安全返回。
    """
    global _global_process_pool, _global_pool_epoch
    global _global_pool_slot_manager, _global_pool_slot_counter
    if _global_process_pool is not None:
        try:
            _emit_log("info", "[Parallel] 关闭全局进程池")
            _global_process_pool.shutdown(wait=True)
        except Exception:
            pass
        _global_process_pool = None
        _global_pool_epoch += 1
    # multiprocessing.Value 不需要 shutdown；置 None 让 GC 释放共享内存。
    _global_pool_slot_manager = None
    _global_pool_slot_counter = None


def destroy_parallel_fetcher() -> Dict[str, Any]:
    """
    主动销毁并行抓取器的全局进程池（公开入口）。

    输入：
    1. 无显式输入参数。
    输出：
    1. 销毁摘要（是否存在旧池、旧 worker 数、销毁后版本号、耗时）。
    用途：
    1. 在长驻服务优雅停机或短脚本结束前主动释放并行 worker 资源。
    边界条件：
    1. 当进程池尚未创建时安全返回，不抛错。
    """
    started_at = time.monotonic()
    with _global_pool_lock:
        existed = _global_process_pool is not None
        old_workers = int(_global_pool_max_workers)
    _shutdown_global_pool()
    return {
        "existed": bool(existed),
        "old_workers": int(old_workers),
        "pool_epoch": int(_snapshot_pool_epoch()),
        "elapsed_seconds": round(time.monotonic() - started_at, 3),
    }


atexit.register(_shutdown_global_pool)


def _snapshot_pool_processes(pool: ProcessPoolExecutor) -> List[Any]:
    """
    读取进程池内当前 worker 进程对象列表。

    输入：
    1. pool: 目标进程池实例。
    输出：
    1. 进程对象列表（去重）。
    用途：
    1. 为强制重启流程提供 worker 级 terminate/kill 控制入口。
    边界条件：
    1. 进程池内部结构变化或属性缺失时返回空列表。
    """
    try:
        raw = getattr(pool, "_processes", None)
        if isinstance(raw, dict):
            return [proc for proc in raw.values() if proc is not None]
        return []
    except Exception:
        return []


def force_restart_parallel_fetcher(
    *,
    prewarm: bool = True,
    prewarm_timeout_seconds: float = 60.0,
    max_rounds: int = 3,
) -> Dict[str, Any]:
    """
    强制重启并行抓取器：终止旧 worker 并可选立即预热重建。

    输入：
    1. prewarm: 是否在重启后立即执行 worker+连接预热。
    2. prewarm_timeout_seconds: 预热超时时间（秒）。
    3. max_rounds: 预热最大轮次。
    输出：
    1. 重启摘要（旧 pid、终止结果、预热摘要、耗时）。
    用途：
    1. 在维护任务“停止超时”时，快速中断 in-flight 并恢复可复用并行连接能力。
    边界条件：
    1. 即便旧池不存在也返回摘要；prewarm 失败时抛出异常并记录摘要细节。
    """
    global _global_process_pool, _global_pool_max_workers, _global_pool_epoch

    started_at = time.monotonic()
    with _global_pool_lock:
        pool = _global_process_pool
        old_workers = int(_global_pool_max_workers)
        _global_process_pool = None
        _global_pool_max_workers = 0
        _global_pool_epoch += 1

    detail: Dict[str, Any] = {
        "prewarm": bool(prewarm),
        "old_workers": int(old_workers),
        "old_pids": [],
        "terminated_pids": [],
        "killed_pids": [],
        "failed_pids": [],
    }
    if pool is None:
        _emit_log("warning", "[Parallel] 强制重启请求时未发现已创建进程池", detail)
    else:
        processes = _snapshot_pool_processes(pool)
        old_pids = []
        for proc in processes:
            try:
                pid = int(getattr(proc, "pid", 0) or 0)
            except Exception:
                pid = 0
            if pid > 0:
                old_pids.append(pid)
        detail["old_pids"] = sorted(set(old_pids))

        for proc in processes:
            pid = int(getattr(proc, "pid", 0) or 0)
            if pid <= 0:
                continue
            try:
                alive = bool(proc.is_alive())
            except Exception:
                alive = False
            if not alive:
                continue
            try:
                proc.terminate()
                proc.join(timeout=2.0)
                detail["terminated_pids"].append(pid)
            except Exception:
                detail["failed_pids"].append(pid)
                continue
            try:
                still_alive = bool(proc.is_alive())
            except Exception:
                still_alive = False
            if still_alive:
                try:
                    proc.kill()
                    proc.join(timeout=2.0)
                    detail["killed_pids"].append(pid)
                except Exception:
                    detail["failed_pids"].append(pid)
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception as exc:
            detail["shutdown_error"] = str(exc)

    _emit_log("warning", "[Parallel] 已强制终止旧并行进程池", detail)

    prewarm_summary: Dict[str, Any] | None = None
    if prewarm:
        prewarm_summary = prewarm_parallel_fetcher(
            require_all_workers=True,
            timeout_seconds=max(1.0, float(prewarm_timeout_seconds)),
            max_rounds=max(1, int(max_rounds)),
        )
        _emit_log("info", "[Parallel] 强制重启后预热完成", prewarm_summary)

    result: Dict[str, Any] = {
        **detail,
        "prewarm_summary": prewarm_summary,
        "elapsed_seconds": round(time.monotonic() - started_at, 3),
    }
    return result


def _close_worker_client_context() -> None:
    """
    关闭当前 worker 进程中的常驻客户端上下文。

    输入：
    1. 无显式输入参数。
    输出：
    1. 无返回值。
    用途：
    1. 在 worker 进程退出时释放已建立的 TDX 连接。
    边界条件：
    1. 若上下文尚未创建或关闭失败，则静默处理，避免影响退出流程。
    """
    global _worker_client_holder, _worker_client_context, _worker_client_pid
    holder = _worker_client_holder
    _worker_client_holder = None
    _worker_client_context = None
    _worker_client_pid = None
    if holder is None:
        return
    try:
        holder.__exit__(None, None, None)
    except Exception:
        pass


def _cleanup_worker_dead_thread_connections() -> None:
    """
    清理 worker 内已退出线程遗留的连接。

    输入：
    1. 无显式输入参数。
    输出：
    1. 无返回值。
    用途：
    1. 防止进程内短生命周期线程遗留 socket/heartbeat。
    边界条件：
    1. 若上下文未初始化或不支持清理接口则静默跳过。
    """
    context = _worker_client_context
    if context is None:
        return
    cleaner = getattr(context, "cleanup_dead_thread_connections", None)
    if not callable(cleaner):
        return
    try:
        cleaner()
    except Exception:
        pass


def _apply_chunk_socket_read_deadline(client_context: Any, deadline: float) -> None:
    """
    按单次抓取尝试 deadline 设置当前线程 std/ex 池 socket 读超时。

    输入：
    1. client_context: worker 常驻 UnifiedTdxClient 上下文。
    2. deadline: monotonic 截止时刻。
    输出：
    1. 无返回值。
    边界条件：
    1. 剩余时间不足 0.1 秒时按 0.1 秒处理。
    """
    remaining = max(0.1, float(deadline) - time.monotonic())
    for pool_name in ("std_pool", "ex_pool"):
        pool = getattr(client_context, pool_name, None)
        setter = getattr(pool, "set_thread_socket_read_timeout", None)
        if callable(setter):
            try:
                setter(remaining)
            except Exception:
                pass


def _restore_chunk_socket_read_timeout(client_context: Any) -> None:
    """
    D3 优化：chunk 结束时还原 std/ex 池的 socket 读超时为 pool 默认。

    输入：
    1. client_context: worker 常驻 UnifiedTdxClient 上下文。
    输出：1. 无返回值。
    用途：
    1. 避免一次短超时的 chunk 残留影响心跳/后续 chunk 读路径。
    """
    for pool_name in ("std_pool", "ex_pool"):
        pool = getattr(client_context, pool_name, None)
        restorer = getattr(pool, "restore_thread_socket_read_timeout", None)
        if callable(restorer):
            try:
                restorer()
            except Exception:
                pass


def _recover_worker_pools_current_thread(
    reason: str = "", target: str = "both"
) -> Dict[str, Any]:
    """
    在当前 worker 线程按路由选择性重建 std/ex 连接（C2 优化：按 chunk 路由单边重置）。

    输入：
    1. reason: 触发原因（仅用于日志）。
    2. target: 重置目标，"std" 仅重置标准行情池、"ex" 仅重置扩展行情池、"both" 同时重置。
       默认 "both" 保持向后兼容；async chunk 链路按 chunk_payload 推断目标避免多余重连握手。
    输出：
    1. 重建摘要字典，未触发的边对应 detail.ok=False, error="skipped"。
    边界条件：
    1. target 非法值视为 "both"。
    """
    target_norm = str(target or "both").strip().lower()
    if target_norm not in {"std", "ex", "both"}:
        target_norm = "both"

    std_detail: Dict[str, Any]
    if target_norm in {"std", "both"}:
        std_detail = _recover_worker_standard_connection_current_thread(reason=reason)
    else:
        std_detail = {"ok": False, "error": "skipped"}

    ex_detail: Dict[str, Any] = {"ok": False, "error": ""}
    if target_norm in {"ex", "both"}:
        try:
            context_client = _ensure_worker_client_context()
            ex_pool = getattr(context_client, "ex_pool", None)
            if ex_pool is None:
                ex_detail["error"] = "ex_pool_missing"
            else:
                resetter = getattr(ex_pool, "reset_thread_connection", None)
                if callable(resetter):
                    ex_detail["ok"] = bool(resetter(reconnect=True))
                else:
                    ensure_connected = getattr(ex_pool, "ensure_connected", None)
                    ex_detail["ok"] = (
                        bool(ensure_connected())
                        if callable(ensure_connected)
                        else False
                    )
        except Exception as exc:
            ex_detail["error"] = str(exc)[:200]
    else:
        ex_detail["error"] = "skipped"
    return {"std": std_detail, "ex": ex_detail, "target": target_norm}


def _infer_recover_target_from_chunk(prep: Dict[str, Any]) -> str:
    """
    从 chunk prep（_prepare_one_task_chunk 返回值）推断重连目标。

    输入：
    1. prep: 含 task_kind 与 normalized_tasks 的 prep 字典。
    输出：
    1. "std" / "ex" / "both"。
    用途：
    1. C2 优化：stock chunk 仅走 std；index chunk 按 `_index_route_source` 单边选择；
       两侧并发任务（罕见）退化为 "both"。
    边界条件：
    1. prep 字段缺失或 task_kind 非 stock/index 时返回 "both" 兜底。
    """
    task_kind = str(prep.get("task_kind", "stock")).strip().lower()
    if task_kind == "stock":
        return "std"
    if task_kind == "index":
        normalized = prep.get("normalized_tasks") or []
        sources = set()
        for t in normalized:
            if not isinstance(t, dict):
                continue
            src = str(t.get("_index_route_source", "")).strip().lower()
            if src in {"std", "ex"}:
                sources.add(src)
        if len(sources) == 1:
            return sources.pop()
        return "both"
    return "both"


def _probe_worker_std_ex_pool_state() -> Dict[str, Any]:
    """
    探测 worker 内标准/扩展行情池连接状态。

    输入：
    1. 无显式输入参数。
    输出：
    1. 连接状态字典（std/ex 是否可用与当前活跃 host）。
    用途：
    1. 供 chunk 预热线程与 worker 主线程预热复用。
    边界条件：
    1. 建连失败时异常上抛，由调用方统一统计。
    """
    context_client = _ensure_worker_client_context()
    std_pool = getattr(context_client, "std_pool", None)
    ex_pool = getattr(context_client, "ex_pool", None)

    std_ok = bool(std_pool.ensure_connected()) if std_pool is not None else False
    ex_ok = bool(ex_pool.ensure_connected()) if ex_pool is not None else False
    std_host = (
        str(std_pool.get_active_host() or "").strip() if std_pool is not None else ""
    )
    ex_host = (
        str(ex_pool.get_active_host() or "").strip() if ex_pool is not None else ""
    )
    return {
        "std_ok": bool(std_ok),
        "ex_ok": bool(ex_ok),
        "std_host": std_host,
        "ex_host": ex_host,
    }


def _worker_chunk_connection_probe() -> Dict[str, Any]:
    """
    在 to_thread 工作线程内触发连接建立并返回状态。

    输入：
    1. 无显式输入参数。
    输出：
    1. 连接状态字典（std/ex 是否可用与当前活跃 host）。
    用途：
    1. 供协程预热阶段提前建连。
    边界条件：
    1. 建连失败时异常上抛。
    """
    return _probe_worker_std_ex_pool_state()


async def _prewarm_worker_chunk_coroutine_connections_async(
    inproc_workers: int,
) -> Dict[str, Any]:
    """
    预热 worker 进程内 chunk 协程并发槽位连接。

    输入：
    1. inproc_workers: 目标协程并发数。
    输出：
    1. 预热摘要。
    边界条件：
    1. 目标 <=1 时跳过；已满足预热槽位时短路。
    """
    global _worker_chunk_coroutine_warmed_slots
    target_workers = max(1, int(inproc_workers))
    if target_workers <= 1:
        return {
            "inproc_target_workers": int(target_workers),
            "inproc_warmed_workers": 0,
        }

    with _worker_chunk_coroutine_warmed_lock:
        warmed_before = int(_worker_chunk_coroutine_warmed_slots)
    if warmed_before >= target_workers:
        return {
            "inproc_target_workers": int(target_workers),
            "inproc_warmed_workers": int(warmed_before),
        }

    sem = asyncio.Semaphore(target_workers)
    warmed_workers = 0
    warmup_errors: List[str] = []

    async def _one_probe() -> None:
        nonlocal warmed_workers
        async with sem:
            try:
                payload = await asyncio.to_thread(_worker_chunk_connection_probe)
                if bool(payload.get("std_ok")) and bool(payload.get("ex_ok")):
                    warmed_workers += 1
            except Exception as exc:
                warmup_errors.append(str(exc)[:200])

    timeout_seconds = max(1.0, min(float(target_workers) * 1.2, 8.0))
    try:
        await asyncio.wait_for(
            asyncio.gather(*[_one_probe() for _ in range(target_workers)]),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        warmup_errors.append("inproc chunk coroutine warmup timeout")

    with _worker_chunk_coroutine_warmed_lock:
        _worker_chunk_coroutine_warmed_slots = max(
            int(_worker_chunk_coroutine_warmed_slots),
            int(warmed_workers),
        )
        warmed_snapshot = int(_worker_chunk_coroutine_warmed_slots)

    result: Dict[str, Any] = {
        "inproc_target_workers": int(target_workers),
        "inproc_warmed_workers": int(warmed_snapshot),
    }
    if warmup_errors:
        result["inproc_warmup_errors"] = warmup_errors[:5]
    return result


def _prewarm_worker_chunk_coroutine_connections(inproc_workers: int) -> Dict[str, Any]:
    """
    同步入口：预热 worker chunk 协程槽位连接。

    输入：
    1. inproc_workers: 目标协程并发数。
    输出：
    1. 预热摘要。
    边界条件：
    1. 在已有事件循环时复用，否则 asyncio.run。
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            _prewarm_worker_chunk_coroutine_connections_async(inproc_workers)
        )
    raise RuntimeError("无法在运行中的事件循环内同步预热 chunk 协程连接")


def _ensure_worker_client_context():
    """
    获取 worker 进程级常驻客户端上下文，不存在时创建并缓存。

    输入：
    1. 无显式输入参数。
    输出：
    1. 返回可直接调用 `get_stock_kline/get_future_kline` 的客户端上下文对象。
    用途：
    1. 避免每个批次任务重复建连，复用同一 worker 内的连接。
    边界条件：
    1. 若检测到 pid 变化，会先清理旧上下文再重建。
    2. 建连异常向上抛出，由调用方统一处理失败分支。
    """
    global _worker_client_holder, _worker_client_context, _worker_client_pid
    current_pid = os.getpid()
    if (
        _worker_client_holder is not None
        and _worker_client_context is not None
        and _worker_client_pid == current_pid
    ):
        return _worker_client_context

    with _worker_client_lock:
        if (
            _worker_client_holder is not None
            and _worker_client_context is not None
            and _worker_client_pid == current_pid
        ):
            return _worker_client_context

        _close_worker_client_context()
        holder = UnifiedTdxClient(
            config_path=_active_config_path,
            presorted_hosts=_worker_sorted_hosts,
            worker_client=True,
        )
        context = holder.__enter__()
        _worker_client_holder = holder
        _worker_client_context = context
        _worker_client_pid = current_pid
        return _worker_client_context


_ENSURE_WORKER_CTX_BUILTIN = _ensure_worker_client_context


def _restore_ensure_worker_client_context_binding() -> None:
    """
    将 `_ensure_worker_client_context` 恢复为模块内置实现。

    输入：无。
    输出：无。
    用途：
    1. 测试 patch 后恢复，避免 MagicMock 残留导致后续用例走错路径。
    边界条件：
    1. 可重复调用；恢复后不影响已缓存的 worker 上下文对象。
    """
    global _ensure_worker_client_context
    _ensure_worker_client_context = _ENSURE_WORKER_CTX_BUILTIN


def _is_connection_unavailable_error(error_text: str) -> bool:
    """
    判断错误是否属于“连接不可用”类别。

    输入：
    1. error_text: 原始错误文本。
    输出：
    1. bool，True 表示命中连接不可用特征。
    用途：
    1. 为 chunk 级“重建连接并重试”提供触发条件。
    边界条件：
    1. 空字符串返回 False。
    """
    plain = str(error_text or "").strip()
    if plain == "":
        return False
    keywords = [
        "无可用连接",
        "连接不可用",
        "connection unavailable",
        "connection timeout",
        "connection reset",
        "broken pipe",
        "not connected",
    ]
    plain_lower = plain.lower()
    for item in keywords:
        token = str(item).strip()
        if token == "":
            continue
        if token in plain:
            return True
        if token.lower() in plain_lower:
            return True
    return False


def _recover_worker_standard_connection_current_thread(
    reason: str = "",
) -> Dict[str, Any]:
    """
    在当前 worker 线程尝试重建标准行情连接。

    输入：
    1. reason: 触发重建的错误原因（用于日志诊断）。
    输出：
    1. 重建摘要字典，包含是否成功与重建前后活跃 host。
    用途：
    1. 当 chunk 命中“standard 无可用连接”时，进行线程级连接自愈。
    边界条件：
    1. 若 std_pool 不支持线程级重置接口，回退为 ensure_connected 探测。
    """
    try:
        context_client = _ensure_worker_client_context()
        pool = getattr(context_client, "std_pool", None)
        if pool is None:
            return {
                "ok": False,
                "reason": str(reason),
                "error": "std_pool_missing",
                "active_host_before": "",
                "active_host_after": "",
            }

        get_active_host = getattr(pool, "get_active_host", None)
        active_before = (
            str(get_active_host() or "").strip() if callable(get_active_host) else ""
        )

        resetter = getattr(pool, "reset_thread_connection", None)
        if callable(resetter):
            ok = bool(resetter(reconnect=True))
        else:
            ensure_connected = getattr(pool, "ensure_connected", None)
            ok = bool(ensure_connected()) if callable(ensure_connected) else False

        active_after = (
            str(get_active_host() or "").strip() if callable(get_active_host) else ""
        )
        return {
            "ok": bool(ok),
            "reason": str(reason),
            "active_host_before": active_before,
            "active_host_after": active_after,
        }
    except Exception as exc:
        return {
            "ok": False,
            "reason": str(reason),
            "error": str(exc),
            "active_host_before": "",
            "active_host_after": "",
        }


def _summarize_worker_std_host_distribution(
    worker_std_host_map: Dict[int, str],
) -> Dict[str, int]:
    """
    汇总 worker 到标准行情 host 的分布统计。

    输入：
    1. worker_std_host_map: worker_pid -> active_std_host 映射。
    输出：
    1. host -> worker_count 统计字典。
    用途：
    1. 在 prewarm 摘要中观察 worker 连接是否明显集中在少数 IP。
    边界条件：
    1. 空映射返回空字典；空 host 会被自动忽略。
    """
    counter: Dict[str, int] = {}
    for raw_host in list(worker_std_host_map.values()):
        host = str(raw_host or "").strip()
        if host == "":
            continue
        counter[host] = int(counter.get(host, 0)) + 1
    return {host: int(counter[host]) for host in sorted(counter.keys())}


def _worker_warmup_probe(
    hold_seconds: float = 0.15,
    inproc_workers: int = 1,
) -> Dict[str, Any]:
    """
    worker 预热探针：确保常驻连接已建立并返回当前进程 pid。

    输入：
    1. hold_seconds: 探针持有时长（秒），用于提高任务分发到不同 worker 的概率。
    2. inproc_workers: worker 进程内 chunk 协程并发数（用于预热 to_thread 槽位连接）。
    输出：
    1. 预热结果字典，包含 `pid` 与标准行情 host 状态。
    用途：
    1. 在启动阶段触发每个 worker 进程主线程的标准/扩展池建连。
    2. 当 inproc_workers>1 时，同时预热 chunk 协程槽位连接，确保后续 chunk 复用。
    边界条件：
    1. 建连失败时异常上抛，由主进程聚合并判定启动是否失败。
    """
    pool_state = _probe_worker_std_ex_pool_state()
    active_std = str(pool_state.get("std_host", "")).strip()
    host_state = {
        "active_std_host_before": active_std,
        "active_std_host": active_std,
        "target_std_host": "",
        "std_host_switched": False,
        "std_host_matched": True,
        "std_ok": bool(pool_state.get("std_ok")),
        "ex_ok": bool(pool_state.get("ex_ok")),
    }
    inproc_summary = _prewarm_worker_chunk_coroutine_connections(inproc_workers)
    hold = max(0.0, min(float(hold_seconds), 1.0))
    if hold > 0:
        time.sleep(hold)
    return {"pid": int(os.getpid()), **host_state, **inproc_summary}


def prewarm_parallel_fetcher(
    *,
    require_all_workers: bool = True,
    timeout_seconds: float = 60.0,
    max_rounds: int = 3,
    target_workers: Optional[int] = None,
) -> Dict[str, Any]:
    """
    预热并行抓取器：强制拉起进程池并建立 worker 常驻连接。

    输入：
    1. require_all_workers: 是否要求全部 worker 均完成预热。
    2. timeout_seconds: 预热总超时时间（秒）。
    3. max_rounds: 预热轮次上限。
    4. target_workers: 可选目标进程数；不传时按当前 fetcher 的 num_processes。
    输出：
    1. 预热统计摘要（目标进程数、已预热进程数、pid 列表、耗时等）。
    用途：
    1. 在服务启动阶段提前完成“进程 + 连接”初始化，缩短首批抓取冷启动。
    边界条件：
    1. 当 `require_all_workers=True` 且预热不足时抛出 RuntimeError。
    2. 当探针执行超时或 worker 建连失败时，错误会进入摘要并参与失败判定。
    """
    target_workers = max(
        1,
        int(target_workers)
        if target_workers is not None
        else int(get_fetcher().num_processes),
    )
    inproc_workers = max(1, int(get_fetcher().task_chunk_inproc_coroutine_workers))
    timeout_budget = max(1.0, float(timeout_seconds))
    rounds_limit = max(1, int(max_rounds))

    started_at = time.monotonic()
    warmed_pids: set[int] = set()
    worker_std_host_map: Dict[int, str] = {}
    round_errors: list[str] = []
    rounds_used = 0

    _emit_log(
        "info",
        "[Parallel] 启动进程池预热",
        {
            "target_workers": int(target_workers),
            "inproc_workers": int(inproc_workers),
            "require_all_workers": bool(require_all_workers),
            "timeout_seconds": float(timeout_budget),
            "max_rounds": int(rounds_limit),
        },
    )
    executor = _get_global_process_pool(target_workers)

    for round_idx in range(1, rounds_limit + 1):
        rounds_used = round_idx
        elapsed = time.monotonic() - started_at
        remaining = timeout_budget - elapsed
        if remaining <= 0:
            round_errors.append("warmup timeout before round dispatch")
            break

        probe_count = max(target_workers * 2, target_workers)
        futures = [
            executor.submit(_worker_warmup_probe, 0.15, inproc_workers)
            for _ in range(probe_count)
        ]
        try:
            for future in as_completed(futures, timeout=remaining):
                try:
                    payload = future.result()
                    pid = int(payload.get("pid", 0))
                    if pid > 0:
                        warmed_pids.add(pid)
                        active_std_host = str(
                            payload.get("active_std_host", "")
                        ).strip()
                        if active_std_host:
                            worker_std_host_map[pid] = active_std_host
                except Exception as exc:
                    round_errors.append(str(exc))
                # C3: 一旦达到目标 pid 覆盖即提前 cancel 剩余探针，避免本轮内空等。
                if len(warmed_pids) >= target_workers:
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break
        except TimeoutError:
            round_errors.append("warmup probe round timeout")
        finally:
            for future in futures:
                if not future.done():
                    future.cancel()

        _emit_log(
            "info",
            "[Parallel] 预热轮次完成",
            {
                "round": int(round_idx),
                "target_workers": int(target_workers),
                "warmed_workers": int(len(warmed_pids)),
                "warmed_pids": sorted(warmed_pids),
                "std_host_distribution": _summarize_worker_std_host_distribution(
                    worker_std_host_map
                ),
            },
        )
        if len(warmed_pids) >= target_workers:
            break

    elapsed_total = round(time.monotonic() - started_at, 3)
    summary: Dict[str, Any] = {
        "target_workers": int(target_workers),
        "warmed_workers": int(len(warmed_pids)),
        "warmed_pids": sorted(warmed_pids),
        "elapsed_seconds": float(elapsed_total),
        "rounds_used": int(rounds_used),
        "require_all_workers": bool(require_all_workers),
        "std_worker_host_distribution": _summarize_worker_std_host_distribution(
            worker_std_host_map
        ),
    }
    if round_errors:
        summary["errors"] = round_errors[:10]

    if require_all_workers and len(warmed_pids) < target_workers:
        message = (
            f"[Parallel] 预热失败：目标进程 {target_workers}，已预热 {len(warmed_pids)}，"
            f"timeout={timeout_budget}s rounds={rounds_limit}"
        )
        _emit_log("error", message, summary)
        raise RuntimeError(message)

    _emit_log("info", "[Parallel] 预热完成", summary)
    return summary


# =============================================================================
# 业务逻辑
# =============================================================================

FUTURE_PATTERNS = {
    "CU",
    "AL",
    "ZN",
    "PB",
    "NI",
    "SN",
    "AU",
    "AG",
    "RB",
    "HC",
    "FU",
    "BU",
    "RU",
    "WR",
    "SS",
    "SP",
    "C",
    "CS",
    "A",
    "B",
    "M",
    "Y",
    "P",
    "FB",
    "BB",
    "JD",
    "L",
    "V",
    "PP",
    "J",
    "JM",
    "I",
    "EG",
    "EB",
    "PG",
    "LH",
    "RR",
    "SR",
    "CF",
    "RI",
    "OI",
    "WH",
    "PM",
    "FG",
    "RS",
    "RM",
    "JR",
    "LR",
    "SF",
    "SM",
    "TA",
    "MA",
    "ZC",
    "CY",
    "AP",
    "CJ",
    "UR",
    "SA",
    "PF",
    "PK",
    "SC",
    "NR",
    "LU",
    "BC",
    "EC",
    "IF",
    "IC",
    "IH",
    "TF",
    "T",
    "TS",
    "IM",
}


def _init_worker(
    config_path: Optional[str] = None,
    slot_assignments: Optional[List[Dict[str, list]]] = None,
    hosts_fingerprint: Optional[Tuple[Any, ...]] = None,
    slot_counter: Any = None,
):
    """
    worker 进程初始化钩子。

    输入：
    1. config_path: 主进程传入的活动配置路径；Windows spawn 模式下必须通过此参数传递。
    2. slot_assignments: 每槽位旋转后的 standard/extended 列表。
    3. hosts_fingerprint: 与主进程一致的 hosts 指纹。
    4. slot_counter: Manager.Value，用于认领唯一槽位下标。
    输出：无。
    用途：
    1. 认领槽位、种子化进程内探测缓存，禁止 worker 内再次 TCP 探测。
    边界条件：
    1. atexit 注册失败时降级忽略；槽位越界时回退 slot % len(assignments)。
    """
    global _active_config_path, _worker_sorted_hosts

    from zsdtdx.unified_client import _seed_probe_result_cache_from_snapshot

    if config_path is not None:
        _active_config_path = config_path

    slot_hosts: Optional[Dict[str, list]] = None
    if (
        slot_assignments
        and isinstance(slot_assignments, list)
        and slot_counter is not None
    ):
        try:
            with slot_counter.get_lock():
                slot = int(slot_counter.value)
                slot_counter.value = slot + 1
        except Exception:
            slot = 0
        if slot >= len(slot_assignments):
            slot = slot % max(1, len(slot_assignments))
        slot_hosts = copy.deepcopy(slot_assignments[slot])

    if slot_hosts:
        _worker_sorted_hosts = slot_hosts
        if hosts_fingerprint is not None:
            _seed_probe_result_cache_from_snapshot(slot_hosts, hosts_fingerprint)

    try:
        atexit.register(_close_worker_client_context)
    except Exception:
        pass


def is_future_code(code: str) -> bool:
    """
    判断代码是否属于期货品种集合。

    输入：
    1. code: 任意代码字符串。
    输出：
    1. bool，True 表示识别为期货代码。
    用途：
    1. 在旧版 DataFrame 接口中做股票/期货分流。
    边界条件：
    1. 空值返回 False；会自动过滤数字，仅按字母前缀判断。
    """
    if not code:
        return False
    base_code = "".join(c for c in code.upper() if c.isalpha())
    return base_code in FUTURE_PATTERNS


def get_optimal_process_count(core_multiplier: float = 1.5) -> int:
    """
    计算并发抓取推荐进程数。

    输入：
    1. core_multiplier: 物理核心数倍率（默认 1.5）。
    输出：
    1. 推荐进程数（`max(2, int(物理核心数 * core_multiplier))`）。
    用途：
    1. 为 I/O 密集型行情抓取提供可配置的默认并发基线。
    边界条件：
    1. 无法获取物理核心数时会回退到 `os.cpu_count()`。
    2. core_multiplier 非法或 <=0 时回退为 1.5。
    """
    try:
        multiplier = float(core_multiplier)
    except Exception:
        multiplier = 1.5
    if multiplier <= 0:
        multiplier = 1.5

    if psutil is not None:
        physical_cores = psutil.cpu_count(logical=False)
        if physical_cores is None or physical_cores < 1:
            physical_cores = os.cpu_count() or 4
    else:
        # 没有 psutil 时，使用 os.cpu_count()
        physical_cores = os.cpu_count() or 4

    optimal = int(physical_cores * multiplier)
    return max(2, optimal)  # 至少2个进程


@dataclass
class TaskChunk:
    """
    任务分片定义：同一 code+freq 的有序任务集合。

    输入：
    1. chunk_id: 分片唯一标识。
    2. code: 股票代码。
    3. freq: 周期。
    4. tasks: 分片内任务列表（已按时间升序）。
    输出：
    1. TaskChunk 数据对象。
    用途：
    1. 作为父进程调度和 worker 执行的最小分片单元。
    边界条件：
    1. tasks 允许为空，但正常调度路径不会生成空分片。
    """

    chunk_id: str
    code: str
    freq: str
    tasks: List[Dict[str, str]]

    @property
    def task_count(self) -> int:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 分片任务数。
        用途：
        1. 供调度时进行负载估算。
        边界条件：
        1. tasks 为空时返回 0。
        """
        return int(len(self.tasks))

    def to_payload(self, *, enable_cache: bool) -> Dict[str, Any]:
        """
        输入：
        1. enable_cache: 是否启用该分片的轻量缓存。
        输出：
        1. 进程间可序列化字典。
        用途：
        1. 用于 ProcessPool 的参数传输。
        边界条件：
        1. tasks 会被复制，避免调用方原地修改导致竞态。
        """
        return {
            "chunk_id": str(self.chunk_id),
            "code": str(self.code),
            "freq": str(self.freq),
            "task_count": int(self.task_count),
            "enable_cache": bool(enable_cache),
            "tasks": [dict(item) for item in self.tasks],
        }


@dataclass
class ChunkBundle:
    """
    分片批次定义：提交到单个 worker future 的 chunk 集合。

    输入：
    1. bundle_id: 批次唯一标识。
    2. chunks: 批次内分片列表。
    输出：
    1. ChunkBundle 数据对象。
    用途：
    1. 控制“父进程并发”和“worker 进程内并发”两层调度。
    边界条件：
    1. chunks 允许为空，但正常调度路径不会提交空批次。
    """

    bundle_id: int
    chunks: List[TaskChunk]

    @property
    def task_count(self) -> int:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 批次总任务数。
        用途：
        1. 用于滑动窗口调度与日志统计。
        边界条件：
        1. chunks 为空时返回 0。
        """
        return int(sum(item.task_count for item in self.chunks))

    def to_payload(self, *, cache_min_tasks: int) -> Dict[str, Any]:
        """
        输入：
        1. cache_min_tasks: 启用分片缓存的最小任务阈值。
        输出：
        1. 进程间可序列化字典。
        用途：
        1. 统一将批次参数下发给 worker。
        边界条件：
        1. cache_min_tasks 会在调用方保证 >=1。
        """
        chunk_payloads: List[Dict[str, Any]] = []
        for chunk in self.chunks:
            enable_cache = bool(chunk.task_count >= int(cache_min_tasks))
            chunk_payloads.append(chunk.to_payload(enable_cache=enable_cache))
        return {
            "bundle_id": int(self.bundle_id),
            "bundle_task_count": int(self.task_count),
            "bundle_chunk_count": int(len(self.chunks)),
            "cache_min_tasks": int(cache_min_tasks),
            "chunks": chunk_payloads,
        }


def _normalize_task_payload(task: Dict[str, Any]) -> Dict[str, str]:
    """
    标准化 task 字段，确保进程间传输结构稳定。

    输入：
    1. task: 任务字典，需包含 code/freq/start_time/end_time。
    输出：
    1. 标准化后的任务字典（全部字符串字段）。
    用途：
    1. 统一 sync/async、串行/并行路径的任务结构。
    边界条件：
    1. 缺失必填字段时抛 ValueError。
    """
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
    return {
        "code": code,
        "freq": freq,
        "start_time": start_time,
        "end_time": end_time,
    }


def _normalize_index_task_payload(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    标准化指数 task 字段，确保进程间传输结构稳定。

    输入：
    1. task: 任务字典，需包含 index_name/freq/start_time/end_time。
    输出：
    1. 标准化后的指数任务字典（全部字符串字段）。
    用途：
    1. 统一指数 sync/async、串行/并行路径的任务结构。
    边界条件：
    1. 缺失必填字段时抛 ValueError。
    """
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
    normalized: Dict[str, Any] = {
        "index_name": index_name,
        "freq": freq,
        "start_time": start_time,
        "end_time": end_time,
    }
    route_source = str(task.get("_index_route_source", "")).strip().lower()
    route_code = str(task.get("_index_route_code", "")).strip()
    route_name = str(task.get("_index_route_name", "")).strip()
    try:
        route_market = int(task.get("_index_route_market", -1))
    except Exception:
        route_market = -1
    if route_source in {"std", "ex"} and route_code != "" and route_market >= 0:
        normalized["_index_route_source"] = route_source
        normalized["_index_route_market"] = int(route_market)
        normalized["_index_route_code"] = route_code
        if route_name != "":
            normalized["_index_route_name"] = route_name
    return normalized


def _to_sortable_task_ts(raw_value: Any) -> pd.Timestamp:
    """
    输入：
    1. raw_value: 任务时间字段原始值。
    输出：
    1. 可排序时间戳；非法值返回最大时间戳。
    用途：
    1. 为 chunk 内任务排序提供稳定时间键。
    边界条件：
    1. 非法时间不抛错，回退到最大时间以放到排序尾部。
    """
    text = str(raw_value or "").strip()
    if text == "":
        return pd.Timestamp.max
    try:
        parsed = pd.Timestamp(text)
    except Exception:
        return pd.Timestamp.max
    if pd.isna(parsed):
        return pd.Timestamp.max
    return parsed


def _build_task_payload(
    *,
    task: Dict[str, Any],
    rows: Optional[List[Dict[str, Any]]] = None,
    error: Optional[str] = None,
    worker_pid: Optional[int] = None,
) -> Dict[str, Any]:
    """
    构建标准 task payload。

    输入：
    1. task: 标准化任务字典。
    2. rows: 任务K线结果列表。
    3. error: 失败原因，成功时为 None。
    4. worker_pid: 处理该任务的进程 ID。
    输出：
    1. 标准化事件 payload，结构固定。
    用途：
    1. 统一队列输出与函数返回的数据契约。
    边界条件：
    1. rows 为空时输出空列表，不返回 None。
    """
    payload = {
        "event": "data",
        "task": {
            "code": str(task.get("code", "")),
            "freq": str(task.get("freq", "")),
            "start_time": str(task.get("start_time", "")),
            "end_time": str(task.get("end_time", "")),
        },
        "rows": list(rows or []),
        "error": None if error is None else str(error),
        "worker_pid": int(worker_pid or 0),
    }
    return payload


def _build_task_payload_for_kind(
    task_kind: str,
    *,
    task: Dict[str, Any],
    rows: Optional[List[Dict[str, Any]]] = None,
    error: Optional[str] = None,
    worker_pid: Optional[int] = None,
) -> Dict[str, Any]:
    """
    按 task_kind 构建标准或指数 task payload。

    输入：
    1. task_kind: `"stock"` 或 `"index"`。
    2. task/rows/error/worker_pid: 与 `_build_task_payload` 相同。
    输出：
    1. 对应种类的事件 payload。
    用途：
    1. 并行回收层统一出口，避免指数任务误用股票 payload 形状。
    边界条件：
    1. 未知 kind 时回退为股票 payload。
    """
    kind = str(task_kind or "stock").strip().lower()
    if kind == "index":
        return _build_index_task_payload(
            task=task,
            rows=rows,
            error=error,
            worker_pid=worker_pid,
        )
    return _build_task_payload(
        task=task,
        rows=rows,
        error=error,
        worker_pid=worker_pid,
    )


def _build_index_task_payload(
    *,
    task: Dict[str, Any],
    rows: Optional[List[Dict[str, Any]]] = None,
    error: Optional[str] = None,
    worker_pid: Optional[int] = None,
) -> Dict[str, Any]:
    """
    构建标准指数 task payload。

    输入：
    1. task: 标准化指数任务字典。
    2. rows: 任务 K 线结果列表。
    3. error: 失败原因，成功时为 None。
    4. worker_pid: 处理该任务的进程 ID。
    输出：
    1. 标准化指数事件 payload，结构固定。
    用途：
    1. 统一指数队列输出与函数返回的数据契约。
    边界条件：
    1. rows 为空时输出空列表，不返回 None。
    """
    return {
        "event": "data",
        "task": {
            "index_name": str(task.get("index_name", "")),
            "freq": str(task.get("freq", "")),
            "start_time": str(task.get("start_time", "")),
            "end_time": str(task.get("end_time", "")),
        },
        "rows": list(rows or []),
        "error": None if error is None else str(error),
        "worker_pid": int(worker_pid or 0),
    }


def _resolve_chunk_task_kind(chunk_payload: Dict[str, Any]) -> str:
    """输入 chunk_payload，输出规范化 task_kind（stock/index）。"""
    return str(chunk_payload.get("task_kind", "stock")).strip().lower()


def _chunk_task_kind_profile(task_kind: str) -> Dict[str, Any]:
    """
    输入 task_kind，输出该链路差异配置（归一化、payload、主键字段、日志标签）。

    边界：未知 kind 回退 stock。
    """
    kind = str(task_kind or "stock").strip().lower()
    if kind == "index":
        return {
            "kind": "index",
            "symbol_field": "index_name",
            "normalize_item": _normalize_index_task_payload,
            "build_payload": _build_index_task_payload,
            "log_tag": "index chunk",
        }
    return {
        "kind": "stock",
        "symbol_field": "code",
        "normalize_item": _normalize_task_payload,
        "build_payload": _build_task_payload,
        "log_tag": "chunk",
    }


def _prepare_one_task_chunk(chunk_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    归一化 chunk 任务；失败任务立即生成 error payload（不计入 chunk 墙钟预算）。

    输入：
    1. chunk_payload: 分片字典。
    输出：
    1. 含 early_report 或 timed 阶段所需字段的准备结果字典。
    边界条件：
    1. 无有效任务时直接返回 early_report。
    """
    task_kind = _resolve_chunk_task_kind(chunk_payload)
    profile = _chunk_task_kind_profile(task_kind)
    symbol_field = str(profile["symbol_field"])
    normalize_item = profile["normalize_item"]
    build_payload = profile["build_payload"]

    worker_pid = int(os.getpid())
    chunk_id = str(chunk_payload.get("chunk_id", ""))
    raw_tasks = list(chunk_payload.get("tasks") or [])
    enable_cache = bool(chunk_payload.get("enable_cache", False))

    task_detail: List[Dict[str, Any]] = []
    normalized_tasks: List[Dict[str, Any]] = []
    payloads: List[Dict[str, Any]] = []
    failures: List[Tuple[str, str, str]] = []

    for item in raw_tasks:
        try:
            normalized = dict(item) if isinstance(item, dict) else normalize_item(item)
            normalized_tasks.append(normalized)
            task_detail.append(dict(normalized))
        except Exception as exc:
            raw = item or {}
            fallback_task = {
                symbol_field: str(raw.get(symbol_field, "")),
                "freq": str(raw.get("freq", "")),
                "start_time": str(raw.get("start_time", "")),
                "end_time": str(raw.get("end_time", "")),
            }
            error_text = str(exc)[:200]
            payloads.append(
                build_payload(
                    task=fallback_task, rows=[], error=error_text, worker_pid=worker_pid
                )
            )
            failures.append(
                (fallback_task[symbol_field], fallback_task["freq"], error_text)
            )

    if not normalized_tasks:
        return {
            "early_report": {
                "chunk_id": chunk_id,
                "chunk_task_count": int(len(raw_tasks)),
                "chunk_hit_tasks": 0,
                "chunk_network_page_calls": 0,
                "task_detail": task_detail,
                "payloads": payloads,
                "failures": failures,
                "worker_pid": worker_pid,
            }
        }

    return {
        "task_kind": task_kind,
        "profile": profile,
        "symbol_field": symbol_field,
        "build_payload": build_payload,
        "log_tag": str(profile["log_tag"]),
        "worker_pid": worker_pid,
        "chunk_id": chunk_id,
        "raw_tasks": raw_tasks,
        "enable_cache": enable_cache,
        "task_detail": task_detail,
        "normalized_tasks": normalized_tasks,
        "payloads": payloads,
        "failures": failures,
        "reconnect_on_unavailable": bool(
            chunk_payload.get("reconnect_on_unavailable", True)
        ),
        "chunk_timeout": max(
            1.0, float(chunk_payload.get("chunk_timeout_seconds", 30.0) or 30.0)
        ),
        "chunk_retry_max": max(
            0, int(chunk_payload.get("chunk_retry_max_attempts", 2) or 0)
        ),
    }


def _fetch_one_chunk_fetch_attempt(prep: Dict[str, Any]) -> Dict[str, Any]:
    """
    执行单次 chunk 抓取尝试（受 chunk_timeout_seconds 约束，不含重试）。

    输入：
    1. prep: `_prepare_one_task_chunk` 返回的准备结果。
    输出：
    1. get_*_kline_rows_for_chunk_tasks 返回的分片结果字典。
    边界条件：
    1. socket 读超时对齐本次尝试的墙钟预算；建连失败直接上抛。
    """
    task_kind = str(prep["task_kind"])
    normalized_tasks = list(prep["normalized_tasks"])
    enable_cache = bool(prep["enable_cache"])
    chunk_timeout = float(prep["chunk_timeout"])
    attempt_deadline = time.monotonic() + chunk_timeout

    client_context = _ensure_worker_client_context()
    _apply_chunk_socket_read_deadline(client_context, attempt_deadline)
    try:
        if task_kind == "index":
            return client_context.get_index_kline_rows_for_chunk_tasks(
                tasks=normalized_tasks,
                enable_cache=enable_cache,
            )
        return client_context.get_stock_kline_rows_for_chunk_tasks(
            tasks=normalized_tasks,
            enable_cache=enable_cache,
        )
    finally:
        # D3 优化：chunk 结束时还原 socket 读超时，避免短超时残留影响后续 chunk/心跳。
        _restore_chunk_socket_read_timeout(client_context)


def _assemble_chunk_success_report(
    prep: Dict[str, Any], chunk_result: Dict[str, Any]
) -> Dict[str, Any]:
    """
    将单次成功的 chunk 抓取结果组装为分片报告字典。

    输入：
    1. prep: chunk 准备结果。
    2. chunk_result: 抓取结果字典。
    输出：
    1. 分片执行结果字典。
    边界条件：
    1. 按 normalized_tasks 顺序对齐 results 条目。
    """
    symbol_field = str(prep["symbol_field"])
    build_payload = prep["build_payload"]
    worker_pid = int(prep["worker_pid"])
    chunk_id = str(prep["chunk_id"])
    raw_tasks = list(prep["raw_tasks"])
    task_detail = list(prep["task_detail"])
    normalized_tasks = list(prep["normalized_tasks"])
    payloads = list(prep["payloads"])
    failures = list(prep["failures"])

    result_items = list((chunk_result or {}).get("results") or [])
    chunk_hit_tasks = int((chunk_result or {}).get("chunk_hit_tasks", 0) or 0)
    chunk_network_page_calls = int(
        (chunk_result or {}).get("chunk_network_page_calls", 0) or 0
    )

    for index, task in enumerate(normalized_tasks):
        item = (
            result_items[index]
            if index < len(result_items) and isinstance(result_items[index], dict)
            else {}
        )
        result_task = item.get("task", task)
        rows = item.get("rows", [])
        item_error_text = item.get("error")
        payloads.append(
            build_payload(
                task=result_task if isinstance(result_task, dict) else task,
                rows=rows if isinstance(rows, list) else [],
                error=item_error_text,
                worker_pid=worker_pid,
            )
        )
        if str(item_error_text or "").strip():
            failures.append(
                (
                    str(task.get(symbol_field, "")),
                    str(task.get("freq", "")),
                    str(item_error_text)[:200],
                )
            )

    return {
        "chunk_id": chunk_id,
        "chunk_task_count": int(len(raw_tasks)),
        "chunk_hit_tasks": int(max(0, chunk_hit_tasks)),
        "chunk_network_page_calls": int(max(0, chunk_network_page_calls)),
        "task_detail": task_detail,
        "payloads": payloads,
        "failures": failures,
        "worker_pid": worker_pid,
    }


def _log_chunk_retry(
    prep: Dict[str, Any], *, retry_count: int, error_text: str
) -> None:
    """
    记录 chunk 重试日志。

    输入：
    1. prep: chunk 准备结果。
    2. retry_count: 当前重试序号（从 1 起）。
    3. error_text: 触发重试的错误摘要。
    输出：
    1. 无返回值。
    用途：
    1. C2 简化：本函数仅记录重试日志。超时分支的连接重置已由
       _fetch_one_task_chunk_async / _fetch_one_task_chunk_timed_body 在调用本函数之前完成；
       不再二次触发 _recover_worker_pools_current_thread，避免对刚重建的连接重复重置。
    边界条件：
    1. 仅写日志，不做任何连接恢复操作。
    """
    log_tag = str(prep["log_tag"])
    chunk_id = str(prep["chunk_id"])
    raw_tasks = list(prep["raw_tasks"])
    chunk_retry_max = int(prep["chunk_retry_max"])

    if _is_connection_unavailable_error(error_text):
        # 仅记录日志（连接是否需要重建在调用方 timeout 分支已统一处理）。
        _emit_log(
            "warning",
            f"[Task Parallel] {log_tag} 检测到连接不可用，将进入重试",
            {
                "stage": "chunk_retry_connection_unavailable",
                "chunk_id": str(chunk_id),
                "chunk_task_count": int(len(raw_tasks)),
                "retry_count": int(retry_count),
                "retry_limit": int(chunk_retry_max),
                "reason": str(error_text),
            },
        )
        return

    _emit_log(
        "warning",
        f"[Task Parallel] {log_tag} 执行失败，开始重试",
        {
            "stage": "chunk_retry",
            "chunk_id": str(chunk_id),
            "chunk_task_count": int(len(raw_tasks)),
            "retry_count": int(retry_count),
            "retry_limit": int(chunk_retry_max),
            "reason": str(error_text),
        },
    )


def _fetch_one_task_chunk_timed_body(prep: Dict[str, Any]) -> Dict[str, Any]:
    """
    chunk 同步执行主体：单次尝试受 chunk_timeout_seconds 限制，重试不受该上限约束。

    输入：
    1. prep: `_prepare_one_task_chunk` 返回的准备结果。
    输出：
    1. 分片执行结果字典。
    边界条件：
    1. 每次尝试独立计时；重试次数由 chunk_retry_max_attempts 控制。
    """
    log_tag = str(prep["log_tag"])
    chunk_id = str(prep["chunk_id"])
    raw_tasks = list(prep["raw_tasks"])
    chunk_retry_max = int(prep["chunk_retry_max"])

    error_text = ""
    chunk_result: Optional[Dict[str, Any]] = None
    try:
        chunk_result = _fetch_one_chunk_attempt_with_timeout(prep)
    except TimeoutError:
        error_text = "chunk attempt timeout"
        try:
            _recover_worker_pools_current_thread(
                "chunk_attempt_timeout",
                _infer_recover_target_from_chunk(prep),
            )
        except Exception:
            pass
    except Exception as exc:
        error_text = str(exc)[:200] or type(exc).__name__

    retry_count = 0
    while error_text and retry_count < chunk_retry_max:
        retry_count += 1
        _recover_chunk_connection_before_retry(prep, error_text)
        _log_chunk_retry(prep, retry_count=retry_count, error_text=error_text)
        try:
            chunk_result = _fetch_one_chunk_attempt_with_timeout(prep)
            error_text = ""
        except TimeoutError:
            error_text = "chunk attempt timeout"
            try:
                _recover_worker_pools_current_thread(
                    "chunk_attempt_timeout",
                    _infer_recover_target_from_chunk(prep),
                )
            except Exception:
                pass
        except Exception as retry_exc:
            error_text = str(retry_exc)[:200] or type(retry_exc).__name__

    if error_text:
        if retry_count > 0:
            _emit_log(
                "error",
                f"[Task Parallel] {log_tag} 重试耗尽仍失败",
                {
                    "stage": "chunk_retry_exhausted",
                    "chunk_id": str(chunk_id),
                    "chunk_task_count": int(len(raw_tasks)),
                    "retry_count": int(retry_count),
                    "retry_limit": int(chunk_retry_max),
                    "last_error": str(error_text),
                },
            )
        return _build_chunk_timeout_error_report(prep, error_text)

    return _assemble_chunk_success_report(prep, dict(chunk_result or {}))


def _build_chunk_timeout_error_report(
    prep: Dict[str, Any], error_text: str
) -> Dict[str, Any]:
    """
    为 chunk 单次尝试超时或最终失败构造整 chunk 失败报告。

    输入：
    1. prep: chunk 准备结果。
    2. error_text: 错误摘要。
    输出：
    1. chunk 报告字典。
    边界条件：
    1. 为每个已归一化任务写入 error payload。
    """
    build_payload = prep["build_payload"]
    symbol_field = str(prep["symbol_field"])
    worker_pid = int(prep["worker_pid"])
    chunk_id = str(prep["chunk_id"])
    raw_tasks = list(prep["raw_tasks"])
    normalized_tasks = list(prep["normalized_tasks"])
    payloads = list(prep["payloads"])
    failures = list(prep["failures"])
    err = str(error_text or "chunk timeout")[:200]

    for task in normalized_tasks:
        payloads.append(
            build_payload(task=task, rows=[], error=err, worker_pid=worker_pid)
        )
        failures.append(
            (str(task.get(symbol_field, "")), str(task.get("freq", "")), err)
        )

    return {
        "chunk_id": chunk_id,
        "chunk_task_count": int(len(raw_tasks)),
        "chunk_hit_tasks": 0,
        "chunk_network_page_calls": 0,
        "task_detail": list(prep["task_detail"]),
        "payloads": payloads,
        "failures": failures,
        "worker_pid": worker_pid,
    }


def _get_worker_chunk_executor(thread_name_prefix: str) -> ThreadPoolExecutor:
    """
    创建 worker 内单 chunk attempt 使用的短生命周期线程池。

    输入：
    1. thread_name_prefix: 线程名前缀，便于诊断卡死 attempt。
    输出：
    1. `ThreadPoolExecutor(max_workers=1)`。
    用途：
    1. 集中封装线程池创建，便于测试以内联 executor 替换，并避免使用 asyncio 默认 executor。
    边界条件：
    1. 调用方负责 shutdown；超时路径使用 wait=False 放弃等待阻塞线程。
    """
    return ThreadPoolExecutor(max_workers=1, thread_name_prefix=str(thread_name_prefix))


def _fetch_one_chunk_attempt_with_timeout(prep: Dict[str, Any]) -> Dict[str, Any]:
    """
    在线程内执行单次 chunk attempt，并在超时时放弃等待阻塞线程。

    输入：
    1. prep: `_prepare_one_task_chunk` 返回的准备结果。
    输出：
    1. 单次 attempt 的 chunk 报告字典。
    用途：
    1. 让同步 chunk 路径也具备外层墙钟超时，避免阻塞调用方。
    边界条件：
    1. Python 线程不能被安全强杀；超时后关闭 executor 的等待语义并抛 TimeoutError，由 socket deadline
       促使底层 I/O 尽快自然退出。
    """
    chunk_timeout = float(prep["chunk_timeout"])
    executor = _get_worker_chunk_executor("zsdtdx_chunk_attempt")
    future = executor.submit(_fetch_one_chunk_fetch_attempt, prep)
    try:
        return future.result(timeout=chunk_timeout)
    except TimeoutError as exc:
        try:
            future.cancel()
        except Exception:
            pass
        raise TimeoutError("chunk attempt timeout") from exc
    finally:
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            executor.shutdown(wait=False)
        except Exception:
            pass


def _recover_chunk_connection_before_retry(
    prep: Dict[str, Any], error_text: str
) -> None:
    """
    在连接不可用错误重试前执行一次连接恢复。

    输入：
    1. prep: chunk 准备结果。
    2. error_text: 本次失败摘要。
    输出：无。
    用途：
    1. 保持“连接不可用先恢复再重试”的目标态语义，同时让 `_log_chunk_retry` 只负责日志。
    边界条件：
    1. 非连接不可用错误不处理；恢复失败不覆盖原始失败原因。
    """
    if not _is_connection_unavailable_error(error_text):
        return
    try:
        _recover_worker_pools_current_thread(
            "chunk_connection_unavailable",
            _infer_recover_target_from_chunk(prep),
        )
    except Exception:
        pass


async def _recover_chunk_connection_before_retry_async(
    prep: Dict[str, Any], error_text: str
) -> None:
    """
    协程路径在连接不可用错误重试前执行一次连接恢复。

    输入：
    1. prep: chunk 准备结果。
    2. error_text: 本次失败摘要。
    输出：无。
    用途：
    1. 避免在事件循环线程内直接执行可能阻塞的重连动作。
    边界条件：
    1. 非连接不可用错误不处理；恢复失败不覆盖原始失败原因。
    """
    if not _is_connection_unavailable_error(error_text):
        return
    try:
        await asyncio.to_thread(
            _recover_worker_pools_current_thread,
            "chunk_connection_unavailable",
            _infer_recover_target_from_chunk(prep),
        )
    except Exception:
        pass


async def _fetch_one_chunk_attempt_with_timeout_async(
    prep: Dict[str, Any],
) -> Dict[str, Any]:
    """
    在线程内执行单次 chunk attempt，并让协程超时不等待默认 executor 收尾。

    输入：
    1. prep: `_prepare_one_task_chunk` 返回的准备结果。
    输出：
    1. 单次 attempt 的 chunk 报告字典。
    用途：
    1. 避免 `asyncio.to_thread` 使用默认 executor 后在 `asyncio.run()` 退出阶段等待卡死线程。
    边界条件：
    1. 超时后仅放弃等待该线程；底层 socket deadline 负责让阻塞 I/O 自然结束。
    """
    chunk_timeout = float(prep["chunk_timeout"])
    loop = asyncio.get_running_loop()
    executor = _get_worker_chunk_executor("zsdtdx_chunk_attempt_async")
    future = executor.submit(_fetch_one_chunk_fetch_attempt, prep)
    wrapped = asyncio.wrap_future(future, loop=loop)
    try:
        return await asyncio.wait_for(wrapped, timeout=chunk_timeout)
    except asyncio.TimeoutError as exc:
        try:
            future.cancel()
        except Exception:
            pass
        raise asyncio.TimeoutError("chunk attempt timeout") from exc
    finally:
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            executor.shutdown(wait=False)
        except Exception:
            pass


async def _fetch_one_task_chunk_async(chunk_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    协程调度单个 chunk：每次尝试单独 wait_for，重试不受 chunk_timeout_seconds 累计限制。

    输入：
    1. chunk_payload: 分片字典。
    输出：
    1. 分片执行结果字典。
    边界条件：
    1. 单次尝试 asyncio.TimeoutError 时可重试；重试耗尽后返回整 chunk error。
    """
    prep = _prepare_one_task_chunk(chunk_payload)
    early = prep.get("early_report")
    if early is not None:
        return dict(early)

    chunk_timeout = float(prep["chunk_timeout"])
    chunk_retry_max = int(prep["chunk_retry_max"])
    log_tag = str(prep["log_tag"])
    chunk_id = str(prep["chunk_id"])
    raw_tasks = list(prep.get("raw_tasks") or [])

    # C2: 按 chunk 路由推断需要重置的连接边（stock=std-only / index=按 _index_route_source）。
    recover_target = _infer_recover_target_from_chunk(prep)

    error_text = ""
    chunk_result: Optional[Dict[str, Any]] = None
    try:
        chunk_result = await _fetch_one_chunk_attempt_with_timeout_async(prep)
    except asyncio.TimeoutError:
        error_text = "chunk attempt timeout"
        await asyncio.to_thread(
            _recover_worker_pools_current_thread,
            "chunk_attempt_timeout",
            recover_target,
        )
        _emit_log(
            "error",
            f"[Task Parallel] {log_tag} 单次尝试调度超时",
            {
                "stage": "chunk_attempt_timeout",
                "chunk_id": str(chunk_id),
                "chunk_task_count": int(len(raw_tasks)),
                "chunk_timeout_seconds": float(chunk_timeout),
                "retry_count": 0,
                "retry_limit": int(chunk_retry_max),
                "recover_target": recover_target,
            },
        )
    except Exception as exc:
        error_text = str(exc)[:200] or type(exc).__name__

    retry_count = 0
    while error_text and retry_count < chunk_retry_max:
        retry_count += 1
        await _recover_chunk_connection_before_retry_async(prep, error_text)
        _log_chunk_retry(prep, retry_count=retry_count, error_text=error_text)
        try:
            chunk_result = await _fetch_one_chunk_attempt_with_timeout_async(prep)
            error_text = ""
        except asyncio.TimeoutError:
            error_text = "chunk attempt timeout"
            await asyncio.to_thread(
                _recover_worker_pools_current_thread,
                "chunk_attempt_timeout",
                recover_target,
            )
            _emit_log(
                "error",
                f"[Task Parallel] {log_tag} 单次尝试调度超时",
                {
                    "stage": "chunk_attempt_timeout",
                    "chunk_id": str(chunk_id),
                    "chunk_task_count": int(len(raw_tasks)),
                    "chunk_timeout_seconds": float(chunk_timeout),
                    "retry_count": int(retry_count),
                    "retry_limit": int(chunk_retry_max),
                    "recover_target": recover_target,
                },
            )
        except Exception as retry_exc:
            error_text = str(retry_exc)[:200] or type(retry_exc).__name__

    if error_text:
        if retry_count > 0:
            _emit_log(
                "error",
                f"[Task Parallel] {log_tag} 重试耗尽仍失败",
                {
                    "stage": "chunk_retry_exhausted",
                    "chunk_id": str(chunk_id),
                    "chunk_task_count": int(len(raw_tasks)),
                    "retry_count": int(retry_count),
                    "retry_limit": int(chunk_retry_max),
                    "last_error": str(error_text),
                },
            )
        return _build_chunk_timeout_error_report(prep, error_text)

    return _assemble_chunk_success_report(prep, dict(chunk_result or {}))


async def _fetch_chunk_bundle_async(bundle_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    进程 worker：协程并发执行 chunk 批次。

    输入：
    1. bundle_payload: 批次字典，包含 chunks/inproc_coroutine_workers 等字段。
    输出：
    1. 批次执行结果字典，包含 chunk_reports 和聚合 payloads。
    用途：
    1. 实现“进程池并发 + 进程内 chunk 协程并发”的两层并发模型。
    边界条件：
    1. bundle 级预热建连失败上抛；单 chunk 失败不影响同批次其它 chunk。
    """
    worker_pid = int(os.getpid())
    chunk_payloads = list(bundle_payload.get("chunks") or [])
    inproc_workers = max(
        1,
        int(
            bundle_payload.get("inproc_coroutine_workers")
            or bundle_payload.get("inproc_workers", 1)
            or 1
        ),
    )
    bundle_id = int(bundle_payload.get("bundle_id", 0) or 0)
    reconnect_on_unavailable = bool(
        bundle_payload.get("reconnect_on_unavailable", True)
    )
    chunk_timeout_seconds = float(
        bundle_payload.get("chunk_timeout_seconds", 30.0) or 30.0
    )
    chunk_retry_max_attempts = int(
        bundle_payload.get("chunk_retry_max_attempts", 2) or 0
    )
    prepared_chunk_payloads: List[Dict[str, Any]] = []
    for chunk_payload in chunk_payloads:
        normalized_payload = (
            dict(chunk_payload) if isinstance(chunk_payload, dict) else {}
        )
        normalized_payload["reconnect_on_unavailable"] = bool(reconnect_on_unavailable)
        normalized_payload["chunk_timeout_seconds"] = float(chunk_timeout_seconds)
        normalized_payload["chunk_retry_max_attempts"] = int(chunk_retry_max_attempts)
        prepared_chunk_payloads.append(normalized_payload)

    chunk_reports: List[Dict[str, Any]] = []

    if not prepared_chunk_payloads:
        # bundle-level payloads 字段已废弃：父进程仅消费 chunk_reports[i].payloads，
        # 此处不再生成全量 payloads 以减半 IPC pickle 体积（C1 优化）。
        return {
            "bundle_id": bundle_id,
            "bundle_chunk_count": 0,
            "worker_pid": worker_pid,
            "chunk_reports": [],
        }

    _ensure_worker_client_context()

    sem = asyncio.Semaphore(
        max(1, min(int(inproc_workers), int(len(prepared_chunk_payloads))))
    )

    async def _run_one(chunk_payload: Dict[str, Any]) -> Dict[str, Any]:
        async with sem:
            try:
                return await _fetch_one_task_chunk_async(chunk_payload)
            except Exception as exc:
                error_text = str(exc)[:200]
                raw_tasks = list(chunk_payload.get("tasks") or [])
                chunk_kind = (
                    str(chunk_payload.get("task_kind", "stock")).strip().lower()
                )
                return {
                    "chunk_id": str(chunk_payload.get("chunk_id", "")),
                    "chunk_task_count": int(len(raw_tasks)),
                    "chunk_hit_tasks": 0,
                    "chunk_network_page_calls": 0,
                    "payloads": [
                        _build_task_payload_for_kind(
                            chunk_kind,
                            task=t,
                            rows=[],
                            error=error_text,
                            worker_pid=worker_pid,
                        )
                        for t in raw_tasks
                    ],
                    "failures": [
                        (
                            str(
                                (t or {}).get("index_name") or (t or {}).get("code", "")
                            ),
                            str((t or {}).get("freq", "")),
                            error_text,
                        )
                        for t in raw_tasks
                    ],
                    "worker_pid": worker_pid,
                }

    try:
        reports = await asyncio.gather(
            *[_run_one(chunk) for chunk in prepared_chunk_payloads]
        )
        chunk_reports.extend(reports)
    finally:
        _cleanup_worker_dead_thread_connections()

    # 不再返回 bundle-level "payloads"：父进程的 _iter_task_payloads_parallel_chunked
    # 通过 chunk_reports[i].payloads 取数，bundle 级聚合曾是无意义的双倍 pickle 浪费。
    return {
        "bundle_id": bundle_id,
        "bundle_chunk_count": int(len(prepared_chunk_payloads)),
        "worker_pid": worker_pid,
        "chunk_reports": chunk_reports,
    }


def _fetch_chunk_bundle(bundle_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    进程池顶层 callable：在 worker 内运行单事件循环执行 chunk 批次。

    输入：
    1. bundle_payload: 批次字典。
    输出：
    1. 批次执行结果字典。
    边界条件：
    1. 仅在本函数内调用一次 asyncio.run，避免嵌套事件循环。
    """
    return asyncio.run(_fetch_chunk_bundle_async(bundle_payload))


class StockKlineJob:
    """异步任务句柄：持有后台 future 与结果队列。"""

    def __init__(self, future: Future, queue_obj: Optional[Any] = None):
        """
        输入：
        1. future: 后台执行 future。
        2. queue_obj: 可选结果队列对象。
        输出：
        1. 异步句柄实例。
        用途：
        1. 对外暴露 done/wait/result/exception 等统一查询接口。
        边界条件：
        1. queue_obj 为 None 时仅支持通过 result 获取最终结果。
        """
        self._future = future
        self.queue = queue_obj

    def done(self) -> bool:
        """输入无，输出是否完成；用于异步轮询；异常任务也视为完成。"""
        return bool(self._future.done())

    def wait(self, timeout: Optional[float] = None) -> bool:
        """输入超时秒数，输出是否完成；用于阻塞等待；超时返回 False，任务失败会抛出原异常。"""
        try:
            self._future.result(timeout=timeout)
            return True
        except TimeoutError:
            return False

    def result(self, timeout: Optional[float] = None) -> Any:
        """输入超时秒数，输出后台返回值；用于同步获取结果；失败会抛原异常。"""
        return self._future.result(timeout=timeout)

    def exception(self, timeout: Optional[float] = None) -> Optional[BaseException]:
        """输入超时秒数，输出异常对象；用于错误检查；正常完成返回 None。"""
        return self._future.exception(timeout=timeout)


def _serialize_task_list(
    task_list: List[Tuple[int, str, str, str, str]],
) -> List[Dict[str, Any]]:
    """
    将批次任务序列化为日志可读结构。

    输入：
    1. task_list: 任务列表，元素格式为 (idx, code, freq, start_time, end_time)。
    输出：
    1. 结构化任务明细列表（code/freq/start_time/end_time）。
    用途：
    1. 在并行分发和回收阶段输出“进程分配到哪些任务”的可追踪日志。
    边界条件：
    1. 输入为空时返回空列表。
    """
    result: List[Dict[str, Any]] = []
    for _idx, code, freq, start_time, end_time in task_list:
        result.append(
            {
                "code": str(code),
                "freq": str(freq),
                "start_time": str(start_time) if start_time is not None else None,
                "end_time": str(end_time) if end_time is not None else None,
            }
        )
    return result


def _fetch_batch(task_list: List[Tuple[int, str, str, str, str]]) -> Dict[str, Any]:
    """
    进程worker：批量获取任务数据

    输入: [(idx, code, freq, start_time, end_time), ...]
    输出: {"data": DataFrame, "failures": [(code, freq, error), ...]}
    """
    all_data = []
    failures = []
    worker_pid = int(os.getpid())
    try:
        client_context = _ensure_worker_client_context()
    except Exception as exc:
        error_msg = str(exc)[:100]
        for _idx, code, freq, _start_time, _end_time in task_list:
            failures.append((code, freq, error_msg))
        return {
            "data": pd.DataFrame(),
            "failures": failures,
            "worker_pid": worker_pid,
            "task_count": len(task_list),
        }

    for task in task_list:
        idx, code, freq, start_time, end_time = task

        try:
            is_future = is_future_code(code)

            if is_future:
                iterator = client_context.get_future_kline(
                    codes=code, freq=freq, start_time=start_time, end_time=end_time
                )
            else:
                iterator = client_context.get_stock_kline(
                    codes=code, freq=freq, start_time=start_time, end_time=end_time
                )

            batch_count = 0
            for batch in iterator:
                if isinstance(batch, pd.DataFrame) and not batch.empty:
                    all_data.append(batch)
                    batch_count += 1
                elif isinstance(batch, list) and batch:
                    all_data.append(pd.DataFrame(batch))
                    batch_count += 1

            # 如果没有获取到任何数据，记录为失败
            if batch_count == 0:
                failures.append((code, freq, "no_data"))

        except Exception as e:
            error_msg = str(e)[:100]  # 限制错误信息长度
            failures.append((code, freq, error_msg))
            continue

    result_data = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    return {
        "data": result_data,
        "failures": failures,
        "worker_pid": worker_pid,
        "task_count": len(task_list),
    }


class ParallelKlineFetcher:
    """并行K线获取器"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化并行抓取器配置。

        输入：
        1. config_path: 配置文件路径；为空时使用当前激活配置。
        输出：
        1. 无返回值。
        用途：
        1. 读取并归一化并行抓取相关配置，初始化超时、并发、预热与重试策略。
        边界条件：
        1. 配置值非法时会回退到默认值并应用最小值保护。
        """
        self.config_path = config_path
        self.config = self._load_config()

        parallel_cfg = self.config.get("parallel", {})
        self.parallel_total_timeout_seconds = self._safe_float_config(
            parallel_cfg.get("parallel_total_timeout_seconds"),
            default=300.0,
            minimum=1.0,
        )
        self.parallel_result_timeout_seconds = self._safe_float_config(
            parallel_cfg.get("parallel_result_timeout_seconds"),
            default=600.0,
            minimum=1.0,
        )
        self.force_recycle_on_timeout = bool(
            parallel_cfg.get("force_recycle_on_timeout", True)
        )
        self.timeout_fallback_to_serial = bool(
            parallel_cfg.get("timeout_fallback_to_serial", True)
        )
        self.task_chunk_cache_min_tasks = self._safe_int_config(
            parallel_cfg.get("task_chunk_cache_min_tasks"),
            default=2,
            minimum=1,
        )
        self.task_chunk_inproc_coroutine_workers = self._safe_int_config(
            parallel_cfg.get("task_chunk_inproc_coroutine_workers"),
            default=3,
            minimum=1,
        )
        self.task_chunk_max_inflight_multiplier = self._safe_int_config(
            parallel_cfg.get("task_chunk_max_inflight_multiplier"),
            default=2,
            minimum=1,
        )
        self.auto_prewarm_on_async = bool(
            parallel_cfg.get("auto_prewarm_on_async", True)
        )
        self.auto_prewarm_require_all_workers = bool(
            parallel_cfg.get("auto_prewarm_require_all_workers", True)
        )
        self.auto_prewarm_timeout_seconds = self._safe_float_config(
            parallel_cfg.get("auto_prewarm_timeout_seconds"),
            default=60.0,
            minimum=1.0,
        )
        self.auto_prewarm_max_rounds = self._safe_int_config(
            parallel_cfg.get("auto_prewarm_max_rounds"),
            default=3,
            minimum=1,
        )
        self.chunk_reconnect_on_unavailable = bool(
            parallel_cfg.get("chunk_reconnect_on_unavailable", True)
        )
        self.chunk_timeout_seconds = self._safe_float_config(
            parallel_cfg.get("chunk_timeout_seconds"),
            default=30.0,
            minimum=1.0,
        )
        self.chunk_retry_max_attempts = self._safe_int_config(
            parallel_cfg.get("chunk_retry_max_attempts"),
            default=2,
            minimum=0,
        )
        self.bundle_watchdog_grace_seconds = self._safe_float_config(
            parallel_cfg.get("bundle_watchdog_grace_seconds"),
            default=10.0,
            minimum=0.0,
        )
        self.process_count_core_multiplier = self._safe_float_config(
            parallel_cfg.get("process_count_core_multiplier"),
            default=1.5,
            minimum=0.1,
        )
        # 自动计算进程数：CPU物理核心数 * 配置倍率（默认 1.5）
        self.num_processes = get_optimal_process_count(
            core_multiplier=self.process_count_core_multiplier
        )
        self._auto_prewarm_lock = threading.Lock()
        self._auto_prewarm_last_epoch: int = -1
        self._auto_prewarm_last_workers: int = 0
        self._auto_prewarm_last_summary: Optional[Dict[str, Any]] = None
        self._auto_prewarm_last_success: bool = False

        try:
            from zsdtdx.unified_client import _ensure_availability_hosts_cache

            _ensure_availability_hosts_cache(
                config_path=self.config_path,
                cfg=self.config,
            )
        except Exception as exc:
            _emit_log("error", f"[Parallel] Fetcher 初始化时 ensure 缓存失败: {exc}")

    def _build_chunk_task_detail(
        self, task_detail: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        构建 chunk 日志中的任务细节字段。

        输入：
        1. task_detail: 分片任务明细列表（已序列化）。
        输出：
        1. 可直接 merge 到日志 detail 的字段字典。
        用途：
        1. 统一控制默认精简日志（计数+采样）与 debug 全量日志输出。
        边界条件：
        1. sample_size 越界时会在全局配置层被收敛到 [1, 200]。
        """
        options = _read_log_detail_options()
        compact_enabled = bool(options.get("compact_enabled", True))
        include_full_tasks_debug = bool(options.get("include_full_tasks_debug", False))
        sample_size = int(options.get("sample_size", 8) or 8)
        sample_size = max(1, min(200, sample_size))

        total_count = int(len(task_detail))
        payload: Dict[str, Any] = {"task_total_count": total_count}
        if compact_enabled and total_count > 0:
            payload["task_sample"] = task_detail[:sample_size]
        elif compact_enabled:
            payload["task_sample"] = []

        if include_full_tasks_debug:
            payload["tasks"] = task_detail
        return payload

    def _build_chunk_failure_payload(
        self,
        *,
        chunk_failures: List[Tuple[str, str, str]],
        task_detail: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        构建 chunk 失败任务明细字段。

        输入：
        1. chunk_failures: 失败列表，元素格式为 (code, freq, error)。
        2. task_detail: 当前 chunk 的任务明细（含 start_time/end_time）。
        输出：
        1. 失败明细字段字典（failure_codes/failure_tasks）。
        用途：
        1. 让上游维护链路可精确识别 no_data 的 code+freq+窗口，避免仅靠采样推断。
        边界条件：
        1. 无失败时返回空字典。
        2. failure_tasks 默认按 sample_size 截断，避免 detail 过大。
        """

        if not chunk_failures:
            return {}

        options = _read_log_detail_options()
        sample_size = int(options.get("sample_size", 8) or 8)
        sample_size = max(1, min(200, sample_size))
        include_full_tasks_debug = bool(options.get("include_full_tasks_debug", False))
        max_failure_items = (
            len(chunk_failures) if include_full_tasks_debug else max(200, sample_size)
        )

        task_lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for item in task_detail:
            if not isinstance(item, dict):
                continue
            code = str(item.get("code") or "").strip()
            freq = str(item.get("freq") or "").strip()
            if not code or not freq:
                continue
            task_lookup[(code, freq)] = item

        failure_codes: List[str] = []
        failure_code_set: set[str] = set()
        failure_tasks: List[Dict[str, Any]] = []
        for raw_code, raw_freq, raw_error in chunk_failures:
            code = str(raw_code or "").strip()
            freq = str(raw_freq or "").strip()
            error = str(raw_error or "").strip()
            if not code or not freq:
                continue
            if code not in failure_code_set:
                failure_code_set.add(code)
                failure_codes.append(code)
            if len(failure_tasks) >= max_failure_items:
                continue
            matched = task_lookup.get((code, freq), {})
            failure_tasks.append(
                {
                    "code": code,
                    "freq": freq,
                    "error": error,
                    "start_time": matched.get("start_time"),
                    "end_time": matched.get("end_time"),
                }
            )

        payload: Dict[str, Any] = {}
        if failure_codes:
            payload["failure_codes"] = failure_codes
        if failure_tasks:
            payload["failure_tasks"] = failure_tasks
        return payload

    def _safe_float_config(
        self, raw_value: Any, *, default: float, minimum: float = 0.0
    ) -> float:
        """
        解析浮点配置并执行下限保护。

        输入：
        1. raw_value: 配置原始值。
        2. default: 解析失败时的默认值。
        3. minimum: 允许的最小值。
        输出：
        1. 安全浮点值。
        用途：
        1. 避免并行超时配置异常导致抓取线程无限等待或立即超时。
        边界条件：
        1. 非法值回退到 default，再应用 minimum 下限约束。
        """
        try:
            parsed = float(raw_value)
        except Exception:
            parsed = float(default)
        return max(float(minimum), parsed)

    def _safe_int_config(
        self, raw_value: Any, *, default: int, minimum: int = 0
    ) -> int:
        """
        解析整数配置并执行下限保护。

        输入：
        1. raw_value: 配置原始值。
        2. default: 解析失败时的默认值。
        3. minimum: 允许的最小值。
        输出：
        1. 安全整数值。
        用途：
        1. 统一处理并行调度相关整数参数（阈值/并发度/窗口）。
        边界条件：
        1. 非法值会回退到 default，再应用 minimum 下限约束。
        """
        try:
            parsed = int(raw_value)
        except Exception:
            parsed = int(default)
        return max(int(minimum), parsed)

    def _resolve_fetcher_config_path(self) -> Path:
        """
        解析并行抓取器使用的配置文件绝对路径。

        输入：无（使用实例 config_path 或全局 _active_config_path）。
        输出：存在的配置文件 Path。
        用途：与 UnifiedTdxClient、set_config_path 共用 _resolve_zsdtdx_config_path 规则。
        边界条件：文件不存在时由 _resolve_zsdtdx_config_path 抛出 FileNotFoundError。
        """
        from zsdtdx.unified_client import _resolve_zsdtdx_config_path

        if self.config_path:
            return _resolve_zsdtdx_config_path(self.config_path)
        if _active_config_path:
            return _resolve_zsdtdx_config_path(_active_config_path)
        return _resolve_zsdtdx_config_path(None)

    def _load_config(self) -> Dict:
        """
        加载 YAML 配置并规范化 config_path 为绝对路径字符串。

        输入：无。
        输出：配置字典；失败时返回空 dict。
        边界条件：与 simple_api 全局 _active_config_path 对齐；无激活路径时用包内默认 config.yaml。
        """
        try:
            resolved = self._resolve_fetcher_config_path()
            self.config_path = str(resolved)
            with open(resolved, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            _emit_log(
                "warning", f"[ParallelKlineFetcher] 配置加载失败: {e}, 使用默认值"
            )
            return {}

    def _validate_queue(self, queue_obj: Optional[Any]) -> None:
        """
        校验队列对象。

        输入：
        1. queue_obj: 待校验队列对象。
        输出：
        1. 无返回值。
        用途：
        1. 保证队列输出阶段一定可调用 put()。
        边界条件：
        1. queue_obj 为 None 时跳过校验。
        """
        if queue_obj is None:
            return
        if not hasattr(queue_obj, "put"):
            raise ValueError("queue 必须提供 put() 方法")

    def _apply_preprocessor_operator(
        self,
        payload: Dict[str, Any],
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ],
    ) -> Optional[Dict[str, Any]]:
        """
        执行可选钩子并规范返回值。

        输入：
        1. payload: 原始 task payload。
        2. preprocessor_operator: 可选钩子，可返回 dict/None。
        输出：
        1. 处理后的 payload；返回 None 表示丢弃该条结果。
        用途：
        1. 可选扩展入口（如字段筛选、重命名、丢弃条目）；数值刻度由协议解析层统一处理。
        边界条件：
        1. 回调抛错时向上抛 ValueError。
        """
        if preprocessor_operator is None:
            return payload
        try:
            transformed = preprocessor_operator(payload)
        except Exception as exc:
            raise ValueError(f"preprocessor_operator 执行失败: {exc}") from exc
        if transformed is None:
            return None
        if isinstance(transformed, dict) and len(transformed) == 0:
            return None
        if not isinstance(transformed, dict):
            raise ValueError("preprocessor_operator 必须返回 dict 或 None")
        return transformed

    def _ensure_async_prewarm(self) -> Optional[Dict[str, Any]]:
        """
        按配置自动执行 async 进程池预热（仅必要时触发一次）。

        输入：
        1. 无显式输入参数。
        输出：
        1. 预热摘要字典；无需预热时返回 None。
        用途：
        1. 在 async 首次执行前自动完成 worker+连接预热，降低冷启动抖动。
        边界条件：
        1. num_processes <= 1 时跳过预热。
        2. 进程池重建（epoch 变化）后会自动再次预热。
        """
        if not bool(self.auto_prewarm_on_async):
            return None

        target_workers = max(1, int(self.num_processes))
        if target_workers <= 1:
            return None

        pool_epoch = _snapshot_pool_epoch()
        with self._auto_prewarm_lock:
            if (
                self._auto_prewarm_last_success
                and int(self._auto_prewarm_last_epoch) == int(pool_epoch)
                and int(self._auto_prewarm_last_workers) == int(target_workers)
            ):
                return dict(self._auto_prewarm_last_summary or {})

            _emit_log(
                "info",
                "[Task Async] 自动预热触发",
                {
                    "target_workers": int(target_workers),
                    "require_all_workers": bool(self.auto_prewarm_require_all_workers),
                    "timeout_seconds": float(self.auto_prewarm_timeout_seconds),
                    "max_rounds": int(self.auto_prewarm_max_rounds),
                },
            )
            try:
                summary = prewarm_parallel_fetcher(
                    require_all_workers=bool(self.auto_prewarm_require_all_workers),
                    timeout_seconds=float(self.auto_prewarm_timeout_seconds),
                    max_rounds=int(self.auto_prewarm_max_rounds),
                    target_workers=int(target_workers),
                )
            except Exception:
                self._auto_prewarm_last_success = False
                self._auto_prewarm_last_epoch = int(_snapshot_pool_epoch())
                self._auto_prewarm_last_workers = int(target_workers)
                self._auto_prewarm_last_summary = None
                raise

            self._auto_prewarm_last_success = True
            self._auto_prewarm_last_epoch = int(_snapshot_pool_epoch())
            self._auto_prewarm_last_workers = int(target_workers)
            self._auto_prewarm_last_summary = dict(summary)
            return dict(summary)

    def _build_grouped_task_chunks(
        self,
        tasks: List[Dict[str, Any]],
        *,
        group_key: str,
    ) -> List[TaskChunk]:
        """
        按 group_key+freq 分组构建 chunk，组内按时间升序排序。

        输入：tasks 标准化任务列表；group_key 为 "code" 或 "index_name"。
        输出：chunk 列表。
        边界：tasks 为空时返回 []。
        """
        grouped: Dict[
            Tuple[str, str],
            List[Tuple[int, Dict[str, str], pd.Timestamp, pd.Timestamp]],
        ] = defaultdict(list)
        to_sortable_ts = _to_sortable_task_ts
        for index, raw_task in enumerate(tasks or []):
            if not isinstance(raw_task, dict):
                raise ValueError("task 必须是 dict")
            normalized_task = dict(raw_task)
            symbol = str(normalized_task.get(group_key, "")).strip()
            freq = str(normalized_task.get("freq", "")).strip()
            grouped[(symbol, freq)].append(
                (
                    int(index),
                    normalized_task,
                    to_sortable_ts(normalized_task.get("start_time")),
                    to_sortable_ts(normalized_task.get("end_time")),
                )
            )

        chunks: List[TaskChunk] = []
        for chunk_idx, ((symbol, freq), items) in enumerate(grouped.items(), start=1):
            items.sort(key=lambda item: (item[2], item[3], item[0]))
            chunk_tasks = [dict(item[1]) for item in items]
            chunks.append(
                TaskChunk(
                    chunk_id=f"{symbol}:{freq}:{chunk_idx}",
                    code=str(symbol),
                    freq=str(freq),
                    tasks=chunk_tasks,
                )
            )

        chunks.sort(
            key=lambda item: (
                -int(item.task_count),
                str(item.code),
                str(item.freq),
                str(item.tasks[0].get("start_time", "")) if item.tasks else "",
                str(item.chunk_id),
            )
        )
        return chunks

    def _build_task_chunks(self, tasks: List[Dict[str, Any]]) -> List[TaskChunk]:
        """构建股票任务 chunk（按 code+freq 分组）。"""
        return self._build_grouped_task_chunks(tasks, group_key="code")

    def _build_index_task_chunks(self, tasks: List[Dict[str, Any]]) -> List[TaskChunk]:
        """构建指数任务 chunk（按 index_name+freq 分组）。"""
        return self._build_grouped_task_chunks(tasks, group_key="index_name")

    def _build_chunk_bundles(
        self, chunks: List[TaskChunk], inproc_workers: int
    ) -> List[ChunkBundle]:
        """
        构建 chunk 批次：先按进程负载均衡，再按进程内并发切批。

        输入：
        1. chunks: 已排序分片列表。
        2. inproc_workers: 进程内 chunk 并发上限。
        输出：
        1. chunk 批次列表。
        用途：
        1. 近似按 task 数将 chunk 贪心分配到进程，再将每个进程分片切成 bundle。
        边界条件：
        1. chunks 为空时返回空列表。
        """
        if not chunks:
            return []

        max_per_bundle = max(1, int(inproc_workers))
        process_count = max(1, int(self.num_processes))
        process_buckets: List[Dict[str, Any]] = [
            {"process_index": idx, "chunks": [], "load": 0}
            for idx in range(1, process_count + 1)
        ]

        for chunk in chunks:
            target = min(
                process_buckets,
                key=lambda item: (
                    int(item["load"]),
                    int(len(item["chunks"])),
                    int(item["process_index"]),
                ),
            )
            target["chunks"].append(chunk)
            target["load"] += int(chunk.task_count)

        raw_bundles: List[Dict[str, Any]] = []
        for bucket in process_buckets:
            assigned_chunks = list(bucket.get("chunks") or [])
            if not assigned_chunks:
                continue
            for offset in range(0, len(assigned_chunks), max_per_bundle):
                chunk_items = assigned_chunks[offset : offset + max_per_bundle]
                if not chunk_items:
                    continue
                raw_bundles.append(
                    {
                        "process_index": int(bucket["process_index"]),
                        "slice_index": int(offset // max_per_bundle),
                        "chunks": chunk_items,
                        "load": int(sum(item.task_count for item in chunk_items)),
                    }
                )

        raw_bundles.sort(
            key=lambda item: (
                -int(item["load"]),
                int(item["slice_index"]),
                int(item["process_index"]),
                str(item["chunks"][0].chunk_id if item["chunks"] else ""),
            )
        )
        return [
            ChunkBundle(bundle_id=index, chunks=list(item["chunks"]))
            for index, item in enumerate(raw_bundles, start=1)
        ]

    def _iter_task_payloads_inproc_chunked(
        self, tasks: List[Dict[str, Any]], task_kind: str = "stock"
    ):
        """
        主进程内 chunk 并发迭代任务结果（不启用多进程）。

        输入：
        1. tasks: 标准化任务列表。
        输出：
        1. 逐条 yield task payload（完成顺序，不保证输入顺序）。
        用途：
        1. 为“单进程 + chunk 协程并发”场景提供结果流。
        边界条件：
        1. 单次调用内仅执行一次 asyncio.run；chunk 墙钟预算由 _fetch_one_task_chunk_async 施加。
        """
        kind = str(task_kind or "stock").strip().lower()
        build_chunks = (
            self._build_index_task_chunks
            if kind == "index"
            else self._build_task_chunks
        )
        task_list = list(tasks or [])
        if not task_list:
            return

        chunks = build_chunks(task_list)
        if not chunks:
            return

        total_tasks = int(len(task_list))
        total_chunks = int(len(chunks))
        max_workers = max(
            1, min(int(self.task_chunk_inproc_coroutine_workers), int(len(chunks)))
        )
        done_tasks = 0

        dispatch_items: List[Tuple[TaskChunk, Dict[str, Any]]] = []
        dispatch_cursor = 0
        for chunk in chunks:
            dispatch_cursor += 1
            task_detail = [dict(item) for item in chunk.tasks]
            task_payload = self._build_chunk_task_detail(task_detail)
            _emit_log(
                "info",
                "[Task Inproc] 分配 chunk 任务",
                {
                    "stage": "chunk_dispatched",
                    "dispatch_index": int(dispatch_cursor),
                    "dispatch_total": int(total_chunks),
                    "bundle_index": 1,
                    "bundle_total": 1,
                    "chunk_id": str(chunk.chunk_id),
                    "chunk_key": f"{chunk.code}|{chunk.freq}",
                    "chunk_task_count": int(chunk.task_count),
                    "chunk_cache_enabled": bool(
                        chunk.task_count >= int(self.task_chunk_cache_min_tasks)
                    ),
                    "fetch_tasks_total": int(total_tasks),
                    **task_payload,
                },
            )
            chunk_payload = chunk.to_payload(
                enable_cache=bool(
                    chunk.task_count >= int(self.task_chunk_cache_min_tasks)
                )
            )
            chunk_payload["task_kind"] = str(kind)
            chunk_payload["reconnect_on_unavailable"] = bool(
                self.chunk_reconnect_on_unavailable
            )
            chunk_payload["chunk_timeout_seconds"] = float(self.chunk_timeout_seconds)
            chunk_payload["chunk_retry_max_attempts"] = int(
                self.chunk_retry_max_attempts
            )
            dispatch_items.append((chunk, chunk_payload))

        async def _gather_inproc() -> List[Tuple[TaskChunk, Dict[str, Any]]]:
            sem = asyncio.Semaphore(max_workers)

            async def _one(
                item: Tuple[TaskChunk, Dict[str, Any]],
            ) -> Tuple[TaskChunk, Dict[str, Any]]:
                chunk_obj, payload = item
                async with sem:
                    report = await _fetch_one_task_chunk_async(payload)
                    return chunk_obj, report

            pending = [asyncio.create_task(_one(item)) for item in dispatch_items]
            completed: List[Tuple[TaskChunk, Dict[str, Any]]] = []
            for task in asyncio.as_completed(pending):
                completed.append(await task)
            return completed

        for chunk, report in asyncio.run(_gather_inproc()):
            chunk_task_count = int(chunk.task_count)
            try:
                payloads = list(report.get("payloads") or [])
                if len(payloads) < len(chunk.tasks):
                    for task in chunk.tasks[len(payloads) :]:
                        payloads.append(
                            _build_task_payload_for_kind(
                                kind,
                                task=task,
                                rows=[],
                                error="missing_chunk_payload",
                                worker_pid=os.getpid(),
                            )
                        )

                chunk_failure_count = 0
                failure_tasks: List[Dict[str, Any]] = []
                for payload in payloads:
                    error_text = str(payload.get("error") or "").strip()
                    if not error_text:
                        continue
                    chunk_failure_count += 1
                    task_obj = (
                        payload.get("task")
                        if isinstance(payload.get("task"), dict)
                        else {}
                    )
                    task_symbol = str(
                        task_obj.get("code") or task_obj.get("index_name") or ""
                    )
                    failure_tasks.append(
                        {
                            "code": task_symbol,
                            "freq": str(task_obj.get("freq", "")),
                            "error": error_text,
                            "start_time": task_obj.get("start_time"),
                            "end_time": task_obj.get("end_time"),
                        }
                    )

                done_tasks += chunk_task_count
                chunk_hit_tasks = int(report.get("chunk_hit_tasks", 0) or 0)
                chunk_network_page_calls = int(
                    report.get("chunk_network_page_calls", 0) or 0
                )
                _emit_log(
                    "info",
                    "[Task Inproc] chunk 任务完成",
                    {
                        "stage": "chunk_completed",
                        "bundle_index": 1,
                        "bundle_total": 1,
                        "chunk_id": str(chunk.chunk_id),
                        "chunk_key": f"{chunk.code}|{chunk.freq}",
                        "chunk_task_count": int(chunk_task_count),
                        "chunk_failure_count": int(chunk_failure_count),
                        "chunk_hit_tasks": int(chunk_hit_tasks),
                        "chunk_network_page_calls": int(chunk_network_page_calls),
                        "fetch_tasks_done": int(done_tasks),
                        "fetch_tasks_total": int(total_tasks),
                        "failure_codes": [str(chunk.code)]
                        if chunk_failure_count > 0
                        else [],
                        "failure_tasks": failure_tasks,
                    },
                )
                for payload in payloads:
                    yield payload
            except Exception as exc:
                error_text = str(exc)[:200]
                done_tasks += chunk_task_count
                _emit_log(
                    "error",
                    f"[Task Inproc] chunk 执行失败: {error_text}",
                    {
                        "stage": "chunk_failed",
                        "bundle_index": 1,
                        "bundle_total": 1,
                        "chunk_id": str(chunk.chunk_id),
                        "chunk_key": f"{chunk.code}|{chunk.freq}",
                        "chunk_task_count": int(chunk_task_count),
                        "chunk_failure_count": int(chunk_task_count),
                        "fetch_tasks_done": int(done_tasks),
                        "fetch_tasks_total": int(total_tasks),
                    },
                )
                for task in chunk.tasks:
                    yield _build_task_payload_for_kind(
                        kind,
                        task=task,
                        rows=[],
                        error=error_text,
                        worker_pid=os.getpid(),
                    )

    def _iter_task_payloads_parallel_chunked(
        self, tasks: List[Dict[str, Any]], task_kind: str = "stock"
    ):
        """
        chunk 化并行迭代任务结果（完成顺序回收）。

        输入：
        1. tasks: 标准化任务列表。
        输出：
        1. 逐条 yield task payload（完成顺序，不保证输入顺序）。
        用途：
        1. 实现“按 code+freq 分片 + 分片内顺序 + 分片缓存 + 两层并发”。
        边界条件：
        1. chunk 级超时与重试由 _fetch_one_task_chunk_async 内部处理。
        """
        kind = str(task_kind or "stock").strip().lower()
        build_chunks = (
            self._build_index_task_chunks
            if kind == "index"
            else self._build_task_chunks
        )
        task_list = list(tasks or [])
        if not task_list:
            return

        chunks = build_chunks(task_list)
        if not chunks:
            return
        bundles = self._build_chunk_bundles(
            chunks, self.task_chunk_inproc_coroutine_workers
        )
        if not bundles:
            return

        total_tasks = int(len(task_list))
        total_chunks = int(len(chunks))
        total_bundles = int(len(bundles))
        max_inflight = max(
            1, int(self.num_processes) * int(self.task_chunk_max_inflight_multiplier)
        )

        executor = _get_global_process_pool(self.num_processes)
        pending_futures: Dict[Any, Dict[str, Any]] = {}
        bundle_cursor = 0
        done_tasks = 0
        dispatch_cursor = 0
        bundle_watchdog_budget = max(
            1.0,
            float(self.chunk_timeout_seconds)
            * float(1 + int(self.chunk_retry_max_attempts))
            + float(self.bundle_watchdog_grace_seconds),
        )
        bundle_watchdog_poll = max(
            0.1, min(1.0, float(self.chunk_timeout_seconds) / 5.0)
        )

        def _iter_bundle_failure_payloads(
            bundle: ChunkBundle,
            *,
            error_text: str,
            stage: str,
        ):
            """
            为整个 bundle 生成失败 payload 并写入统一日志。

            输入：
            1. bundle: 需要标记失败的批次。
            2. error_text/stage: 失败原因与日志阶段。
            输出：
            1. 逐条 yield 该 bundle 内所有任务的失败 payload。
            用途：
            1. 父级 watchdog 或进程池异常时补齐 payload，确保 async 队列最终能收到 done。
            边界条件：
            1. 不依赖 worker 返回值；按父进程保存的 chunk.tasks 构造失败结果。
            """
            nonlocal done_tasks
            for chunk in bundle.chunks:
                chunk_task_count = int(chunk.task_count)
                done_tasks += chunk_task_count
                _emit_log(
                    "error",
                    f"[Task Parallel] chunk 批次执行失败: {error_text}",
                    {
                        "stage": str(stage),
                        "bundle_index": int(bundle.bundle_id),
                        "bundle_total": int(total_bundles),
                        "chunk_id": str(chunk.chunk_id),
                        "chunk_key": f"{chunk.code}|{chunk.freq}",
                        "chunk_task_count": int(chunk_task_count),
                        "chunk_failure_count": int(chunk_task_count),
                        "fetch_tasks_done": int(done_tasks),
                        "fetch_tasks_total": int(total_tasks),
                    },
                )
                for task in chunk.tasks:
                    yield _build_task_payload_for_kind(
                        kind,
                        task=task,
                        rows=[],
                        error=error_text,
                        worker_pid=os.getpid(),
                    )

        def _submit_one_bundle() -> bool:
            """
            提交一个 bundle 到进程池并记录 chunk 分发日志。

            输入：
            1. 无显式输入参数（使用外层 bundles/bundle_cursor 等闭包变量）。
            输出：
            1. bool，True 表示成功提交一个 bundle，False 表示已无可提交 bundle。
            用途：
            1. 控制在飞窗口内的增量派发，并统一写入分发阶段日志。
            边界条件：
            1. 当 bundle_cursor 越界时直接返回 False，不抛异常。
            """
            nonlocal bundle_cursor, dispatch_cursor
            if bundle_cursor >= len(bundles):
                return False
            bundle = bundles[bundle_cursor]
            bundle_cursor += 1

            bundle_payload = bundle.to_payload(
                cache_min_tasks=self.task_chunk_cache_min_tasks
            )
            for chunk_payload in list(bundle_payload.get("chunks") or []):
                if isinstance(chunk_payload, dict):
                    chunk_payload["task_kind"] = str(kind)
            bundle_payload["inproc_coroutine_workers"] = int(
                self.task_chunk_inproc_coroutine_workers
            )
            bundle_payload["inproc_workers"] = int(
                self.task_chunk_inproc_coroutine_workers
            )
            bundle_payload["reconnect_on_unavailable"] = bool(
                self.chunk_reconnect_on_unavailable
            )
            bundle_payload["chunk_timeout_seconds"] = float(self.chunk_timeout_seconds)
            bundle_payload["chunk_retry_max_attempts"] = int(
                self.chunk_retry_max_attempts
            )
            submitted_at = time.monotonic()
            future = executor.submit(_fetch_chunk_bundle, bundle_payload)
            pending_futures[future] = {
                "bundle": bundle,
                "submitted_at": float(submitted_at),
                "deadline_at": float(submitted_at + bundle_watchdog_budget),
            }

            for chunk in bundle.chunks:
                dispatch_cursor += 1
                task_detail = [dict(item) for item in chunk.tasks]
                task_payload = self._build_chunk_task_detail(task_detail)
                _emit_log(
                    "info",
                    "[Task Parallel] 分配 chunk 任务",
                    {
                        "stage": "chunk_dispatched",
                        "dispatch_index": int(dispatch_cursor),
                        "dispatch_total": int(total_chunks),
                        "bundle_index": int(bundle.bundle_id),
                        "bundle_total": int(total_bundles),
                        "chunk_id": str(chunk.chunk_id),
                        "chunk_key": f"{chunk.code}|{chunk.freq}",
                        "chunk_task_count": int(chunk.task_count),
                        "chunk_cache_enabled": bool(
                            chunk.task_count >= int(self.task_chunk_cache_min_tasks)
                        ),
                        "fetch_tasks_total": int(total_tasks),
                        **task_payload,
                    },
                )
            return True

        while len(pending_futures) < max_inflight and _submit_one_bundle():
            pass

        while pending_futures or bundle_cursor < len(bundles):
            while len(pending_futures) < max_inflight and _submit_one_bundle():
                pass
            if not pending_futures:
                break

            done_future = None
            try:
                for item in as_completed(
                    list(pending_futures.keys()), timeout=bundle_watchdog_poll
                ):
                    done_future = item
                    break
            except TimeoutError:
                done_future = None

            if done_future is None:
                now = time.monotonic()
                overdue_items = [
                    (future, meta)
                    for future, meta in list(pending_futures.items())
                    if float(meta.get("deadline_at", 0.0) or 0.0) <= now
                ]
                if not overdue_items:
                    continue

                if bool(self.force_recycle_on_timeout):
                    affected_items = list(pending_futures.items())
                    for future, _meta in affected_items:
                        try:
                            future.cancel()
                        except Exception:
                            pass
                    overdue_bundle_ids = [
                        int(item[1]["bundle"].bundle_id)
                        for item in overdue_items
                        if isinstance(item[1].get("bundle"), ChunkBundle)
                    ]
                    _emit_log(
                        "error",
                        "[Task Parallel] bundle watchdog 超时，开始回收并行进程池",
                        {
                            "stage": "bundle_watchdog_recycle",
                            "overdue_bundle_ids": overdue_bundle_ids,
                            "pending_bundle_count": int(len(affected_items)),
                            "watchdog_budget_seconds": float(bundle_watchdog_budget),
                        },
                    )
                    for future, meta in affected_items:
                        pending_futures.pop(future, None)
                        bundle = meta["bundle"]
                        error_text = "bundle watchdog timeout"
                        for payload in _iter_bundle_failure_payloads(
                            bundle,
                            error_text=error_text,
                            stage="bundle_watchdog_timeout",
                        ):
                            yield payload
                    try:
                        force_restart_parallel_fetcher(prewarm=False)
                    except Exception as exc:
                        _emit_log(
                            "error",
                            f"[Task Parallel] bundle watchdog 回收进程池失败: {exc}",
                            {"stage": "bundle_watchdog_recycle_failed"},
                        )
                    executor = _get_global_process_pool(self.num_processes)
                    continue

                for future, meta in overdue_items:
                    pending_futures.pop(future, None)
                    try:
                        future.cancel()
                    except Exception:
                        pass
                    bundle = meta["bundle"]
                    for payload in _iter_bundle_failure_payloads(
                        bundle,
                        error_text="bundle watchdog timeout",
                        stage="bundle_watchdog_timeout",
                    ):
                        yield payload
                continue

            meta = pending_futures.pop(done_future)
            bundle = meta["bundle"]
            try:
                bundle_result = done_future.result()
            except Exception as exc:
                error_text = str(exc)[:200]
                for payload in _iter_bundle_failure_payloads(
                    bundle,
                    error_text=error_text,
                    stage="chunk_failed",
                ):
                    yield payload
                continue

            report_map: Dict[str, Dict[str, Any]] = {}
            for report in list(bundle_result.get("chunk_reports") or []):
                if isinstance(report, dict):
                    report_map[str(report.get("chunk_id", ""))] = report

            for chunk in bundle.chunks:
                report = report_map.get(str(chunk.chunk_id), {})
                payloads = list(report.get("payloads") or [])
                if len(payloads) < len(chunk.tasks):
                    for task in chunk.tasks[len(payloads) :]:
                        payloads.append(
                            _build_task_payload_for_kind(
                                kind,
                                task=task,
                                rows=[],
                                error="missing_chunk_payload",
                                worker_pid=os.getpid(),
                            )
                        )

                chunk_task_count = int(chunk.task_count)
                chunk_failure_count = 0
                failure_tasks: List[Dict[str, Any]] = []
                for payload in payloads:
                    error_text = str(payload.get("error") or "").strip()
                    if not error_text:
                        continue
                    chunk_failure_count += 1
                    task_obj = (
                        payload.get("task")
                        if isinstance(payload.get("task"), dict)
                        else {}
                    )
                    task_symbol = str(
                        task_obj.get("code") or task_obj.get("index_name") or ""
                    )
                    failure_tasks.append(
                        {
                            "code": task_symbol,
                            "freq": str(task_obj.get("freq", "")),
                            "error": error_text,
                            "start_time": task_obj.get("start_time"),
                            "end_time": task_obj.get("end_time"),
                        }
                    )

                done_tasks += chunk_task_count
                chunk_hit_tasks = int(report.get("chunk_hit_tasks", 0) or 0)
                chunk_network_page_calls = int(
                    report.get("chunk_network_page_calls", 0) or 0
                )
                _emit_log(
                    "info",
                    "[Task Parallel] chunk 任务完成",
                    {
                        "stage": "chunk_completed",
                        "bundle_index": int(bundle.bundle_id),
                        "bundle_total": int(total_bundles),
                        "chunk_id": str(chunk.chunk_id),
                        "chunk_key": f"{chunk.code}|{chunk.freq}",
                        "chunk_task_count": int(chunk_task_count),
                        "chunk_failure_count": int(chunk_failure_count),
                        "chunk_hit_tasks": int(chunk_hit_tasks),
                        "chunk_network_page_calls": int(chunk_network_page_calls),
                        "fetch_tasks_done": int(done_tasks),
                        "fetch_tasks_total": int(total_tasks),
                        "failure_codes": [str(chunk.code)]
                        if chunk_failure_count > 0
                        else [],
                        "failure_tasks": failure_tasks,
                    },
                )

                for payload in payloads:
                    yield payload

    def _fetch_tasks_sync(
        self,
        *,
        task_kind: str,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
    ) -> List[Dict[str, Any]]:
        """
        同步执行 task 列表（stock/index 共用），并实时写入队列。

        输入：task_kind、tasks、queue、preprocessor_operator。
        输出：处理后的 task payload 列表。
        边界：tasks 为空时返回 [] 并发送 done；主进程内 chunk 调度，不用多进程池。
        """
        kind = str(task_kind or "stock").strip().lower()
        normalize_item = _chunk_task_kind_profile(kind)["normalize_item"]
        sync_log_label = (
            "主进程指数 chunk 执行模式" if kind == "index" else "主进程 chunk 执行模式"
        )

        self._validate_queue(queue)
        normalized_tasks = [normalize_item(item) for item in list(tasks or [])]
        total_tasks = int(len(normalized_tasks))
        outputs: List[Dict[str, Any]] = []
        success_tasks = 0
        failed_tasks = 0

        if total_tasks <= 0:
            done_payload = {
                "event": "done",
                "total_tasks": 0,
                "success_tasks": 0,
                "failed_tasks": 0,
            }
            if queue is not None:
                queue.put(done_payload)
            return outputs

        _emit_log(
            "info",
            (
                f"[Task Sync] {sync_log_label}（不启用多进程池） "
                f"(num_processes_config={self.num_processes}, "
                f"inproc_workers={self.task_chunk_inproc_coroutine_workers}, "
                f"任务数={total_tasks})"
            ),
        )
        iterator = self._iter_task_payloads_inproc_chunked(
            normalized_tasks, task_kind=kind
        )

        for raw_payload in iterator:
            error_text = str(raw_payload.get("error") or "").strip()
            if error_text:
                failed_tasks += 1
            else:
                success_tasks += 1
            processed_payload = self._apply_preprocessor_operator(
                raw_payload, preprocessor_operator
            )
            if processed_payload is None:
                continue
            outputs.append(processed_payload)
            if queue is not None:
                queue.put(processed_payload)

        done_payload = {
            "event": "done",
            "total_tasks": int(total_tasks),
            "success_tasks": int(success_tasks),
            "failed_tasks": int(failed_tasks),
        }
        if queue is not None:
            queue.put(done_payload)
        return outputs

    def fetch_stock_tasks_sync(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
    ) -> List[Dict[str, Any]]:
        """同步执行股票 task 列表，并实时写入队列。"""
        return self._fetch_tasks_sync(
            task_kind="stock",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
        )

    def fetch_index_tasks_sync(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
    ) -> List[Dict[str, Any]]:
        """同步执行指数 task 列表，并实时写入队列。"""
        return self._fetch_tasks_sync(
            task_kind="index",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
        )

    def _fetch_tasks_async(
        self,
        *,
        task_kind: str,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
    ) -> StockKlineJob:
        """
        异步启动 task 执行（stock/index 共用）并立即返回句柄。

        输入：task_kind、tasks、queue、preprocessor_operator。
        输出：StockKlineJob 异步句柄。
        边界：任务异常由 job.exception()/job.result() 暴露。
        """
        kind = str(task_kind or "stock").strip().lower()
        normalize_item = _chunk_task_kind_profile(kind)["normalize_item"]
        async_log_label = (
            "进程池指数 chunk 执行模式" if kind == "index" else "进程池 chunk 执行模式"
        )
        thread_prefix = (
            "zsdtdx_index_kline_async"
            if kind == "index"
            else "zsdtdx_stock_kline_async"
        )

        self._validate_queue(queue)
        self._ensure_async_prewarm()
        normalized_tasks = [normalize_item(item) for item in list(tasks or [])]
        total_tasks = int(len(normalized_tasks))

        def _run_async_worker() -> List[Dict[str, Any]]:
            outputs: List[Dict[str, Any]] = []
            success_tasks = 0
            failed_tasks = 0

            if total_tasks <= 0:
                done_payload = {
                    "event": "done",
                    "total_tasks": 0,
                    "success_tasks": 0,
                    "failed_tasks": 0,
                }
                if queue is not None:
                    queue.put(done_payload)
                return outputs

            _emit_log(
                "info",
                (
                    f"[Task Async] {async_log_label} "
                    f"(num_processes={self.num_processes}, "
                    f"inproc_workers={self.task_chunk_inproc_coroutine_workers}, "
                    f"任务数={total_tasks})"
                ),
            )
            iterator = self._iter_task_payloads_parallel_chunked(
                normalized_tasks, task_kind=kind
            )

            for raw_payload in iterator:
                error_text = str(raw_payload.get("error") or "").strip()
                if error_text:
                    failed_tasks += 1
                else:
                    success_tasks += 1
                processed_payload = self._apply_preprocessor_operator(
                    raw_payload, preprocessor_operator
                )
                if processed_payload is None:
                    continue
                outputs.append(processed_payload)
                if queue is not None:
                    queue.put(processed_payload)

            done_payload = {
                "event": "done",
                "total_tasks": int(total_tasks),
                "success_tasks": int(success_tasks),
                "failed_tasks": int(failed_tasks),
            }
            if queue is not None:
                queue.put(done_payload)
            return outputs

        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=thread_prefix)
        future = executor.submit(_run_async_worker)

        def _cleanup(_future: Future) -> None:
            try:
                executor.shutdown(wait=False, cancel_futures=False)
            except Exception:
                pass

        future.add_done_callback(_cleanup)
        return StockKlineJob(future=future, queue_obj=queue)

    def fetch_stock_tasks_async(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
    ) -> StockKlineJob:
        """异步启动股票 task 执行并立即返回句柄。"""
        return self._fetch_tasks_async(
            task_kind="stock",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
        )

    def fetch_index_tasks_async(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
    ) -> StockKlineJob:
        """异步启动指数 task 执行并立即返回句柄。"""
        return self._fetch_tasks_async(
            task_kind="index",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
        )

    def fetch_stock(
        self,
        codes: List[str],
        freqs: List[str],
        start_time: Optional[str],
        end_time: Optional[str],
    ) -> pd.DataFrame:
        """
        获取股票/期货K线数据

        输入: 代码列表、频率列表、时间范围
        输出: 合并后的单个 DataFrame
        """
        tasks = [(code, freq) for code in codes for freq in freqs]

        if len(tasks) <= 0:
            _emit_log("info", "[Info] 任务数为 0，跳过抓取")
            return pd.DataFrame()

        if self.num_processes <= 1:
            _emit_log(
                "info",
                f"[Info] 进程数不足，使用串行模式 (进程数: {self.num_processes}, 任务数: {len(tasks)})",
            )
            return self._fetch_serial(tasks, start_time, end_time)

        _emit_log(
            "info",
            f"[Info] 默认并行模式 (进程数: {self.num_processes}, 任务数: {len(tasks)})",
        )
        return self._fetch_parallel(tasks, start_time, end_time)

    def _fetch_serial(
        self,
        tasks: List[Tuple[str, str]],
        start_time: Optional[str],
        end_time: Optional[str],
    ) -> pd.DataFrame:
        """串行模式获取，返回合并后的DataFrame"""
        all_data = []
        failures = []

        # 优先使用上下文客户端
        context_client = UnifiedTdxClient.get_active_context_client()
        if context_client is not None:
            client = context_client
            should_close = False
        else:
            client = UnifiedTdxClient(config_path=_active_config_path)
            should_close = True

        try:
            for code, freq in tasks:
                try:
                    is_future = is_future_code(code)
                    iterator = (
                        client.get_future_kline if is_future else client.get_stock_kline
                    )

                    batch_count = 0
                    for df in iterator(
                        codes=code, freq=freq, start_time=start_time, end_time=end_time
                    ):
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            all_data.append(df)
                            batch_count += 1

                    # 如果没有获取到任何数据，记录为失败
                    if batch_count == 0:
                        failures.append((code, freq, "no_data"))

                except Exception as e:
                    error_msg = str(e)[:100]
                    failures.append((code, freq, error_msg))
                    _emit_log("warning", f"[Serial Fetch Error] {code}/{freq}: {e}")
                    continue

            # 记录失败信息到上下文客户端（如果存在）
            if (
                failures
                and context_client
                and hasattr(context_client, "_record_failure")
            ):
                for code, freq, error in failures:
                    task_type = (
                        "future_kline" if is_future_code(code) else "stock_kline"
                    )
                    context_client._record_failure(
                        task_type, code, "fetch_error", error, freq
                    )
        finally:
            if should_close:
                try:
                    client.close()
                except Exception as exc:
                    _emit_log("warning", f"[Serial Fetch] 关闭客户端失败: {exc}")

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def _fetch_parallel(
        self,
        tasks: List[Tuple[str, str]],
        start_time: Optional[str],
        end_time: Optional[str],
    ) -> pd.DataFrame:
        """
        并行模式获取，返回合并后的单个 DataFrame
        """
        # 准备任务参数
        task_args = [
            (i, code, freq, start_time, end_time)
            for i, (code, freq) in enumerate(tasks)
        ]

        # 按进程数分片
        num_processes = self.num_processes
        chunks = [[] for _ in range(num_processes)]
        for i, task in enumerate(task_args):
            chunks[i % num_processes].append(task)
        non_empty_chunks = [chunk for chunk in chunks if chunk]
        total_chunks = len(non_empty_chunks)
        total_tasks = len(task_args)

        all_data = []
        all_failures = []
        done_tasks = 0
        total_timeout = max(1.0, float(self.parallel_total_timeout_seconds))
        per_future_timeout = max(1.0, float(self.parallel_result_timeout_seconds))

        try:
            # 获取全局进程池
            executor = _get_global_process_pool(num_processes)

            # 提交所有任务
            futures: Dict[Any, Dict[str, Any]] = {}
            for dispatch_idx, chunk in enumerate(non_empty_chunks, start=1):
                task_detail = _serialize_task_list(chunk)
                task_payload = self._build_chunk_task_detail(task_detail)
                _emit_log(
                    "info",
                    "[Parallel] 分配进程任务",
                    {
                        "stage": "chunk_dispatched",
                        "dispatch_index": int(dispatch_idx),
                        "dispatch_total": int(total_chunks),
                        "chunk_task_count": int(len(chunk)),
                        "fetch_tasks_total": int(total_tasks),
                        **task_payload,
                    },
                )
                future = executor.submit(_fetch_batch, chunk)
                futures[future] = {
                    "dispatch_index": int(dispatch_idx),
                    "chunk": chunk,
                    "task_detail": task_detail,
                    "task_payload": task_payload,
                }

            # 收集结果
            pending_futures = set(futures.keys())
            timeout_started = time.monotonic()
            try:
                for future in as_completed(list(futures.keys()), timeout=total_timeout):
                    pending_futures.discard(future)
                    meta = futures[future]
                    chunk = meta["chunk"]
                    chunk_task_count = int(len(chunk))
                    try:
                        result = future.result(timeout=per_future_timeout)
                        chunk_df = result.get("data", pd.DataFrame())
                        chunk_failures = result.get("failures", [])
                        worker_pid = int(result.get("worker_pid", 0) or 0)
                        chunk_failure_payload = self._build_chunk_failure_payload(
                            chunk_failures=chunk_failures,
                            task_detail=meta["task_detail"],
                        )

                        if not chunk_df.empty:
                            all_data.append(chunk_df)
                        if chunk_failures:
                            all_failures.extend(chunk_failures)
                        done_tasks += chunk_task_count
                        _emit_log(
                            "info",
                            "[Parallel] 进程任务完成",
                            {
                                "stage": "chunk_completed",
                                "dispatch_index": int(meta["dispatch_index"]),
                                "dispatch_total": int(total_chunks),
                                "worker_pid": int(worker_pid),
                                "chunk_task_count": int(chunk_task_count),
                                "chunk_failure_count": int(len(chunk_failures)),
                                "fetch_tasks_done": int(done_tasks),
                                "fetch_tasks_total": int(total_tasks),
                                **meta["task_payload"],
                                **chunk_failure_payload,
                            },
                        )
                    except Exception as e:
                        done_tasks += chunk_task_count
                        _emit_log(
                            "error",
                            f"[Parallel Error] 进程失败: {e}",
                            {
                                "stage": "chunk_failed",
                                "dispatch_index": int(meta["dispatch_index"]),
                                "dispatch_total": int(total_chunks),
                                "chunk_task_count": int(chunk_task_count),
                                "chunk_failure_count": int(chunk_task_count),
                                "fetch_tasks_done": int(done_tasks),
                                "fetch_tasks_total": int(total_tasks),
                                **meta["task_payload"],
                            },
                        )
                        continue
            except TimeoutError:
                unfinished = [future for future in pending_futures if not future.done()]
                unfinished_meta = [futures[item] for item in unfinished]
                unfinished_count = int(
                    sum(len(meta.get("chunk", [])) for meta in unfinished_meta)
                )
                elapsed = round(time.monotonic() - timeout_started, 3)
                _emit_log(
                    "error",
                    "[Parallel Timeout] 并行抓取总超时，开始回收未完成 future",
                    {
                        "stage": "chunk_timeout",
                        "elapsed_seconds": float(elapsed),
                        "timeout_seconds": float(total_timeout),
                        "unfinished_futures": int(len(unfinished)),
                        "unfinished_tasks": int(unfinished_count),
                        "fetch_tasks_done": int(done_tasks),
                        "fetch_tasks_total": int(total_tasks),
                    },
                )
                for future in unfinished:
                    try:
                        future.cancel()
                    except Exception:
                        pass

                if self.force_recycle_on_timeout:
                    try:
                        recycle_summary = force_restart_parallel_fetcher(prewarm=False)
                        _emit_log(
                            "warning",
                            "[Parallel Timeout] 已触发并行进程强制回收",
                            {
                                "stage": "chunk_timeout_recycled",
                                "summary": recycle_summary,
                            },
                        )
                    except Exception as exc:
                        _emit_log(
                            "error",
                            f"[Parallel Timeout] 并行进程强制回收失败: {exc}",
                            {"stage": "chunk_timeout_recycle_failed"},
                        )

                timeout_tasks: List[Tuple[str, str]] = []
                for meta in unfinished_meta:
                    for _idx, code, freq, _start, _end in meta.get("chunk", []):
                        all_failures.append((code, freq, "parallel_total_timeout"))
                        timeout_tasks.append((code, freq))

                if timeout_tasks and self.timeout_fallback_to_serial:
                    _emit_log(
                        "warning",
                        "[Parallel Timeout] 未完成任务回退串行补拉",
                        {
                            "stage": "chunk_timeout_fallback_serial",
                            "fallback_task_count": int(len(timeout_tasks)),
                        },
                    )
                    fallback_df = self._fetch_serial(
                        timeout_tasks, start_time, end_time
                    )
                    if isinstance(fallback_df, pd.DataFrame) and not fallback_df.empty:
                        all_data.append(fallback_df)

            # 记录失败信息到上下文客户端（如果存在）
            if all_failures:
                try:
                    context_client = UnifiedTdxClient.get_active_context_client()
                    if context_client and hasattr(context_client, "_record_failure"):
                        for code, freq, error in all_failures:
                            task_type = (
                                "future_kline"
                                if is_future_code(code)
                                else "stock_kline"
                            )
                            context_client._record_failure(
                                task_type, code, "fetch_error", error, freq
                            )
                except Exception as e:
                    _emit_log("warning", f"[Parallel] 记录失败信息时出错: {e}")

        except Exception as e:
            _emit_log("error", f"[Parallel Error] 执行失败: {e}")
            # 出错时回退到串行
            return self._fetch_serial(tasks, start_time, end_time)

        # 合并所有结果
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()


# 全局获取器实例
_fetcher = None


def set_active_config_path(config_path: Optional[str]) -> None:
    """
    设置并行抓取器和 worker 使用的活动配置路径。

    输入：
    1. config_path: 绝对或相对路径；空则清空。
    输出：无。
    边界条件：
    1. 不在此处同步 TCP 探测；由 set_config_path 或建池前 ensure 负责。
    2. 路径变更时销毁旧进程池，下次并行时按新配置重建。
    """
    global _active_config_path, _fetcher
    normalized = str(config_path or "").strip() or None
    old_path = _active_config_path
    _active_config_path = normalized
    if _fetcher is not None and _fetcher.config_path != normalized:
        _fetcher = ParallelKlineFetcher(config_path=normalized)
    if old_path != normalized:
        _shutdown_global_pool()


def get_fetcher() -> ParallelKlineFetcher:
    """获取全局 fetcher 实例"""
    global _fetcher
    if _fetcher is None:
        _fetcher = ParallelKlineFetcher(config_path=_active_config_path)
    return _fetcher
