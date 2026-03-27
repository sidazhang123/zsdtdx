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
import atexit
import json
import os
import threading
import time
import warnings
from collections import defaultdict
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, TimeoutError, as_completed
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
_log_callback_lock = threading.Lock()
_log_detail_options_lock = threading.Lock()
_log_detail_options: Dict[str, Any] = {
    "compact_enabled": True,
    "sample_size": 8,
    "include_full_tasks_debug": False,
}
_worker_client_holder: Any = None
_worker_client_context: Any = None
_worker_client_pid: Optional[int] = None
_worker_client_lock = threading.Lock()
_worker_chunk_executor: Optional[ThreadPoolExecutor] = None
_worker_chunk_executor_workers: int = 0
_worker_chunk_executor_lock = threading.Lock()
_worker_chunk_executor_warmed_workers: int = 0
_active_config_path: Optional[str] = None
_worker_sorted_hosts: Optional[Dict[str, list]] = None


def set_log_callback(callback: Optional[Callable[[str, str, Optional[Dict[str, Any]]], None]]):
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
    读取并行日志细节配置快照。

    输入：
    1. 无显式输入参数。
    输出：
    1. 返回配置字典快照，包含 compact_enabled/sample_size/include_full_tasks_debug。
    用途：
    1. 避免调用方直接读写全局可变配置，保证线程安全。
    边界条件：
    1. 返回值始终为副本，调用方修改不会影响全局配置。
    """
    with _log_detail_options_lock:
        return dict(_log_detail_options)


def set_log_detail_options(
    *,
    compact_enabled: Optional[bool] = None,
    sample_size: Optional[int] = None,
    include_full_tasks_debug: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    设置并行日志明细输出策略（线程安全）。

    输入：
    1. compact_enabled: 是否启用精简日志（仅计数+采样）。
    2. sample_size: 任务采样条数上限。
    3. include_full_tasks_debug: 是否在日志 detail 中包含全量 tasks。
    输出：
    1. 返回更新前配置，用于调用方在退出时恢复。
    用途：
    1. 让维护链路按场景切换“默认精简/调试全量”日志策略。
    边界条件：
    1. sample_size 非法时回退默认值 8，并限制在 [1, 200]。
    """
    with _log_detail_options_lock:
        previous = dict(_log_detail_options)
        if compact_enabled is not None:
            _log_detail_options["compact_enabled"] = bool(compact_enabled)
        if sample_size is not None:
            try:
                parsed = int(sample_size)
            except Exception:
                parsed = 8
            _log_detail_options["sample_size"] = max(1, min(200, parsed))
        if include_full_tasks_debug is not None:
            _log_detail_options["include_full_tasks_debug"] = bool(include_full_tasks_debug)
        return previous


def _emit_log(level: str, message: str, detail: Optional[Dict[str, Any]] = None) -> None:
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
    """
    global _global_process_pool, _global_pool_max_workers, _global_pool_epoch
    
    with _global_pool_lock:
        if _global_process_pool is None or _global_pool_max_workers != max_workers:
            if _global_process_pool is not None:
                try:
                    _global_process_pool.shutdown(wait=False)
                except:
                    pass
            
            from zsdtdx.unified_client import get_probe_result_cache
            sorted_hosts_snapshot = get_probe_result_cache() or None

            _global_pool_max_workers = max_workers
            _global_process_pool = ProcessPoolExecutor(
                max_workers=max_workers,
                initializer=_init_worker,
                initargs=(_active_config_path, sorted_hosts_snapshot)
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
    if _global_process_pool is not None:
        try:
            _emit_log("info", "[Parallel] 关闭全局进程池")
            _global_process_pool.shutdown(wait=True)
        except:
            pass
        _global_process_pool = None
        _global_pool_epoch += 1


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
    _shutdown_worker_chunk_executor()
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


def _shutdown_worker_chunk_executor() -> None:
    """
    关闭当前 worker 进程内的 chunk 线程池。

    输入：
    1. 无显式输入参数。
    输出：
    1. 无返回值。
    用途：
    1. 在 worker 退出或重建线程池时回收进程内线程资源。
    边界条件：
    1. 未创建线程池时安全返回；关闭异常会被忽略。
    """
    global _worker_chunk_executor, _worker_chunk_executor_workers, _worker_chunk_executor_warmed_workers
    with _worker_chunk_executor_lock:
        executor = _worker_chunk_executor
        _worker_chunk_executor = None
        _worker_chunk_executor_workers = 0
        _worker_chunk_executor_warmed_workers = 0
    if executor is None:
        return
    try:
        executor.shutdown(wait=True, cancel_futures=False)
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


def _get_worker_chunk_executor(max_workers: int) -> ThreadPoolExecutor:
    """
    获取 worker 进程级 chunk 线程池（同进程复用）。

    输入：
    1. max_workers: 线程池目标并发数。
    输出：
    1. ThreadPoolExecutor 实例。
    用途：
    1. 避免每个 chunk 批次重复创建/销毁线程，减少连接抖动并提升复用率。
    边界条件：
    1. 并发数变化时会先关闭旧线程池再创建新线程池。
    """
    global _worker_chunk_executor, _worker_chunk_executor_workers, _worker_chunk_executor_warmed_workers
    target_workers = max(1, int(max_workers))
    stale_executor = None
    with _worker_chunk_executor_lock:
        if (
            _worker_chunk_executor is None
            or int(_worker_chunk_executor_workers) != int(target_workers)
        ):
            stale_executor = _worker_chunk_executor
            _worker_chunk_executor = ThreadPoolExecutor(
                max_workers=target_workers,
                thread_name_prefix="zsdtdx_chunk_worker",
            )
            _worker_chunk_executor_workers = int(target_workers)
            _worker_chunk_executor_warmed_workers = 0
        executor = _worker_chunk_executor

    if stale_executor is not None:
        try:
            stale_executor.shutdown(wait=True, cancel_futures=False)
        except Exception:
            pass
        _cleanup_worker_dead_thread_connections()

    return executor


def _worker_chunk_connection_probe() -> Dict[str, Any]:
    """
    在 chunk 线程池线程内触发连接建立并返回状态。

    输入：
    1. 无显式输入参数。
    输出：
    1. 连接状态字典（std/ex 是否可用与当前活跃 host）。
    用途：
    1. 让 worker 进程内 chunk 线程在预热阶段提前建连，后续 chunk 可直接复用。
    边界条件：
    1. 建连失败时异常上抛，由调用方统一统计。
    """
    context_client = _ensure_worker_client_context()
    std_pool = getattr(context_client, "std_pool", None)
    ex_pool = getattr(context_client, "ex_pool", None)

    std_ok = bool(std_pool.ensure_connected()) if std_pool is not None else False
    ex_ok = bool(ex_pool.ensure_connected()) if ex_pool is not None else False
    std_host = str(std_pool.get_active_host() or "").strip() if std_pool is not None else ""
    ex_host = str(ex_pool.get_active_host() or "").strip() if ex_pool is not None else ""
    return {
        "std_ok": bool(std_ok),
        "ex_ok": bool(ex_ok),
        "std_host": std_host,
        "ex_host": ex_host,
    }


def _prewarm_worker_chunk_executor_connections(inproc_workers: int) -> Dict[str, Any]:
    """
    预热 worker 进程内 chunk 线程池连接。

    输入：
    1. inproc_workers: 目标线程数。
    输出：
    1. 预热摘要（目标线程数、已预热线程数、错误样本）。
    用途：
    1. 在 async prewarm 阶段把 chunk 执行线程也提前建连，避免首批 chunk 临时建连抖动。
    边界条件：
    1. 目标线程数 <=1 时跳过；重复调用会按已预热线程数短路。
    """
    global _worker_chunk_executor_warmed_workers
    target_workers = max(1, int(inproc_workers))
    if target_workers <= 1:
        return {
            "inproc_target_workers": int(target_workers),
            "inproc_warmed_workers": 0,
        }

    with _worker_chunk_executor_lock:
        warmed_before = int(_worker_chunk_executor_warmed_workers)
    if warmed_before >= target_workers:
        return {
            "inproc_target_workers": int(target_workers),
            "inproc_warmed_workers": int(warmed_before),
        }

    executor = _get_worker_chunk_executor(target_workers)
    futures = [executor.submit(_worker_chunk_connection_probe) for _ in range(target_workers)]
    warmed_workers = 0
    warmup_errors: List[str] = []
    timeout_seconds = max(1.0, min(float(target_workers) * 1.2, 8.0))
    try:
        for future in as_completed(list(futures), timeout=timeout_seconds):
            try:
                payload = dict(future.result() or {})
                if bool(payload.get("std_ok")) and bool(payload.get("ex_ok")):
                    warmed_workers += 1
            except Exception as exc:
                warmup_errors.append(str(exc))
    except TimeoutError:
        warmup_errors.append("inproc chunk executor warmup timeout")
    finally:
        for future in futures:
            if not future.done():
                future.cancel()

    with _worker_chunk_executor_lock:
        _worker_chunk_executor_warmed_workers = max(
            int(_worker_chunk_executor_warmed_workers),
            int(warmed_workers),
        )
        warmed_snapshot = int(_worker_chunk_executor_warmed_workers)

    result: Dict[str, Any] = {
        "inproc_target_workers": int(target_workers),
        "inproc_warmed_workers": int(warmed_snapshot),
    }
    if warmup_errors:
        result["inproc_warmup_errors"] = warmup_errors[:5]
    return result


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
        holder = UnifiedTdxClient(config_path=_active_config_path, presorted_hosts=_worker_sorted_hosts)
        context = holder.__enter__()
        _worker_client_holder = holder
        _worker_client_context = context
        _worker_client_pid = current_pid
        return _worker_client_context


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


def _recover_worker_standard_connection_current_thread(reason: str = "") -> Dict[str, Any]:
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
        active_before = str(get_active_host() or "").strip() if callable(get_active_host) else ""

        resetter = getattr(pool, "reset_thread_connection", None)
        if callable(resetter):
            ok = bool(resetter(reconnect=True))
        else:
            ensure_connected = getattr(pool, "ensure_connected", None)
            ok = bool(ensure_connected()) if callable(ensure_connected) else False

        active_after = str(get_active_host() or "").strip() if callable(get_active_host) else ""
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


def _format_host_port(host: str, port: int) -> str:
    """
    组装 host:port 字符串。

    输入：
    1. host: 主机地址。
    2. port: 端口号。
    输出：
    1. 标准化 host:port 字符串。
    用途：
    1. 统一预热阶段 host 标识格式，便于主进程与 worker 共享映射。
    边界条件：
    1. host 会 strip；port 非法时由调用方保证不传入。
    """
    return f"{str(host).strip()}:{int(port)}"


def _summarize_worker_std_host_distribution(worker_std_host_map: Dict[int, str]) -> Dict[str, int]:
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


def _probe_valid_standard_hosts_for_prewarm(config_path: Optional[str]) -> List[str]:
    """
    探测标准行情 host 池中的可连接 host 列表（主进程）。

    输入：
    1. config_path: 配置文件路径；None 时走默认配置。
    输出：
    1. 按配置顺序返回可连接的标准行情 host 列表。
    用途：
    1. 为 async 预热阶段提供“可用 host 候选池”，用于 worker 分散建连。
    边界条件：
    1. 当探测失败或全部不可连时返回空列表，不中断预热主流程。
    """
    client = UnifiedTdxClient(config_path=config_path)
    try:
        pool = getattr(client, "std_pool", None)
        if pool is None:
            return []
        hosts = list(getattr(pool, "hosts", []) or [])
        connector = getattr(pool, "_connect_to_index", None)
        if not callable(connector):
            ensure_connected = getattr(pool, "ensure_connected", None)
            get_active_host = getattr(pool, "get_active_host", None)
            if callable(ensure_connected) and callable(get_active_host) and bool(ensure_connected()):
                active = str(get_active_host() or "").strip()
                return [active] if active else []
            return []

        valid_hosts: List[str] = []
        seen_hosts: set[str] = set()
        for index, item in enumerate(hosts):
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                continue
            host = _format_host_port(str(item[0]), int(item[1]))
            try:
                ok = bool(connector(int(index)))
            except Exception:
                ok = False
            if ok and host not in seen_hosts:
                seen_hosts.add(host)
                valid_hosts.append(host)
        return valid_hosts
    finally:
        try:
            client.close()
        except Exception:
            pass


def _build_worker_standard_host_assignment(
    warmed_pids: set[int],
    valid_standard_hosts: List[str],
) -> Dict[int, str]:
    """
    生成 worker -> 标准行情 host 分配计划（轮询）。

    输入：
    1. warmed_pids: 已探测到的 worker pid 集合。
    2. valid_standard_hosts: 可连接标准行情 host 列表。
    输出：
    1. pid -> host 映射字典。
    用途：
    1. 在 prewarm 后让各 worker 尽量分散到不同有效 host。
    边界条件：
    1. host 数小于 worker 数时会循环复用 host。
    2. 任一输入为空时返回空映射。
    """
    normalized_hosts = [str(item).strip() for item in list(valid_standard_hosts or []) if str(item).strip()]
    normalized_pids = sorted({int(pid) for pid in set(warmed_pids or set()) if int(pid) > 0})
    if not normalized_hosts or not normalized_pids:
        return {}

    assignment: Dict[int, str] = {}
    host_count = int(len(normalized_hosts))
    for idx, pid in enumerate(normalized_pids):
        assignment[int(pid)] = str(normalized_hosts[idx % host_count])
    return assignment


def _switch_worker_standard_host(preferred_standard_host: Optional[str]) -> Dict[str, Any]:
    """
    在当前 worker 进程尝试切换标准行情连接到指定 host。

    输入：
    1. preferred_standard_host: 期望连接的标准行情 host（host:port）。
    输出：
    1. host 切换结果字典（当前 host、目标 host、是否命中目标）。
    用途：
    1. 支持 prewarm 阶段按计划让不同 worker 尽量连接不同标准行情 IP。
    边界条件：
    1. 目标 host 为空或不存在时保持当前连接并返回未命中状态。
    2. 切换失败不抛错，返回当前 host，由主进程统计分布后再决定是否重试。
    """
    context_client = _ensure_worker_client_context()
    pool = getattr(context_client, "std_pool", None)
    get_active_host = getattr(pool, "get_active_host", None)
    active_before = str(get_active_host() or "").strip() if callable(get_active_host) else ""

    target_host = str(preferred_standard_host or "").strip()
    switched = False
    active_after = active_before
    matched = False
    if target_host != "" and pool is not None:
        hosts = list(getattr(pool, "hosts", []) or [])
        host_index_map: Dict[str, int] = {}
        for index, item in enumerate(hosts):
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                continue
            host_index_map[_format_host_port(str(item[0]), int(item[1]))] = int(index)

        connector = getattr(pool, "_connect_to_index", None)
        target_index = host_index_map.get(target_host)
        if target_index is not None and callable(connector):
            try:
                switched = bool(connector(int(target_index)))
            except Exception:
                switched = False

        if callable(get_active_host):
            active_after = str(get_active_host() or "").strip()
        matched = bool(active_after == target_host)

    return {
        "active_std_host_before": active_before,
        "active_std_host": active_after,
        "target_std_host": target_host,
        "std_host_switched": bool(switched),
        "std_host_matched": bool(matched),
    }


def _worker_warmup_probe(
    hold_seconds: float = 0.15,
    preferred_standard_host: Optional[str] = None,
    inproc_workers: int = 1,
) -> Dict[str, Any]:
    """
    worker 预热探针：确保常驻连接已建立并返回当前进程 pid。

    输入：
    1. hold_seconds: 探针持有时长（秒），用于提高任务分发到不同 worker 的概率。
    2. preferred_standard_host: 期望绑定的标准行情 host（host:port，可选）。
    3. inproc_workers: worker 进程内 chunk 并发线程数（用于同步预热线程池连接）。
    输出：
    1. 预热结果字典，包含 `pid` 与标准行情 host 状态。
    用途：
    1. 在启动阶段触发每个 worker 的首次建连，并可按需切换到指定标准 host。
    2. 当 inproc_workers>1 时，同时预热 chunk 线程池线程连接，确保后续 chunk 复用。
    边界条件：
    1. 建连失败时异常上抛，由主进程聚合并判定启动是否失败。
    """
    host_state = _switch_worker_standard_host(preferred_standard_host)
    inproc_summary = _prewarm_worker_chunk_executor_connections(inproc_workers)
    hold = max(0.0, min(float(hold_seconds), 1.0))
    if hold > 0:
        time.sleep(hold)
    return {"pid": int(os.getpid()), **host_state, **inproc_summary}


def _worker_apply_standard_host_assignment(
    pid_to_standard_host: Dict[int, str],
    hold_seconds: float = 0.05,
    inproc_workers: int = 1,
) -> Dict[str, Any]:
    """
    worker 执行标准行情 host 定向分配任务。

    输入：
    1. pid_to_standard_host: pid -> host 的分配计划。
    2. hold_seconds: 任务持有时长（秒）。
    3. inproc_workers: worker 进程内 chunk 并发线程数（用于同步预热线程池连接）。
    输出：
    1. 包含 pid、目标 host、实际 host 的执行结果字典。
    用途：
    1. 在主进程完成 worker 探测后，按 pid 精确推动 host 分散绑定。
    边界条件：
    1. 当前 pid 未命中分配计划时，仅返回现状，不做主动切换。
    """
    current_pid = int(os.getpid())
    target_host = str((pid_to_standard_host or {}).get(current_pid, "")).strip()
    payload = _worker_warmup_probe(
        hold_seconds=float(hold_seconds),
        preferred_standard_host=target_host if target_host else None,
        inproc_workers=max(1, int(inproc_workers)),
    )
    payload["planned_std_host"] = target_host
    payload["assignment_applied"] = bool(target_host)
    return payload


def prewarm_parallel_fetcher(
    *,
    require_all_workers: bool = True,
    timeout_seconds: float = 60.0,
    max_rounds: int = 3,
    target_workers: Optional[int] = None,
    spread_standard_hosts: bool = False,
) -> Dict[str, Any]:
    """
    预热并行抓取器：强制拉起进程池并建立 worker 常驻连接。

    输入：
    1. require_all_workers: 是否要求全部 worker 均完成预热。
    2. timeout_seconds: 预热总超时时间（秒）。
    3. max_rounds: 预热轮次上限。
    4. target_workers: 可选目标进程数；不传时按当前 fetcher 的 num_processes。
    5. spread_standard_hosts: 是否在 async 预热中对标准行情有效 host 做分散绑定。
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
        int(target_workers) if target_workers is not None else int(get_fetcher().num_processes),
    )
    inproc_workers = max(1, int(get_fetcher().task_chunk_inproc_future_workers))
    timeout_budget = max(1.0, float(timeout_seconds))
    rounds_limit = max(1, int(max_rounds))

    started_at = time.monotonic()
    warmed_pids: set[int] = set()
    worker_std_host_map: Dict[int, str] = {}
    valid_standard_hosts: List[str] = []
    round_errors: list[str] = []
    assignment_errors: list[str] = []
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
            "spread_standard_hosts": bool(spread_standard_hosts),
        },
    )
    executor = _get_global_process_pool(target_workers)
    if bool(spread_standard_hosts):
        try:
            valid_standard_hosts = _probe_valid_standard_hosts_for_prewarm(config_path=_active_config_path)
        except Exception as exc:
            round_errors.append(f"probe valid standard hosts failed: {exc}")
            valid_standard_hosts = []
        if valid_standard_hosts:
            _emit_log(
                "info",
                "[Parallel] 标准行情有效IP探测完成",
                {
                    "valid_standard_host_count": int(len(valid_standard_hosts)),
                    "valid_standard_hosts": list(valid_standard_hosts),
                },
            )
        else:
            _emit_log(
                "warning",
                "[Parallel] 标准行情有效IP探测为空，回退默认预热策略",
                {"spread_standard_hosts": bool(spread_standard_hosts)},
            )

    for round_idx in range(1, rounds_limit + 1):
        rounds_used = round_idx
        elapsed = time.monotonic() - started_at
        remaining = timeout_budget - elapsed
        if remaining <= 0:
            round_errors.append("warmup timeout before round dispatch")
            break

        probe_count = max(target_workers * 2, target_workers)
        if spread_standard_hosts and valid_standard_hosts:
            futures = [
                executor.submit(
                    _worker_warmup_probe,
                    0.15,
                    valid_standard_hosts[(round_idx + dispatch_idx) % len(valid_standard_hosts)],
                    inproc_workers,
                )
                for dispatch_idx in range(probe_count)
            ]
        else:
            futures = [
                executor.submit(_worker_warmup_probe, 0.15, None, inproc_workers)
                for _ in range(probe_count)
            ]
        try:
            for future in as_completed(futures, timeout=remaining):
                try:
                    payload = future.result()
                    pid = int(payload.get("pid", 0))
                    if pid > 0:
                        warmed_pids.add(pid)
                        active_std_host = str(payload.get("active_std_host", "")).strip()
                        if active_std_host:
                            worker_std_host_map[pid] = active_std_host
                except Exception as exc:
                    round_errors.append(str(exc))
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
                "std_host_distribution": _summarize_worker_std_host_distribution(worker_std_host_map),
            },
        )
        if len(warmed_pids) >= target_workers:
            break

    assignment_plan: Dict[int, str] = {}
    assignment_rounds_used = 0
    assignment_applied_pids: set[int] = set()
    if spread_standard_hosts and valid_standard_hosts and warmed_pids:
        assignment_plan = _build_worker_standard_host_assignment(
            warmed_pids=set(warmed_pids),
            valid_standard_hosts=list(valid_standard_hosts),
        )
        if assignment_plan:
            for assignment_round in range(1, rounds_limit + 1):
                assignment_rounds_used = assignment_round
                elapsed = time.monotonic() - started_at
                remaining = timeout_budget - elapsed
                if remaining <= 0:
                    assignment_errors.append("worker host assignment timeout before round dispatch")
                    break

                probe_count = max(target_workers * 2, target_workers)
                futures = [
                    executor.submit(
                        _worker_apply_standard_host_assignment,
                        assignment_plan,
                        0.05,
                        inproc_workers,
                    )
                    for _ in range(probe_count)
                ]
                try:
                    for future in as_completed(futures, timeout=remaining):
                        try:
                            payload = future.result()
                            pid = int(payload.get("pid", 0))
                            if pid <= 0:
                                continue
                            if pid in assignment_plan:
                                assignment_applied_pids.add(pid)
                            active_std_host = str(payload.get("active_std_host", "")).strip()
                            if active_std_host:
                                worker_std_host_map[pid] = active_std_host
                        except Exception as exc:
                            assignment_errors.append(str(exc))
                except TimeoutError:
                    assignment_errors.append("worker host assignment round timeout")
                finally:
                    for future in futures:
                        if not future.done():
                            future.cancel()

                distribution = _summarize_worker_std_host_distribution(worker_std_host_map)
                mismatch_count = 0
                for pid, host in assignment_plan.items():
                    current_host = str(worker_std_host_map.get(int(pid), "")).strip()
                    if current_host != str(host).strip():
                        mismatch_count += 1

                _emit_log(
                    "info",
                    "[Parallel] worker 标准行情IP分散分配轮次完成",
                    {
                        "round": int(assignment_round),
                        "planned_workers": int(len(assignment_plan)),
                        "applied_workers": int(len(assignment_applied_pids)),
                        "mismatch_workers": int(mismatch_count),
                        "std_host_distribution": distribution,
                    },
                )
                if len(assignment_applied_pids) >= len(assignment_plan) and mismatch_count <= 0:
                    break

    elapsed_total = round(time.monotonic() - started_at, 3)
    summary: Dict[str, Any] = {
        "target_workers": int(target_workers),
        "warmed_workers": int(len(warmed_pids)),
        "warmed_pids": sorted(warmed_pids),
        "elapsed_seconds": float(elapsed_total),
        "rounds_used": int(rounds_used),
        "require_all_workers": bool(require_all_workers),
        "spread_standard_hosts": bool(spread_standard_hosts),
    }
    if spread_standard_hosts:
        summary["valid_standard_host_count"] = int(len(valid_standard_hosts))
        summary["valid_standard_hosts"] = list(valid_standard_hosts)
        summary["std_worker_host_distribution"] = _summarize_worker_std_host_distribution(worker_std_host_map)
    if assignment_plan:
        summary["std_host_assignment_plan"] = {str(pid): host for pid, host in sorted(assignment_plan.items())}
        summary["std_host_assignment_rounds_used"] = int(assignment_rounds_used)
        summary["std_host_assignment_applied_workers"] = int(len(assignment_applied_pids))
    if round_errors:
        summary["errors"] = round_errors[:10]
    if assignment_errors:
        summary["assignment_errors"] = assignment_errors[:10]

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
    'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'AU', 'AG', 'RB', 'HC', 'FU', 'BU', 'RU', 'WR', 'SS', 'SP',
    'C', 'CS', 'A', 'B', 'M', 'Y', 'P', 'FB', 'BB', 'JD', 'L', 'V', 'PP', 'J', 'JM', 'I', 'EG', 'EB', 'PG', 'LH', 'RR',
    'SR', 'CF', 'RI', 'OI', 'WH', 'PM', 'FG', 'RS', 'RM', 'JR', 'LR', 'SF', 'SM', 'TA', 'MA', 'ZC', 'CY', 'AP', 'CJ', 'UR', 'SA', 'PF', 'PK',
    'SC', 'NR', 'LU', 'BC', 'EC',
    'IF', 'IC', 'IH', 'TF', 'T', 'TS', 'IM'
}


def _init_worker(config_path: Optional[str] = None, sorted_hosts_map: Optional[Dict[str, list]] = None):
    """
    worker 进程初始化钩子。

    输入：
    1. config_path: 主进程传入的活动配置路径；Windows spawn 模式下必须通过此参数传递。
    2. sorted_hosts_map: 主进程 TCP 探测排序结果，key 为 standard/extended。
    输出：
    1. 无返回值。
    用途：
    1. 在 worker 进程中设置活动配置路径，注册退出清理逻辑。
    边界条件：
    1. atexit 注册失败时会降级忽略，避免阻断 worker 启动。
    """
    global _active_config_path, _worker_sorted_hosts
    if config_path is not None:
        _active_config_path = config_path
    if sorted_hosts_map is not None and isinstance(sorted_hosts_map, dict) and sorted_hosts_map:
        _worker_sorted_hosts = sorted_hosts_map
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
    base_code = ''.join(c for c in code.upper() if c.isalpha())
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
class FetchTask:
    """
    旧版 DataFrame 接口任务定义。

    输入：
    1. idx: 任务序号。
    2. code: 标的代码。
    3. freq: 周期。
    输出：
    1. FetchTask 数据对象。
    用途：
    1. 兼容旧接口 `_fetch_parallel/_fetch_serial` 的任务编排。
    边界条件：
    1. 仅承载轻量字段，不包含时间窗口。
    """
    idx: int
    code: str
    freq: str


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
    return {
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


def _fetch_one_task_chunk(chunk_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    进程 worker：执行单个 chunk（同 code+freq）并返回任务 payload 列表。

    输入：
    1. chunk_payload: 分片字典，包含 chunk_id/code/freq/tasks/enable_cache。
    输出：
    1. 分片执行结果字典，包含 payloads/failures/命中统计等字段。
    用途：
    1. 作为 worker 内 chunk 级并发的最小执行单元。
    边界条件：
    1. 客户端建连失败或分片执行失败时，会为所有任务返回 error payload。
    """
    worker_pid = int(os.getpid())
    chunk_id = str(chunk_payload.get("chunk_id", ""))
    raw_tasks = list(chunk_payload.get("tasks") or [])
    enable_cache = bool(chunk_payload.get("enable_cache", False))

    task_detail: List[Dict[str, Any]] = []
    normalized_tasks: List[Dict[str, str]] = []
    payloads: List[Dict[str, Any]] = []
    failures: List[Tuple[str, str, str]] = []

    for item in raw_tasks:
        try:
            normalized = _normalize_task_payload(item)
            normalized_tasks.append(normalized)
            task_detail.append(dict(normalized))
        except Exception as exc:
            fallback_task = {
                "code": str((item or {}).get("code", "")),
                "freq": str((item or {}).get("freq", "")),
                "start_time": str((item or {}).get("start_time", "")),
                "end_time": str((item or {}).get("end_time", "")),
            }
            error_text = str(exc)[:200]
            payloads.append(_build_task_payload(task=fallback_task, rows=[], error=error_text, worker_pid=worker_pid))
            failures.append((fallback_task["code"], fallback_task["freq"], error_text))

    if not normalized_tasks:
        return {
            "chunk_id": chunk_id,
            "chunk_task_count": int(len(raw_tasks)),
            "chunk_hit_tasks": 0,
            "chunk_network_page_calls": 0,
            "task_detail": task_detail,
            "payloads": payloads,
            "failures": failures,
            "worker_pid": worker_pid,
        }

    try:
        client_context = _ensure_worker_client_context()
    except Exception as exc:
        error_text = str(exc)[:200]
        for task in normalized_tasks:
            payloads.append(_build_task_payload(task=task, rows=[], error=error_text, worker_pid=worker_pid))
            failures.append((str(task.get("code", "")), str(task.get("freq", "")), error_text))
        return {
            "chunk_id": chunk_id,
            "chunk_task_count": int(len(raw_tasks)),
            "chunk_hit_tasks": 0,
            "chunk_network_page_calls": 0,
            "task_detail": task_detail,
            "payloads": payloads,
            "failures": failures,
            "worker_pid": worker_pid,
        }

    chunk_hit_tasks = 0
    chunk_network_page_calls = 0
    reconnect_on_unavailable = bool(chunk_payload.get("reconnect_on_unavailable", True))
    chunk_timeout = max(1.0, float(chunk_payload.get("chunk_timeout_seconds", 30.0) or 30.0))
    chunk_retry_max = max(0, int(chunk_payload.get("chunk_retry_max_attempts", 2) or 0))

    def _run_chunk_fetch_once() -> Dict[str, Any]:
        """
        执行一次 chunk 抓取调用。

        输入：
        1. 使用外层闭包变量 client_context/normalized_tasks/enable_cache。
        输出：
        1. `get_stock_kline_rows_for_chunk_tasks` 返回字典。
        用途：
        1. 为失败后重试复用同一段调用逻辑。
        边界条件：
        1. 调用异常向上抛出，由上层统一处理。
        """
        return client_context.get_stock_kline_rows_for_chunk_tasks(
            tasks=normalized_tasks,
            enable_cache=enable_cache,
        )

    def _run_chunk_fetch_with_timeout() -> Dict[str, Any]:
        """
        带超时包裹执行一次 chunk 抓取。

        输入：
        1. 使用外层闭包变量 chunk_timeout。
        输出：
        1. chunk 抓取结果字典。
        用途：
        1. 防止单个 chunk 无限阻塞，超时后抛出 TimeoutError 由重试循环捕获。
        边界条件：
        1. 超时时 future 被取消，抛出 TimeoutError。
        2. 超时后 executor 以 wait=False 关闭，不阻塞当前线程；
           被放弃的工作线程持有独立的线程本地 socket，会在底层 IO 结束后自行退出。
        """
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="zsdtdx_chunk_timeout")
        f = executor.submit(_run_chunk_fetch_once)
        try:
            return f.result(timeout=chunk_timeout)
        except Exception:
            f.cancel()
            raise
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    # 首次执行
    error_text = ""
    chunk_result = None
    try:
        chunk_result = _run_chunk_fetch_with_timeout()
    except Exception as exc:
        error_text = str(exc)[:200] or type(exc).__name__

    # 通用重试循环：任何异常（含超时）均触发重试
    retry_count = 0
    while error_text and retry_count < chunk_retry_max:
        retry_count += 1
        # 若连接不可用，重试前尝试重建连接
        if bool(reconnect_on_unavailable) and _is_connection_unavailable_error(error_text):
            recover_detail = _recover_worker_standard_connection_current_thread(reason=error_text)
            _emit_log(
                "warning",
                "[Task Parallel] chunk 重试前尝试重建标准连接",
                {
                    "stage": "chunk_retry_reconnect",
                    "chunk_id": str(chunk_id),
                    "chunk_task_count": int(len(raw_tasks)),
                    "retry_count": int(retry_count),
                    "retry_limit": int(chunk_retry_max),
                    "recover_ok": bool(recover_detail.get("ok")),
                    "reason": str(error_text),
                    "active_host_before": str(recover_detail.get("active_host_before", "")),
                    "active_host_after": str(recover_detail.get("active_host_after", "")),
                    "recover_error": str(recover_detail.get("error", "")),
                },
            )
        else:
            _emit_log(
                "warning",
                "[Task Parallel] chunk 执行失败，开始重试",
                {
                    "stage": "chunk_retry",
                    "chunk_id": str(chunk_id),
                    "chunk_task_count": int(len(raw_tasks)),
                    "retry_count": int(retry_count),
                    "retry_limit": int(chunk_retry_max),
                    "reason": str(error_text),
                },
            )
        try:
            chunk_result = _run_chunk_fetch_with_timeout()
            error_text = ""
        except Exception as retry_exc:
            error_text = str(retry_exc)[:200] or type(retry_exc).__name__

    # 重试耗尽仍失败：标记所有 tasks 为 failed
    if error_text:
        if retry_count > 0:
            _emit_log(
                "error",
                "[Task Parallel] chunk 重试耗尽仍失败",
                {
                    "stage": "chunk_retry_exhausted",
                    "chunk_id": str(chunk_id),
                    "chunk_task_count": int(len(raw_tasks)),
                    "retry_count": int(retry_count),
                    "retry_limit": int(chunk_retry_max),
                    "last_error": str(error_text),
                },
            )
        for task in normalized_tasks:
            payloads.append(_build_task_payload(task=task, rows=[], error=error_text, worker_pid=worker_pid))
            failures.append((str(task.get("code", "")), str(task.get("freq", "")), error_text))
        return {
            "chunk_id": chunk_id,
            "chunk_task_count": int(len(raw_tasks)),
            "chunk_hit_tasks": 0,
            "chunk_network_page_calls": 0,
            "task_detail": task_detail,
            "payloads": payloads,
            "failures": failures,
            "worker_pid": worker_pid,
        }

    # 成功：解析结果
    result_items = list(chunk_result.get("results") or [])
    chunk_hit_tasks = int(chunk_result.get("chunk_hit_tasks", 0) or 0)
    chunk_network_page_calls = int(chunk_result.get("chunk_network_page_calls", 0) or 0)

    for index, task in enumerate(normalized_tasks):
        item = result_items[index] if index < len(result_items) and isinstance(result_items[index], dict) else {}
        result_task = item.get("task", task)
        rows = item.get("rows", [])
        item_error_text = item.get("error")
        payloads.append(
            _build_task_payload(
                task=result_task if isinstance(result_task, dict) else task,
                rows=rows if isinstance(rows, list) else [],
                error=item_error_text,
                worker_pid=worker_pid,
            )
        )
        if str(item_error_text or "").strip():
            failures.append((str(task.get("code", "")), str(task.get("freq", "")), str(item_error_text)[:200]))

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


def _fetch_chunk_bundle(bundle_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    进程 worker：执行 chunk 批次并在进程内并发多个 chunk。

    输入：
    1. bundle_payload: 批次字典，包含 chunks/inproc_workers 等字段。
    输出：
    1. 批次执行结果字典，包含 chunk_reports 和聚合 payloads。
    用途：
    1. 实现“进程池并发 + 进程内 chunk future 并发”的两层并发模型。
    边界条件：
    1. 任一 chunk 失败不影响同批次其他 chunk 执行。
    """
    worker_pid = int(os.getpid())
    chunk_payloads = list(bundle_payload.get("chunks") or [])
    inproc_workers = max(1, int(bundle_payload.get("inproc_workers", 1) or 1))
    bundle_id = int(bundle_payload.get("bundle_id", 0) or 0)
    reconnect_on_unavailable = bool(bundle_payload.get("reconnect_on_unavailable", True))
    chunk_timeout_seconds = float(bundle_payload.get("chunk_timeout_seconds", 30.0) or 30.0)
    chunk_retry_max_attempts = int(bundle_payload.get("chunk_retry_max_attempts", 2) or 0)
    prepared_chunk_payloads: List[Dict[str, Any]] = []
    for chunk_payload in chunk_payloads:
        normalized_payload = dict(chunk_payload) if isinstance(chunk_payload, dict) else {}
        normalized_payload["reconnect_on_unavailable"] = bool(reconnect_on_unavailable)
        normalized_payload["chunk_timeout_seconds"] = float(chunk_timeout_seconds)
        normalized_payload["chunk_retry_max_attempts"] = int(chunk_retry_max_attempts)
        prepared_chunk_payloads.append(normalized_payload)

    chunk_reports: List[Dict[str, Any]] = []
    payloads: List[Dict[str, Any]] = []

    if not prepared_chunk_payloads:
        return {
            "bundle_id": bundle_id,
            "bundle_chunk_count": 0,
            "worker_pid": worker_pid,
            "chunk_reports": [],
            "payloads": [],
        }

    # 先在 worker 主线程确保常驻上下文，避免首次建连发生在 chunk 线程中触发资源回收竞态。
    try:
        _ensure_worker_client_context()
    except Exception:
        pass

    if len(prepared_chunk_payloads) <= 1 or inproc_workers <= 1:
        for chunk_payload in prepared_chunk_payloads:
            report = _fetch_one_task_chunk(chunk_payload)
            chunk_reports.append(report)
            payloads.extend(list(report.get("payloads") or []))
        _cleanup_worker_dead_thread_connections()
        return {
            "bundle_id": bundle_id,
            "bundle_chunk_count": int(len(prepared_chunk_payloads)),
            "worker_pid": worker_pid,
            "chunk_reports": chunk_reports,
            "payloads": payloads,
        }

    max_workers = max(1, min(int(inproc_workers), int(len(prepared_chunk_payloads))))
    executor = _get_worker_chunk_executor(max_workers)
    try:
        future_map = {executor.submit(_fetch_one_task_chunk, chunk): chunk for chunk in prepared_chunk_payloads}
        for future in as_completed(list(future_map.keys())):
            chunk_payload = future_map[future]
            try:
                report = future.result()
            except Exception as exc:
                error_text = str(exc)[:200]
                raw_tasks = list(chunk_payload.get("tasks") or [])
                report = {
                    "chunk_id": str(chunk_payload.get("chunk_id", "")),
                    "chunk_task_count": int(len(raw_tasks)),
                    "chunk_hit_tasks": 0,
                    "chunk_network_page_calls": 0,
                    "payloads": [
                        _build_task_payload(task=t, rows=[], error=error_text, worker_pid=worker_pid)
                        for t in raw_tasks
                    ],
                    "failures": [
                        (str((t or {}).get("code", "")), str((t or {}).get("freq", "")), error_text)
                        for t in raw_tasks
                    ],
                    "worker_pid": worker_pid,
                }
            chunk_reports.append(report)
            payloads.extend(list(report.get("payloads") or []))
    finally:
        _cleanup_worker_dead_thread_connections()

    return {
        "bundle_id": bundle_id,
        "bundle_chunk_count": int(len(prepared_chunk_payloads)),
        "worker_pid": worker_pid,
        "chunk_reports": chunk_reports,
        "payloads": payloads,
    }


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
        """输入超时秒数，输出是否完成；用于阻塞等待；超时返回 False。"""
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


def _serialize_task_list(task_list: List[Tuple[int, str, str, str, str]]) -> List[Dict[str, Any]]:
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
        return {"data": pd.DataFrame(), "failures": failures, "worker_pid": worker_pid, "task_count": len(task_list)}

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
    return {"data": result_data, "failures": failures, "worker_pid": worker_pid, "task_count": len(task_list)}


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
        self.force_recycle_on_timeout = bool(parallel_cfg.get("force_recycle_on_timeout", True))
        self.timeout_fallback_to_serial = bool(parallel_cfg.get("timeout_fallback_to_serial", True))
        self.task_chunk_cache_min_tasks = self._safe_int_config(
            parallel_cfg.get("task_chunk_cache_min_tasks"),
            default=2,
            minimum=1,
        )
        self.task_chunk_inproc_future_workers = self._safe_int_config(
            parallel_cfg.get("task_chunk_inproc_future_workers"),
            default=3,
            minimum=1,
        )
        self.task_chunk_max_inflight_multiplier = self._safe_int_config(
            parallel_cfg.get("task_chunk_max_inflight_multiplier"),
            default=2,
            minimum=1,
        )
        self.auto_prewarm_on_async = bool(parallel_cfg.get("auto_prewarm_on_async", True))
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
        self.auto_prewarm_spread_standard_hosts = bool(
            parallel_cfg.get("auto_prewarm_spread_standard_hosts", False)
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

    def _build_chunk_task_detail(self, task_detail: List[Dict[str, Any]]) -> Dict[str, Any]:
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
        max_failure_items = len(chunk_failures) if include_full_tasks_debug else max(200, sample_size)

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

    def _safe_float_config(self, raw_value: Any, *, default: float, minimum: float = 0.0) -> float:
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

    def _safe_int_config(self, raw_value: Any, *, default: int, minimum: int = 0) -> int:
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
        
    def _load_config(self) -> Dict:
        """加载配置"""
        try:
            path = self.config_path or str(Path(__file__).parent / "config.yaml")
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            _emit_log("warning", f"[ParallelKlineFetcher] 配置加载失败: {e}, 使用默认值")
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
        preprocessor_operator: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]],
    ) -> Optional[Dict[str, Any]]:
        """
        执行预处理函数并规范返回值。

        输入：
        1. payload: 原始 task payload。
        2. preprocessor_operator: 预处理函数，可返回 dict/None。
        输出：
        1. 处理后的 payload；返回 None 表示丢弃该条结果。
        用途：
        1. 支持字段改名、值变换、字段删除与空值过滤。
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
                    "spread_standard_hosts": bool(self.auto_prewarm_spread_standard_hosts),
                },
            )
            try:
                summary = prewarm_parallel_fetcher(
                    require_all_workers=bool(self.auto_prewarm_require_all_workers),
                    timeout_seconds=float(self.auto_prewarm_timeout_seconds),
                    max_rounds=int(self.auto_prewarm_max_rounds),
                    target_workers=int(target_workers),
                    spread_standard_hosts=bool(self.auto_prewarm_spread_standard_hosts),
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

    def _build_task_chunks(self, tasks: List[Dict[str, Any]]) -> List[TaskChunk]:
        """
        构建任务 chunk：按 code+freq 分组，并在组内按时间升序排序。

        输入：
        1. tasks: 标准化任务列表。
        输出：
        1. chunk 列表（同 code+freq 聚合后的有序任务分片）。
        用途：
        1. 为并行路径提供“局部顺序执行 + 局部缓存复用”的分片基础。
        边界条件：
        1. tasks 为空时返回空列表。
        """
        grouped: Dict[Tuple[str, str], List[Tuple[int, Dict[str, str], pd.Timestamp, pd.Timestamp]]] = defaultdict(list)
        normalize_task = _normalize_task_payload
        to_sortable_ts = _to_sortable_task_ts
        for index, raw_task in enumerate(tasks or []):
            normalized_task = normalize_task(raw_task)
            code = str(normalized_task.get("code", "")).strip()
            freq = str(normalized_task.get("freq", "")).strip()
            grouped[(code, freq)].append(
                (
                    int(index),
                    normalized_task,
                    to_sortable_ts(normalized_task.get("start_time")),
                    to_sortable_ts(normalized_task.get("end_time")),
                )
            )

        chunks: List[TaskChunk] = []
        for chunk_idx, ((code, freq), items) in enumerate(grouped.items(), start=1):
            items.sort(key=lambda item: (item[2], item[3], item[0]))
            chunk_tasks = [dict(item[1]) for item in items]
            chunks.append(
                TaskChunk(
                    chunk_id=f"{code}:{freq}:{chunk_idx}",
                    code=str(code),
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

    def _build_chunk_bundles(self, chunks: List[TaskChunk], inproc_workers: int) -> List[ChunkBundle]:
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
            {"process_index": idx, "chunks": [], "load": 0} for idx in range(1, process_count + 1)
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

    def _iter_task_payloads_inproc_chunked(self, tasks: List[Dict[str, Any]]):
        """
        主进程内 chunk 并发迭代任务结果（不启用多进程）。

        输入：
        1. tasks: 标准化任务列表。
        输出：
        1. 逐条 yield task payload（完成顺序，不保证输入顺序）。
        用途：
        1. 为“单进程 + chunk future 并发”场景提供结果流。
        边界条件：
        1. chunk 级超时与重试由 _fetch_one_task_chunk 内部处理。
        """
        task_list = [_normalize_task_payload(item) for item in list(tasks or [])]
        if not task_list:
            return

        chunks = self._build_task_chunks(task_list)
        if not chunks:
            return

        total_tasks = int(len(task_list))
        total_chunks = int(len(chunks))
        max_workers = max(1, min(int(self.task_chunk_inproc_future_workers), int(len(chunks))))
        done_tasks = 0

        future_map: Dict[Future, TaskChunk] = {}
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="zsdtdx_main_chunk_worker") as executor:
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
                        "chunk_cache_enabled": bool(chunk.task_count >= int(self.task_chunk_cache_min_tasks)),
                        "fetch_tasks_total": int(total_tasks),
                        **task_payload,
                    },
                )
                chunk_payload = chunk.to_payload(
                    enable_cache=bool(chunk.task_count >= int(self.task_chunk_cache_min_tasks))
                )
                chunk_payload["reconnect_on_unavailable"] = bool(self.chunk_reconnect_on_unavailable)
                chunk_payload["chunk_timeout_seconds"] = float(self.chunk_timeout_seconds)
                chunk_payload["chunk_retry_max_attempts"] = int(self.chunk_retry_max_attempts)
                future = executor.submit(_fetch_one_task_chunk, chunk_payload)
                future_map[future] = chunk

            for future in as_completed(list(future_map.keys())):
                chunk = future_map[future]
                chunk_task_count = int(chunk.task_count)
                try:
                    report = future.result()
                    payloads = list(report.get("payloads") or [])
                    if len(payloads) < len(chunk.tasks):
                        for task in chunk.tasks[len(payloads):]:
                            payloads.append(
                                _build_task_payload(
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
                        task_obj = payload.get("task") if isinstance(payload.get("task"), dict) else {}
                        failure_tasks.append(
                            {
                                "code": str(task_obj.get("code", "")),
                                "freq": str(task_obj.get("freq", "")),
                                "error": error_text,
                                "start_time": task_obj.get("start_time"),
                                "end_time": task_obj.get("end_time"),
                            }
                        )

                    done_tasks += chunk_task_count
                    chunk_hit_tasks = int(report.get("chunk_hit_tasks", 0) or 0)
                    chunk_network_page_calls = int(report.get("chunk_network_page_calls", 0) or 0)
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
                            "failure_codes": [str(chunk.code)] if chunk_failure_count > 0 else [],
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
                        yield _build_task_payload(
                            task=task,
                            rows=[],
                            error=error_text,
                            worker_pid=os.getpid(),
                        )

    def _iter_task_payloads_parallel_chunked(self, tasks: List[Dict[str, Any]]):
        """
        chunk 化并行迭代任务结果（完成顺序回收）。

        输入：
        1. tasks: 标准化任务列表。
        输出：
        1. 逐条 yield task payload（完成顺序，不保证输入顺序）。
        用途：
        1. 实现“按 code+freq 分片 + 分片内顺序 + 分片缓存 + 两层并发”。
        边界条件：
        1. chunk 级超时与重试由 _fetch_one_task_chunk 内部处理。
        """
        task_list = [_normalize_task_payload(item) for item in list(tasks or [])]
        if not task_list:
            return

        chunks = self._build_task_chunks(task_list)
        if not chunks:
            return
        bundles = self._build_chunk_bundles(chunks, self.task_chunk_inproc_future_workers)
        if not bundles:
            return

        total_tasks = int(len(task_list))
        total_chunks = int(len(chunks))
        total_bundles = int(len(bundles))
        max_inflight = max(1, int(self.num_processes) * int(self.task_chunk_max_inflight_multiplier))

        executor = _get_global_process_pool(self.num_processes)
        pending_futures: Dict[Any, Dict[str, Any]] = {}
        bundle_cursor = 0
        done_tasks = 0
        dispatch_cursor = 0

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

            bundle_payload = bundle.to_payload(cache_min_tasks=self.task_chunk_cache_min_tasks)
            bundle_payload["inproc_workers"] = int(self.task_chunk_inproc_future_workers)
            bundle_payload["reconnect_on_unavailable"] = bool(self.chunk_reconnect_on_unavailable)
            bundle_payload["chunk_timeout_seconds"] = float(self.chunk_timeout_seconds)
            bundle_payload["chunk_retry_max_attempts"] = int(self.chunk_retry_max_attempts)
            future = executor.submit(_fetch_chunk_bundle, bundle_payload)
            pending_futures[future] = {"bundle": bundle}

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
                        "chunk_cache_enabled": bool(chunk.task_count >= int(self.task_chunk_cache_min_tasks)),
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

            done_future = next(as_completed(list(pending_futures.keys())))

            meta = pending_futures.pop(done_future)
            bundle = meta["bundle"]
            try:
                bundle_result = done_future.result()
            except Exception as exc:
                error_text = str(exc)[:200]
                for chunk in bundle.chunks:
                    chunk_task_count = int(chunk.task_count)
                    done_tasks += chunk_task_count
                    _emit_log(
                        "error",
                        f"[Task Parallel] chunk 批次执行失败: {error_text}",
                        {
                            "stage": "chunk_failed",
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
                        yield _build_task_payload(
                            task=task,
                            rows=[],
                            error=error_text,
                            worker_pid=os.getpid(),
                        )
                continue

            report_map: Dict[str, Dict[str, Any]] = {}
            for report in list(bundle_result.get("chunk_reports") or []):
                if isinstance(report, dict):
                    report_map[str(report.get("chunk_id", ""))] = report

            for chunk in bundle.chunks:
                report = report_map.get(str(chunk.chunk_id), {})
                payloads = list(report.get("payloads") or [])
                if len(payloads) < len(chunk.tasks):
                    for task in chunk.tasks[len(payloads):]:
                        payloads.append(
                            _build_task_payload(
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
                    task_obj = payload.get("task") if isinstance(payload.get("task"), dict) else {}
                    failure_tasks.append(
                        {
                            "code": str(task_obj.get("code", "")),
                            "freq": str(task_obj.get("freq", "")),
                            "error": error_text,
                            "start_time": task_obj.get("start_time"),
                            "end_time": task_obj.get("end_time"),
                        }
                    )

                done_tasks += chunk_task_count
                chunk_hit_tasks = int(report.get("chunk_hit_tasks", 0) or 0)
                chunk_network_page_calls = int(report.get("chunk_network_page_calls", 0) or 0)
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
                        "failure_codes": [str(chunk.code)] if chunk_failure_count > 0 else [],
                        "failure_tasks": failure_tasks,
                    },
                )

                for payload in payloads:
                    yield payload

    def fetch_stock_tasks_sync(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        同步执行 task 列表，并实时写入队列。

        输入：
        1. tasks: 任务列表。
        2. queue: 可选结果队列（需支持 put）。
        3. preprocessor_operator: 可选预处理函数。
        输出：
        1. 处理后的 task payload 列表。
        用途：
        1. 支持“阻塞调用 + 实时队列”模式。
        边界条件：
        1. tasks 为空时返回空列表并发送 done 事件。
        2. 同步模式固定在主进程内执行 chunk 调度，不使用多进程池。
        """
        self._validate_queue(queue)
        normalized_tasks = [_normalize_task_payload(item) for item in list(tasks or [])]
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
                "[Task Sync] 主进程 chunk 执行模式（不启用多进程池） "
                f"(num_processes_config={self.num_processes}, "
                f"inproc_workers={self.task_chunk_inproc_future_workers}, "
                f"任务数={total_tasks})"
            ),
        )
        iterator = self._iter_task_payloads_inproc_chunked(normalized_tasks)

        for raw_payload in iterator:
            error_text = str(raw_payload.get("error") or "").strip()
            if error_text:
                failed_tasks += 1
            else:
                success_tasks += 1
            processed_payload = self._apply_preprocessor_operator(raw_payload, preprocessor_operator)
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

    def fetch_stock_tasks_async(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    ) -> StockKlineJob:
        """
        异步启动 task 执行并立即返回句柄。

        输入：
        1. tasks: 任务列表。
        2. queue: 可选结果队列。
        3. preprocessor_operator: 可选预处理函数。
        输出：
        1. StockKlineJob 异步句柄。
        用途：
        1. 支持“非阻塞调用 + 队列持续消费”模式。
        边界条件：
        1. 任务执行异常由 job.exception()/job.result() 暴露。
        """
        self._validate_queue(queue)
        self._ensure_async_prewarm()
        normalized_tasks = [_normalize_task_payload(item) for item in list(tasks or [])]
        total_tasks = int(len(normalized_tasks))

        def _run_async_worker() -> List[Dict[str, Any]]:
            """
            async 后台执行体：固定走进程池 chunk 并行路径。

            输入：
            1. 使用外层闭包变量 normalized_tasks/queue/preprocessor_operator。
            输出：
            1. 处理后的 task payload 列表。
            用途：
            1. 与 sync 主进程路径分离，确保 async 真正落到进程池并行执行。
            边界条件：
            1. tasks 为空时返回空列表并发送 done 事件。
            """
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
                    "[Task Async] 进程池 chunk 执行模式 "
                    f"(num_processes={self.num_processes}, "
                    f"inproc_workers={self.task_chunk_inproc_future_workers}, "
                    f"任务数={total_tasks})"
                ),
            )
            iterator = self._iter_task_payloads_parallel_chunked(normalized_tasks)

            for raw_payload in iterator:
                error_text = str(raw_payload.get("error") or "").strip()
                if error_text:
                    failed_tasks += 1
                else:
                    success_tasks += 1
                processed_payload = self._apply_preprocessor_operator(raw_payload, preprocessor_operator)
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

        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="zsdtdx_stock_kline_async")
        future = executor.submit(_run_async_worker)

        def _cleanup(_future: Future) -> None:
            """
            异步任务结束后的线程池清理回调。

            输入：
            1. _future: 已完成的 future（仅用于回调签名占位）。
            输出：
            1. 无返回值。
            用途：
            1. 在后台任务结束后关闭承载 async 包装任务的单线程执行器。
            边界条件：
            1. 关闭异常会被吞掉，避免影响 future 完成态传播。
            """
            try:
                executor.shutdown(wait=False, cancel_futures=False)
            except Exception:
                pass

        future.add_done_callback(_cleanup)
        return StockKlineJob(future=future, queue_obj=queue)
    
    def fetch_stock(self, 
                   codes: List[str],
                   freqs: List[str],
                   start_time: Optional[str],
                   end_time: Optional[str]) -> pd.DataFrame:
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
            _emit_log("info", f"[Info] 进程数不足，使用串行模式 (进程数: {self.num_processes}, 任务数: {len(tasks)})")
            return self._fetch_serial(tasks, start_time, end_time)

        _emit_log("info", f"[Info] 默认并行模式 (进程数: {self.num_processes}, 任务数: {len(tasks)})")
        return self._fetch_parallel(tasks, start_time, end_time)
    
    def _fetch_serial(self, tasks: List[Tuple[str, str]], 
                     start_time: Optional[str], 
                     end_time: Optional[str]) -> pd.DataFrame:
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
                    iterator = client.get_future_kline if is_future else client.get_stock_kline
                    
                    batch_count = 0
                    for df in iterator(codes=code, freq=freq, start_time=start_time, end_time=end_time):
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
            if failures and context_client and hasattr(context_client, '_record_failure'):
                for code, freq, error in failures:
                    task_type = "future_kline" if is_future_code(code) else "stock_kline"
                    context_client._record_failure(task_type, code, "fetch_error", error, freq)
        finally:
            if should_close:
                try:
                    client.close()
                except:
                    pass
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
    
    def _fetch_parallel(self, tasks: List[Tuple[str, str]],
                       start_time: Optional[str],
                       end_time: Optional[str]) -> pd.DataFrame:
        """
        并行模式获取，返回合并后的单个 DataFrame
        """
        # 准备任务参数
        task_args = [(i, code, freq, start_time, end_time) 
                     for i, (code, freq) in enumerate(tasks)]
        
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
                unfinished_count = int(sum(len(meta.get("chunk", [])) for meta in unfinished_meta))
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
                            {"stage": "chunk_timeout_recycled", "summary": recycle_summary},
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
                    fallback_df = self._fetch_serial(timeout_tasks, start_time, end_time)
                    if isinstance(fallback_df, pd.DataFrame) and not fallback_df.empty:
                        all_data.append(fallback_df)
            
            # 记录失败信息到上下文客户端（如果存在）
            if all_failures:
                try:
                    context_client = UnifiedTdxClient.get_active_context_client()
                    if context_client and hasattr(context_client, '_record_failure'):
                        for code, freq, error in all_failures:
                            task_type = "future_kline" if is_future_code(code) else "stock_kline"
                            context_client._record_failure(task_type, code, "fetch_error", error, freq)
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
    """设置并行抓取器和 worker 使用的活动配置路径。"""
    global _active_config_path, _fetcher
    normalized = str(config_path or "").strip() or None
    old_path = _active_config_path
    _active_config_path = normalized
    if _fetcher is not None and _fetcher.config_path != normalized:
        _fetcher = ParallelKlineFetcher(config_path=normalized)
    # 配置路径变更时销毁旧进程池，下次使用时按新配置重建
    if old_path != normalized:
        _shutdown_global_pool()


def get_fetcher() -> ParallelKlineFetcher:
    """获取全局 fetcher 实例"""
    global _fetcher
    if _fetcher is None:
        _fetcher = ParallelKlineFetcher(config_path=_active_config_path)
    return _fetcher

