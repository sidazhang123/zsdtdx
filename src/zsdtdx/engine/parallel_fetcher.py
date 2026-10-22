"""
模块：`engine/parallel_fetcher.py`。

并行数据获取模块：使用全局 ThreadPoolExecutor

职责：
1. 提供股票/期货K线数据的并行获取能力。
2. 使用全局 ThreadPoolExecutor 复用工作线程。
3. 兼容旧版 DataFrame 聚合接口与新版 task payload 接口。

边界：
1. 全局线程池在首次并行调用时创建，程序退出时统一关闭。
2. 旧接口返回 DataFrame；任务接口返回 list[dict] 并支持队列实时事件。
"""

import atexit
import json
import queue as queue_mod
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import (
    CancelledError,
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    TimeoutError,
    wait,
)
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from zsdtdx.util.company_info_state import (
    company_info_outcome,
    normalize_company_info_codes,
    publish_company_info_outcome,
    record_company_info_outcome,
)
from zsdtdx.engine.adaptive_scheduler import (
    AdaptiveConcurrencyController,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient
from zsdtdx.util.errors import CompanyInfoFetchError
from zsdtdx.util.log import emit_log, event_level_name, is_event_enabled


def _kline_symbol_field(kind: str) -> str:
    """
    输入任务种类，输出主键字段名。

    输入：kind 为 stock/index/block。
    输出：code、index_name 或 block_name。
    用途：分片与 payload 共用主键。
    边界条件：未知种类按 code。
    """
    key = str(kind or "").strip().lower()
    if key == "block":
        return "block_name"
    if key == "index":
        return "index_name"
    return "code"


try:
    import psutil
except ImportError:
    psutil = None


# =============================================================================
# 全局线程池管理
# =============================================================================
_thread_runtime = None
_global_pool_lock = threading.RLock()
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
_shared_client_holder: Any = None
_shared_client_context: Any = None
_shared_client_lock = threading.Lock()

_active_config_path: Optional[str] = None
_global_adaptive_controller: Optional[AdaptiveConcurrencyController] = None
_global_adaptive_controller_lock = threading.Lock()


def _get_global_adaptive_controller(
    *,
    max_threads: int,
    chunks_per_lease: int,
    threads_per_host_std: int,
    threads_per_host_ex: int,
    decrease_factor: float,
    cooldown_seconds: float,
) -> AdaptiveConcurrencyController:
    """
    获取并配置跨 async job 共享的自适应控制器。

    输入：线程硬顶、单租约 chunk 数、两侧每地址线程数与拥塞参数。
    输出：全局控制器。
    边界：每次调用以当前可达 host 快照刷新地址，但保留未变化 host 的拥塞状态。
    """
    global _global_adaptive_controller
    from zsdtdx.engine.unified_client import get_probe_result_cache

    with _global_adaptive_controller_lock:
        if _global_adaptive_controller is None:
            _global_adaptive_controller = AdaptiveConcurrencyController(
                max_threads=max_threads,
                chunks_per_lease=chunks_per_lease,
                threads_per_host_std=threads_per_host_std,
                threads_per_host_ex=threads_per_host_ex,
                decrease_factor=decrease_factor,
                cooldown_seconds=cooldown_seconds,
            )
        else:
            _global_adaptive_controller.configure(
                max_threads=max_threads,
                chunks_per_lease=chunks_per_lease,
                threads_per_host_std=threads_per_host_std,
                threads_per_host_ex=threads_per_host_ex,
                decrease_factor=decrease_factor,
                cooldown_seconds=cooldown_seconds,
            )
        snapshot = get_probe_result_cache()
        _global_adaptive_controller.update_hosts(
            "std", list(snapshot.get("standard") or [])
        )
        _global_adaptive_controller.update_hosts(
            "ex", list(snapshot.get("extended") or [])
        )
        return _global_adaptive_controller


def _reset_global_adaptive_controller() -> None:
    """重置全局自适应学习状态；用于线程池/配置生命周期切换。"""
    global _global_adaptive_controller
    with _global_adaptive_controller_lock:
        if _global_adaptive_controller is not None:
            _global_adaptive_controller.reset()
        _global_adaptive_controller = None


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
    统一输出并行获取日志：YAML 级别过滤后的包级 logger + 可选回调。

    输入：
    1. level: 代码中固定的日志级别（debug/info/error）。
    2. message: 日志消息。
    3. detail: 可选结构化明细。
    输出：
    1. 无返回值。
    """
    if not is_event_enabled(level):
        return

    text = str(message)
    if detail:
        try:
            text = f"{text} | {json.dumps(detail, ensure_ascii=False, default=str)}"
        except Exception:
            text = f"{text} | {detail}"
    callback = _log_callback
    if callback is None:
        emit_log(level, text)
        return
    try:
        callback(event_level_name(level), str(message), detail)
    except Exception as exc:
        emit_log("error", "[Parallel Logger Error] %s", exc)
        emit_log(level, text)


def _get_global_thread_runtime(max_workers):
    """输入线程数，输出唯一运行时；旧代未回收时禁止建立新代。"""
    global _thread_runtime
    from zsdtdx.engine.thread_runtime import ThreadRuntime

    with _global_pool_lock:
        if _thread_runtime is not None and _thread_runtime.stopped.is_set():
            raise RuntimeError("线程回收未完成；请重试 destroy_parallel_fetcher")
        if _thread_runtime is not None and _thread_runtime.thread_count != max_workers:
            _shutdown_global_pool()
        if _thread_runtime is None:
            _thread_runtime = ThreadRuntime(
                max_workers, get_fetcher().task_chunk_max_inflight_multiplier
            )
        return _thread_runtime


def _refresh_thread_hosts(sources):
    """输入已探测行情侧，输出无；向已存在运行时补充新侧，不重复探测。"""
    from zsdtdx.engine.unified_client import get_probe_result_cache

    controller = _global_adaptive_controller
    if controller is not None:
        snapshot = get_probe_result_cache()
        for source in sources:
            key = "standard" if source in {"std", "standard"} else "extended"
            controller.update_hosts(
                "std" if key == "standard" else "ex", snapshot.get(key) or []
            )


def _run_business_call(runtime, fn, payload):
    """输入业务执行体，输出业务结果；统一尝试预算、重试和连接回收。"""
    fetcher = get_fetcher()
    client = _ensure_shared_client_context()
    for attempt in range(fetcher.chunk_retry_max_attempts + 1):
        if runtime.stopped.is_set():
            raise RuntimeError("fetcher restarted or destroyed")
        success = False
        error_text = ""
        for pool in (client.std_pool, client.ex_pool):
            pool.begin_attempt()
        try:
            result = runtime.attempt(fn, fetcher.chunk_timeout_seconds, payload)
            if runtime.stopped.is_set():
                raise RuntimeError("fetcher restarted or destroyed")
            success = True
            return result
        except Exception as exc:
            error_text = str(exc) or type(exc).__name__
            # stop 会主动关闭尝试中的 socket；由此冒出的 TimeoutError 属于取消，
            # 不能混入真实网络超时统计，否则同一次停机会产生两种终态原因。
            if runtime.stopped.is_set():
                raise RuntimeError("fetcher restarted or destroyed") from exc
            if (
                attempt == fetcher.chunk_retry_max_attempts
                or isinstance(exc, CancelledError)
                or getattr(exc, "reason", None) == "cancelled"
                or not getattr(exc, "retryable", True)
            ):
                raise
        finally:
            contacted = max(
                (client.std_pool, client.ex_pool),
                key=lambda pool: pool._get_thread_data().get("last_lease_at", 0),
            )
            for pool in (client.std_pool, client.ex_pool):
                pool.release_thread(
                    success=success,
                    congested=not success
                    and attempt == fetcher.chunk_retry_max_attempts
                    and pool is contacted
                    and _is_transport_congestion_error(error_text),
                    discard=not success,
                )


def _run_thread_chunk(runtime, payload):
    """输入运行时和 chunk，输出事件报告；每次重试单独预算，结束后归还真实租约。"""
    from zsdtdx.engine.columnar_kline import (
        fetch_chunk_columns,
        materialize_task_columns,
    )

    prep = _prepare_one_task_chunk(payload)
    if "early_report" in prep:
        return prep["early_report"]
    client = _ensure_shared_client_context()
    last_error = ""
    for attempt in range(prep["chunk_retry_max"] + 1):
        success = False
        for pool in (client.std_pool, client.ex_pool):
            pool.begin_attempt()
        if attempt:
            _log_chunk_retry(prep, retry_count=attempt, error_text=last_error)
        try:
            result = runtime.attempt(
                fetch_chunk_columns,
                prep["chunk_timeout"],
                client,
                prep["normalized_tasks"],
                prep["task_kind"],
                prep["qfq"],
            )
            # 交付适配在计时内核之外，列式结果可独立交付。
            if runtime.stopped.is_set():
                raise RuntimeError("fetcher restarted or destroyed")
            target_pool = (
                client.std_pool if prep["route_source"] == "std" else client.ex_pool
            )
            result["_active_host"] = target_pool.get_active_host()
            conversion_started, conversion_cpu = time.perf_counter(), time.thread_time()
            for item in result["results"]:
                item["rows"] = materialize_task_columns(client, item)
            runtime.record_stage(
                "materialization",
                time.perf_counter() - conversion_started,
                time.thread_time() - conversion_cpu,
            )
            report = _assemble_chunk_success_report(prep, result)
            if runtime.stopped.is_set():
                raise RuntimeError("fetcher restarted or destroyed")
            success = True
            report["congested"] = False
            return report
        except Exception as exc:
            last_error = str(exc)[:200] or type(exc).__name__
            if runtime.stopped.is_set():
                break
        finally:
            final = not success and (
                attempt >= prep["chunk_retry_max"] or runtime.stopped.is_set()
            )
            for pool in (client.std_pool, client.ex_pool):
                pool.release_thread(
                    success=success,
                    congested=final
                    and pool.source == prep["route_source"]
                    and _is_transport_congestion_error(last_error),
                    discard=not success,
                )
    report = _build_chunk_timeout_error_report(prep, last_error)
    report["congested"] = _is_transport_congestion_error(last_error)
    return report


def _snapshot_pool_epoch() -> int:
    """
    读取全局线程池版本号快照。

    输入：
    1. 无显式输入参数。
    输出：
    1. 当前线程池版本号（单调递增整数）。
    用途：
    1. 让 async 自动预热逻辑判断“线程池是否已重建，需要重新预热”。
    边界条件：
    1. 仅用于当前解释器内的状态比较。
    """
    with _global_pool_lock:
        return int(_global_pool_epoch)


def _shutdown_global_pool():
    """输入无，输出无；中断旧代并等待受控 IO 退出，不提前复用其配额。"""
    global _thread_runtime, _global_pool_epoch
    with _global_pool_lock:
        runtime = _thread_runtime
        if runtime is None:
            _reset_global_adaptive_controller()
            return
        _global_pool_epoch += 1
        pending = runtime.stop(timeout=5.0)
        if pending:
            raise RuntimeError(f"线程回收未完成: {pending}")
        _thread_runtime = None
        _close_shared_client_context()
        _reset_global_adaptive_controller()


def destroy_parallel_fetcher() -> Dict[str, Any]:
    """输入无，输出线程回收摘要；重复销毁安全，未回收工作明确抛错。"""
    started = time.monotonic()
    runtime = _thread_runtime
    _shutdown_global_pool()
    return {
        "existed": runtime is not None,
        "old_threads": runtime.thread_count if runtime else 0,
        "pool_epoch": _snapshot_pool_epoch(),
        "pending_tasks": 0,
        "elapsed_seconds": time.monotonic() - started,
    }


atexit.register(_shutdown_global_pool)


def force_restart_parallel_fetcher(
    *, prewarm=True, prewarm_timeout_seconds=60.0, max_rounds=3
):
    """输入预热设置，输出重启摘要；旧代未实际退出时禁止新代扩容。"""
    result = destroy_parallel_fetcher()
    result["prewarm_summary"] = (
        prewarm_parallel_fetcher(
            require_all_threads=True,
            timeout_seconds=prewarm_timeout_seconds,
            max_rounds=max_rounds,
        )
        if prewarm
        else None
    )
    return result


def _close_shared_client_context():
    """输入无，输出无；关闭执行器持有的客户端，不影响用户 with 上下文。"""
    global _shared_client_holder, _shared_client_context
    holder = _shared_client_holder
    _shared_client_holder = _shared_client_context = None
    if holder is not None:
        holder.close()


def _target_pool_names(target: str = "both") -> Tuple[str, ...]:
    """输入 std/ex/both，输出需要操作的连接池属性名；非法值回退 both。"""
    normalized = str(target or "both").strip().lower()
    if normalized == "std":
        return ("std_pool",)
    if normalized == "ex":
        return ("ex_pool",)
    return ("std_pool", "ex_pool")


def _cleanup_dead_thread_connections(target: str = "both") -> None:
    """
    清理已退出短生命周期线程遗留的连接。

    输入：
    1. 无显式输入参数。
    输出：
    1. 无返回值。
    用途：
    1. 防止公司信息内部短生命周期线程遗留 socket。
    边界条件：
    1. 若上下文未初始化或不支持清理接口则静默跳过。
    """
    context = _shared_client_context
    if context is None:
        return
    for pool_name in _target_pool_names(target):
        pool = getattr(context, pool_name, None)
        cleaner = getattr(pool, "cleanup_dead_thread_connections", None)
        if callable(cleaner):
            try:
                cleaner()
            except Exception:
                pass


def _ensure_shared_client_context():
    """输入无，输出共享元数据客户端；网络连接由独占租约保护，不进入用户上下文栈。"""
    global _shared_client_holder, _shared_client_context
    if _shared_client_context is not None:
        return _shared_client_context
    with _shared_client_lock:
        if _shared_client_context is None:
            from zsdtdx.engine.thread_runtime import LeasedFailoverPool
            from zsdtdx.engine.unified_client import get_probe_result_cache

            fetcher = get_fetcher()
            snapshot = get_probe_result_cache()
            holder = UnifiedTdxClient(
                config_path=_active_config_path,
                presorted_hosts=snapshot,
                shared_runtime_client=True,
            )
            controller = _get_global_adaptive_controller(
                max_threads=fetcher.num_threads,
                chunks_per_lease=1,
                threads_per_host_std=fetcher.threads_per_host_std,
                threads_per_host_ex=fetcher.threads_per_host_ex,
                decrease_factor=fetcher.adaptive_decrease_factor,
                cooldown_seconds=fetcher.adaptive_cooldown_seconds,
            )
            for source, name in (("std", "std_pool"), ("ex", "ex_pool")):
                original = getattr(holder, name)
                setattr(holder, name, LeasedFailoverPool(original, controller, source))
            holder.std_pool.peer = holder.ex_pool
            holder.ex_pool.peer = holder.std_pool
            _shared_client_holder = _shared_client_context = holder
    return _shared_client_context


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


def _is_transport_congestion_error(error_text: str) -> bool:
    """
    判断错误是否属于应降低地址配额的传输失败。

    输入：error_text 为原始错误文本。
    输出：连接不可用或传输超时返回 True。
    用途：重试耗尽后的拥塞标记。成功切站或重试成功不得调用本函数去覆盖已清除的标记。
    边界：空字符串返回 False。同时识别 timeout、timed out 与「超时」。
    """
    if _is_connection_unavailable_error(error_text):
        return True
    plain = str(error_text or "").strip().lower()
    if plain == "":
        return False
    return ("timeout" in plain) or ("timed out" in plain) or ("超时" in plain)


def prewarm_parallel_fetcher(
    *, require_all_threads=True, timeout_seconds=60.0, max_rounds=3, target_threads=None
):
    """输入预热预算，输出线程/编译就绪摘要；预热不建立行情连接。"""
    from zsdtdx.parser.kline_columns import warmup_kline_decoders

    started = time.monotonic()
    fetcher = get_fetcher()
    runtime = _get_global_thread_runtime(fetcher.num_threads)
    target = min(
        runtime.thread_count,
        max(1, int(target_threads or runtime.thread_count)),
    )
    warmup_kline_decoders()
    deadline = started + max(0.0, float(timeout_seconds))
    rounds = max(1, int(max_rounds))
    idents, completed_rounds = set(), 0
    for round_index in range(rounds):
        remaining_budget = deadline - time.monotonic()
        if remaining_budget <= 0 or runtime.stopped.is_set():
            break
        round_deadline = time.monotonic() + remaining_budget / (rounds - round_index)
        barrier = threading.Barrier(target)

        def ready(barrier=barrier, round_deadline=round_deadline):
            """输入本轮屏障和截止时间，输出线程标识；不同线程共同通过才算就绪。"""
            barrier.wait(timeout=max(0.001, round_deadline - time.monotonic()))
            return threading.get_ident()

        futures = []
        try:
            for _ in range(target):
                futures.append(
                    runtime.submit(
                        ready,
                        submit_timeout=max(0.0, round_deadline - time.monotonic()),
                    )
                )
            done, pending = wait(
                futures, timeout=max(0.0, round_deadline - time.monotonic())
            )
            idents.update(
                f.result() for f in done if not f.cancelled() and f.exception() is None
            )
        except (TimeoutError, RuntimeError):
            pass
        finally:
            barrier.abort()
            for future in futures:
                future.cancel()
        completed_rounds += 1
        if len(idents) >= target:
            break
    idents = sorted(idents)
    if len(idents) < target and require_all_threads:
        raise RuntimeError("线程预热超时，未达到目标就绪数量")
    return {
        "target_threads": target,
        "ready_threads": len(idents),
        "thread_ids": idents,
        "decoder_ready": True,
        "rounds": completed_rounds,
        "pool_epoch": _snapshot_pool_epoch(),
        "elapsed_seconds": time.monotonic() - started,
    }


# =============================================================================
# 业务逻辑
# =============================================================================


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
    1. 作为线程调度和列式抓取的最小分片单元。
    边界条件：
    1. tasks 允许为空，但正常调度路径不会生成空分片。
    """

    chunk_id: str
    code: str
    freq: str
    tasks: List[Dict[str, str]]
    route_source: str = "std"

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
        1. 可独立提交给线程运行时的字典。
        用途：
        1. 用于 线程池 的参数传输。
        边界条件：
        1. tasks 会被复制，避免调用方原地修改导致竞态。
        """
        return {
            "chunk_id": str(self.chunk_id),
            "code": str(self.code),
            "freq": str(self.freq),
            "route_source": str(self.route_source),
            "task_count": int(self.task_count),
            "enable_cache": bool(enable_cache),
            "tasks": [dict(item) for item in self.tasks],
        }


def _infer_stock_task_route_source(code: str) -> str:
    """
    根据股票代码契约推断行情侧。

    输入：支持 sh./sz./bj./hk. 前缀的股票代码。
    输出："std" 或 "ex"。
    边界：无前缀时 5 位数字视为港股通，6 位数字视为标准行情；其它格式抛错。
    """
    raw_code = str(code or "").strip()
    prefix = ""
    compact = raw_code
    if "." in raw_code:
        prefix, compact = raw_code.split(".", 1)
        prefix = prefix.strip().lower()
        compact = compact.strip()
    if prefix == "hk":
        return "ex"
    if prefix in {"sh", "sz", "bj"}:
        return "std"
    if compact.isdigit() and len(compact) == 5:
        return "ex"
    if compact.isdigit() and len(compact) == 6:
        return "std"
    raise ValueError(f"无法判断股票代码行情路由: {code}")


def _normalize_task_payload(task: Dict[str, Any]) -> Dict[str, str]:
    """
    标准化 task 字段，确保线程间传递结构稳定。

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
    route_source = str(task.get("_route_source", "")).strip().lower()
    if route_source not in {"std", "ex"}:
        route_source = _infer_stock_task_route_source(code)
    return {
        "code": code,
        "freq": freq,
        "start_time": start_time,
        "end_time": end_time,
        "_route_source": route_source,
    }


def _normalize_index_task_payload(task: Dict[str, Any]) -> Dict[str, Any]:
    """输入指数任务，输出标准化字典。边界：缺 index_name 时抛 ValueError。"""
    return _normalize_symbol_kline_task(task, "index_name")


def _normalize_block_task_payload(task: Dict[str, Any]) -> Dict[str, Any]:
    """输入板块任务，输出标准化字典。边界：缺 block_name 时抛 ValueError。"""
    return _normalize_symbol_kline_task(task, "block_name")


def _normalize_symbol_kline_task(
    task: Dict[str, Any], symbol_field: str
) -> Dict[str, Any]:
    """
    输入名称类 K 线任务，输出线程调度使用的标准字典。

    输入：task 含 symbol_field、freq、start_time、end_time，可带路由字段。
    输出：标准化任务字典。
    用途：指数与板块共用字段整理。
    边界条件：名称或时间为空时抛 ValueError。
    """
    if not isinstance(task, dict):
        raise ValueError("task 必须是 dict")
    symbol = str(task.get(symbol_field, "")).strip()
    freq = str(task.get("freq", "")).strip().lower()
    start_time = str(task.get("start_time", "")).strip()
    end_time = str(task.get("end_time", "")).strip()
    if symbol == "":
        raise ValueError(f"task.{symbol_field} 不能为空")
    if freq == "":
        raise ValueError("task.freq 不能为空")
    if start_time == "":
        raise ValueError("task.start_time 不能为空")
    if end_time == "":
        raise ValueError("task.end_time 不能为空")
    normalized: Dict[str, Any] = {
        symbol_field: symbol,
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
        normalized["_route_source"] = route_source
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
) -> Dict[str, Any]:
    """
    构建标准 task payload。

    输入：
    1. task: 标准化任务字典。
    2. rows: 任务K线结果列表。
    3. error: 失败原因，成功时为 None。
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
    }
    return payload


def _build_task_payload_for_kind(
    task_kind: str,
    *,
    task: Dict[str, Any],
    rows: Optional[List[Dict[str, Any]]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    按 task_kind 构建标准或指数 task payload。

    输入：
    1. task_kind: `"stock"` 或 `"index"`。
    2. task/rows/error: 与 `_build_task_payload` 相同。
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
        )
    if kind == "block":
        return _build_symbol_kline_payload(
            "block_name",
            task=task,
            rows=rows,
            error=error,
        )
    return _build_task_payload(
        task=task,
        rows=rows,
        error=error,
    )


def _build_index_task_payload(
    *,
    task: Dict[str, Any],
    rows: Optional[List[Dict[str, Any]]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    构建标准指数 task payload。

    输入：
    1. task: 标准化指数任务字典。
    2. rows: 任务 K 线结果列表。
    3. error: 失败原因，成功时为 None。
    输出：
    1. 标准化指数事件 payload，结构固定。
    用途：
    1. 统一指数队列输出与函数返回的数据契约。
    边界条件：
    1. rows 为空时输出空列表，不返回 None。
    """
    return _build_symbol_kline_payload("index_name", task=task, rows=rows, error=error)


def _build_symbol_kline_payload(
    symbol_field: str,
    *,
    task: Dict[str, Any],
    rows: Optional[List[Dict[str, Any]]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    输入主键字段与任务结果，输出事件 payload。

    输入：symbol_field 为 index_name 或 block_name。
    输出：event=data 的字典。
    用途：指数与板块共用出口，不再互相改写字段。
    边界条件：rows 为空时输出空列表。
    """
    return {
        "event": "data",
        "task": {
            symbol_field: str(task.get(symbol_field, "")),
            "freq": str(task.get("freq", "")),
            "start_time": str(task.get("start_time", "")),
            "end_time": str(task.get("end_time", "")),
        },
        "rows": list(rows or []),
        "error": None if error is None else str(error),
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
    if kind == "block":
        return {
            "kind": "block",
            "symbol_field": "block_name",
            "normalize_item": _normalize_block_task_payload,
            "build_payload": lambda **kwargs: _build_symbol_kline_payload(
                "block_name", **kwargs
            ),
            "log_tag": "block chunk",
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
                build_payload(task=fallback_task, rows=[], error=error_text)
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
            }
        }

    return {
        "task_kind": task_kind,
        "profile": profile,
        "symbol_field": symbol_field,
        "build_payload": build_payload,
        "log_tag": str(profile["log_tag"]),
        "chunk_id": chunk_id,
        "route_source": str(
            chunk_payload.get("route_source")
            or normalized_tasks[0].get("_route_source")
            or normalized_tasks[0].get("_index_route_source")
            or "std"
        )
        .strip()
        .lower(),
        "preferred_host": chunk_payload.get("preferred_host"),
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
            1.0, float(chunk_payload.get("chunk_timeout_seconds", 15.0) or 15.0)
        ),
        "chunk_retry_max": max(
            0, int(chunk_payload.get("chunk_retry_max_attempts", 2) or 0)
        ),
        "qfq": bool(chunk_payload.get("qfq", True)),
    }


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
    active_host = str((chunk_result or {}).get("_active_host") or "").strip()

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
        "active_host": active_host,
        "task_detail": task_detail,
        "payloads": payloads,
        "failures": failures,
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
    1. 本函数仅记录重试日志；连接丢弃与换站由线程连接租约统一处理。
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
            "debug",
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
        "debug",
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
    chunk_id = str(prep["chunk_id"])
    raw_tasks = list(prep["raw_tasks"])
    normalized_tasks = list(prep["normalized_tasks"])
    payloads = list(prep["payloads"])
    failures = list(prep["failures"])
    err = str(error_text or "chunk timeout")[:200]

    for task in normalized_tasks:
        payloads.append(build_payload(task=task, rows=[], error=err))
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


def _fetch_future_batch(task_list):
    """输入期货任务元组，输出边缘 DataFrame 与失败列表；分页期间保持列式。"""
    from zsdtdx.engine.columnar_kline import (
        fetch_chunk_columns,
        materialize_task_columns,
    )

    client = _ensure_shared_client_context()
    frames, failures = [], []
    for _idx, code, freq, start, end in task_list:
        try:
            result = fetch_chunk_columns(
                client,
                [dict(code=code, freq=freq, start_time=start, end_time=end)],
                kind="future",
                qfq=False,
            )
            for item in result["results"]:
                if item["error"]:
                    failures.append((code, freq, item["error"]))
                else:
                    frames.append(materialize_task_columns(client, item))
        except Exception as exc:
            failures.append((code, freq, str(exc)[:200]))
    return {
        "data": pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(),
        "failures": failures,
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
        # get_future_kline 的 DataFrame 批处理仍使用这四项，不再从配置读取。
        self.parallel_total_timeout_seconds = 300.0
        self.force_recycle_on_timeout = True
        self.timeout_fallback_to_serial = True
        self.task_chunk_cache_min_tasks = 2
        self.task_chunk_max_inflight_multiplier = self._safe_int_config(
            parallel_cfg.get("task_chunk_max_inflight_multiplier"),
            default=2,
            minimum=1,
        )
        self.adaptive_concurrency_enabled = True
        self.threads_per_host_std = self._safe_int_config(
            parallel_cfg.get("threads_per_host_std"),
            default=4,
            minimum=1,
        )
        self.threads_per_host_ex = self._safe_int_config(
            parallel_cfg.get("threads_per_host_ex"),
            default=4,
            minimum=1,
        )
        self.adaptive_decrease_factor = min(
            0.95,
            self._safe_float_config(
                parallel_cfg.get("adaptive_decrease_factor"),
                default=0.75,
                minimum=0.1,
            ),
        )
        self.adaptive_cooldown_seconds = self._safe_float_config(
            parallel_cfg.get("adaptive_cooldown_seconds"),
            default=1.5,
            minimum=0.0,
        )
        self.auto_prewarm_on_async = True
        self.auto_prewarm_require_all_threads = False
        self.auto_prewarm_timeout_seconds = 60.0
        self.auto_prewarm_max_rounds = 3
        self.chunk_reconnect_on_unavailable = True
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
        self.chunk_watchdog_grace_seconds = 10.0
        self.num_threads = self._safe_int_config(
            parallel_cfg.get("thread_workers"), default=44, minimum=1
        )
        self.company_info_stock_workers = 1
        self.company_info_category_workers = 1
        self.company_info_codes_per_chunk = 1
        self.company_info_max_inflight_multiplier = 2
        self._auto_prewarm_lock = threading.Lock()
        # 地址探测延迟到已知任务路由后按侧执行，避免纯 std/ex 调用触碰无关地址池。

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
        from zsdtdx.engine.unified_client import _resolve_zsdtdx_config_path

        if self.config_path:
            return _resolve_zsdtdx_config_path(self.config_path)
        if _active_config_path:
            return _resolve_zsdtdx_config_path(_active_config_path)
        return _resolve_zsdtdx_config_path(None)

    def _load_config(self) -> Dict:
        """
        加载 YAML 配置并规范化 config_path 为绝对路径字符串。

        输入：无。
        输出：合并后的配置字典；失败时返回空 dict。
        边界条件：与 simple_api 全局 _active_config_path 对齐；无激活路径时用包内默认；
        用户文件与内置深合并（未知键丢弃、列表整段替换）。
        """
        try:
            from zsdtdx.engine.unified_client import _load_merged_zsdtdx_config

            resolved = self._resolve_fetcher_config_path()
            self.config_path = str(resolved)
            return _load_merged_zsdtdx_config(str(resolved))
        except Exception as e:
            _emit_log("error", f"[ParallelKlineFetcher] 配置加载失败: {e}, 使用默认值")
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

    def _ensure_async_prewarm(self, target_threads=None):
        """输入可选工作数，输出无；内核编译按需完成，不对异步启动建立网络连接。"""
        _get_global_thread_runtime(self.num_threads)

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
            Tuple[str, str, str],
            List[Tuple[int, Dict[str, Any], pd.Timestamp, pd.Timestamp]],
        ] = defaultdict(list)
        to_sortable_ts = _to_sortable_task_ts
        for index, raw_task in enumerate(tasks or []):
            if not isinstance(raw_task, dict):
                raise ValueError("task 必须是 dict")
            normalized_task = dict(raw_task)
            symbol = str(normalized_task.get(group_key, "")).strip()
            freq = str(normalized_task.get("freq", "")).strip()
            route_source = (
                str(
                    normalized_task.get("_route_source")
                    or normalized_task.get("_index_route_source")
                    or "std"
                )
                .strip()
                .lower()
            )
            if route_source not in {"std", "ex"}:
                route_source = "std"
            normalized_task["_route_source"] = route_source
            grouped[(route_source, symbol, freq)].append(
                (
                    int(index),
                    normalized_task,
                    to_sortable_ts(normalized_task.get("start_time")),
                    to_sortable_ts(normalized_task.get("end_time")),
                )
            )

        chunks: List[TaskChunk] = []
        for chunk_idx, (
            (route_source, symbol, freq),
            items,
        ) in enumerate(grouped.items(), start=1):
            items.sort(key=lambda item: (item[2], item[3], item[0]))
            chunk_tasks = [dict(item[1]) for item in items]
            chunks.append(
                TaskChunk(
                    chunk_id=f"{route_source}:{symbol}:{freq}:{chunk_idx}",
                    code=str(symbol),
                    freq=str(freq),
                    tasks=chunk_tasks,
                    route_source=str(route_source),
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

    def _iter_task_payloads_threaded(self, tasks, task_kind="stock", qfq=True):
        """输入任务，输出完成事件；sync 与 async 共用线程及配额预算。"""
        yield from self._iter_task_payloads_parallel_chunked(
            tasks, task_kind=task_kind, qfq=qfq
        )

    def _iter_task_payloads_parallel_chunked(self, tasks, task_kind="stock", qfq=True):
        """输入标准化任务，输出逐任务事件；窗口有界，取消和异常补齐失败。"""
        from zsdtdx.engine.unified_client import _ensure_availability_hosts_cache
        from zsdtdx.parser.kline_columns import warmup_kline_decoders

        if not tasks:
            return
        sources = {
            str(t.get("_route_source") or t.get("_index_route_source") or "std")
            for t in tasks
        }
        _ensure_availability_hosts_cache(
            config_path=self.config_path,
            required_sources={
                "standard" if source == "std" else "extended" for source in sources
            },
        )
        _refresh_thread_hosts(sources)
        warmup_kline_decoders()
        runtime = _get_global_thread_runtime(self.num_threads)
        grouped = self._build_grouped_task_chunks(
            tasks, group_key=_kline_symbol_field(task_kind)
        )
        chunks = {
            source: deque(c for c in grouped if c.route_source == source)
            for source in ("std", "ex")
        }
        # 扩展侧不放大提交窗口，避免大量等待扩展站的线程饿死标准侧。
        from zsdtdx.engine.unified_client import get_probe_result_cache

        ex_hosts = get_probe_result_cache().get("extended") or []
        ex_window = max(
            1, min(self.num_threads, len(ex_hosts) * self.threads_per_host_ex)
        )
        next_source = "std"
        pending = {}
        window = self.num_threads * self.task_chunk_max_inflight_multiplier
        while any(chunks.values()) or pending:
            while (
                any(chunks.values())
                and len(pending) < window
                and not runtime.stopped.is_set()
            ):
                ex_pending = sum(
                    chunk.route_source == "ex" for chunk, _ in pending.values()
                )
                order = (next_source, "ex" if next_source == "std" else "std")
                source = next(
                    (
                        s
                        for s in order
                        if chunks[s] and (s != "ex" or ex_pending < ex_window)
                    ),
                    None,
                )
                if source is None:
                    break
                chunk = chunks[source].popleft()
                next_source = "ex" if source == "std" else "std"
                payload = chunk.to_payload(enable_cache=True)
                payload.update(
                    task_kind=task_kind,
                    qfq=qfq,
                    chunk_timeout_seconds=self.chunk_timeout_seconds,
                    chunk_retry_max_attempts=self.chunk_retry_max_attempts,
                )
                _emit_log(
                    "debug",
                    "[Task Thread] 分配 chunk 任务",
                    {
                        "stage": "chunk_dispatched",
                        "chunk_id": chunk.chunk_id,
                        "chunk_task_count": chunk.task_count,
                        "route_source": chunk.route_source,
                        "fetch_tasks_total": len(tasks),
                        **self._build_chunk_task_detail(chunk.tasks),
                    },
                )
                try:
                    future = runtime.submit(_run_thread_chunk, runtime, payload)
                    pending[future] = (chunk, time.monotonic())
                except Exception as exc:
                    for task in chunk.tasks:
                        yield _build_task_payload_for_kind(
                            task_kind, task=task, rows=[], error=str(exc)
                        )
            if runtime.stopped.is_set():
                for source_chunks in chunks.values():
                    while source_chunks:
                        chunk = source_chunks.popleft()
                        for task in chunk.tasks:
                            yield _build_task_payload_for_kind(
                                task_kind,
                                task=task,
                                rows=[],
                                error="fetcher restarted or destroyed",
                            )
            if not pending:
                continue
            done, _ = wait(list(pending), timeout=0.1, return_when=FIRST_COMPLETED)
            budget = (
                self.chunk_timeout_seconds * (1 + self.chunk_retry_max_attempts)
                + self.chunk_watchdog_grace_seconds
            )
            for future, (chunk, submitted) in list(pending.items()):
                started = getattr(future, "_zsdtdx_state", {}).get("started")
                if (
                    future not in done
                    and not runtime.stopped.is_set()
                    and (started is None or time.monotonic() - started <= budget)
                ):
                    continue
                del pending[future]
                if (
                    future in done
                    and not future.cancelled()
                    and not runtime.stopped.is_set()
                ):
                    try:
                        report = future.result()
                    except Exception as exc:
                        report = {
                            "payloads": [
                                _build_task_payload_for_kind(
                                    task_kind, task=t, rows=[], error=str(exc)
                                )
                                for t in chunk.tasks
                            ]
                        }
                    failures = [
                        {**item["task"], "error": item["error"]}
                        for item in report["payloads"]
                        if item.get("error")
                    ]
                    _emit_log(
                        "debug",
                        "[Task Thread] chunk 完成",
                        {
                            "stage": "chunk_completed",
                            "chunk_id": chunk.chunk_id,
                            "chunk_task_count": chunk.task_count,
                            "chunk_hit_tasks": report.get("chunk_hit_tasks", 0),
                            "chunk_network_page_calls": report.get(
                                "chunk_network_page_calls", 0
                            ),
                            "active_host": report.get("active_host", ""),
                            "failure_tasks": failures[
                                : int(_read_log_detail_options().get("sample_size", 8))
                            ],
                        },
                    )
                    for item in report["payloads"]:
                        yield item
                else:
                    future.cancel()
                    for task in chunk.tasks:
                        yield _build_task_payload_for_kind(
                            task_kind,
                            task=task,
                            rows=[],
                            error="fetcher restarted or destroyed"
                            if runtime.stopped.is_set()
                            else "chunk watchdog timeout",
                        )

    def _fetch_tasks_sync(
        self,
        *,
        task_kind: str,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
        qfq: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        同步执行 task 列表（stock/index 共用），并实时写入队列。

        输入：task_kind、tasks、queue、preprocessor_operator、qfq。
        输出：处理后的 task payload 列表。
        边界：tasks 为空时返回 [] 并发送 done；与 async 共享全局有界线程预算。
        """
        kind = str(task_kind or "stock").strip().lower()
        normalize_item = _chunk_task_kind_profile(kind)["normalize_item"]
        sync_log_label = (
            "板块线程 chunk 执行模式"
            if kind == "block"
            else "指数线程 chunk 执行模式"
            if kind == "index"
            else "股票线程 chunk 执行模式"
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
                f"[Task Sync] {sync_log_label} "
                f"(thread_workers={self.num_threads}, "
                f"任务数={total_tasks})"
            ),
        )
        iterator = self._iter_task_payloads_threaded(
            normalized_tasks, task_kind=kind, qfq=bool(qfq)
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
        qfq: bool = True,
    ) -> List[Dict[str, Any]]:
        """同步执行股票 task 列表，并实时写入队列。"""
        return self._fetch_tasks_sync(
            task_kind="stock",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
            qfq=bool(qfq),
        )

    def fetch_index_tasks_sync(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
        qfq: bool = True,
    ) -> List[Dict[str, Any]]:
        """同步执行指数 task 列表，并实时写入队列。"""
        return self._fetch_tasks_sync(
            task_kind="index",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
            qfq=bool(qfq),
        )

    def fetch_block_tasks_sync(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
    ) -> List[Dict[str, Any]]:
        """
        输入板块任务，输出同步结果列表。

        输入：tasks 含 block_name 与路由。
        输出：payload 列表。
        用途：板块 K 线 sync 入口。
        边界条件：空任务返回空列表并发送 done。
        """
        return self._fetch_tasks_sync(
            task_kind="block",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
            qfq=False,
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
        qfq: bool = True,
    ) -> StockKlineJob:
        """
        异步启动 task 执行（stock/index 共用）并立即返回句柄。

        输入：task_kind、tasks、queue、preprocessor_operator、qfq。
        输出：StockKlineJob 异步句柄。
        边界：任务异常由 job.exception()/job.result() 暴露。
        """
        kind = str(task_kind or "stock").strip().lower()
        normalize_item = _chunk_task_kind_profile(kind)["normalize_item"]
        async_log_label = (
            "线程池板块 chunk 执行模式"
            if kind == "block"
            else "线程池指数 chunk 执行模式"
            if kind == "index"
            else "线程池 chunk 执行模式"
        )
        thread_prefix = (
            "zsdtdx_block_kline_async"
            if kind == "block"
            else "zsdtdx_index_kline_async"
            if kind == "index"
            else "zsdtdx_stock_kline_async"
        )

        self._validate_queue(queue)
        normalized_tasks = [normalize_item(item) for item in list(tasks or [])]
        total_tasks = int(len(normalized_tasks))
        symbol_field = _kline_symbol_field(kind)
        unique_chunks = {
            (
                str(item.get("_route_source", "")).strip().lower(),
                str(item.get(symbol_field, "")).strip(),
                str(item.get("freq", "")).strip(),
            )
            for item in normalized_tasks
        }
        useful_threads = max(1, min(self.num_threads, len(unique_chunks)))
        self._ensure_async_prewarm(target_threads=useful_threads)

        def _run_async_coordinator() -> List[Dict[str, Any]]:
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
                    f"(num_threads={self.num_threads}, "
                    f"任务数={total_tasks})"
                ),
            )
            iterator = self._iter_task_payloads_parallel_chunked(
                normalized_tasks, task_kind=kind, qfq=bool(qfq)
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
        future = executor.submit(_run_async_coordinator)

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
        qfq: bool = True,
    ) -> StockKlineJob:
        """异步启动股票 task 执行并立即返回句柄。"""
        return self._fetch_tasks_async(
            task_kind="stock",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
            qfq=bool(qfq),
        )

    def fetch_index_tasks_async(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
        qfq: bool = True,
    ) -> StockKlineJob:
        """异步启动指数 task 执行并立即返回句柄。"""
        return self._fetch_tasks_async(
            task_kind="index",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
            qfq=bool(qfq),
        )

    def fetch_block_tasks_async(
        self,
        *,
        tasks: List[Dict[str, Any]],
        queue: Optional[Any] = None,
        preprocessor_operator: Optional[
            Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
        ] = None,
    ) -> StockKlineJob:
        """
        输入板块任务，输出异步句柄。

        输入：tasks 含 block_name 与路由。
        输出：StockKlineJob。
        用途：板块 K 线 async 入口。
        边界条件：异常由 job.result() 暴露。
        """
        return self._fetch_tasks_async(
            task_kind="block",
            tasks=tasks,
            queue=queue,
            preprocessor_operator=preprocessor_operator,
            qfq=False,
        )

    def fetch_future_kline(
        self,
        codes: List[str],
        freqs: List[str],
        start_time: Optional[str],
        end_time: Optional[str],
    ) -> pd.DataFrame:
        """
        获取期货 K 线数据。

        输入: 期货代码列表、频率列表、时间范围
        输出: 合并后的单个 DataFrame
        """
        tasks = [(code, freq) for code in codes for freq in freqs]

        if len(tasks) <= 0:
            _emit_log("info", "[Info] 任务数为 0，跳过抓取")
            return pd.DataFrame()

        return self._fetch_future_parallel(tasks, start_time, end_time)

    def _fetch_future_serial(
        self,
        tasks: List[Tuple[str, str]],
        start_time: Optional[str],
        end_time: Optional[str],
    ) -> pd.DataFrame:
        """串行获取期货 K 线，返回合并后的 DataFrame。"""
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
                    batch_count = 0
                    for df in client.get_future_kline(
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
                    _emit_log("error", f"[Serial Fetch Error] {code}/{freq}: {e}")
                    continue

            # 记录失败信息到上下文客户端（如果存在）
            if (
                failures
                and context_client
                and hasattr(context_client, "_record_failure")
            ):
                for code, freq, error in failures:
                    context_client._record_failure(
                        "future_kline", code, "fetch_error", error, freq
                    )
        finally:
            if should_close:
                try:
                    client.close()
                except Exception as exc:
                    _emit_log("error", f"[Serial Fetch] 关闭客户端失败: {exc}")

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def _fetch_future_parallel(self, tasks, start_time, end_time):
        """输入期货组合，输出 DataFrame；逐标的周期有界提交，异常不串行补拉。"""
        from zsdtdx.engine.unified_client import (
            _ensure_availability_hosts_cache,
            get_probe_result_cache,
        )

        _ensure_availability_hosts_cache(
            config_path=getattr(self, "config_path", None),
            required_sources={"extended"},
        )
        _refresh_thread_hosts({"ex"})
        runtime = _get_global_thread_runtime(self.num_threads)
        deadline = time.monotonic() + self.parallel_total_timeout_seconds
        queue = deque(
            [
                (i, code, freq, start_time, end_time)
                for i, (code, freq) in enumerate(tasks)
            ]
        )
        pending, frames, failures = {}, [], []
        host_count = len(get_probe_result_cache().get("extended") or [])
        window = max(1, min(self.num_threads, host_count * self.threads_per_host_ex))
        while pending or queue:
            if time.monotonic() >= deadline or runtime.stopped.is_set():
                reason = (
                    "fetcher restarted or destroyed"
                    if runtime.stopped.is_set()
                    else "future batch timeout"
                )
                for future, task in pending.items():
                    future.cancel()
                    failures.append((task[1], task[2], reason))
                failures.extend((task[1], task[2], reason) for task in queue)
                break
            while queue and len(pending) < window:
                task = queue.popleft()
                try:
                    future = runtime.submit(
                        _run_business_call,
                        runtime,
                        _fetch_future_batch,
                        [task],
                        submit_timeout=max(0.0, deadline - time.monotonic()),
                    )
                    pending[future] = task
                except Exception as exc:
                    failures.append((task[1], task[2], str(exc)))
            if not pending:
                continue
            done, _ = wait(list(pending), timeout=0.1, return_when=FIRST_COMPLETED)
            for future in done:
                task = pending.pop(future)
                try:
                    result = future.result()
                    data = result.get("data")
                    if data is not None and not data.empty:
                        frames.append(data)
                    failures.extend(result.get("failures") or [])
                except Exception as exc:
                    failures.append((task[1], task[2], str(exc)))
        context = UnifiedTdxClient.get_active_context_client()
        if context is not None:
            for code, freq, error in failures:
                context._record_failure(
                    "future_kline", code, "fetch_error", error, freq
                )
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# 全局获取器实例
_fetcher = None


def set_active_config_path(config_path: Optional[str]) -> None:
    """
    设置并行抓取器和共享线程客户端使用的活动配置路径。

    输入：
    1. config_path: 绝对或相对路径；空则清空。
    输出：无。
    边界条件：
    1. 不在此处同步 TCP 探测；由 set_config_path 或建池前 ensure 负责。
    2. 路径变更时销毁旧线程池，下次并行时按新配置重建。
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


def _fetch_one_company_info(payload: Dict[str, Any]):
    """输入单股 payload，输出原始正文；异常交给运行时重试，禁止在此吞异常。"""
    client = _ensure_shared_client_context()
    try:
        return client.get_company_info_content(
            code=payload["code"],
            category=payload.get("category"),
            return_df=False,
            category_workers=1,
        )
    except ValueError as exc:
        raise CompanyInfoFetchError(
            str(exc), reason="invalid_code", retryable=False
        ) from exc


def _fetch_company_info_chunk(runtime, payload: Dict[str, Any]) -> Dict[str, Any]:
    """输入代码分片和运行时，输出逐股终态；逐股预算与重试，不重复已完成股票。

    边界：股票与栏目均串行，不创建嵌套线程；空目录正常结束且不网络重试。
    """
    outcomes = []
    for code in normalize_company_info_codes(payload.get("codes")):
        try:
            rows = _run_business_call(
                runtime,
                _fetch_one_company_info,
                {"code": code, "category": payload.get("category")},
            )
            outcome = company_info_outcome(code, rows=rows)
        except Exception as exc:
            outcome = company_info_outcome(code, error=exc)
        outcomes.append(outcome)
    return {
        "rows": [row for outcome in outcomes for row in outcome["rows"]],
        "errors": [outcome for outcome in outcomes if outcome["status"] != "success"],
        "outcomes": outcomes,
    }


def _iter_company_info_chunk_results(
    *,
    codes: List[str],
    category: Optional[Any],
    auto_prewarm: Optional[bool] = None,
) -> Any:
    """
    按 chunk 提交公司信息并行抓取，产出每个 chunk 的原始结果字典。

    输入：
    1. codes/category: 股票列表与分类过滤。
    2. auto_prewarm: 是否预热；None 跟随配置。
    输出：
    1. 迭代器，元素为 {"rows": list[dict], "errors": list[dict], ...}。
    用途：
    1. sync 聚合与 async 队列推送共用；全程不经过 DataFrame。
    边界条件：
    1. codes 为空时不产出任何元素。
    """
    normalized_codes = normalize_company_info_codes(codes)
    if not normalized_codes:
        return

    fetcher = get_fetcher()
    from zsdtdx.engine.unified_client import _ensure_availability_hosts_cache

    sources = {_infer_stock_task_route_source(code) for code in normalized_codes}
    _ensure_availability_hosts_cache(
        config_path=getattr(fetcher, "config_path", None),
        required_sources={
            "standard" if source == "std" else "extended" for source in sources
        },
    )
    _refresh_thread_hosts(sources)
    # 协调线程统一准备一次，避免 60 个单股任务从空路由各自刷新全市场。
    _ensure_shared_client_context().prepare_company_info_routes()
    do_prewarm = (
        bool(fetcher.auto_prewarm_on_async)
        if auto_prewarm is None
        else bool(auto_prewarm)
    )
    if do_prewarm:
        try:
            prewarm_parallel_fetcher(
                require_all_threads=bool(fetcher.auto_prewarm_require_all_threads),
                timeout_seconds=float(fetcher.auto_prewarm_timeout_seconds),
                max_rounds=int(fetcher.auto_prewarm_max_rounds),
                target_threads=int(fetcher.num_threads),
            )
        except Exception as exc:
            _emit_log(
                "error",
                "[CompanyInfo Parallel] 预热失败，继续尝试抓取",
                {"error": str(exc)},
            )

    chunk_size = max(1, int(fetcher.company_info_codes_per_chunk))
    chunks: List[List[str]] = [
        normalized_codes[i : i + chunk_size]
        for i in range(0, len(normalized_codes), chunk_size)
    ]
    stock_workers = 1
    category_workers = 1
    max_inflight = max(
        1,
        int(fetcher.num_threads) * int(fetcher.company_info_max_inflight_multiplier),
    )

    _emit_log(
        "info",
        "[CompanyInfo Parallel] 开始调度",
        {
            "codes": len(normalized_codes),
            "chunks": len(chunks),
            "num_threads": int(fetcher.num_threads),
            "stock_workers": int(stock_workers),
            "category_workers": int(category_workers),
            "max_inflight": int(max_inflight),
        },
    )

    executor = _get_global_thread_runtime(int(fetcher.num_threads))
    pending: Dict[Future, int] = {}
    next_idx = 0

    def _submit(idx: int) -> None:
        """输入 chunk 下标，提交到线程池。"""
        payload = {
            "codes": chunks[idx],
            "category": category,
            "stock_workers": stock_workers,
            "category_workers": category_workers,
        }
        pending[executor.submit(_fetch_company_info_chunk, executor, payload)] = idx

    while pending or next_idx < len(chunks):
        while next_idx < len(chunks) and len(pending) < max_inflight:
            _submit(next_idx)
            next_idx += 1
        if not pending:
            break
        done_set, _ = wait(list(pending.keys()), return_when=FIRST_COMPLETED)
        for fut in done_set:
            idx = pending.pop(fut)
            try:
                result = dict(fut.result())
                result["requested_codes"] = chunks[idx]
                yield result
            except Exception as exc:
                outcomes = [
                    company_info_outcome(code, error=exc) for code in chunks[idx]
                ]
                yield {
                    "rows": [],
                    "errors": outcomes,
                    "outcomes": outcomes,
                    "requested_codes": chunks[idx],
                }


def fetch_company_info_parallel(
    codes: List[str],
    category: Optional[Any] = None,
    *,
    auto_prewarm: Optional[bool] = None,
    queue: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """
    多线程并行拉取公司信息，阻塞至完成；中间与返回均为 list[dict]。

    输入：
    1. codes/category: 股票列表与分类过滤。
    2. auto_prewarm: 是否预热。
    3. queue: 可选；若提供则边抓边 put data/done 事件（与 async 事件结构一致）。
    输出：
    1. [{"code","category","content"}, ...]；不含 DataFrame。
    """
    normalized = normalize_company_info_codes(codes)
    requested = set(normalized)
    terminal: Dict[str, Dict[str, Any]] = {}
    all_rows: List[Dict[str, Any]] = []

    def _accept(outcome):
        """输入一股终态，输出无；仅首次接受请求集合内代码，避免重复聚合和事件。"""
        code = outcome["code"]
        if code not in requested or code in terminal:
            return
        terminal[code] = outcome
        all_rows.extend(outcome["rows"])
        record_company_info_outcome(_shared_client_context, outcome)
        publish_company_info_outcome(outcome, queue)

    iterator_error = None
    try:
        for result in _iter_company_info_chunk_results(
            codes=normalized, category=category, auto_prewarm=auto_prewarm
        ):
            outcomes = result.get("outcomes")
            if outcomes is None:
                # 接受仅含 rows/errors 的内部结果，同时对已知请求分片补齐空终态。
                by_code: Dict[str, List[Dict[str, Any]]] = {}
                for row in result.get("rows") or []:
                    code = str(row.get("code", "")).strip()
                    by_code.setdefault(code, []).append(row)
                errors = {
                    str(error.get("code", "")).strip(): error
                    for error in result.get("errors") or []
                }
                outcomes = []
                for code in result.get(
                    "requested_codes", by_code.keys() | errors.keys()
                ):
                    if code in errors:
                        outcome = company_info_outcome(
                            code,
                            error=RuntimeError(
                                str(errors[code].get("error") or "fetch_failed")
                            ),
                        )
                        for key in ("reason", "status", "detail"):
                            if key in errors[code]:
                                outcome[key] = errors[code][key]
                    else:
                        outcome = company_info_outcome(code, rows=by_code.get(code))
                    outcomes.append(outcome)
            for outcome in outcomes:
                _accept(outcome)
    except Exception as exc:
        iterator_error = exc

    for code in normalized:
        if code not in terminal:
            outcome = company_info_outcome(
                code,
                error=iterator_error
                or RuntimeError("missing_result: 未收到该代码最终结果"),
            )
            if iterator_error is None:
                outcome["reason"] = "missing_result"
            _accept(outcome)
    failed = [
        outcome for outcome in terminal.values() if outcome["status"] != "success"
    ]
    if failed:
        _emit_log(
            "error",
            "[CompanyInfo Parallel] 部分股票失败",
            {
                "error_count": len(failed),
                "sample": [
                    {"code": item["code"], "reason": item["reason"]}
                    for item in failed[:5]
                ],
            },
        )
    if queue is not None:
        queue.put(
            {
                "event": "done",
                "total_codes": len(normalized),
                "success_codes": len(terminal) - len(failed),
                "failed_codes": len(failed),
            }
        )
    return all_rows


def fetch_company_info_async(
    codes: List[str],
    category: Optional[Any] = None,
    *,
    queue: Optional[Any] = None,
    auto_prewarm: Optional[bool] = None,
) -> StockKlineJob:
    """
    异步并行拉取公司信息，立即返回 Job；结果经 queue 流式推送。

    输入：
    1. codes/category: 股票列表与分类过滤。
    2. queue: 可选；不传则自动创建。
    3. auto_prewarm: 是否预热。
    输出：
    1. StockKlineJob；`job.queue` 事件为 data/done；`job.result()` 为 list[dict] 全文行。
    边界条件：
    1. 中间传递与队列 payload 均不使用 DataFrame。
    """
    result_queue = queue if queue is not None else queue_mod.Queue()
    if not hasattr(result_queue, "put"):
        raise ValueError("queue 必须提供 put() 方法")

    def _run() -> List[Dict[str, Any]]:
        """后台线程执行并行抓取并写队列。"""
        return fetch_company_info_parallel(
            codes=codes,
            category=category,
            auto_prewarm=auto_prewarm,
            queue=result_queue,
        )

    executor = ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="zsdtdx_company_info_async"
    )
    future = executor.submit(_run)

    def _shutdown_executor(_fut: Future) -> None:
        """输入完成 future，关闭临时线程池；用于避免线程池泄漏。"""
        try:
            executor.shutdown(wait=False)
        except Exception:
            pass

    future.add_done_callback(_shutdown_executor)
    return StockKlineJob(future=future, queue_obj=result_queue)
