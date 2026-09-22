# -*- coding: utf-8 -*-
"""
模块：`run_kline_universe_week_validate.py`。

职责：
1. 使用 simple_api 拉取 yaml 过滤后的京沪深+港股、商品期货、指定指数 K 线。
2. 优先 async（股票/指数）；期货走 get_future_kline 并行 DataFrame 路径。
3. 校验每 bar 的 datetime/OHLCV 自洽与奇异值，汇总各周期 bar 数、分周期耗时与错误原因。

边界：
1. 参数写在同目录 `kline_universe_week_validate_config.yaml`，不暴露 CLI。
2. 需访问标准/扩展行情；结果写入 artifacts JSON。
3. 收到 async `event=done` 后立刻 `destroy_parallel_fetcher()`。
4. `measure_elapsed_per_freq=true` 时按周期分批提交，墙钟才可按周期拆分。
"""

from __future__ import annotations

import json
import math
import re
import sys
import time
import traceback
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

_MANUAL_DIR = Path(__file__).resolve().parent
_ROOT = _MANUAL_DIR.parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_CFG_PATH = _MANUAL_DIR / "kline_universe_week_validate_config.yaml"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")


def _load_run_cfg() -> Dict[str, Any]:
    """
    输入：无。
    输出：run 配置字典。
    用途：读取手工脚本参数。
    边界：缺少 run 段抛 ValueError。
    """
    with _CFG_PATH.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh) or {}
    run = dict(doc.get("run") or {})
    if not run:
        raise ValueError("kline_universe_week_validate_config.yaml 缺少 run 段")
    return run


def _prepare_runtime_config(run: Dict[str, Any], art_dir: Path) -> Path:
    """
    输入：run 配置与产物目录。
    输出：可 set_config_path 的 yaml 路径。
    用途：复制包内配置并拉长 chunk 超时、写入股票 scope。
    边界：只覆盖仍存在的 parallel 键；预热与期货批处理超时是代码常数。
    """
    base_rel = str(run.get("base_config") or "../../src/zsdtdx/config.yaml")
    base = (_MANUAL_DIR / base_rel).resolve()
    with base.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}

    scopes = list(run.get("stock_scopes") or ["szsh", "bj", "hk"])
    stock_scope = dict(cfg.get("stock_scope") or {})
    defaults = dict(stock_scope.get("defaults_when_codes_none") or {})
    defaults["get_stock_code_name"] = scopes
    defaults["get_stock_kline"] = scopes
    defaults["get_stock_latest_price"] = scopes
    stock_scope["defaults_when_codes_none"] = defaults
    cfg["stock_scope"] = stock_scope

    parallel = dict(cfg.get("parallel") or {})
    parallel["chunk_timeout_seconds"] = 30
    parallel["chunk_retry_max_attempts"] = 3
    cfg["parallel"] = parallel

    out = art_dir / "run_config.yaml"
    with out.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh, allow_unicode=True, sort_keys=False)
    return out


def _write_json(path: Path, payload: Any) -> None:
    """
    输入：路径与可序列化对象。
    输出：无。
    用途：UTF-8 落盘 JSON。
    边界：default=str 兜底不可序列化对象。
    """
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _classify_error(text: str) -> str:
    """
    输入：错误字符串。
    输出：错误分类桶名。
    用途：汇总失败原因。
    边界：空串归 empty。
    """
    raw = str(text or "").strip().lower()
    if not raw:
        return "empty"
    if "timeout" in raw or "超时" in raw:
        return "timeout"
    if "retry" in raw or "重试" in raw:
        return "retry"
    if (
        "unavailable" in raw
        or "不可用" in raw
        or "无可用连接" in raw
        or "disconnect" in raw
    ):
        return "conn_unavailable"
    if "no_data" in raw or "nodata" in raw or "无数据" in raw:
        return "no_data"
    if "not_found" in raw or "code_not_found" in raw or "未找到" in raw:
        return "not_found"
    if "ohlc" in raw or "奇异" in raw or "不自洽" in raw:
        return "ohlc_bad"
    return "other"


def _is_bad_number(v: float) -> bool:
    """
    输入：浮点值。
    输出：是否 NaN/Inf。
    用途：奇异值检测。
    边界：非 float 转换失败视为坏值由上层处理。
    """
    return math.isnan(v) or math.isinf(v)


def _check_bar(row: Dict[str, Any], freq: str, start_day: date, end_day: date) -> Optional[str]:
    """
    输入：单根 K 线、周期、窗口日期。
    输出：通过返回 None，否则原因字符串。
    用途：校验 datetime/OHLC/volume/amount 奇异值与自洽性。
    边界：期货无 amount 时跳过 amount；settlement_price 若存在则校验非负。
    """
    if not isinstance(row, dict):
        return "row非dict"
    dt_text = str(row.get("datetime", ""))
    if not _DT_RE.match(dt_text):
        return f"datetime格式非法:{dt_text}"
    try:
        bar_dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return f"datetime无法解析:{dt_text}"
    bar_day = bar_dt.date()
    if bar_day < start_day or bar_day > end_day:
        return f"日期越窗:{dt_text}"
    if freq in {"5", "15", "30", "60"} and bar_day.weekday() >= 5:
        return f"分钟线落在周末:{dt_text}"

    try:
        o = float(row["open"])
        h = float(row["high"])
        low = float(row["low"])
        c = float(row["close"])
    except Exception:
        return "OHLC无法转float"
    for name, val in (("open", o), ("high", h), ("low", low), ("close", c)):
        if _is_bad_number(val):
            return f"{name}为NaN/Inf"
        if val <= 0:
            return f"{name}非正:{val}"
    if h < low:
        return "high<low"
    if h + 1e-9 < max(o, c) or low - 1e-9 > min(o, c):
        return "OHLC不自洽"

    if "volume" in row and row.get("volume") is not None:
        try:
            vol = float(row.get("volume"))
        except Exception:
            return "volume非法"
        if _is_bad_number(vol) or vol < 0:
            return f"volume奇异:{vol}"

    if "amount" in row and row.get("amount") is not None:
        try:
            amt = float(row.get("amount"))
        except Exception:
            return "amount非法"
        if _is_bad_number(amt) or amt < 0:
            return f"amount奇异:{amt}"

    if "settlement_price" in row and row.get("settlement_price") is not None:
        try:
            sp = float(row.get("settlement_price"))
        except Exception:
            return "settlement_price非法"
        if _is_bad_number(sp) or sp < 0:
            return f"settlement_price奇异:{sp}"
    return None


def _push_sample(bucket: List[Dict[str, Any]], item: Dict[str, Any], limit: int = 30) -> None:
    """
    输入：样本桶、条目、上限。
    输出：无。
    用途：保留有限错误样本。
    边界：超出上限丢弃。
    """
    if len(bucket) < limit:
        bucket.append(item)


def _empty_asset_stats() -> Dict[str, Any]:
    """
    输入：无。
    输出：资产类统计骨架。
    用途：股票/指数/期货共用计数结构。
    边界：Counter/defaultdict 需在写 JSON 前转普通 dict。
    """
    return {
        "expected_tasks": 0,
        "data_events": 0,
        "ok_tasks": 0,
        "empty_rows": 0,
        "error_tasks": 0,
        "ohlc_bad_tasks": 0,
        "dup_datetime_tasks": 0,
        "unsorted_tasks": 0,
        "elapsed_seconds": 0.0,
        "bars_total": 0,
        "bars_by_freq": Counter(),
        "ok_by_freq": Counter(),
        "empty_by_freq": Counter(),
        "error_by_freq": Counter(),
        "ohlc_bad_by_freq": Counter(),
        "error_by_kind": Counter(),
        "error_samples": [],
        "ohlc_samples": [],
        "empty_samples": [],
        "ohlc_bad_by_prefix": Counter(),
        "ohlc_reason_counts": Counter(),
        "error_by_prefix": Counter(),
        "done": {},
        "destroy": {},
    }


def _entity_prefix(entity: str, id_key: str) -> str:
    """
    输入：实体标识与字段名。
    输出：市场前缀（sz/sh/bj/hk）或 index/future/other。
    用途：奇异值/错误按市场归类。
    边界：无点号前缀时按 id_key 回退。
    """
    text = str(entity or "").strip()
    if "." in text:
        return text.split(".", 1)[0].lower()
    if id_key == "index_name":
        return "index"
    if id_key == "code" and text and text[:1].isalpha():
        return "future"
    return "other"


def _merge_asset_stats(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    """
    输入：累计目标与本批统计。
    输出：无；原地累加 dst。
    用途：多周期分批跑完后合并。
    边界：Counter 字段相加；列表样本有限追加。
    """
    for key in (
        "expected_tasks",
        "data_events",
        "ok_tasks",
        "empty_rows",
        "error_tasks",
        "ohlc_bad_tasks",
        "dup_datetime_tasks",
        "unsorted_tasks",
        "bars_total",
    ):
        dst[key] = int(dst.get(key, 0)) + int(src.get(key, 0))
    dst["elapsed_seconds"] = round(
        float(dst.get("elapsed_seconds", 0.0)) + float(src.get("elapsed_seconds", 0.0)),
        3,
    )
    for key in (
        "bars_by_freq",
        "ok_by_freq",
        "empty_by_freq",
        "error_by_freq",
        "ohlc_bad_by_freq",
        "error_by_kind",
        "ohlc_bad_by_prefix",
        "ohlc_reason_counts",
        "error_by_prefix",
    ):
        left = dst.get(key)
        right = src.get(key)
        if not isinstance(left, Counter):
            left = Counter(left or {})
        if isinstance(right, Counter):
            left.update(right)
        elif isinstance(right, dict):
            left.update(right)
        dst[key] = left
    for key in ("error_samples", "ohlc_samples", "empty_samples"):
        bucket = list(dst.get(key) or [])
        for item in list(src.get(key) or []):
            _push_sample(bucket, item, limit=30)
        dst[key] = bucket
    if src.get("done"):
        dst.setdefault("done_by_batch", []).append(src.get("done"))
    if src.get("destroy"):
        dst.setdefault("destroy_by_batch", []).append(src.get("destroy"))


def _finalize_counters(stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    输入：含 Counter 的统计字典。
    输出：可 JSON 序列化的深拷贝。
    用途：写 summary 前规范化。
    边界：嵌套 dict 递归转换。
    """
    out = deepcopy(stats)
    for key, val in list(out.items()):
        if isinstance(val, Counter):
            out[key] = dict(val)
        elif isinstance(val, defaultdict):
            out[key] = dict(val)
    return out


def _consume_async_job(
    job: Any,
    *,
    id_key: str,
    start_day: date,
    end_day: date,
    queue_timeout: float,
    progress_every: int,
    progress_path: Path,
    phase: str,
    expected: int,
    destroy_pool: bool = True,
) -> Dict[str, Any]:
    """
    输入：StockKlineJob 与校验/进度参数。
    输出：统计字典。
    用途：消费 async 队列直到 done，边收边校验。
    边界：超时抛 queue.Empty；destroy_pool=False 时保留进程池供下一批复用。
    """
    from zsdtdx import destroy_parallel_fetcher

    stats = _empty_asset_stats()
    stats["expected_tasks"] = expected
    t0 = time.perf_counter()
    q = job.queue
    while True:
        event = q.get(timeout=queue_timeout)
        name = str(event.get("event", "")).strip().lower()
        if name == "done":
            stats["done"] = {
                "total_tasks": event.get("total_tasks"),
                "success_tasks": event.get("success_tasks"),
                "failed_tasks": event.get("failed_tasks"),
            }
            break
        if name != "data":
            continue
        stats["data_events"] += 1
        task = dict(event.get("task") or {})
        freq = str(task.get("freq", "")).strip()
        entity = str(task.get(id_key, "")).strip()
        err = event.get("error")
        rows = list(event.get("rows") or [])
        prefix = _entity_prefix(entity, id_key)
        if err:
            stats["error_tasks"] += 1
            stats["error_by_freq"][freq] += 1
            kind = _classify_error(str(err))
            stats["error_by_kind"][kind] += 1
            stats["error_by_prefix"][prefix] += 1
            _push_sample(
                stats["error_samples"],
                {
                    id_key: entity,
                    "freq": freq,
                    "kind": kind,
                    "error": str(err)[:300],
                },
            )
        elif not rows:
            stats["empty_rows"] += 1
            stats["empty_by_freq"][freq] += 1
            _push_sample(stats["empty_samples"], {id_key: entity, "freq": freq})
        else:
            bad = None
            dts: List[str] = []
            for row in rows:
                dts.append(str(row.get("datetime", "")))
                bad = _check_bar(row, freq, start_day, end_day)
                if bad:
                    break
            if bad:
                stats["ohlc_bad_tasks"] += 1
                stats["ohlc_bad_by_freq"][freq] += 1
                stats["error_by_kind"]["ohlc_bad"] += 1
                stats["ohlc_bad_by_prefix"][prefix] += 1
                reason_key = (
                    "amount_negative"
                    if "amount" in bad
                    else ("ohlc_inconsistent" if "不自洽" in bad else bad[:48])
                )
                stats["ohlc_reason_counts"][reason_key] += 1
                _push_sample(
                    stats["ohlc_samples"],
                    {
                        id_key: entity,
                        "freq": freq,
                        "reason": bad,
                        "n": len(rows),
                    },
                )
            else:
                if len(dts) != len(set(dts)):
                    stats["dup_datetime_tasks"] += 1
                if dts != sorted(dts):
                    stats["unsorted_tasks"] += 1
                stats["ok_tasks"] += 1
                stats["ok_by_freq"][freq] += 1
                stats["bars_by_freq"][freq] += len(rows)
                stats["bars_total"] += len(rows)
        if stats["data_events"] % progress_every == 0:
            elapsed = time.perf_counter() - t0
            print(
                f"[{phase}] events={stats['data_events']}/{expected} "
                f"ok={stats['ok_tasks']} empty={stats['empty_rows']} "
                f"err={stats['error_tasks']} ohlc_bad={stats['ohlc_bad_tasks']} "
                f"elapsed={elapsed:.1f}s",
                flush=True,
            )
            _write_json(
                progress_path,
                {
                    "phase": phase,
                    "elapsed_seconds": round(elapsed, 3),
                    "data_events": stats["data_events"],
                    "expected_tasks": expected,
                    "ok_tasks": stats["ok_tasks"],
                    "empty_rows": stats["empty_rows"],
                    "error_tasks": stats["error_tasks"],
                    "ohlc_bad_tasks": stats["ohlc_bad_tasks"],
                    "bars_total": stats["bars_total"],
                },
            )
    if destroy_pool:
        stats["destroy"] = dict(destroy_parallel_fetcher())
    else:
        stats["destroy"] = {"skipped": True}
    try:
        job.result()
    except Exception as exc:
        stats["job_result_error"] = f"{type(exc).__name__}: {exc}"
    stats["elapsed_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def _start_async_with_retry(start_fn: Callable[[], Any], label: str) -> Any:
    """
    输入：启动 async 任务的无参函数与日志标签。
    输出：StockKlineJob。
    用途：预热不足时强制重启进程池再试一次。
    边界：第二次仍失败则抛出。
    """
    from zsdtdx import restart_parallel_fetcher

    try:
        return start_fn()
    except RuntimeError as exc:
        text = str(exc)
        if "预热失败" not in text and "prewarm" not in text.lower():
            raise
        print(f"{label} prewarm_fail_retry: {exc}", flush=True)
        try:
            restart_parallel_fetcher(
                prewarm=True, prewarm_timeout_seconds=300, max_rounds=5
            )
        except Exception as restart_exc:
            print(f"{label} restart_warn: {restart_exc}", flush=True)
        return start_fn()

def _validate_future_df(
    df: Any,
    freqs: List[str],
    start_day: date,
    end_day: date,
) -> Dict[str, Any]:
    """
    输入：期货 DataFrame、周期列表、窗口。
    输出：统计字典。
    用途：按 code×freq 分组校验。
    边界：空表记 empty_frame。
    """
    stats = _empty_asset_stats()
    if df is None or getattr(df, "empty", True):
        stats["empty_frame"] = True
        stats["error_tasks"] = 1
        stats["error_by_kind"]["empty_frame"] = 1
        _push_sample(stats["error_samples"], {"error": "future DataFrame 为空"})
        return stats

    groups = list(df.groupby(["code", "freq"], sort=False))
    stats["expected_tasks"] = len(groups)
    stats["data_events"] = len(groups)
    for (code, freq), grp in groups:
        code_s = str(code).strip()
        freq_s = str(freq).strip()
        rows = grp.to_dict(orient="records")
        if not rows:
            stats["empty_rows"] += 1
            stats["empty_by_freq"][freq_s] += 1
            _push_sample(stats["empty_samples"], {"code": code_s, "freq": freq_s})
            continue
        bad = None
        dts: List[str] = []
        for row in rows:
            dts.append(str(row.get("datetime", "")))
            bad = _check_bar(row, freq_s, start_day, end_day)
            if bad:
                break
        if bad:
            stats["ohlc_bad_tasks"] += 1
            stats["ohlc_bad_by_freq"][freq_s] += 1
            stats["error_by_kind"]["ohlc_bad"] += 1
            stats["ohlc_bad_by_prefix"]["future"] += 1
            reason_key = (
                "amount_negative"
                if "amount" in bad
                else ("ohlc_inconsistent" if "不自洽" in bad else bad[:48])
            )
            stats["ohlc_reason_counts"][reason_key] += 1
            _push_sample(
                stats["ohlc_samples"],
                {"code": code_s, "freq": freq_s, "reason": bad, "n": len(rows)},
            )
            continue
        if len(dts) != len(set(dts)):
            stats["dup_datetime_tasks"] += 1
        if dts != sorted(dts):
            stats["unsorted_tasks"] += 1
        stats["ok_tasks"] += 1
        stats["ok_by_freq"][freq_s] += 1
        stats["bars_by_freq"][freq_s] += len(rows)
        stats["bars_total"] += len(rows)
    # 期望组合缺席：调用方用 universe×freqs 再核对
    stats["observed_freqs"] = sorted({str(f) for f in df["freq"].unique().tolist()})
    stats["observed_codes"] = int(df["code"].nunique())
    return stats


def _merge_freq_report(
    freqs: List[str],
    elapsed_by_freq: Dict[str, Dict[str, float]],
    *asset_stats: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    输入：周期列表、分周期耗时、各资产统计。
    输出：按周期汇总的 bar/错误/耗时。
    用途：最终向用户汇报。
    边界：缺失周期填 0。
    """
    report: Dict[str, Dict[str, Any]] = {}
    for freq in freqs:
        bars = 0
        ok = 0
        empty = 0
        err = 0
        ohlc = 0
        for st in asset_stats:
            bars += int(st.get("bars_by_freq", {}).get(freq, 0))
            ok += int(st.get("ok_by_freq", {}).get(freq, 0))
            empty += int(st.get("empty_by_freq", {}).get(freq, 0))
            err += int(st.get("error_by_freq", {}).get(freq, 0))
            ohlc += int(st.get("ohlc_bad_by_freq", {}).get(freq, 0))
        timing = dict(elapsed_by_freq.get(freq) or {})
        report[freq] = {
            "bars": bars,
            "ok_tasks": ok,
            "empty_tasks": empty,
            "error_tasks": err,
            "ohlc_bad_tasks": ohlc,
            "elapsed_seconds": timing,
        }
    return report


def main() -> int:
    """
    输入：无。
    输出：成功 0，致命失败 1。
    用途：全量一周 K 线拉取校验入口。
    边界：任意阶段异常写 summary 后返回 1。
    """
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_all_future_list,
        get_client,
        get_future_kline,
        get_index_kline,
        get_stock_code_name,
        get_stock_kline,
        prewarm_parallel_fetcher,
        set_config_path,
    )

    run = _load_run_cfg()
    art_rel = str(run.get("artifacts_dir") or "artifacts/kline_universe_week_validate")
    art_dir = (_MANUAL_DIR / art_rel).resolve()
    art_dir.mkdir(parents=True, exist_ok=True)
    progress_path = art_dir / "progress.json"
    summary_path = art_dir / "summary.json"

    start_text = str(run["start_time"])
    end_text = str(run["end_time"])
    start_day = datetime.strptime(start_text[:10], "%Y-%m-%d").date()
    end_day = datetime.strptime(end_text[:10], "%Y-%m-%d").date()
    freqs = [str(x).strip() for x in list(run.get("freqs") or [])]
    indices = [str(x).strip() for x in list(run.get("indices") or [])]
    queue_timeout = float(run.get("queue_timeout_seconds") or 7200)
    progress_every = int(run.get("progress_every") or 200)

    cfg_path = _prepare_runtime_config(run, art_dir)
    set_config_path(str(cfg_path), async_background_probe=True)
    print(f"config={cfg_path}", flush=True)

    summary: Dict[str, Any] = {
        "ok": False,
        "window": {"start": start_text, "end": end_text},
        "freqs": freqs,
        "indices": indices,
        "stock_scopes": list(run.get("stock_scopes") or []),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "universe": {},
        "stock": {},
        "index": {},
        "future": {},
        "by_freq": {},
        "elapsed_by_freq": {},
        "ohlc_bad_by_prefix": {},
        "elapsed_seconds_total": 0.0,
        "fatal": None,
    }
    t_all = time.perf_counter()

    try:
        _write_json(progress_path, {"phase": "catalog", "status": "loading"})
        with get_client():
            stock_map = get_stock_code_name(use_cache=True)
            future_list = get_all_future_list(return_df=False, use_cache=True)
        stock_codes = sorted(stock_map.keys())
        future_codes_all = sorted(
            {
                str(r.get("code", "")).strip()
                for r in future_list
                if str(r.get("code", "")).strip()
            }
        )
        prefix_counts = Counter(c.split(".", 1)[0] for c in stock_codes if "." in c)
        summary["universe"] = {
            "stock_count": len(stock_codes),
            "stock_prefix_counts": dict(prefix_counts),
            "future_count": len(future_codes_all),
            "index_count": len(indices),
        }
        print(
            f"universe stocks={len(stock_codes)} futures={len(future_codes_all)} "
            f"indices={len(indices)} prefixes={dict(prefix_counts)}",
            flush=True,
        )
        _write_json(progress_path, {"phase": "catalog", "universe": summary["universe"]})

        measure_per_freq = bool(run.get("measure_elapsed_per_freq", True))
        freq_batches: List[List[str]] = (
            [[f] for f in freqs] if measure_per_freq else [list(freqs)]
        )
        summary["measure_elapsed_per_freq"] = measure_per_freq

        # 降低并行层逐 chunk 刷屏，避免日志拖慢与编码膨胀
        try:
            from zsdtdx.parallel_fetcher import set_log_callback

            def _quiet_parallel_log(
                level: str, message: str, detail: Optional[Dict[str, Any]] = None
            ) -> None:
                text = str(message or "")
                if "chunk" in text.lower():
                    return
                print(f"[Parallel:{level}] {text}", flush=True)

            set_log_callback(_quiet_parallel_log)
        except Exception:
            pass

        try:
            prewarm_info = prewarm_parallel_fetcher()
            print(f"prewarm={prewarm_info}", flush=True)
        except Exception as exc:
            print(f"prewarm_warn={exc}", flush=True)

        stock_stats = _empty_asset_stats()
        index_stats = _empty_asset_stats()
        future_stats = _empty_asset_stats()
        elapsed_by_freq: Dict[str, Dict[str, float]] = {
            f: {"stock": 0.0, "index": 0.0, "future": 0.0, "total": 0.0} for f in freqs
        }

        future_codes = run.get("future_codes")
        codes_arg = None if future_codes is None else list(future_codes)
        expect_codes = (
            future_codes_all if codes_arg is None else [str(c) for c in codes_arg]
        )

        for batch_freqs in freq_batches:
            batch_label = "+".join(batch_freqs)
            t_batch = time.perf_counter()

            # ---- 股票 async（本批周期）----
            stock_tasks = [
                {
                    "code": code,
                    "freq": freq,
                    "start_time": start_text,
                    "end_time": end_text,
                }
                for code in stock_codes
                for freq in batch_freqs
            ]
            print(
                f"stock_kline freqs={batch_freqs} tasks={len(stock_tasks)} mode=async",
                flush=True,
            )
            _write_json(
                progress_path,
                {
                    "phase": f"stock:{batch_label}",
                    "tasks": len(stock_tasks),
                },
            )
            job = _start_async_with_retry(
                lambda: get_stock_kline(task=stock_tasks, mode="async"),
                label=f"stock:{batch_label}",
            )
            stock_batch = _consume_async_job(
                job,
                id_key="code",
                start_day=start_day,
                end_day=end_day,
                queue_timeout=queue_timeout,
                progress_every=progress_every,
                progress_path=progress_path,
                phase=f"stock:{batch_label}",
                expected=len(stock_tasks),
                destroy_pool=False,
            )
            _merge_asset_stats(stock_stats, stock_batch)
            for freq in batch_freqs:
                # 单周期批：耗时即本批；多周期合并批则均分不可靠，标 shared
                if len(batch_freqs) == 1:
                    elapsed_by_freq[freq]["stock"] = float(
                        stock_batch.get("elapsed_seconds", 0.0)
                    )
            print(
                f"stock:{batch_label} bars={stock_batch['bars_total']} "
                f"ok={stock_batch['ok_tasks']} err={stock_batch['error_tasks']} "
                f"ohlc_bad={stock_batch['ohlc_bad_tasks']} "
                f"elapsed={stock_batch['elapsed_seconds']}s "
                f"ohlc_prefix={dict(stock_batch.get('ohlc_bad_by_prefix') or {})}",
                flush=True,
            )

            # ---- 指数 async ----
            index_tasks = [
                {
                    "index_name": name,
                    "freq": freq,
                    "start_time": start_text,
                    "end_time": end_text,
                }
                for name in indices
                for freq in batch_freqs
            ]
            print(
                f"index_kline freqs={batch_freqs} tasks={len(index_tasks)} mode=async",
                flush=True,
            )
            job = _start_async_with_retry(
                lambda: get_index_kline(task=index_tasks, mode="async"),
                label=f"index:{batch_label}",
            )
            index_batch = _consume_async_job(
                job,
                id_key="index_name",
                start_day=start_day,
                end_day=end_day,
                queue_timeout=queue_timeout,
                progress_every=max(1, min(progress_every, 5)),
                progress_path=progress_path,
                phase=f"index:{batch_label}",
                expected=len(index_tasks),
                destroy_pool=True,
            )
            _merge_asset_stats(index_stats, index_batch)
            if len(batch_freqs) == 1:
                elapsed_by_freq[batch_freqs[0]]["index"] = float(
                    index_batch.get("elapsed_seconds", 0.0)
                )
            print(
                f"index:{batch_label} bars={index_batch['bars_total']} "
                f"ok={index_batch['ok_tasks']} err={index_batch['error_tasks']} "
                f"elapsed={index_batch['elapsed_seconds']}s",
                flush=True,
            )

            # ---- 期货 ----
            print(
                f"future_kline freqs={batch_freqs} "
                f"codes={'ALL' if codes_arg is None else len(codes_arg)}",
                flush=True,
            )
            _write_json(
                progress_path,
                {"phase": f"future:{batch_label}", "status": "fetching"},
            )
            t_fut = time.perf_counter()
            with get_client():
                fut_df = get_future_kline(
                    codes=codes_arg,
                    freq=batch_freqs,
                    start_time=start_text,
                    end_time=end_text,
                )
            fut_elapsed = round(time.perf_counter() - t_fut, 3)
            try:
                destroy_info = destroy_parallel_fetcher()
            except Exception as exc:
                destroy_info = {"error": str(exc)}
            future_batch = _validate_future_df(
                fut_df, batch_freqs, start_day, end_day
            )
            future_batch["elapsed_seconds"] = fut_elapsed
            future_batch["destroy"] = (
                dict(destroy_info)
                if isinstance(destroy_info, dict)
                else {"raw": str(destroy_info)}
            )
            if fut_df is not None and not getattr(fut_df, "empty", True):
                got = {
                    (str(c).strip(), str(f).strip())
                    for c, f in zip(fut_df["code"].tolist(), fut_df["freq"].tolist())
                }
            else:
                got = set()
            missing = 0
            for code in expect_codes:
                for freq in batch_freqs:
                    if (code, freq) not in got:
                        missing += 1
            future_batch["missing_code_freq_pairs"] = int(
                future_batch.get("missing_code_freq_pairs", 0)
            ) + missing
            future_batch["expected_code_freq_pairs"] = len(expect_codes) * len(
                batch_freqs
            )
            _merge_asset_stats(future_stats, future_batch)
            if len(batch_freqs) == 1:
                elapsed_by_freq[batch_freqs[0]]["future"] = fut_elapsed
                elapsed_by_freq[batch_freqs[0]]["total"] = round(
                    elapsed_by_freq[batch_freqs[0]]["stock"]
                    + elapsed_by_freq[batch_freqs[0]]["index"]
                    + elapsed_by_freq[batch_freqs[0]]["future"],
                    3,
                )
            print(
                f"future:{batch_label} bars={future_batch['bars_total']} "
                f"ok={future_batch['ok_tasks']} ohlc_bad={future_batch['ohlc_bad_tasks']} "
                f"missing_pairs={missing} elapsed={fut_elapsed}s",
                flush=True,
            )

            batch_elapsed = round(time.perf_counter() - t_batch, 3)
            print(f"batch:{batch_label} wall={batch_elapsed}s", flush=True)
            summary["stock"] = _finalize_counters(stock_stats)
            summary["index"] = _finalize_counters(index_stats)
            summary["future"] = _finalize_counters(future_stats)
            summary["elapsed_by_freq"] = elapsed_by_freq
            summary["by_freq"] = _merge_freq_report(
                freqs,
                elapsed_by_freq,
                summary["stock"],
                summary["index"],
                summary["future"],
            )
            _write_json(summary_path, summary)

        # 期货缺组合汇总（累计 missing 已在各批写入 batch；这里用期望核对）
        future_stats["expected_code_freq_pairs"] = len(expect_codes) * len(freqs)
        summary["stock"] = _finalize_counters(stock_stats)
        summary["index"] = _finalize_counters(index_stats)
        summary["future"] = _finalize_counters(future_stats)
        summary["elapsed_by_freq"] = elapsed_by_freq
        summary["by_freq"] = _merge_freq_report(
            freqs,
            elapsed_by_freq,
            summary["stock"],
            summary["index"],
            summary["future"],
        )
        summary["elapsed_seconds_total"] = round(time.perf_counter() - t_all, 3)
        summary["ok"] = True
        summary["error_reason_rollup"] = {
            "stock": summary["stock"].get("error_by_kind", {}),
            "index": summary["index"].get("error_by_kind", {}),
            "future": summary["future"].get("error_by_kind", {}),
        }
        summary["ohlc_bad_by_prefix"] = {
            "stock": summary["stock"].get("ohlc_bad_by_prefix", {}),
            "index": summary["index"].get("ohlc_bad_by_prefix", {}),
            "future": summary["future"].get("ohlc_bad_by_prefix", {}),
        }
        _write_json(summary_path, summary)
        _write_json(
            progress_path,
            {
                "phase": "completed",
                "elapsed_seconds_total": summary["elapsed_seconds_total"],
                "elapsed_by_freq": elapsed_by_freq,
                "by_freq": summary["by_freq"],
                "ohlc_bad_by_prefix": summary["ohlc_bad_by_prefix"],
            },
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "elapsed": summary["elapsed_seconds_total"],
                    "elapsed_by_freq": elapsed_by_freq,
                    "ohlc_bad_by_prefix": summary["ohlc_bad_by_prefix"],
                    "by_freq": summary["by_freq"],
                    "path": str(summary_path),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 0
    except Exception as exc:
        summary["fatal"] = f"{type(exc).__name__}: {exc}"
        summary["elapsed_seconds_total"] = round(time.perf_counter() - t_all, 3)
        traceback.print_exc()
        try:
            destroy_parallel_fetcher()
        except Exception:
            pass
        _write_json(summary_path, summary)
        _write_json(
            progress_path,
            {"phase": "failed", "fatal": summary["fatal"]},
        )
        print("FATAL", summary["fatal"], flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
