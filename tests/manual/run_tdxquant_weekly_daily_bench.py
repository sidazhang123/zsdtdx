# -*- coding: utf-8 -*-
"""
模块：run_tdxquant_weekly_daily_bench.py

用途：
1. 使用 TdxQuant 多进程拉取本周一至五京沪深全 A 股日线 OHLCVA。
2. 将每只股票结果写入 artifacts，供与 zsdtdx 离线对比。

边界：
1. 需通达信客户端已启动并登录。
2. 并行参数读取配置里的 process_count_core_multiplier（建议 0.5~3.0）。
3. 由主 Agent 单独启动，不与 zsdtdx 同进程串联。
"""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

_MANUAL_DIR = Path(__file__).resolve().parent
_CFG_PATH = _MANUAL_DIR / "tdxquant_zsdtdx_weekly_compare_config.yaml"

LIFECYCLE_RUNNING = "running"
LIFECYCLE_COMPLETED = "completed"
LIFECYCLE_FAILED = "failed"


def _load_cfg() -> Dict[str, Any]:
    """读取对比配置。"""
    with _CFG_PATH.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh) or {}
    cfg = dict(doc.get("compare") or {})
    if not cfg:
        raise ValueError("缺少 compare 配置段")
    return cfg


def _get_physical_cores() -> int:
    """获取物理核心数；失败时回退逻辑核。"""
    try:
        import psutil

        cores = psutil.cpu_count(logical=False)
        if cores and cores >= 1:
            return int(cores)
    except Exception:
        pass
    return int(os.cpu_count() or 4)


def get_optimal_process_count(core_multiplier: float) -> int:
    """计算推荐进程数：max(2, int(物理核 * 倍率))。"""
    try:
        multiplier = float(core_multiplier)
    except Exception:
        multiplier = 1.5
    if multiplier <= 0:
        multiplier = 1.5
    return max(2, int(_get_physical_cores() * multiplier))


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    """按固定大小切分列表。"""
    batch = max(1, int(size))
    return [items[i : i + batch] for i in range(0, len(items), batch)]


def _to_zsdtdx_code(tq_code: str) -> str:
    """将 TdxQuant 代码（600000.SH）转为 zsdtdx 前缀代码（sh.600000）。"""
    text = str(tq_code or "").strip()
    if "." not in text:
        return text.lower()
    body, suffix = text.rsplit(".", 1)
    return f"{suffix.lower()}.{body}"


def _serialize_batch_rows(
    data: Dict[str, Any],
    stock_list: List[str],
    start_time: str,
    end_time: str,
) -> List[Dict[str, Any]]:
    """
    将 TdxQuant get_market_data 返回的宽表拆成逐股票记录。

    输入：data 字段字典、股票列表、时间窗。
    输出：可写入 jsonl 的记录列表。
    """
    if not isinstance(data, dict):
        return []
    open_df = data.get("Open")
    high_df = data.get("High")
    low_df = data.get("Low")
    close_df = data.get("Close")
    vol_df = data.get("Volume")
    amt_df = data.get("Amount")
    if close_df is None or getattr(close_df, "empty", True):
        return []

    records: List[Dict[str, Any]] = []
    for code in stock_list:
        if code not in close_df.columns:
            records.append(
                {
                    "task_key": f"{_to_zsdtdx_code(code)}|d|{start_time}|{end_time}",
                    "tq_code": code,
                    "zsdtdx_code": _to_zsdtdx_code(code),
                    "rows": [],
                    "error": "missing_in_close_columns",
                }
            )
            continue
        rows: List[Dict[str, Any]] = []
        for dt_idx in close_df.index:
            dt_text = str(dt_idx)
            if " " not in dt_text and len(dt_text) == 10:
                dt_text = f"{dt_text} 15:00:00"
            elif dt_text.count(":") == 1:
                dt_text = f"{dt_text}:00"
            try:
                rows.append(
                    {
                        "datetime": dt_text,
                        "open": float(open_df.at[dt_idx, code])
                        if open_df is not None
                        else 0.0,
                        "high": float(high_df.at[dt_idx, code])
                        if high_df is not None
                        else 0.0,
                        "low": float(low_df.at[dt_idx, code])
                        if low_df is not None
                        else 0.0,
                        "close": float(close_df.at[dt_idx, code]),
                        "volume": int(vol_df.at[dt_idx, code])
                        if vol_df is not None
                        else 0,
                        "amount": int(amt_df.at[dt_idx, code])
                        if amt_df is not None
                        else 0,
                    }
                )
            except Exception:
                continue
        records.append(
            {
                "task_key": f"{_to_zsdtdx_code(code)}|d|{start_time}|{end_time}",
                "tq_code": code,
                "zsdtdx_code": _to_zsdtdx_code(code),
                "rows": rows,
                "error": None if rows else "empty_rows",
            }
        )
    return records


def _worker_fetch_batch(
    args: Tuple[int, List[str], str, str, str, str],
) -> Dict[str, Any]:
    """
    子进程 worker：初始化 TdxQuant 并抓取一批股票周线日线。

    输入：batch_id、stock_list、tdx start/end、zsdtdx start/end。
    输出：批次统计与序列化记录。
    """
    (
        batch_id,
        stock_list,
        tdx_start,
        tdx_end,
        zsd_start,
        zsd_end,
        plugins_user,
        worker_script,
    ) = args
    t0 = time.perf_counter()
    if plugins_user not in sys.path:
        sys.path.insert(0, plugins_user)
    from tqcenter import tq

    try:
        tq.initialize(worker_script)
        data = tq.get_market_data(
            field_list=["Open", "High", "Low", "Close", "Volume", "Amount"],
            stock_list=stock_list,
            start_time=tdx_start,
            end_time=tdx_end,
            count=-1,
            dividend_type="none",
            period="1d",
            fill_data=True,
        )
        records = _serialize_batch_rows(
            data=data,
            stock_list=stock_list,
            start_time=zsd_start,
            end_time=zsd_end,
        )
        ok_stocks = sum(1 for r in records if r.get("rows"))
        elapsed = time.perf_counter() - t0
        return {
            "batch_id": batch_id,
            "requested": len(stock_list),
            "ok_stocks": ok_stocks,
            "records": records,
            "elapsed_s": elapsed,
            "ok": True,
            "error": "",
        }
    except Exception as exc:
        return {
            "batch_id": batch_id,
            "requested": len(stock_list),
            "ok_stocks": 0,
            "records": [],
            "elapsed_s": time.perf_counter() - t0,
            "ok": False,
            "error": str(exc),
        }


def _write_lifecycle(
    path: Path, status: str, extra: Optional[Dict[str, Any]] = None
) -> None:
    """写入 lifecycle 状态。"""
    payload: Dict[str, Any] = {
        "lifecycle_status": status,
        "runner": "tdxquant",
        "updated_at_unix": time.time(),
    }
    if extra:
        payload.update(extra)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    """执行 TdxQuant 本周日线基准并落盘。"""
    cfg = _load_cfg()
    plugins_user = str(cfg.get("tdx_pyplugins_user", "")).replace("/", "\\")
    if plugins_user not in sys.path:
        sys.path.insert(0, plugins_user)

    from tqcenter import tq

    multiplier = float(cfg.get("process_count_core_multiplier", 5))
    batch_size = int(cfg.get("default_batch_size", 100))
    num_processes = get_optimal_process_count(multiplier)
    max_inflight = num_processes
    tdx_start = str(cfg.get("tdxquant_start", "")).strip()
    tdx_end = str(cfg.get("tdxquant_end", "")).strip()
    zsd_start = str(cfg.get("start_time", "")).strip()
    zsd_end = str(cfg.get("end_time", "")).strip()

    out_dir = (
        _MANUAL_DIR
        / str(cfg.get("artifacts_root", "artifacts/tdxquant_vs_zsdtdx_weekly_d"))
        / "tdxquant"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    lifecycle_path = out_dir / "lifecycle.json"
    records_path = out_dir / "task_records.jsonl"
    meta_path = out_dir / "run_meta.json"
    if records_path.is_file():
        records_path.unlink()
    if meta_path.is_file():
        meta_path.unlink()

    _write_lifecycle(lifecycle_path, LIFECYCLE_RUNNING, {"phase": "init"})

    t_all = time.perf_counter()
    try:
        tq.initialize(__file__)
        stock_list = list(tq.get_stock_list(market="5") or [])
        list_s = time.perf_counter() - t_all
        if not stock_list:
            raise RuntimeError("TdxQuant 股票列表为空，请确认通达信已登录")

        batches = _chunk_list(stock_list, batch_size)
        batch_results: List[Dict[str, Any]] = []
        errors: List[str] = []
        record_count = 0
        ok_stock_count = 0

        _write_lifecycle(
            lifecycle_path,
            LIFECYCLE_RUNNING,
            {
                "phase": "fetch_parallel",
                "stock_count": len(stock_list),
                "batch_count": len(batches),
            },
        )

        t_fetch = time.perf_counter()
        with ProcessPoolExecutor(max_workers=num_processes) as pool:
            futures = [
                pool.submit(
                    _worker_fetch_batch,
                    (
                        batch_id,
                        codes,
                        tdx_start,
                        tdx_end,
                        zsd_start,
                        zsd_end,
                        plugins_user,
                        __file__,
                    ),
                )
                for batch_id, codes in enumerate(batches)
            ]
            for fut in as_completed(futures):
                res = fut.result()
                batch_results.append(res)
                if not res.get("ok"):
                    errors.append(f"batch {res.get('batch_id')}: {res.get('error')}")
                for rec in list(res.get("records") or []):
                    with records_path.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    record_count += 1
                    if rec.get("rows"):
                        ok_stock_count += 1

        fetch_s = time.perf_counter() - t_fetch
        elapsed = time.perf_counter() - t_all

        meta = {
            "runner": "tdxquant",
            "tdx_pyplugins_user": plugins_user,
            "stock_count": len(stock_list),
            "ok_stock_count": ok_stock_count,
            "record_count": record_count,
            "batch_count": len(batches),
            "ok_batches": sum(1 for r in batch_results if r.get("ok")),
            "error_batches": sum(1 for r in batch_results if not r.get("ok")),
            "start_time": zsd_start,
            "end_time": zsd_end,
            "freq": "d",
            "num_processes": num_processes,
            "batch_size": batch_size,
            "max_inflight": max_inflight,
            "list_elapsed_seconds": list_s,
            "fetch_elapsed_seconds": fetch_s,
            "elapsed_seconds": elapsed,
            "errors_sample": errors[:10],
        }
        meta_path.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        _write_lifecycle(lifecycle_path, LIFECYCLE_COMPLETED, meta)
        print(json.dumps(meta, ensure_ascii=False))
        try:
            tq.close()
        except Exception:
            pass
        return 0
    except Exception as exc:
        _write_lifecycle(lifecycle_path, LIFECYCLE_FAILED, {"error": str(exc)})
        raise


if __name__ == "__main__":
    raise SystemExit(main())
