"""模块：`manual/run_thread_contract_compare.py`。

职责：在独立解释器核对股票复权、港股、指数、板块、期货和公司信息契约。
边界：配置来自 YAML，复用固定基线快照；可固定真实回包，错误或双方同时空结果不判通过。
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import yaml

HERE = Path(__file__).resolve().parent


def summarize(value):
    """输入业务返回值，输出类型、行数与完整摘要；忽略旧版基线的进程标识元数据。"""
    dtypes = (
        {str(k): str(v) for k, v in value.dtypes.items()}
        if hasattr(value, "dtypes")
        else None
    )
    if hasattr(value, "to_dict"):
        value = value.to_dict(orient="records")
    if isinstance(value, list):
        value = [
            {key: item for key, item in record.items() if key != "worker_pid"}
            if isinstance(record, dict)
            else record
            for record in value
        ]
    if isinstance(value, list) and value and value[0].get("event") == "data":
        errors = [item["error"] for item in value if item.get("error")]
        count = sum(len(item["rows"]) for item in value)
        value.sort(
            key=lambda item: json.dumps(
                item.get("task", {}), sort_keys=True, ensure_ascii=False
            )
        )
    else:
        errors, count = [], len(value)
    serialized = json.dumps(
        value, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False
    )
    return {
        "rows": count,
        "errors": errors,
        "dtypes": dtypes,
        "digest": hashlib.sha256(serialized.encode()).hexdigest(),
    }


def child(cfg, engine):
    """输入配置及实现名称，输出各 API 实际业务摘要；每个异常独立记录。"""
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_block_kline,
        get_block_names,
        get_company_info,
        get_future_kline,
        get_index_kline,
        get_stock_kline,
        set_config_path,
    )

    artifacts = HERE / cfg["artifact_dir"]
    set_config_path(
        str(artifacts / f"contract_{engine}.yaml"), async_background_probe=False
    )

    def stock(code, qfq=True):
        """
        输入代码与复权设置，输出两周期 data 事件。

        用途：覆盖扩展与标准路由的股票契约。
        边界：result() 为列表时直接返回；否则从队列读取 data 直到 done。
        """
        job = get_stock_kline(
            task=[
                dict(
                    code=code,
                    freq=f,
                    start_time=cfg["start_time"],
                    end_time=cfg["end_time"],
                )
                for f in ("15", "d")
            ],
            mode="async",
            qfq=qfq,
        )
        result = job.result(timeout=cfg["timeout_seconds"])
        if isinstance(result, list):
            return result
        payloads = []
        while True:
            event = job.queue.get(timeout=cfg["timeout_seconds"])
            if not isinstance(event, dict):
                continue
            if event.get("event") == "done":
                return payloads
            if event.get("event") == "data":
                payloads.append(event)

    results = {}
    try:
        names = get_block_names()
        block = "汽车拆解" if "汽车拆解" in names else names[0]
        cases = {
            "stock_qfq": lambda: stock(cfg["stock_code"]),
            "stock_unadjusted": lambda: stock(cfg["stock_code"], False),
            "hk": lambda: stock(cfg["hk_code"]),
            "index": lambda: get_index_kline(
                task=[
                    dict(
                        index_name=cfg["index_name"],
                        freq=f,
                        start_time=cfg["start_time"],
                        end_time=cfg["end_time"],
                    )
                    for f in ("15", "d")
                ],
                mode="sync",
            ),
            "block": lambda: get_block_kline(
                task=[
                    dict(
                        block_name=block,
                        freq=f,
                        start_time=cfg["start_time"],
                        end_time=cfg["end_time"],
                    )
                    for f in ("15", "d")
                ],
                mode="sync",
            ),
            "future": lambda: get_future_kline(
                codes=[cfg["future_code"]],
                freq="d",
                start_time=cfg["start_time"],
                end_time=cfg["end_time"],
            ),
            "company": lambda: get_company_info(
                codes=[cfg["stock_code"]], category=["公司概况"], mode="async"
            ).result(timeout=cfg["timeout_seconds"]),
        }
        for name, call in cases.items():
            try:
                value = call()
                records = (
                    value.to_dict(orient="records")
                    if hasattr(value, "to_dict")
                    else value
                )
                (artifacts / f"contract_{engine}_{name}_rows.json").write_text(
                    json.dumps(records, ensure_ascii=False, default=str, indent=2),
                    encoding="utf-8",
                )
                results[name] = summarize(value)
            except Exception as exc:
                results[name] = {"exception": f"{type(exc).__name__}: {exc}"}
    finally:
        destroy_parallel_fetcher()
    (artifacts / f"contract_{engine}.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main():
    """输入相邻配置，输出跨实现契约报告；子进程错误与空结果均显式失败。"""
    cfg = yaml.safe_load(
        (HERE / "thread_contract_compare.yaml").read_text(encoding="utf-8")
    )
    artifacts = HERE / cfg["artifact_dir"]
    for engine in ("baseline", "thread"):
        config = yaml.safe_load(
            (artifacts / f"{engine}.yaml").read_text(encoding="utf-8")
        )
        if cfg.get("pin_hosts", True):
            standard = config["hosts"]["standard"]
            if isinstance(standard, dict):
                standard["szshbj"] = standard.get("szshbj", [])[:1]
                standard["szsh_only"] = []
            else:
                # baseline 快照仍读取其发布时的扁平配置结构。
                config["hosts"]["standard"] = standard[:1]
            config["hosts"]["extended"] = config["hosts"]["extended"][:1]
        (artifacts / f"contract_{engine}.yaml").write_text(
            yaml.safe_dump(config, allow_unicode=True), encoding="utf-8"
        )
    for engine, folder in (
        ("baseline", "baseline"),
        ("thread", cfg.get("thread_snapshot", "thread_snapshot")),
    ):
        engine_src = (
            HERE.parents[1] / "src"
            if engine == "thread" and folder == "current"
            else artifacts / folder / "src"
        )
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [str(HERE / "kline_capture_hook"), str(engine_src)]
            ),
            "ZSDTDX_CONTRACT_ENGINE": engine,
            "PYTHONIOENCODING": "utf-8",
        }
        if cfg.get("freeze_kline_responses", False):
            env["ZSDTDX_CAPTURE_SPEC"] = json.dumps(
                {
                    "mode": "record" if engine == "baseline" else "replay",
                    "directory": str(artifacts / "captured_kline_frames"),
                }
            )
        subprocess.run(
            [sys.executable, str(Path(__file__).resolve())],
            env=env,
            check=True,
            timeout=cfg["timeout_seconds"] * 3,
        )
    a, b = [
        json.loads((artifacts / f"contract_{engine}.json").read_text(encoding="utf-8"))
        for engine in ("baseline", "thread")
    ]
    report = {
        name: {
            "baseline": a[name],
            "thread": b[name],
            "passed": a[name] == b[name]
            and a[name].get("rows", 0) > 0
            and not a[name].get("errors"),
        }
        for name in a
    }
    (artifacts / "contract_summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (artifacts / "contract_method.json").write_text(
        json.dumps(
            {
                "fixed_actual_kline_responses": bool(
                    cfg.get("freeze_kline_responses", False)
                ),
                "captured_frames": len(
                    list((artifacts / "captured_kline_frames").glob("*/body.bin"))
                ),
                "same_host_config": bool(cfg.get("pin_hosts", True)),
                "note": "K线使用真实回包固定重放，仍走公开入口、网络请求、解析、分页和交付；公司信息实时对拍。",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not all(value["passed"] for value in report.values()):
        raise RuntimeError("跨 API 对拍未全部通过，请查看 contract_summary.json")


if __name__ == "__main__":
    configuration = yaml.safe_load(
        (HERE / "thread_contract_compare.yaml").read_text(encoding="utf-8")
    )
    selected = os.environ.get("ZSDTDX_CONTRACT_ENGINE")
    child(configuration, selected) if selected else main()
