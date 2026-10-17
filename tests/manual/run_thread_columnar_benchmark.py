"""模块：`manual/run_thread_columnar_benchmark.py`。

职责：独立子进程交替对拍基线和线程版，按本次进程树测量 CPU/内存。
边界：参数来自相邻 YAML；产物写入 artifacts，不采样其它 Python 进程。
"""

from __future__ import annotations

import gc
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import zipfile

import psutil
import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


class TreeSampler:
    """只采集当前子进程及其后代，保留已退出进程的最后 CPU 累计值。"""

    def __init__(self):
        """输入无，输出采样器；初始进程 CPU 作为基准偏移。"""
        self.root = psutil.Process()
        self.stop = threading.Event()
        self.peak = 0
        self.values = {}
        self.initial = {}
        self.capture(initial=True)
        self.thread = threading.Thread(target=self.run, daemon=True)

    def capture(self, initial=False):
        """输入是否基准采样，输出无；进程消失不覆盖其已记录 CPU。"""
        processes = [self.root] + self.root.children(recursive=True)
        memory = 0
        for process in processes:
            try:
                key = (process.pid, process.create_time())
                cpu = process.cpu_times()
                value = cpu.user + cpu.system
                self.values[key] = value
                if initial:
                    self.initial[key] = value
                memory += process.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        self.peak = max(self.peak, memory)

    def run(self):
        """输入无，输出无；周期采样至结束事件，不阻止程序退出。"""
        while not self.stop.wait(0.03):
            self.capture()

    def finish(self):
        """输入无，输出 CPU 秒和峰值 RSS；最终采样在销毁工作线程/进程之前。"""
        self.stop.set()
        self.thread.join()
        self.capture()
        return sum(
            v - self.initial.get(k, 0.0) for k, v in self.values.items()
        ), self.peak


def fingerprint(payloads):
    """输入完整事件列表，输出保持重复任务数量的摘要；不以字典键覆盖任务。"""
    values = []
    rows = failures = 0
    for item in payloads:
        canonical = dict(item)
        encoded = json.dumps(
            canonical,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        values.append(hashlib.sha256(encoded.encode("utf-8")).hexdigest())
        rows += len(item["rows"])
        failures += bool(item.get("error"))
    error_kinds = dict(
        Counter(str(item.get("error")) for item in payloads if item.get("error"))
    )
    total = hashlib.sha256("".join(sorted(values)).encode()).hexdigest()
    return {
        "tasks": len(values),
        "rows": rows,
        "failures": failures,
        "digest": total,
        "error_kinds": error_kinds,
    }


def child(spec):
    """输入内部测量清单，输出 JSON 产物；冷启动和热批次分别测量。"""
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_stock_kline,
        prewarm_parallel_fetcher,
        set_config_path,
    )

    set_config_path(spec["config"], async_background_probe=False)
    tasks = json.loads(Path(spec["tasks"]).read_text(encoding="utf-8"))
    output = []
    try:
        for phase in ("cold", "warm"):
            if spec.get("engine") == "thread":
                from zsdtdx.engine import parallel_fetcher as pf

                if pf._thread_runtime is not None:
                    pf._thread_runtime.metrics.clear()
            sampler = TreeSampler()
            sampler.thread.start()
            start = time.perf_counter()
            prewarm = None
            if phase == "cold":
                prewarm = prewarm_parallel_fetcher()
            payloads = get_stock_kline(
                task=tasks, mode="async", qfq=spec.get("qfq", True)
            ).result(timeout=spec["timeout"])
            wall = time.perf_counter() - start
            cpu, peak = sampler.finish()
            result = {
                "phase": phase,
                "wall_seconds": wall,
                "cpu_seconds": cpu,
                "peak_rss_bytes": peak,
                **fingerprint(payloads),
            }
            if spec.get("engine") == "thread":
                result["pipeline"] = pf._thread_runtime.metrics.copy()
            assert result["tasks"] == len(tasks), (result["tasks"], len(tasks))
            output.append(result)
            del payloads, sampler, prewarm
            gc.collect()
        Path(spec["output"]).write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    finally:
        destroy_parallel_fetcher()


def terminate_tree(process):
    """输入超时子进程，输出无；只终止本次基准启动的进程树。"""
    try:
        children = psutil.Process(process.pid).children(recursive=True)
    except psutil.NoSuchProcess:
        children = []
    for target in reversed(children):
        try:
            target.kill()
        except psutil.NoSuchProcess:
            continue
    process.kill()
    process.wait()


def main():
    """输入相邻配置，输出多轮交替基准及一致性报告；任何失败保留日志。"""
    cfg = yaml.safe_load(
        (HERE / "thread_columnar_benchmark.yaml").read_text(encoding="utf-8")
    )
    artifacts = HERE / cfg["artifact_dir"]
    artifacts.mkdir(parents=True, exist_ok=True)
    baseline = artifacts / "baseline"
    if not baseline.exists():
        with zipfile.ZipFile(HERE / cfg["baseline_archive"]) as archive:
            archive.extractall(baseline)
    snapshot = artifacts / "thread_snapshot"
    if not cfg.get("reuse_snapshot", False):
        shutil.copytree(
            ROOT / "src",
            snapshot / "src",
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.nbc", "*.nbi"),
        )
    with zipfile.ZipFile(HERE / cfg["baseline_archive"]) as archive:
        original_cfg = yaml.safe_load(archive.read("src/zsdtdx/config.yaml"))
    original_cfg["logging"]["level"] = "OFF"
    thread_cfg = yaml.safe_load(
        (ROOT / "src/zsdtdx/config.yaml").read_text(encoding="utf-8")
    )
    thread_cfg["logging"]["level"] = "OFF"
    for name, config in (("baseline", original_cfg), ("thread", thread_cfg)):
        (artifacts / f"{name}.yaml").write_text(
            yaml.safe_dump(config, allow_unicode=True), encoding="utf-8"
        )
    demo = json.loads((HERE / cfg["task_file"]).read_text(encoding="utf-8"))
    clean = [
        {k: t[k] for k in ("code", "freq", "start_time", "end_time")} for t in demo
    ]
    workloads = {"demo_10k": clean}
    if cfg["include_full_market"]:
        sys.path.insert(0, str(snapshot / "src"))
        from zsdtdx import get_client, get_stock_code_name, set_config_path

        set_config_path(str(artifacts / "thread.yaml"), async_background_probe=False)
        with get_client():
            names = get_stock_code_name()
        workloads["full_market_daily"] = [
            dict(
                code=str(code).split(".")[-1],
                freq="d",
                start_time=cfg["benchmark_start"],
                end_time=cfg["benchmark_end"],
            )
            for code in names
        ]
    codes = list(dict.fromkeys(t["code"] for t in clean))[: cfg["mixed_code_count"]]
    workloads["mixed_periods"] = [
        dict(
            code=code,
            freq=freq,
            start_time=cfg["benchmark_start"],
            end_time=cfg["benchmark_end"],
        )
        for code in codes
        for freq in ("5", "15", "30", "60", "d", "w", "m")
    ]
    summary_path = artifacts / "summary.json"
    summaries = (
        json.loads(summary_path.read_text(encoding="utf-8"))
        if cfg.get("resume", False) and summary_path.exists()
        else []
    )
    for name, tasks in workloads.items():
        if cfg.get("workloads") and name not in cfg["workloads"]:
            continue
        task_path = artifacts / f"{name}_tasks.json"
        task_path.write_text(json.dumps(tasks, ensure_ascii=False), encoding="utf-8")
        for round_index in range(cfg["rounds"]):
            if any(
                s["workload"] == name and s["round"] == round_index + 1
                for s in summaries
            ):
                continue
            order = (
                ("baseline", "thread")
                if round_index % 2 == 0
                else ("thread", "baseline")
            )
            pair = {}
            for engine in order:
                reference_dir = cfg.get("baseline_results_dir")
                if engine == "baseline" and reference_dir:
                    reference = (
                        HERE / reference_dir / f"{name}_{round_index + 1}_baseline.json"
                    )
                    pair[engine] = json.loads(reference.read_text(encoding="utf-8"))
                    continue
                target = baseline if engine == "baseline" else snapshot
                stem = f"{name}_{round_index + 1}_{engine}"
                output = artifacts / f"{stem}.json"
                spec = {
                    "config": str(artifacts / f"{engine}.yaml"),
                    "tasks": str(task_path),
                    "output": str(output),
                    "timeout": cfg["child_timeout_seconds"],
                    "engine": engine,
                }
                if name == "mixed_periods" and cfg.get("pin_mixed_hosts", True):
                    config = yaml.safe_load(
                        Path(spec["config"]).read_text(encoding="utf-8")
                    )
                    for source in ("standard", "extended"):
                        config["hosts"][source] = config["hosts"][source][:1]
                    pinned = artifacts / f"mixed_{engine}.yaml"
                    pinned.write_text(
                        yaml.safe_dump(config, allow_unicode=True), encoding="utf-8"
                    )
                    spec["config"] = str(pinned)
                env = {
                    **os.environ,
                    "PYTHONPATH": str(target / "src"),
                    "ZSDTDX_INTERNAL_BENCH_SPEC": json.dumps(spec),
                    "PYTHONIOENCODING": "utf-8",
                }
                with (artifacts / f"{stem}.log").open("w", encoding="utf-8") as log:
                    process = subprocess.Popen(
                        [sys.executable, str(Path(__file__).resolve())],
                        env=env,
                        stdout=log,
                        stderr=log,
                    )
                    try:
                        code = process.wait(
                            timeout=cfg["child_timeout_seconds"] * 2 + 60
                        )
                    except subprocess.TimeoutExpired:
                        terminate_tree(process)
                        raise RuntimeError(f"基准超时: {stem}") from None
                if code:
                    raise RuntimeError(f"基准失败: {stem}，请查看日志")
                pair[engine] = json.loads(output.read_text(encoding="utf-8"))
                print(stem, json.dumps(pair[engine], ensure_ascii=False), flush=True)
            for a, b in zip(pair["baseline"], pair["thread"]):
                assert (a["tasks"], a["rows"], a["failures"], a["digest"]) == (
                    b["tasks"],
                    b["rows"],
                    b["failures"],
                    b["digest"],
                ), f"结果不一致: {name}/{round_index + 1}"
            summaries.append(
                {
                    "workload": name,
                    "round": round_index + 1,
                    "engines": pair,
                    "baseline_reused": bool(cfg.get("baseline_results_dir")),
                }
            )
            (artifacts / "summary.json").write_text(
                json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8"
            )


if __name__ == "__main__":
    specification = os.environ.get("ZSDTDX_INTERNAL_BENCH_SPEC")
    child(json.loads(specification)) if specification else main()
