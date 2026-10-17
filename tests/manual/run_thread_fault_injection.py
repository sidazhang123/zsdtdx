"""模块：`manual/run_thread_fault_injection.py`。

职责：真实行情批次中关闭一部分连接，核对恢复后的完整结果和资源回收。
边界：只关闭本次运行时的 socket；参数来自相邻 YAML，不修改 host 数据。
"""

from __future__ import annotations

import gc
import json
from pathlib import Path
import random
import socket
import sys
import threading

import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "src"))

from run_thread_columnar_benchmark import fingerprint  # noqa: E402
from zsdtdx import destroy_parallel_fetcher, get_stock_kline, set_config_path  # noqa: E402
from zsdtdx.engine import parallel_fetcher as pf  # noqa: E402


def sockets_in_runtime():
    """输入无，输出本次运行时连接快照；只读注册表，关闭时不持有池锁。"""
    client = pf._shared_client_context
    if client is None:
        return []
    sockets = {}
    for pool in (client.std_pool, client.ex_pool):
        with pool._thread_registry_lock:
            apis = [
                entry["data"].get("api") for entry in pool._thread_registry.values()
            ]
        with pool._idle_lock:
            apis += [api for group in pool._idle.values() for api in group]
        for api in apis:
            sock = getattr(api, "client", None)
            if sock is not None and sock.fileno() >= 0:
                sockets[id(sock)] = sock
    return list(sockets.values())


def main():
    """输入 YAML，输出完整指纹与回收报告；恢复后任何数据变化均失败。"""
    cfg = yaml.safe_load(
        (HERE / "thread_fault_injection.yaml").read_text(encoding="utf-8")
    )
    set_config_path(str(HERE / cfg["config"]), async_background_probe=False)
    tasks = json.loads((HERE / cfg["task_file"]).read_text(encoding="utf-8"))
    tasks = [
        {k: task[k] for k in ("code", "freq", "start_time", "end_time")}
        for task in tasks
    ]
    report, cuts = {}, []
    try:
        clean = get_stock_kline(task=tasks, mode="async").result(
            timeout=cfg["timeout_seconds"]
        )
        report["clean"] = fingerprint(clean)
        del clean
        gc.collect()
        finished = threading.Event()

        def inject():
            """输入无，输出无；定期切断随机四分之一连接，结束立即停止。"""
            rng = random.Random(cfg["seed"])
            for _ in range(cfg["rounds"]):
                if finished.wait(cfg["interval_seconds"]):
                    return
                sockets = sockets_in_runtime()
                selected = rng.sample(
                    sockets,
                    min(len(sockets), max(1, int(len(sockets) * cfg["fraction"]))),
                )
                count = 0
                for sock in selected:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    try:
                        sock.close()
                        count += 1
                    except OSError:
                        pass
                cuts.append(count)

        injector = threading.Thread(
            target=inject, name="test_socket_injection", daemon=True
        )
        injector.start()
        try:
            fault = get_stock_kline(task=tasks, mode="async").result(
                timeout=cfg["timeout_seconds"]
            )
            report["fault"] = fingerprint(fault)
            del fault
        finally:
            finished.set()
            injector.join(timeout=2)
        report["closed_connections"] = cuts
        report["passed"] = report["clean"] == report["fault"] and sum(cuts) > 0
    finally:
        report["destroy"] = destroy_parallel_fetcher()
        report["residual_fetch_threads"] = [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith(("zsdtdx_fetch", "zsdtdx_deadlines"))
        ]
        output = HERE / cfg["output"]
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    assert report["passed"] and not report["residual_fetch_threads"], (
        "故障注入或资源回收未通过"
    )


if __name__ == "__main__":
    main()
