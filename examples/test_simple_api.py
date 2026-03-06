"""Quick sanity test for simple_api functions. This script exercises all public
interfaces with small dummy inputs. It is intended to be run in an environment with
a valid TDX config or using the builtin default config. The tests are simple and
print results/exception information; they are not meant as exhaustive unit tests.

Usage:
    python examples/test_simple_api.py

Adjust codes/time windows as needed for your environment.
"""

import os
import sys
import queue
import traceback

# ensure local workspace modules take precedence over installed package
# ensure local workspace modules take precedence over installed package
root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
src = os.path.join(root, "src")
if src not in sys.path:
    sys.path.insert(0, src)


from zsdtdx.simple_api import (
    StockKlineTask,
    StockKlineJob,
    set_config_path,
    get_client,
    get_supported_markets,
    get_stock_code_name,
    get_all_future_list,
    get_stock_kline,
    prewarm_parallel_fetcher,
    restart_parallel_fetcher,
    destroy_parallel_fetcher,
    get_future_kline,
    get_company_info,
    get_stock_latest_price,
    get_future_latest_price,
    get_runtime_failures,
    get_runtime_metadata,
)


def run():
    print("[test] starting simple_api quick test")

    # set config path to default (optional)
    try:
        set_config_path("")
    except Exception:
        # ignore if empty path not allowed
        pass

    # client-based calls
    with get_client() as client:
        print("supported markets:", get_supported_markets(return_df=True))
        stock_map = get_stock_code_name(use_cache=False)
        print("stock code name sample entries:", list(stock_map.items())[:5])
        print("all future list (sample):", get_all_future_list(return_df=True).head())
        print("stock latest price sample:", get_stock_latest_price(["600000", "000001"]))
        print("future latest price sample:", get_future_latest_price(["CU2603"]))
        print("company info sample:", get_company_info("600000", category=["公司概况"]))
        print("runtime failures:", get_runtime_failures())
        print("runtime metadata:", get_runtime_metadata())

    # stock kline sync with queue
    q = queue.Queue()
    task_list = [
        StockKlineTask(code="600000", freq="d", start_time="2026-02-01", end_time="2026-02-02"),
    ]
    try:
        sync_result = get_stock_kline(task=task_list, queue=q, mode="sync")
        print("sync result length", len(sync_result))
        while not q.empty():
            print("queue event", q.get())
    except Exception:
        print("sync kline failed")
        traceback.print_exc()

    # stock kline async with auto queue
    try:
        job: StockKlineJob = get_stock_kline(task=task_list, mode="async")
        print("async job returned", job)
        # consume some events
        import queue as _q
        while True:
            try:
                ev = job.queue.get(timeout=5)
            except _q.Empty:
                # queue drained
                break
            print("async event", ev)
            if ev.get("event") == "done":
                break
        try:
            job.result()
        except Exception as e:
            print("job.result() raised", e)
    except Exception:
        print("async kline failed")
        traceback.print_exc()

    # prewarm / restart / destroy
    try:
        print("prewarm summary", prewarm_parallel_fetcher(require_all_workers=False, timeout_seconds=5))
    except Exception:
        print("prewarm failed")
        traceback.print_exc()
    try:
        print("restart summary", restart_parallel_fetcher(prewarm=False))
    except Exception:
        print("restart failed")
        traceback.print_exc()
    try:
        print("destroy summary", destroy_parallel_fetcher())
    except Exception:
        print("destroy failed")
        traceback.print_exc()

    # future kline
    try:
        df = get_future_kline(codes=["CU"], freq="d", start_time="2026-02-01", end_time="2026-02-02")
        print("future kline df head", df.head())
    except Exception:
        print("future kline failed")
        traceback.print_exc()

    print("[test] finished")


if __name__ == "__main__":
    run()
