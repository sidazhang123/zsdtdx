"""
模块：`test_stock_concept_concurrency.py`。

职责：
1. 验证并发目录请求不会覆盖股票归属容错路径已取得的部分来源。
2. 验证容错时码表获取失败仍会清理本次调用的暂存状态。

边界：
1. 只替换下载与码表依赖，不联网、不读写磁盘缓存。
2. 使用事件协调并发顺序，线程均有退出期限。
"""

import threading

import pytest

from zsdtdx.engine.unified_client import UnifiedTdxClient


def _partial_client():
    """输入无，输出独立锁的离线客户端；仅infoharbor来源可用，禁止磁盘缓存。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client._catalog_cache_enabled = False
    client._block_catalog_lock = threading.RLock()
    client.get_stock_code_name_map = lambda: {"sz.000001": "平安银行"}
    client._download_named_hq_file = lambda name: (
        "#ZS_独立指数,1,,\n0#000001,\n".encode("gbk")
        if name == "infoharbor_block.dat"
        else b""
    )
    return client


def test_parallel_directory_request_preserves_partial_stock_concepts():
    """输入并发目录与部分归属请求，输出有效归属；等待目录尝试后仍不得抢占容错状态。"""
    client = _partial_client()
    first_failed = threading.Event()
    second_attempted = threading.Event()
    second_downloading = threading.Event()
    release_second = threading.Event()
    results = {}
    errors = []
    original_ensure = client._ensure_block_catalog_snapshot
    original_download = client._download_named_hq_file

    def download(name):
        """输入远程文件名，输出夹具；第二请求下载时停住，以暴露覆盖暂存字段的竞态。"""
        if threading.current_thread().name == "catalog-reader":
            second_downloading.set()
            if not release_second.wait(2):
                errors.append(AssertionError("第二请求等待放行超时"))
            return b""
        return original_download(name)

    def ensure():
        """输入无，输出真实构建结果；首次来源失败后给另一线程尝试进入的机会。"""
        try:
            return original_ensure()
        except ValueError:
            if threading.current_thread().name == "concept-reader":
                first_failed.set()
                if not second_attempted.wait(2):
                    raise AssertionError("第二请求未到达")
                # 正确持锁时第二请求只能等待；旧实现会先清空暂存原文再停在下载。
                results["interleaved"] = second_downloading.wait(0.2)
            raise

    def read_concepts():
        """输入无，保存公开股票归属结果或异常；无论成功失败均放行另一请求。"""
        try:
            results["payload"] = client.get_stock_concepts()
        except BaseException as exc:
            errors.append(exc)
        finally:
            release_second.set()

    def read_directory():
        """输入无，尝试公开目录请求；模拟缺源导致的ValueError属于预期结果。"""
        if not first_failed.wait(2):
            errors.append(AssertionError("首次来源失败未发生"))
            return
        second_attempted.set()
        try:
            client.get_block_names()
        except ValueError:
            pass
        except BaseException as exc:
            errors.append(exc)

    client._download_named_hq_file = download
    client._ensure_block_catalog_snapshot = ensure
    first = threading.Thread(target=read_concepts, name="concept-reader", daemon=True)
    second = threading.Thread(target=read_directory, name="catalog-reader", daemon=True)
    first.start()
    second.start()
    try:
        first.join(timeout=3)
        second.join(timeout=3)
        assert not first.is_alive() and not second.is_alive()
        assert not errors, repr(errors)
        assert results["interleaved"] is False
        assert results["payload"] == {
            "names": [("指数成分", "独立指数")],
            "map": {"平安银行": [("指数成分", "独立指数")]},
        }
    finally:
        release_second.set()


def test_fallback_code_name_error_clears_temporary_state():
    """输入部分来源和码表异常，输出同一异常；待处理码表与原文必须全部清理。"""
    client = _partial_client()
    original_error = RuntimeError("码表获取失败")

    def get_names():
        """输入无，抛出固定异常；不允许用成功空结果掩盖码表失败。"""
        raise original_error

    client.get_stock_code_name_map = get_names
    with pytest.raises(RuntimeError, match="码表获取失败") as caught:
        client.get_stock_concepts()
    assert caught.value is original_error
    assert client._block_pending_code_name is None
    assert client._block_partial_named_files == {}
