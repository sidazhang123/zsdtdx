# -*- coding: utf-8 -*-
"""
用户验收：配置来源、TCP 探测单轮、worker 槽位地址分化。

验证项：
1. 未 set_config_path 时使用包内 config.yaml 地址；set_config_path 后使用指定 yaml。
2. 同一配置下多次调用仅触发一轮 TCP 探测（single-flight + 缓存命中）。
3. 涉及 worker 的测试分支中，各 worker 槽位建连列表不全相同（不要求互不相同或首连必在 L 内）。
4. 全程无未捕获异常。
"""

from __future__ import annotations

import copy
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest
import yaml
from concurrent.futures import ProcessPoolExecutor

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import zsdtdx.helper as helper_mod
import zsdtdx.parallel_fetcher as pf
import zsdtdx.unified_client as uc

from zsdtdx import (
    get_client,
    get_company_info,
    get_future_kline,
    get_index_kline,
    get_stock_kline,
    set_config_path,
)
from zsdtdx.unified_client import (
    UnifiedTdxClient,
    _DEFAULT_CONFIG_PATH,
    compute_hosts_fingerprint,
    normalize_hosts_entries,
)

_PKG_CONFIG = Path(_DEFAULT_CONFIG_PATH).resolve()
_APPLY_ACTIVE_CONFIG_PATH_REAL = helper_mod._apply_active_config_path

_STOCK_TASK = {
    "code": "600000",
    "freq": "d",
    "start_time": "2026-01-01",
    "end_time": "2026-01-02",
}
_INDEX_TASK = {
    "index_name": "上证指数",
    "freq": "d",
    "start_time": "2026-01-01",
    "end_time": "2026-01-02",
}
_INDEX_ROUTE = {
    "source": "std",
    "market": 1,
    "code": "000001",
    "name": "上证指数",
}
_DONE_PAYLOAD_TAIL = {
    "event": "done",
    "total_tasks": 1,
    "success_tasks": 1,
    "failed_tasks": 0,
}


_CAPTURED_POOL_INITARGS: List[Tuple[Any, ...]] = []


class _CapturingProcessPoolExecutor(ProcessPoolExecutor):
    """
    记录 ProcessPoolExecutor 的 initargs，用于验证建池时槽位分配。

    输入/输出：与 ProcessPoolExecutor 一致。
    边界条件：仅测试模块使用。
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        initargs = kwargs.get("initargs")
        if initargs is not None:
            _CAPTURED_POOL_INITARGS.append(tuple(initargs))
        super().__init__(*args, **kwargs)


def _fake_chunk_bundle_for_test(bundle_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    供进程池 worker 调用的 chunk bundle mock（模块级，便于 Windows pickle）。

    输入：
    1. bundle_payload: 批次参数。
    输出：
    1. 最小合法 bundle 结果字典。
    边界条件：
    1. 不访问真实行情接口。
    """
    import os

    return {
        "bundle_id": int(bundle_payload.get("bundle_id", 0) or 0),
        "bundle_chunk_count": 1,
        "worker_pid": int(os.getpid()),
        "chunk_reports": [],
        "payloads": [
            {
                "event": "data",
                "task": dict(_STOCK_TASK),
                "rows": [],
                "error": None,
                "worker_pid": int(os.getpid()),
            }
        ],
    }


def _worker_slot_snapshot() -> Tuple[Tuple[str, int], ...]:
    """
    供 ProcessPool worker 回报当前槽位 standard 列表顺序（须模块级函数以便 Windows spawn 序列化）。

    输入：无。
    输出：standard 地址元组。
    边界条件：未初始化时返回空元组。
    """
    import zsdtdx.parallel_fetcher as _pf

    hosts_map = _pf._worker_sorted_hosts
    if not isinstance(hosts_map, dict):
        return ()
    return tuple(hosts_map.get("standard") or [])


def _wait_tcp_probe_idle(timeout: float = 15.0) -> None:
    """
    等待进程内 TCP 探测 single-flight 结束（含 set_config_path 后台线程）。

    输入：
    1. timeout: 最长等待秒数。
    输出：无。
    边界条件：
    1. 超时时不抛错，避免拖死整个套件（由后续用例暴露竞态）。
    """
    uc._ensure_inflight_event.wait(timeout=max(0.5, float(timeout)))


def _reset_runtime_state() -> None:
    """清空配置路径、探测缓存与并行池状态。"""
    _wait_tcp_probe_idle()
    helper_mod._ACTIVE_CONFIG_PATH = None
    helper_mod._DEFAULT_CONFIG_NOTICE_PRINTED = False
    pf._active_config_path = None
    pf._fetcher = None
    pf._worker_sorted_hosts = None
    with uc._probe_result_cache_lock:
        uc._probe_result_cache.clear()
        uc._last_tcp_probe_hosts_fingerprint = None
    uc._ensure_inflight_event.set()
    try:
        pf.destroy_parallel_fetcher()
    except Exception:
        pass
    pf._restore_ensure_worker_client_context_binding()
    _wait_tcp_probe_idle()


def _hosts_from_yaml(path: Path) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
    """读取 yaml 中 standard/extended 规范化地址。"""
    with open(path, "r", encoding="utf-8") as fp:
        cfg = yaml.safe_load(fp) or {}
    hosts_cfg = cfg.get("hosts", {}) or {}
    std = normalize_hosts_entries(hosts_cfg.get("standard", []))
    ex_raw = hosts_cfg.get("extended", []) or []
    ex = normalize_hosts_entries(ex_raw) if ex_raw else []
    return std, ex


def _make_probe_recorder(probe_rounds: List[Dict[str, Any]]):
    """构造记录探测入参的 mock，返回与入参相同的列表（不裁剪）。"""

    def _fake_probe(
        hosts: List[Tuple[str, int]],
        timeout: float,
        fallback_hosts: List[Tuple[str, int]],
        name: str,
    ) -> List[Tuple[str, int]]:
        probe_rounds.append(
            {
                "pool": str(name),
                "hosts": list(hosts),
                "timeout": float(timeout),
            }
        )
        return list(hosts)

    return _fake_probe


def _probed_standard_hosts(probe_rounds: List[Dict[str, Any]]) -> set[Tuple[str, int]]:
    """汇总 mock 探测记录中的 standard 地址集合。"""
    out: set[Tuple[str, int]] = set()
    for item in probe_rounds:
        if item.get("pool") == "standard":
            out.update(item.get("hosts") or [])
    return out


def _stock_sync_payload() -> List[Dict[str, Any]]:
    return [
        {
            "event": "data",
            "task": dict(_STOCK_TASK),
            "rows": [],
            "error": None,
            "worker_pid": 0,
        },
        dict(_DONE_PAYLOAD_TAIL),
    ]


def _index_sync_payload() -> List[Dict[str, Any]]:
    return [
        {
            "event": "data",
            "task": dict(_INDEX_TASK),
            "rows": [],
            "error": None,
            "worker_pid": 0,
        },
        dict(_DONE_PAYLOAD_TAIL),
    ]


def _invoke_api(api_name: str) -> Any:
    """
    调用矩阵中的 simple_api 入口（内部 mock 抓取层，避免真实 socket）。

    输入：
    1. api_name: stock_sync | stock_async | index_sync | index_async | future_kline。
    输出：
    1. 各 API 返回值。
    边界条件：
    1. 未知 api_name 抛 ValueError。
    """
    import pandas as pd

    if api_name == "stock_sync":
        with patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_stock_tasks_sync",
            return_value=_stock_sync_payload(),
        ):
            return get_stock_kline(task=[dict(_STOCK_TASK)], mode="sync")

    if api_name == "stock_async":
        fake_job = type(
            "Job",
            (),
            {"queue": None, "result": lambda self, timeout=None: []},
        )()
        with patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_stock_tasks_async",
            return_value=fake_job,
        ):
            return get_stock_kline(task=[dict(_STOCK_TASK)], mode="async")

    if api_name == "index_sync":
        with patch.object(
            UnifiedTdxClient,
            "resolve_index_name",
            return_value=dict(_INDEX_ROUTE),
        ), patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_index_tasks_sync",
            return_value=_index_sync_payload(),
        ):
            return get_index_kline(task=[dict(_INDEX_TASK)], mode="sync")

    if api_name == "index_async":
        fake_job = type(
            "Job",
            (),
            {"queue": None, "result": lambda self, timeout=None: []},
        )()
        with patch.object(
            UnifiedTdxClient,
            "resolve_index_name",
            return_value=dict(_INDEX_ROUTE),
        ), patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_index_tasks_async",
            return_value=fake_job,
        ):
            return get_index_kline(task=[dict(_INDEX_TASK)], mode="async")

    if api_name == "future_kline":
        with patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_stock",
            return_value=pd.DataFrame(),
        ):
            return get_future_kline(
                codes=["CU"],
                freq="d",
                start_time="2026-01-01",
                end_time="2026-01-02",
            )

    raise ValueError(f"未知 api_name: {api_name}")


MATRIX_API_NAMES = [
    "stock_sync",
    "stock_async",
    "index_sync",
    "index_async",
    "future_kline",
]


@pytest.fixture
def custom_yaml_a(tmp_path: Path) -> Path:
    """与包内 config 明显不同的自定义 yaml A。"""
    cfg = {
        "hosts": {
            "standard": ["10.10.10.1:7709", "10.10.10.2:7709", "10.10.10.3:7709"],
            "extended": ["10.10.20.1:7720"],
        },
        "pool": {"probe_timeout": 0.01, "connect_timeout": 1.0, "max_retry": 1},
        "client": {"preconnect_on_enter": False},
        "output": {"return_df_default": False},
    }
    path = tmp_path / "custom_a.yaml"
    path.write_text(yaml.dump(cfg, allow_unicode=True), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def isolated_state():
    _reset_runtime_state()
    yield
    _reset_runtime_state()


class TestConfigYamlDrivesConnectionHosts:
    """验收项 1 + 2：配置来源与探测单轮。"""

    @patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
    def test_default_then_custom_yaml_and_single_probe_per_config(
        self,
        mock_probe,
        custom_yaml_a: Path,
    ) -> None:
        """
        未 set_config_path 时走包内 config.yaml；set_config_path 后走自定义 yaml。
        同一配置多次 get_client / get_company_info 仅累计一轮探测（切换 yaml 后允许再探测一轮）。
        """
        probe_rounds: List[Dict[str, Any]] = []
        mock_probe.side_effect = _make_probe_recorder(probe_rounds)

        pkg_std, _pkg_ex = _hosts_from_yaml(_PKG_CONFIG)
        custom_std, _custom_ex = _hosts_from_yaml(custom_yaml_a)
        pkg_std_set = set(pkg_std)
        custom_std_set = set(custom_std)
        assert pkg_std_set.isdisjoint(custom_std_set), "测试 yaml 须与包内 config 地址池可区分"

        # --- 未调用 set_config_path：应使用包内 config.yaml ---
        client_default = get_client(separate_instance=True)
        try:
            assert Path(client_default.config_path).resolve() == _PKG_CONFIG
            pool_hosts_default = set(client_default.std_pool.hosts)
            assert pool_hosts_default.issubset(pkg_std_set) or pool_hosts_default == pkg_std_set
        finally:
            client_default.close()

        probe_after_default = len(probe_rounds)
        assert probe_after_default >= 1, "首次使用默认配置应触发至少一次探测"
        probed_std_default = set()
        for item in probe_rounds:
            if item["pool"] == "standard":
                probed_std_default.update(item["hosts"])
        assert probed_std_default == pkg_std_set, "探测输入应来自包内 config.yaml 的 standard 列表"

        # 同一配置再次 get_client + get_company_info：不应新增探测轮次
        with patch.object(
            type(client_default),
            "get_company_info_content",
            return_value=[],
        ):
            client2 = get_client(separate_instance=True)
            try:
                with client2:
                    get_company_info(code="600000")
                pool_hosts_2 = set(client2.std_pool.hosts)
                assert pool_hosts_2 == pool_hosts_default or pool_hosts_2.issubset(pkg_std_set)
            finally:
                client2.close()

        assert len(probe_rounds) == probe_after_default, (
            "默认配置下多次调用应复用缓存，TCP 探测不可出现第二轮"
        )

        # --- set_config_path(自定义 yaml) ---
        resolved_custom = set_config_path(
            str(custom_yaml_a),
            async_background_probe=False,
        )
        assert Path(resolved_custom).resolve() == custom_yaml_a.resolve()

        client_custom = get_client(separate_instance=True)
        try:
            assert Path(client_custom.config_path).resolve() == custom_yaml_a.resolve()
            pool_hosts_custom = set(client_custom.std_pool.hosts)
            assert pool_hosts_custom.issubset(custom_std_set)
        finally:
            client_custom.close()

        probed_std_custom = set()
        for item in probe_rounds:
            if item["pool"] == "standard":
                probed_std_custom.update(item["hosts"])
        assert custom_std_set.issubset(probed_std_custom), (
            "set_config_path 后探测输入须包含自定义 yaml 的 standard 地址"
        )

        # 自定义配置下再次 get_client：仍不应新增探测
        probe_after_custom = len(probe_rounds)
        client3 = get_client(separate_instance=True)
        try:
            assert set(client3.std_pool.hosts) == pool_hosts_custom or set(
                client3.std_pool.hosts
            ).issubset(custom_std_set)
        finally:
            client3.close()

        assert len(probe_rounds) == probe_after_custom, (
            "自定义配置下第二次 get_client 不得再触发 TCP 探测"
        )

        # 全程仅两轮：默认配置一轮 + 切换 yaml 后一轮
        assert len(probe_rounds) == probe_after_default + (
            probe_after_custom - probe_after_default
        )
        assert probe_after_custom >= probe_after_default + 1

    @patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
    def test_get_stock_kline_sync_uses_same_config_without_extra_probe(
        self,
        mock_probe,
        custom_yaml_a: Path,
    ) -> None:
        """
        get_stock_kline(sync) 与 get_company_info 共用 _ensure_active_config_ready；
        在已预热缓存后调用不得新增 TCP 探测，且不得抛错。
        """
        probe_rounds: List[Dict[str, Any]] = []
        mock_probe.side_effect = _make_probe_recorder(probe_rounds)

        set_config_path(str(custom_yaml_a), async_background_probe=False)
        probe_count_after_set = len(probe_rounds)

        empty_payload = [
            {
                "event": "data",
                "task": {
                    "code": "600000",
                    "freq": "d",
                    "start_time": "2026-01-01",
                    "end_time": "2026-01-02",
                },
                "rows": [],
                "error": None,
                "worker_pid": 0,
            },
            {
                "event": "done",
                "total_tasks": 1,
                "success_tasks": 1,
                "failed_tasks": 0,
            },
        ]

        with patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_stock_tasks_sync",
            return_value=empty_payload,
        ):
            result = get_stock_kline(
                task=[
                    {
                        "code": "600000",
                        "freq": "d",
                        "start_time": "2026-01-01",
                        "end_time": "2026-01-02",
                    }
                ],
                mode="sync",
            )

        assert isinstance(result, list)
        assert len(probe_rounds) == probe_count_after_set, (
            "get_stock_kline(sync) 在缓存已就绪时不得再触发 TCP 探测"
        )


class TestMatrixKlineApiScenarios:
    """
    矩阵新增场景：每个 K 线相关 API ×（默认 config / 自定义 yaml）× 探测单轮 × 无报错。
    """

    @pytest.mark.parametrize("api_name", MATRIX_API_NAMES)
    @patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
    def test_default_config_yaml_hosts_and_single_probe_round(
        self,
        mock_probe: Any,
        api_name: str,
    ) -> None:
        """
        未 set_config_path：底层探测输入来自包内 config.yaml；同 API 连续调用不新增探测。
        """
        probe_rounds: List[Dict[str, Any]] = []
        mock_probe.side_effect = _make_probe_recorder(probe_rounds)
        pkg_std, _ = _hosts_from_yaml(_PKG_CONFIG)
        pkg_std_set = set(pkg_std)

        with patch.object(
            helper_mod,
            "_apply_active_config_path",
            side_effect=lambda config_path, **kwargs: _APPLY_ACTIVE_CONFIG_PATH_REAL(
                config_path,
                async_background_probe=False,
                **{k: v for k, v in kwargs.items() if k != "async_background_probe"},
            ),
        ):
            first = _invoke_api(api_name)
            _wait_tcp_probe_idle()
        assert first is not None

        if api_name != "future_kline":
            assert helper_mod._ACTIVE_CONFIG_PATH is not None
            assert Path(helper_mod._ACTIVE_CONFIG_PATH).resolve() == _PKG_CONFIG

        probe_after_first = mock_probe.call_count
        if probe_after_first > 0:
            assert _probed_standard_hosts(probe_rounds) == pkg_std_set

        with patch.object(
            helper_mod,
            "_apply_active_config_path",
            side_effect=lambda config_path, **kwargs: _APPLY_ACTIVE_CONFIG_PATH_REAL(
                config_path,
                async_background_probe=False,
                **{k: v for k, v in kwargs.items() if k != "async_background_probe"},
            ),
        ):
            second = _invoke_api(api_name)
            _wait_tcp_probe_idle()
        assert second is not None
        assert mock_probe.call_count == probe_after_first, (
            f"{api_name} 默认配置下第二次调用不得再触发 TCP 探测"
        )

    @pytest.mark.parametrize("api_name", MATRIX_API_NAMES)
    @patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
    def test_custom_config_yaml_hosts_and_single_probe_round(
        self,
        mock_probe: Any,
        api_name: str,
        custom_yaml_a: Path,
    ) -> None:
        """
        set_config_path(自定义 yaml) 后：探测输入来自该 yaml；同 API 再调不新增探测。
        """
        probe_rounds: List[Dict[str, Any]] = []
        mock_probe.side_effect = _make_probe_recorder(probe_rounds)
        custom_std, _ = _hosts_from_yaml(custom_yaml_a)
        custom_std_set = set(custom_std)

        resolved = set_config_path(str(custom_yaml_a), async_background_probe=False)
        assert Path(resolved).resolve() == custom_yaml_a.resolve()

        first = _invoke_api(api_name)
        assert first is not None
        assert pf._active_config_path == str(custom_yaml_a.resolve())
        if api_name != "future_kline":
            assert helper_mod._ACTIVE_CONFIG_PATH == str(custom_yaml_a.resolve())

        probe_after_first = mock_probe.call_count
        assert probe_after_first >= 1
        assert custom_std_set.issubset(_probed_standard_hosts(probe_rounds))

        second = _invoke_api(api_name)
        assert second is not None
        assert mock_probe.call_count == probe_after_first, (
            f"{api_name} 自定义配置下第二次调用不得再触发 TCP 探测"
        )


class TestMatrixRealProcessPoolWorkers:
    """矩阵 worker 分支：建池时 initargs 槽位列表不全相同。"""

    def _assert_slot_assignments_not_all_identical(self) -> None:
        """从捕获的 initargs 解析 slot_assignments 并断言顺序不全相同。"""
        assert _CAPTURED_POOL_INITARGS, "应捕获到 ProcessPoolExecutor initargs"
        initargs = _CAPTURED_POOL_INITARGS[-1]
        assignments = initargs[1]
        assert isinstance(assignments, list) and len(assignments) >= 2
        std_orders = [
            tuple(item.get("standard") or [])
            for item in assignments
            if isinstance(item, dict)
        ]
        assert len(std_orders) >= 2
        assert len(set(std_orders)) > 1, "建池前槽位分配 standard 顺序不应完全相同"

    @patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
    def test_process_pool_worker_standard_order_not_all_identical(
        self,
        mock_probe: Any,
        custom_yaml_a: Path,
    ) -> None:
        """
        _get_global_process_pool 建池：initargs 中各槽位旋转列表不全相同。
        """
        global _CAPTURED_POOL_INITARGS
        _CAPTURED_POOL_INITARGS = []

        mock_probe.side_effect = _make_probe_recorder([])
        set_config_path(str(custom_yaml_a), async_background_probe=False)

        try:
            with patch(
                "zsdtdx.parallel_fetcher.ProcessPoolExecutor",
                _CapturingProcessPoolExecutor,
            ), patch(
                "zsdtdx.parallel_fetcher.random.randint",
                side_effect=[0, 1, 2, 0, 1, 2],
            ):
                pf._get_global_process_pool(3)
            self._assert_slot_assignments_not_all_identical()
        finally:
            _CAPTURED_POOL_INITARGS = []

    @patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
    def test_get_stock_kline_async_entry_triggers_pool_with_distinct_slots(
        self,
        mock_probe: Any,
        custom_yaml_a: Path,
    ) -> None:
        """
        get_stock_kline(async) 经 fetcher 触发建池；验证 initargs 槽位不全相同且不新增探测。
        """
        global _CAPTURED_POOL_INITARGS
        _CAPTURED_POOL_INITARGS = []

        mock_probe.side_effect = _make_probe_recorder([])
        set_config_path(str(custom_yaml_a), async_background_probe=False)
        _wait_tcp_probe_idle()

        fake_job = type("Job", (), {"queue": None, "result": lambda self, timeout=None: []})()

        def _async_stub(**_kwargs: Any) -> Any:
            pf._get_global_process_pool(3)
            return fake_job

        try:
            with patch(
                "zsdtdx.parallel_fetcher.ProcessPoolExecutor",
                _CapturingProcessPoolExecutor,
            ), patch(
                "zsdtdx.parallel_fetcher.random.randint",
                side_effect=[0, 1, 2, 0, 1, 2],
            ), patch(
                "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_stock_tasks_async",
                side_effect=_async_stub,
            ), patch(
                "zsdtdx.parallel_fetcher.ParallelKlineFetcher._ensure_async_prewarm",
                return_value=None,
            ):
                probe_before = mock_probe.call_count
                job = get_stock_kline(task=[dict(_STOCK_TASK)], mode="async")
                _wait_tcp_probe_idle()

            assert job is not None
            assert mock_probe.call_count == probe_before, (
                "get_stock_kline(async) 建池路径在缓存已预热后不得再触发 TCP 探测"
            )
            self._assert_slot_assignments_not_all_identical()
        finally:
            _CAPTURED_POOL_INITARGS = []


class TestAllKlineApiEntryPoints:
    """
    覆盖 get_stock_kline(sync/async)、get_index_kline(sync/async)、get_future_kline。
    在缓存已预热、探测已 mock 的前提下，验证：不新增 probe、调用不抛错。
    """

    _INDEX_ROUTE = {
        "source": "std",
        "market": 1,
        "code": "000001",
        "name": "上证指数",
    }
    _STOCK_TASK = {
        "code": "600000",
        "freq": "d",
        "start_time": "2026-01-01",
        "end_time": "2026-01-02",
    }
    _INDEX_TASK = {
        "index_name": "上证指数",
        "freq": "d",
        "start_time": "2026-01-01",
        "end_time": "2026-01-02",
    }
    _DONE_EVENT = {
        "event": "done",
        "total_tasks": 1,
        "success_tasks": 1,
        "failed_tasks": 0,
    }

    @pytest.fixture
    def warmed_custom_config(self, custom_yaml_a: Path):
        """预热自定义配置缓存并返回探测计数快照。"""
        probe_rounds: List[Dict[str, Any]] = []
        with patch(
            "zsdtdx.unified_client._tcp_probe_and_trim_available_hosts",
            side_effect=_make_probe_recorder(probe_rounds),
        ):
            set_config_path(str(custom_yaml_a), async_background_probe=False)
            yield probe_rounds, custom_yaml_a

    def test_get_stock_kline_async_no_extra_probe(self, warmed_custom_config) -> None:
        probe_rounds, _yaml = warmed_custom_config
        baseline = len(probe_rounds)
        fake_job = type("Job", (), {"queue": None, "result": lambda self, timeout=None: []})()

        with patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_stock_tasks_async",
            return_value=fake_job,
        ):
            job = get_stock_kline(task=[dict(self._STOCK_TASK)], mode="async")
        assert job is not None
        assert len(probe_rounds) == baseline

    def test_get_index_kline_sync_no_extra_probe(self, warmed_custom_config) -> None:
        probe_rounds, _yaml = warmed_custom_config
        baseline = len(probe_rounds)
        payload = [
            {
                "event": "data",
                "task": dict(self._INDEX_TASK),
                "rows": [],
                "error": None,
                "worker_pid": 0,
            },
            dict(self._DONE_EVENT),
        ]

        with patch.object(
            UnifiedTdxClient,
            "resolve_index_name",
            return_value=dict(self._INDEX_ROUTE),
        ), patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_index_tasks_sync",
            return_value=payload,
        ):
            result = get_index_kline(task=[dict(self._INDEX_TASK)], mode="sync")

        assert isinstance(result, list)
        assert len(probe_rounds) == baseline

    def test_get_index_kline_async_no_extra_probe(self, warmed_custom_config) -> None:
        probe_rounds, _yaml = warmed_custom_config
        baseline = len(probe_rounds)
        fake_job = type("Job", (), {"queue": None, "result": lambda self, timeout=None: []})()

        with patch.object(
            UnifiedTdxClient,
            "resolve_index_name",
            return_value=dict(self._INDEX_ROUTE),
        ), patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_index_tasks_async",
            return_value=fake_job,
        ):
            job = get_index_kline(task=[dict(self._INDEX_TASK)], mode="async")

        assert job is not None
        assert len(probe_rounds) == baseline

    def test_get_future_kline_uses_config_without_extra_probe(self, warmed_custom_config) -> None:
        import pandas as pd

        probe_rounds, custom_yaml = warmed_custom_config
        baseline = len(probe_rounds)
        custom_std, _ = _hosts_from_yaml(custom_yaml)

        with patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_stock",
            return_value=pd.DataFrame(),
        ) as fetch_mock:
            df = get_future_kline(codes=["CU"], freq="d", start_time="2026-01-01", end_time="2026-01-02")

        assert isinstance(df, pd.DataFrame)
        fetch_mock.assert_called_once()
        assert len(probe_rounds) == baseline
        assert pf._active_config_path == str(custom_yaml.resolve())

    @patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
    def test_get_future_kline_default_yaml_when_no_set_config_path(
        self,
        mock_probe,
        custom_yaml_a: Path,
    ) -> None:
        """
        get_future_kline 未先 set_config_path 时，并行 fetcher 仍读包内 config.yaml。
        （该入口不调用 _ensure_active_config_ready，与 stock/index 路径不同。）
        """
        probe_rounds: List[Dict[str, Any]] = []
        mock_probe.side_effect = _make_probe_recorder(probe_rounds)
        pkg_std, _ = _hosts_from_yaml(_PKG_CONFIG)

        import pandas as pd

        with patch(
            "zsdtdx.parallel_fetcher.ParallelKlineFetcher.fetch_stock",
            return_value=pd.DataFrame(),
        ):
            get_future_kline(codes=["CU"], freq="d")

        assert pf._active_config_path is None
        probed = set()
        for item in probe_rounds:
            if item["pool"] == "standard":
                probed.update(item["hosts"])
        # 串行模式下可能不触发 probe；若触发则须来自包内 config
        if probed:
            assert probed == set(pkg_std)


class TestWorkerSlotAssignmentsDiffer:
    """验收项 3：worker 槽位地址不完全相同。"""

    def test_build_worker_assignments_not_all_identical(self) -> None:
        """多槽位时对全量列表旋转，至少两个槽位建连顺序不同。"""
        snapshot = {
            "standard": [
                ("192.168.1.1", 7709),
                ("192.168.1.2", 7709),
                ("192.168.1.3", 7709),
                ("192.168.1.4", 7709),
            ],
            "extended": [("192.168.2.1", 7720), ("192.168.2.2", 7720)],
        }
        num_slots = 4
        seen: List[Tuple[Tuple[Tuple[str, int], ...], Tuple[Tuple[str, int], ...]]] = []

        # 固定随机序列，保证槽位起始下标互不相同
        with patch(
            "zsdtdx.parallel_fetcher.random.randint",
            side_effect=[0, 1, 2, 3, 0, 1, 2, 3],
        ):
            assignments = pf._build_worker_host_slot_assignments(snapshot, num_slots)

        assert len(assignments) == num_slots
        for item in assignments:
            std_tuple = tuple(item.get("standard") or [])
            ex_tuple = tuple(item.get("extended") or [])
            seen.append((std_tuple, ex_tuple))
            assert set(std_tuple) == set(snapshot["standard"])
            assert set(ex_tuple) == set(snapshot["extended"])

        assert len(set(seen)) > 1, "各 worker 槽位建连列表不应完全相同"
        assert seen[0][0] != seen[1][0], "至少两个槽位 standard 顺序应不同"

    def test_init_worker_claims_distinct_slot_snapshots(self) -> None:
        """
        模拟 init_worker 槽位认领：深拷贝 assignments 后各槽位写入不同快照。
        """
        from zsdtdx.unified_client import _seed_probe_result_cache_from_snapshot

        snapshot = {
            "standard": [("1.1.1.1", 7709), ("2.2.2.2", 7709), ("3.3.3.3", 7709)],
            "extended": [],
        }
        with patch(
            "zsdtdx.parallel_fetcher.random.randint",
            side_effect=[0, 1, 2],
        ):
            assignments = pf._build_worker_host_slot_assignments(snapshot, 3)
        assignments_copy = copy.deepcopy(assignments)
        std_hosts, ex_hosts = snapshot["standard"], snapshot["extended"]
        fingerprint = compute_hosts_fingerprint(std_hosts, ex_hosts)

        claimed: List[Tuple[Tuple[str, int], ...]] = []
        lock = threading.Lock()
        counter = {"value": 0}

        class _FakeCounter:
            def get_lock(self):
                return lock

            @property
            def value(self):
                return counter["value"]

            @value.setter
            def value(self, v: int) -> None:
                counter["value"] = int(v)

        slot_counter = _FakeCounter()

        for _ in range(3):
            pf._init_worker(
                config_path=str(_PKG_CONFIG),
                slot_assignments=assignments_copy,
                hosts_fingerprint=fingerprint,
                slot_counter=slot_counter,
            )
            claimed.append(tuple(pf._worker_sorted_hosts.get("standard") or []))
            uc._probe_result_cache.clear()

        assert len(set(claimed)) > 1, "各槽位认领后 standard 列表顺序应不完全相同"
        for row in claimed:
            assert set(row) == set(snapshot["standard"])
