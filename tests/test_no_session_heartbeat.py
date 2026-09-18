"""离线验收：已建立会话不再发送 pytdx 空闲心跳。"""

import inspect
from pathlib import Path

import yaml

from zsdtdx.base_socket_client import BaseSocketClient
from zsdtdx.exhq import TdxExHq_API
from zsdtdx.hq import TdxHq_API


def test_heartbeat_module_removed():
    """输入：包导入；输出：不存在 zsdtdx.heartbeat 模块。"""
    import importlib

    try:
        importlib.import_module("zsdtdx.heartbeat")
    except ModuleNotFoundError:
        return
    raise AssertionError("zsdtdx.heartbeat 应已删除")


def test_api_has_no_do_heartbeat():
    """输入：标准/扩展 API 类；输出：无 do_heartbeat。"""
    assert not hasattr(TdxHq_API, "do_heartbeat")
    assert not hasattr(TdxExHq_API, "do_heartbeat")


def test_socket_client_has_no_heartbeat_param():
    """输入：BaseSocketClient 构造签名；输出：无 heartbeat 参数。"""
    params = inspect.signature(BaseSocketClient.__init__).parameters
    assert "heartbeat" not in params


def test_config_has_no_heartbeat_switch():
    """输入：包内 config.yaml；输出：pool 不再包含 heartbeat。"""
    cfg = yaml.safe_load(
        (
            Path(__file__).resolve().parents[1] / "src" / "zsdtdx" / "config.yaml"
        ).read_text(encoding="utf-8")
    )
    assert "heartbeat" not in cfg.get("pool", {})
