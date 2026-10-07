"""业务层主进程客户端借用回归测试（不联网）。"""

from __future__ import annotations

import pytest

import zsdtdx.biz._client_context as client_context


def test_call_with_main_client_reuses_active_context(monkeypatch):
    """
    输入活跃上下文客户端，输出回调结果。
    用途：确认业务层不重复创建或关闭用户管理的客户端。
    边界：不调用配置解析与网络连接。
    """
    active = object()
    monkeypatch.setattr(
        client_context.UnifiedTdxClient,
        "get_active_context_client",
        staticmethod(lambda: active),
    )
    monkeypatch.setattr(
        client_context,
        "_ensure_active_config_ready",
        lambda **_kwargs: pytest.fail("活跃上下文不应重建客户端"),
    )
    assert (
        client_context.call_with_main_client(
            lambda client: client is active,
            caller_name="unit_test",
        )
        is True
    )


def test_call_with_main_client_closes_temporary_client_on_error(monkeypatch):
    """
    输入无活跃上下文且回调抛错的场景，输出原异常并关闭临时客户端。
    用途：防止去除 simple_api 反向依赖后出现连接泄漏。
    边界：仅验证生命周期，不建立真实 socket。
    """
    created = []

    class FakeClient:
        @staticmethod
        def get_active_context_client():
            return None

        def __init__(self, config_path):
            self.config_path = config_path
            self.closed = False
            created.append(self)

        def close(self):
            self.closed = True

    monkeypatch.setattr(client_context, "UnifiedTdxClient", FakeClient)
    monkeypatch.setattr(
        client_context,
        "_ensure_active_config_ready",
        lambda **_kwargs: "merged-config.yaml",
    )

    def fail(_client):
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        client_context.call_with_main_client(fail, caller_name="unit_test")
    assert len(created) == 1
    assert created[0].config_path == "merged-config.yaml"
    assert created[0].closed is True
