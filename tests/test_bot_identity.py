"""离线回归：Resume 不得把 AppID 登记为 Bot 账号。"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from maibot_sdk import PluginContext

from core.constants import (
    EVENT_C2C_MESSAGE_CREATE,
    EVENT_GROUP_MESSAGE_CREATE,
    EVENT_READY,
    EVENT_RESUMED,
    SUPPORTED_CONFIG_VERSION,
)
from core.gateway import QQOfficialAdapterPlugin


APP_ID = "test-app-id"
APP_SECRET = "test-app-secret"
BOT_USER_ID = "test-bot-user-id"
LEGACY_IDENTITY_KEYS = ("self_id", "platform_io_account_id", "bot_account")


def _plugin() -> tuple[QQOfficialAdapterPlugin, list[tuple[str, dict[str, Any]]]]:
    """构造不连网的插件，并记录发往 Host 的网关调用。"""

    plugin = QQOfficialAdapterPlugin()
    plugin.set_plugin_config(
        {
            "plugin": {"enabled": True, "config_version": SUPPORTED_CONFIG_VERSION},
            "credentials": {"appid": APP_ID, "app_secret": APP_SECRET},
        }
    )
    calls: list[tuple[str, dict[str, Any]]] = []

    async def rpc_call(
        method: str,
        plugin_id: str,
        payload: dict[str, Any] | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, bool]:
        del plugin_id, timeout_ms
        calls.append((method, dict(payload or {})))
        return {"accepted": True}

    plugin._set_context(PluginContext("qq-official-adapter", rpc_call=rpc_call))
    return plugin, calls


def _state_updates(calls: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    return [payload for method, payload in calls if method == "host.update_message_gateway_state"]


def _routes(calls: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    return [payload for method, payload in calls if method == "host.route_message"]


def test_stop_keeps_ready_user_id_and_offline_report_does_not_use_appid() -> None:
    """配置重载停连后仍记得 READY user.id，离线上报也不回退到 AppID。"""

    async def scenario() -> None:
        plugin, calls = _plugin()
        plugin._connected_account_id = BOT_USER_ID
        plugin._resumable = True
        plugin._session_id = "test-session"

        await plugin._stop_connection()

        assert plugin._connected_account_id == BOT_USER_ID
        assert plugin._resumable is True
        assert plugin._session_id == "test-session"
        updates = _state_updates(calls)
        assert len(updates) == 1
        assert updates[0]["ready"] is False
        assert updates[0]["account_id"] == BOT_USER_ID
        assert updates[0]["account_id"] != APP_ID
        assert updates[0]["metadata"]["appid"] == APP_ID

    asyncio.run(scenario())


def test_ready_without_bot_user_id_is_not_reported() -> None:
    """没有 READY user.id 时不标记就绪，也不向 Host 发送 AppID。"""

    async def scenario() -> None:
        plugin, calls = _plugin()

        assert await plugin._report_gateway_ready(True, settings=plugin.config) is False
        plugin._connected_account_id = "   "
        assert await plugin._report_gateway_ready(True, settings=plugin.config) is False
        assert _state_updates(calls) == []

    asyncio.run(scenario())


def test_offline_report_without_identity_leaves_account_id_empty() -> None:
    """尚未识别机器人时，未就绪上报的 account_id 保持为空。"""

    async def scenario() -> None:
        plugin, calls = _plugin()

        assert await plugin._report_gateway_ready(False, settings=plugin.config) is True
        updates = _state_updates(calls)
        assert len(updates) == 1
        assert updates[0]["ready"] is False
        assert updates[0]["account_id"] == ""
        assert updates[0]["account_id"] != APP_ID

    asyncio.run(scenario())


def test_ready_event_and_resume_after_stop_report_bot_user_id() -> None:
    """READY 记下 user.id；停连后再 RESUMED，仍用该 ID 而不是 AppID 标记就绪。"""

    async def scenario() -> None:
        plugin, calls = _plugin()
        await plugin._handle_dispatch_event(
            {
                "t": EVENT_READY,
                "d": {
                    "session_id": "test-session",
                    "user": {"id": BOT_USER_ID, "username": "tester"},
                },
            }
        )
        assert plugin._connected_account_id == BOT_USER_ID
        ready_update = _state_updates(calls)[-1]
        assert ready_update["ready"] is True
        assert ready_update["account_id"] == BOT_USER_ID
        assert ready_update["account_id"] != ready_update["metadata"]["appid"]

        await plugin._stop_connection()
        calls.clear()
        await plugin._handle_dispatch_event({"t": EVENT_RESUMED, "d": {}})

        resumed = _state_updates(calls)
        assert len(resumed) == 1
        assert resumed[0]["ready"] is True
        assert resumed[0]["account_id"] == BOT_USER_ID
        assert resumed[0]["account_id"] != APP_ID

    asyncio.run(scenario())


def test_resume_without_ready_user_id_does_not_fall_back_to_appid() -> None:
    """进程内从未取得 user.id 时，RESUMED 不向 Host 登记任何账号。"""

    async def scenario() -> None:
        plugin, calls = _plugin()
        plugin._resumable = True
        plugin._session_id = "test-session"

        await plugin._handle_dispatch_event({"t": EVENT_RESUMED, "d": {}})

        assert _state_updates(calls) == []
        assert plugin._connected_account_id == ""

    asyncio.run(scenario())


def test_ready_missing_user_id_does_not_report_appid() -> None:
    """READY 缺少 user.id 时直接失败，不会用 AppID 补一次就绪上报。"""

    async def scenario() -> None:
        plugin, calls = _plugin()

        with pytest.raises(RuntimeError, match="机器人自身 ID"):
            await plugin._handle_dispatch_event(
                {
                    "t": EVENT_READY,
                    "d": {"session_id": "test-session", "user": {}},
                }
            )

        assert _state_updates(calls) == []
        assert plugin._connected_account_id == ""

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("event_type", "data"),
    [
        (
            EVENT_GROUP_MESSAGE_CREATE,
            {
                "id": "test-group-message",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "content": "hello",
                "group_openid": "test-group-openid",
                "author": {"member_openid": "test-member-openid", "username": "member"},
            },
        ),
        (
            EVENT_C2C_MESSAGE_CREATE,
            {
                "id": "test-c2c-message",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "content": "hello",
                "author": {"user_openid": "test-user-openid", "username": "user"},
            },
        ),
    ],
)
def test_inbound_account_id_is_ready_user_id_without_legacy_fields(
    event_type: str,
    data: dict[str, Any],
) -> None:
    """群聊和单聊都把真实 Bot ID 放在顶层，旧身份字段不再进入 additional_config。"""

    async def scenario() -> None:
        plugin, _calls = _plugin()
        plugin._connected_account_id = BOT_USER_ID

        message = await plugin._build_inbound_message_dict(event_type, data)

        assert message["account_id"] == BOT_USER_ID
        additional = message["message_info"]["additional_config"]
        for key in (*LEGACY_IDENTITY_KEYS, "account_id"):
            assert key not in additional
        assert APP_ID not in additional.values()
        assert message["message_info"]["user_info"]["user_id"] != BOT_USER_ID
        assert message["message_info"]["user_info"]["user_id"] != APP_ID

        plugin._connected_account_id = ""
        anonymous = await plugin._build_inbound_message_dict(
            event_type,
            {**data, "id": f"{data['id']}-anonymous"},
        )
        assert "account_id" not in anonymous
        for key in LEGACY_IDENTITY_KEYS:
            assert key not in anonymous["message_info"]["additional_config"]

    asyncio.run(scenario())


def test_dispatched_message_route_has_no_legacy_identity() -> None:
    """入站 route_metadata 不再携带 self_id，Host 只看消息顶层 account_id。"""

    async def scenario() -> None:
        plugin, calls = _plugin()
        plugin._connected_account_id = BOT_USER_ID

        await plugin._handle_dispatch_event(
            {
                "t": EVENT_GROUP_MESSAGE_CREATE,
                "d": {
                    "id": "test-routed-message",
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "content": "hello",
                    "group_openid": "test-group-openid",
                    "author": {"member_openid": "test-member-openid", "username": "member"},
                },
            }
        )

        routes = _routes(calls)
        assert len(routes) == 1
        assert routes[0]["route_metadata"] == {}
        message = routes[0]["message"]
        assert message["account_id"] == BOT_USER_ID
        additional = message["message_info"]["additional_config"]
        for key in (*LEGACY_IDENTITY_KEYS, "account_id"):
            assert key not in additional
        assert _state_updates(calls) == []

    asyncio.run(scenario())
