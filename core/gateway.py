"""实现 QQ 官方 WebSocket 网关生命周期。"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Mapping, Optional

import asyncio
import json
import time

from aiohttp import ClientSession, ClientWebSocketResponse, WSMsgType
from maibot_sdk import (
    CONFIG_RELOAD_SCOPE_SELF,
    MaiBotPlugin,
    MessageGateway,
    ON_BOT_CONFIG_RELOAD,
    PluginConfigBase,
)

from .constants import (
    API_BASE_PRODUCTION,
    API_BASE_SANDBOX,
    DEFAULT_INTENTS,
    DEFAULT_RECONNECT_DELAY_SEC,
    DEFAULT_SHARD_COUNT,
    DEFAULT_SHARD_ID,
    EVENT_READY,
    EVENT_RESUMED,
    HEARTBEAT_FAIL_LIMIT,
    MESSAGE_CREATE_EVENTS,
    OP_DISPATCH,
    OP_HEARTBEAT,
    OP_HEARTBEAT_ACK,
    OP_HELLO,
    OP_IDENTIFY,
    OP_INVALID_SESSION,
    OP_RECONNECT,
    OP_RESUME,
    QQ_OFFICIAL_GATEWAY_NAME,
)
from .client import QQAPIClientMixin
from .messages import QQMessageMixin
from .models import PassiveReplyContext
from .settings import QQOfficialAdapterSettings


class QQOfficialAdapterPlugin(QQAPIClientMixin, QQMessageMixin, MaiBotPlugin):
    """QQ 官方机器人 WebSocket 适配器插件。"""

    config_model: ClassVar[type[PluginConfigBase] | None] = QQOfficialAdapterSettings
    config_reload_subscriptions: ClassVar[tuple[str, ...]] = (ON_BOT_CONFIG_RELOAD,)

    def __init__(self) -> None:
        """初始化适配器插件状态。"""

        super().__init__()
        self._access_token: str = ""
        self._access_token_expire_at: float = 0.0
        self._token_lock: asyncio.Lock = asyncio.Lock()
        self._token_session: Optional[ClientSession] = None
        self._api_session: Optional[ClientSession] = None
        self._media_session: Optional[ClientSession] = None
        self._ws: Optional[ClientWebSocketResponse] = None
        self._connection_task: Optional[asyncio.Task[None]] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._session_id: str = ""
        self._last_seq: Optional[int] = None
        self._heartbeat_interval_ms: int = 0
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._heartbeat_fail_count: int = 0
        self._connected_account_id: str = ""
        self._connected_account_name: str = ""
        self._bot_display_name: str = ""
        self._bot_mention_ids: set[str] = set()
        self._passive_replies: Dict[str, PassiveReplyContext] = {}
        self._reply_sequences: Dict[str, int] = {}
        self._seen_inbound_message_ids: Dict[str, float] = {}
        self._seq_lock: asyncio.Lock = asyncio.Lock()
        self._resumable: bool = False

    async def on_load(self) -> None:
        """插件加载后按配置启动 WebSocket 连接。"""

        await self._refresh_bot_profile()
        await self._restart_connection_if_needed()

    async def on_unload(self) -> None:
        """插件卸载前关闭 WebSocket 连接并清理资源。"""

        await self._stop_connection()

    async def on_config_update(self, scope: str, config_data: Dict[str, Any], version: str) -> None:
        """同步宿主机器人资料或重启 WebSocket 连接。"""

        if scope == ON_BOT_CONFIG_RELOAD:
            await self._refresh_bot_profile()
            return
        if scope != CONFIG_RELOAD_SCOPE_SELF:
            return

        del config_data
        if version:
            self.ctx.logger.debug(f"QQ 官方适配器收到配置更新: {version}")
        await self._restart_connection_if_needed()

    async def _refresh_bot_profile(self) -> None:
        """通过 MaiBotSDK 同步机器人展示名称。"""

        bot_nickname = await self.ctx.config.get("bot.nickname", "")
        if not isinstance(bot_nickname, str):
            raise TypeError("bot.nickname 必须是字符串")
        self._bot_display_name = bot_nickname.strip()
        self.ctx.logger.debug(f"QQ 官方适配器已同步机器人展示名: {self._bot_display_name or '<未设置>'}")

    @MessageGateway(
        name=QQ_OFFICIAL_GATEWAY_NAME,
        route_type="duplex",
        platform="qq",
        protocol="qq_official",
        description="QQ 官方机器人 WebSocket 双工消息网关",
    )
    async def handle_qq_official_gateway(
        self,
        message: Dict[str, Any],
        route: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """处理 Host 出站消息并通过 QQ REST API 发送。

        QQ 群聊与 C2C 的富媒体需要先上传再以 msg_type=7 发送。标准消息链中的
        文本、媒体和结构化消息会分别发送；用于上下文的 ``[图片]``、``[表情]``
        摘要不会被误当成真实文本。
        """

        del metadata
        del kwargs

        settings = self._load_settings()

        try:
            target, reply_msg_id = self._build_outbound_target(message, route or {})

            content = self._extract_outbound_text(message)
            media_list = self._extract_outbound_media(message)
            rich_payloads = self._extract_outbound_rich_payloads(message)

            if not content and not media_list and not rich_payloads:
                return {"success": False, "error": "出站消息内容为空"}

            last_external_id = ""
            if content:
                last_external_id = await self._send_text(
                    settings,
                    target=target,
                    content=content,
                    reply_msg_id=reply_msg_id,
                )

            for media in media_list:
                external_id = await self._send_media(
                    settings,
                    target=target,
                    media=media,
                    reply_msg_id=reply_msg_id,
                )
                if external_id:
                    last_external_id = external_id

            for rich_payload in rich_payloads:
                external_id = await self._send_structured_message(
                    settings,
                    target=target,
                    payload=rich_payload,
                    reply_msg_id=reply_msg_id,
                )
                if external_id:
                    last_external_id = external_id

            external_message_id = last_external_id
        except Exception as exc:
            self.ctx.logger.error(f"QQ 官方出站消息发送失败: {type(exc).__name__}: {exc}", exc_info=True)
            return {"success": False, "error": str(exc)}

        self.ctx.logger.debug(
            f"QQ 官方出站消息发送成功: target_type={target.kind} "
            f"target_id={target.target_id} external_message_id={external_message_id or '?'}"
        )
        internal_message_id = str(message.get("message_id") or "").strip()
        adapter_callbacks: List[Dict[str, Any]] = []
        if internal_message_id and external_message_id and internal_message_id != external_message_id:
            adapter_callbacks.append(
                {
                    "name": "message_id_echo",
                    "payload": {
                        "content": {
                            "type": "echo",
                            "echo": internal_message_id,
                            "actual_id": external_message_id,
                        }
                    },
                }
            )

        return {
            "success": True,
            "external_message_id": external_message_id or None,
            "metadata": {"adapter_callbacks": adapter_callbacks},
        }

    def _load_settings(self) -> QQOfficialAdapterSettings:
        """返回当前强类型配置。"""

        return self.config  # type: ignore[return-value]

    async def _restart_connection_if_needed(self) -> None:
        """根据当前配置重启 WebSocket 连接循环。"""

        await self._stop_connection()
        settings = self._load_settings()
        if not settings.should_connect():
            self.ctx.logger.info("QQ 官方适配器保持空闲状态，因为插件未启用")
            return

        if not settings.credentials.appid or not settings.credentials.app_secret:
            self.ctx.logger.warning("QQ 官方适配器缺少 appid 或 app_secret，不启动 WebSocket 连接")
            return

        self._stop_event = asyncio.Event()
        self._connection_task = asyncio.create_task(self._run_connection_loop(), name="qq-official-adapter-loop")

    async def _stop_connection(self) -> None:
        """停止连接循环并清理资源。"""

        if self._stop_event is not None:
            self._stop_event.set()

        task = self._connection_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._connection_task = None

        await self._disconnect()
        await self._report_gateway_ready(False)
        self._connected_account_id = ""

    async def _run_connection_loop(self) -> None:
        """维持到 QQ 官方 WebSocket 网关的连接。"""

        while self._stop_event is not None and not self._stop_event.is_set():
            settings = self._load_settings()
            try:
                await self._get_access_token()
                await self._connect(settings)
                await self._identify_or_resume(settings)
                await self._listen()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ctx.logger.error(
                    f"QQ 官方 WebSocket 连接异常，稍后重试: {type(exc).__name__}: {exc!r}",
                    exc_info=True,
                )
            finally:
                try:
                    await self._disconnect()
                except Exception as cleanup_exc:
                    self.ctx.logger.warning(
                        f"QQ 官方 WebSocket 断开清理异常: {cleanup_exc!r}",
                        exc_info=True,
                    )
                await self._report_gateway_ready(False)

            if self._stop_event is None or self._stop_event.is_set():
                break
            await asyncio.sleep(DEFAULT_RECONNECT_DELAY_SEC)

    async def _connect(self, settings: QQOfficialAdapterSettings) -> None:
        """获取 WebSocket URL 并建立连接。"""

        base_url = API_BASE_SANDBOX if settings.credentials.sandbox else API_BASE_PRODUCTION
        gateway_url = await self._fetch_gateway_url(base_url)
        self.ctx.logger.info(f"QQ 官方 WebSocket 正在连接: {gateway_url}")
        assert self._api_session is not None
        self._ws = await self._api_session.ws_connect(gateway_url)

    async def _fetch_gateway_url(self, base_url: str) -> str:
        """调用 /gateway/bot 获取 WebSocket URL（同时返回分片建议，这里只用 url）。"""

        await self._get_access_token()
        assert self._api_session is not None
        request_url = f"{base_url}/gateway/bot"
        async with self._api_session.get(request_url) as response:
            status = response.status
            response_text = await response.text()

        if status == 401:
            self.ctx.logger.debug("QQ 网关地址请求 401，刷新 access_token 后重试")
            self._access_token = ""
            await self._get_access_token()
            assert self._api_session is not None
            async with self._api_session.get(request_url) as response:
                status = response.status
                response_text = await response.text()

        if status >= 400:
            raise RuntimeError(f"获取 QQ WebSocket 网关地址失败: HTTP {status} {response_text}")
        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"QQ 网关响应不是有效 JSON: HTTP {status}") from exc
        if not isinstance(data, Mapping):
            raise RuntimeError(f"QQ 网关响应结构无效: {data!r}")
        url = str(data.get("url") or "").strip()
        if not url:
            raise RuntimeError(f"QQ 网关响应缺少 url 字段: {data}")
        return url

    async def _identify_or_resume(self, settings: QQOfficialAdapterSettings) -> None:
        """等待 Hello 后发送 Identify 或 Resume。"""

        if self._ws is None:
            raise RuntimeError("QQ 官方 WebSocket 尚未连接")

        hello_payload = await self._receive_payload(expect_op=OP_HELLO, timeout=10)
        hello_data = hello_payload.get("d") or {}
        self._heartbeat_interval_ms = int(hello_data.get("heartbeat_interval") or 30000)
        self.ctx.logger.debug(f"QQ 官方 WebSocket Hello: heartbeat_interval={self._heartbeat_interval_ms}ms")

        self._heartbeat_fail_count = 0
        self._heartbeat_task = asyncio.create_task(self._run_heartbeat(), name="qq-official-heartbeat")

        if self._resumable and self._session_id:
            await self._send_resume(settings)
        else:
            await self._send_identify(settings)

    async def _send_identify(self, settings: QQOfficialAdapterSettings) -> None:
        """发送 Identify 鉴权包。"""

        access_token = await self._get_access_token()
        payload = {
            "op": OP_IDENTIFY,
            "d": {
                "token": f"QQBot {access_token}",
                "intents": DEFAULT_INTENTS,
                "shard": [DEFAULT_SHARD_ID, DEFAULT_SHARD_COUNT],
                "properties": {
                    "$os": "linux",
                    "$browser": "maibot-qq-official-adapter",
                    "$device": "maibot",
                },
            },
        }
        await self._send_payload(payload)
        self.ctx.logger.debug("QQ 官方 WebSocket 已发送 Identify")

    async def _send_resume(self, settings: QQOfficialAdapterSettings) -> None:
        """发送 Resume 断线续传包。"""

        if self._last_seq is None:
            self._resumable = False
            await self._send_identify(settings)
            return

        access_token = await self._get_access_token()
        payload = {
            "op": OP_RESUME,
            "d": {
                "token": f"QQBot {access_token}",
                "session_id": self._session_id,
                "seq": self._last_seq,
            },
        }
        await self._send_payload(payload)
        self.ctx.logger.debug(f"QQ 官方 WebSocket 已发送 Resume: session_id={self._session_id} seq={self._last_seq}")

    async def _run_heartbeat(self) -> None:
        """按固定间隔发送心跳。"""

        if self._heartbeat_interval_ms <= 0:
            return
        interval_sec = self._heartbeat_interval_ms / 1000.0
        while self._stop_event is not None and not self._stop_event.is_set():
            try:
                await asyncio.sleep(interval_sec)
            except asyncio.CancelledError:
                raise
            try:
                await self._send_payload({"op": OP_HEARTBEAT, "d": self._last_seq})
            except Exception as exc:
                self._heartbeat_fail_count += 1
                self.ctx.logger.warning(
                    f"QQ 官方 WebSocket 心跳发送失败 ({self._heartbeat_fail_count}/{HEARTBEAT_FAIL_LIMIT}): {exc}"
                )
                if self._heartbeat_fail_count >= HEARTBEAT_FAIL_LIMIT:
                    self.ctx.logger.error(f"QQ 官方 WebSocket 心跳连续失败 {HEARTBEAT_FAIL_LIMIT} 次，连接将被关闭")
                    if self._ws is not None and not self._ws.closed:
                        await self._ws.close()
                    return

    async def _listen(self) -> None:
        """监听 QQ 官方 WebSocket 推送。"""

        if self._ws is None:
            return

        async for ws_message in self._ws:
            if ws_message.type == WSMsgType.TEXT:
                await self._handle_text_payload(ws_message.data)
                continue
            if ws_message.type == WSMsgType.BINARY:
                self.ctx.logger.debug("QQ 官方 WebSocket 收到二进制消息，已忽略")
                continue
            if ws_message.type == WSMsgType.ERROR:
                self.ctx.logger.error(f"QQ 官方 WebSocket 接收失败: {self._ws.exception()!r}")
                break
            if ws_message.type == WSMsgType.CLOSED:
                self.ctx.logger.warning(f"QQ 官方 WebSocket 连接已关闭: close_code={self._ws.close_code}")
                break

    async def _handle_text_payload(self, raw_payload: str) -> None:
        """处理 WebSocket 文本载荷。"""

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            self.ctx.logger.warning(f"QQ 官方 WebSocket 收到非 JSON 文本: {raw_payload[:120]}")
            return
        if not isinstance(payload, dict):
            self.ctx.logger.warning(f"QQ 官方 WebSocket 收到非对象 JSON: type={type(payload).__name__}")
            return

        op = payload.get("op")
        seq = payload.get("s")
        if isinstance(seq, int):
            self._last_seq = seq

        if op == OP_HEARTBEAT_ACK:
            self._heartbeat_fail_count = 0
            return

        if op == OP_RECONNECT:
            self.ctx.logger.info("QQ 官方 WebSocket 收到 Reconnect 请求，准备重连")
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
            return

        if op == OP_INVALID_SESSION:
            self.ctx.logger.warning("QQ 官方 WebSocket 收到 Invalid Session，将重新 Identify")
            self._resumable = False
            self._session_id = ""
            self._last_seq = None
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
            return

        if op == OP_DISPATCH:
            await self._handle_dispatch_event(payload)
            return

        self.ctx.logger.debug(f"QQ 官方 WebSocket 收到未知 opcode={op}，已忽略")

    async def _handle_dispatch_event(self, payload: Dict[str, Any]) -> None:
        """处理 QQ 官方 Dispatch 事件。"""

        event_type = str(payload.get("t") or "").strip()
        data = payload.get("d") or {}
        if not isinstance(data, Mapping):
            self.ctx.logger.warning(
                f"QQ 官方 Dispatch 事件数据格式无效: event={event_type or '?'} type={type(data).__name__}"
            )
            return

        if event_type == EVENT_READY:
            self._session_id = str(data.get("session_id") or "").strip()
            user = data.get("user") or {}
            if isinstance(user, Mapping):
                self._connected_account_id = str(user.get("id") or self._load_settings().credentials.appid).strip()
                self._connected_account_name = str(user.get("username") or user.get("name") or "").strip()
                self._remember_bot_identity(user)
            else:
                self._connected_account_id = self._load_settings().credentials.appid
            self._resumable = True
            await self._report_gateway_ready(True, settings=self._load_settings())
            self.ctx.logger.info(
                f"QQ 官方 WebSocket 已就绪: session_id={self._session_id} self_id={self._connected_account_id}"
            )
            return

        if event_type == EVENT_RESUMED:
            self.ctx.logger.info("QQ 官方 WebSocket Resume 成功")
            await self._report_gateway_ready(True, settings=self._load_settings())
            return

        if event_type not in MESSAGE_CREATE_EVENTS:
            self.ctx.logger.debug(f"QQ 官方 WebSocket 忽略事件类型: {event_type}")
            return

        author = data.get("author")
        if isinstance(author, Mapping) and author.get("bot") is True and self._is_current_bot_identity(author):
            self._remember_bot_identity(author)
            self.ctx.logger.debug(f"QQ 官方自身消息已忽略: event={event_type}")
            return

        external_message_id = str(data.get("id") or "").strip()
        if not external_message_id:
            self.ctx.logger.warning(f"QQ 官方 {event_type} 事件缺少消息 ID，已丢弃")
            return
        self.ctx.logger.debug(
            f"QQ 官方入站事件结构: event={event_type} fields={sorted(str(key) for key in data.keys())} "
            f"attachments={len(data.get('attachments') or []) if isinstance(data.get('attachments'), list) else 0}"
        )
        try:
            message_dict = await self._build_inbound_message_dict(event_type, data)
        except (RuntimeError, ValueError) as exc:
            self.ctx.logger.warning(f"QQ 官方入站消息处理失败，已丢弃: {exc}")
            return

        if not self._is_inbound_message_allowed(message_dict):
            return
        if self._is_duplicate_inbound_message(external_message_id):
            self.ctx.logger.debug(f"QQ 官方重复消息已忽略: event={event_type} id={external_message_id}")
            return

        self._record_passive_reply_msg_id(message_dict, external_message_id)
        self._purge_expired_runtime_state()

        message_info = message_dict.get("message_info", {})
        if isinstance(message_info, Mapping):
            user_info = message_info.get("user_info", {})
            if not isinstance(user_info, Mapping):
                user_info = {}
            additional_config = message_info.get("additional_config", {})
            if not isinstance(additional_config, Mapping):
                additional_config = {}
            group_info = message_info.get("group_info", {})
            if not isinstance(group_info, Mapping):
                group_info = {}
            sender_label = str(user_info.get("user_nickname") or user_info.get("user_id") or "?")
            scope_label = ""
            if additional_config.get("qq_official_message_type") == "group":
                scope_label = f" 群={group_info.get('group_id') or '?'}"
            plain_text = str(message_dict.get("processed_plain_text") or "")[:60]
            self.ctx.logger.debug(
                f"收到 QQ 官方入站消息: event={event_type} id={external_message_id or '?'} "
                f"from={sender_label}{scope_label} text={plain_text!r}"
            )

        route_metadata: Dict[str, Any] = {"self_id": self._connected_account_id}

        try:
            accepted = await self.ctx.gateway.route_message(
                gateway_name=QQ_OFFICIAL_GATEWAY_NAME,
                message=message_dict,
                route_metadata=route_metadata,
                external_message_id=external_message_id,
                dedupe_key=external_message_id,
            )
        except Exception:
            self._seen_inbound_message_ids.pop(external_message_id, None)
            raise
        if not accepted:
            self.ctx.logger.warning(f"Host 拒绝了 QQ 官方入站消息: id={external_message_id or '?'}")
        else:
            self.ctx.logger.debug(f"QQ 官方入站消息已上报 Host: id={external_message_id or '?'}")

    async def _send_payload(self, payload: Dict[str, Any]) -> None:
        """发送 WebSocket JSON 载荷。"""

        if self._ws is None:
            raise RuntimeError("QQ 官方 WebSocket 尚未连接")
        await self._ws.send_str(json.dumps(payload, ensure_ascii=False))

    async def _receive_payload(self, expect_op: int, timeout: float) -> Dict[str, Any]:
        """等待并返回指定 opcode 的 WebSocket 载荷。"""

        if self._ws is None:
            raise RuntimeError("QQ 官方 WebSocket 尚未连接")
        deadline = time.monotonic() + max(1.0, timeout)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"等待 QQ 官方 opcode={expect_op} 超时")
            try:
                ws_message = await asyncio.wait_for(self._ws.receive(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise TimeoutError(f"等待 QQ 官方 opcode={expect_op} 超时") from exc
            if ws_message.type == WSMsgType.TEXT:
                try:
                    payload = json.loads(ws_message.data)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict) and payload.get("op") == expect_op:
                    return payload
                continue
            if ws_message.type in {WSMsgType.CLOSED, WSMsgType.ERROR}:
                raise RuntimeError("QQ 官方 WebSocket 在等待 Hello 时已关闭")

    async def _disconnect(self) -> None:
        """关闭 WebSocket、心跳任务和 HTTP session。"""

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._ws is not None:
            await self._ws.close()
            self._ws = None

        if self._api_session is not None and not self._api_session.closed:
            await self._api_session.close()
        self._api_session = None

        if self._token_session is not None and not self._token_session.closed:
            await self._token_session.close()
        self._token_session = None

        if self._media_session is not None and not self._media_session.closed:
            await self._media_session.close()
        self._media_session = None

        self._heartbeat_fail_count = 0

    async def _report_gateway_ready(
        self,
        ready: bool,
        *,
        settings: Optional[QQOfficialAdapterSettings] = None,
    ) -> bool:
        """向 Host 上报消息网关运行状态。

        account_id 优先使用 READY 事件里拿到的 bot user.id
        （与入站消息 route_metadata.self_id 保持一致，否则 Host 路由键不匹配会丢弃消息）；
        连接尚未建立时回退到 appid 仅用于标记离线状态。
        """

        metadata: Dict[str, Any] = {}
        if settings is not None:
            metadata["sandbox"] = bool(settings.credentials.sandbox)
            metadata["intents"] = DEFAULT_INTENTS
            metadata["appid"] = settings.credentials.appid
        account_id = self._connected_account_id or (settings.credentials.appid if settings else "")

        try:
            return await self.ctx.gateway.update_state(
                gateway_name=QQ_OFFICIAL_GATEWAY_NAME,
                ready=ready,
                platform="qq",
                account_id=account_id,
                scope="",
                metadata=metadata,
            )
        except Exception as exc:
            self.ctx.logger.error(f"QQ 官方消息网关状态上报失败: {exc}", exc_info=True)
            return False
