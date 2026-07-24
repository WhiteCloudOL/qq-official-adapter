"""封装 QQ OpenAPI 鉴权与消息发送。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Tuple

import base64
import json
import time

from aiohttp import ClientSession, ClientTimeout, FormData

from .constants import (
    ACCESS_TOKEN_REFRESH_MARGIN_SEC,
    API_BASE_PRODUCTION,
    API_BASE_SANDBOX,
    APP_ACCESS_TOKEN_URL,
    DEFAULT_ACTION_TIMEOUT_SEC,
    MAX_GROUP_PASSIVE_REPLIES_PER_MESSAGE,
    MAX_PRIVATE_PASSIVE_REPLIES_PER_MESSAGE,
)
from .models import OutboundMedia, QQMessageTarget
from .settings import QQOfficialAdapterSettings


class QQAPIClientMixin:
    """提供 access token、HTTP 会话和消息发送能力。"""

    async def _get_access_token(self) -> str:
        """获取有效的 access_token，过期前自动刷新。"""

        async with self._token_lock:
            now = time.time()
            if self._access_token and now < self._access_token_expire_at - ACCESS_TOKEN_REFRESH_MARGIN_SEC:
                if self._api_session is None or self._api_session.closed:
                    await self._rebuild_api_session()
                return self._access_token

            settings = self._load_settings()
            await self._ensure_token_session()

            body = {
                "appId": settings.credentials.appid,
                "clientSecret": settings.credentials.app_secret,
            }
            assert self._token_session is not None
            async with self._token_session.post(APP_ACCESS_TOKEN_URL, json=body) as response:
                status = response.status
                try:
                    data = await response.json()
                except Exception as exc:
                    raise RuntimeError(f"获取 access_token 响应解析失败: HTTP {status} {exc}") from exc
                if status >= 400 or not isinstance(data, Mapping):
                    raise RuntimeError(f"获取 access_token 失败: HTTP {status} {data}")
                access_token = str(data.get("access_token") or "").strip()
                raw_expires_in = data.get("expires_in")
                if not access_token:
                    raise RuntimeError(f"access_token 响应缺少 access_token 字段: {data}")
                try:
                    expires_in = float(raw_expires_in)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(f"access_token 响应的 expires_in 无效: {raw_expires_in!r}") from exc
                if expires_in <= 0:
                    raise RuntimeError(f"access_token 响应的 expires_in 无效: {raw_expires_in!r}")

            self._access_token = access_token
            self._access_token_expire_at = now + float(expires_in)
            await self._rebuild_api_session()
            self.ctx.logger.debug(
                f"QQ 官方 access_token 已刷新: expires_in={int(expires_in)}s "
                f"expire_at={datetime.fromtimestamp(self._access_token_expire_at, tz=timezone.utc).isoformat()}"
            )
            return self._access_token

    async def _ensure_token_session(self) -> None:
        """确保用于获取 access_token 的 session 存在。"""

        if self._token_session is None or self._token_session.closed:
            self._token_session = ClientSession(
                timeout=ClientTimeout(total=15),
                headers={"Content-Type": "application/json"},
            )

    async def _ensure_media_session(self) -> None:
        """确保不携带机器人鉴权信息的附件下载 session 存在。"""

        if self._media_session is None or self._media_session.closed:
            self._media_session = ClientSession(timeout=ClientTimeout(total=DEFAULT_ACTION_TIMEOUT_SEC))

    async def _rebuild_api_session(self) -> None:
        """用当前 access_token 重建 OpenAPI 调用 session。"""

        if self._api_session is not None and not self._api_session.closed:
            await self._api_session.close()
        self._api_session = ClientSession(
            timeout=ClientTimeout(total=DEFAULT_ACTION_TIMEOUT_SEC),
            headers={
                "Authorization": f"QQBot {self._access_token}",
            },
        )

    async def _send_media(
        self,
        settings: QQOfficialAdapterSettings,
        *,
        target: QQMessageTarget,
        media: OutboundMedia,
        reply_msg_id: str,
    ) -> str:
        """发送群聊、单聊或频道媒体。"""

        base_url = API_BASE_SANDBOX if settings.credentials.sandbox else API_BASE_PRODUCTION
        if target.kind in {"channel", "direct"}:
            return await self._send_channel_image(
                base_url=base_url,
                target=target,
                media=media,
                reply_msg_id=reply_msg_id,
            )

        target_prefix = "groups" if target.kind == "group" else "users"
        files_url = f"{base_url}/v2/{target_prefix}/{target.target_id}/files"
        upload_body: Dict[str, Any] = {"file_type": media.file_type}
        if media.file_data:
            upload_body["file_data"] = media.file_data
        elif media.url:
            upload_body["url"] = media.url
        else:
            raise ValueError("QQ 富媒体缺少 file_data 或 url")

        upload_data = await self._post_json(
            files_url,
            upload_body,
            operation="上传富媒体",
        )
        file_info = str(upload_data.get("file_info") or "").strip()
        if not file_info:
            raise RuntimeError(f"QQ 上传富媒体响应缺少 file_info: {upload_data}")

        body: Dict[str, Any] = {
            "content": "",
            "msg_type": 7,
            "media": {"file_info": file_info},
        }
        await self._add_reply_fields(body, target, reply_msg_id)
        send_data = await self._post_json(
            self._message_url(base_url, target),
            body,
            operation="发送富媒体",
        )
        return self._parse_send_data(send_data)

    async def _send_text(
        self,
        settings: QQOfficialAdapterSettings,
        *,
        target: QQMessageTarget,
        content: str,
        reply_msg_id: str,
    ) -> str:
        """调用 QQ 官方 REST API 发送文本消息，返回平台消息 ID。"""

        base_url = API_BASE_SANDBOX if settings.credentials.sandbox else API_BASE_PRODUCTION
        body: Dict[str, Any] = {"content": content}
        if target.kind in {"group", "user"}:
            body["msg_type"] = 0
        await self._add_reply_fields(body, target, reply_msg_id)
        response_data = await self._post_json(
            self._message_url(base_url, target),
            body,
            operation="发送文本消息",
        )
        return self._parse_send_data(response_data)

    async def _send_structured_message(
        self,
        settings: QQOfficialAdapterSettings,
        *,
        target: QQMessageTarget,
        payload: Mapping[str, Any],
        reply_msg_id: str,
    ) -> str:
        """发送 QQ Markdown、ARK 或 Embed 扩展消息。"""

        body = dict(payload)
        if target.kind in {"channel", "direct"}:
            body.pop("msg_type", None)
        await self._add_reply_fields(body, target, reply_msg_id)
        base_url = API_BASE_SANDBOX if settings.credentials.sandbox else API_BASE_PRODUCTION
        response_data = await self._post_json(
            self._message_url(base_url, target),
            body,
            operation="发送结构化消息",
        )
        return self._parse_send_data(response_data)

    async def _send_channel_image(
        self,
        *,
        base_url: str,
        target: QQMessageTarget,
        media: OutboundMedia,
        reply_msg_id: str,
    ) -> str:
        """通过频道消息接口发送图片。"""

        if media.file_type != 1:
            raise ValueError("QQ 频道与频道私信仅支持此方式发送图片附件")

        url = self._message_url(base_url, target)
        body: Dict[str, Any] = {}
        await self._add_reply_fields(body, target, reply_msg_id)
        if media.url:
            body["image"] = media.url
            response_data = await self._post_json(url, body, operation="发送频道图片")
        elif media.file_data:
            response_data = await self._post_multipart(
                url,
                body,
                file_data=base64.b64decode(media.file_data, validate=True),
                file_name=media.name or "image.png",
                operation="发送频道图片",
            )
        else:
            raise ValueError("QQ 频道图片缺少 file_data 或 url")
        return self._parse_send_data(response_data)

    async def _add_reply_fields(
        self,
        body: Dict[str, Any],
        target: QQMessageTarget,
        reply_msg_id: str,
    ) -> None:
        """按场景添加被动回复字段，并维护同消息下的 msg_seq。"""

        if not reply_msg_id:
            return
        body["msg_id"] = reply_msg_id
        if target.kind in {"group", "user"}:
            reply_limit = (
                MAX_GROUP_PASSIVE_REPLIES_PER_MESSAGE
                if target.kind == "group"
                else MAX_PRIVATE_PASSIVE_REPLIES_PER_MESSAGE
            )
            async with self._seq_lock:
                next_seq = self._reply_sequences.get(reply_msg_id, 0) + 1
                if next_seq > reply_limit:
                    raise RuntimeError(f"QQ {target.kind} 被动回复次数已达到平台上限 {reply_limit}")
                self._reply_sequences[reply_msg_id] = next_seq
            body["msg_seq"] = next_seq

    @staticmethod
    def _message_url(base_url: str, target: QQMessageTarget) -> str:
        """返回四类官方消息场景各自的发送地址。"""

        if target.kind == "group":
            return f"{base_url}/v2/groups/{target.target_id}/messages"
        if target.kind == "user":
            return f"{base_url}/v2/users/{target.target_id}/messages"
        if target.kind == "channel":
            return f"{base_url}/channels/{target.target_id}/messages"
        if not target.guild_id:
            raise ValueError("频道私信缺少 guild_id")
        return f"{base_url}/dms/{target.guild_id}/messages"

    async def _post_json(
        self,
        url: str,
        body: Mapping[str, Any],
        *,
        operation: str,
    ) -> Mapping[str, Any]:
        """执行带一次 401 刷新重试的 QQ OpenAPI POST 请求。"""

        await self._get_access_token()
        status, response_text = await self._post_json_once(url, body)
        if status == 401:
            self.ctx.logger.debug(f"QQ {operation}返回 401，刷新 access_token 后重试")
            self._access_token = ""
            await self._get_access_token()
            status, response_text = await self._post_json_once(url, body)

        response_data: Any = {}
        if response_text:
            try:
                response_data = json.loads(response_text)
            except json.JSONDecodeError:
                response_data = {"raw": response_text[:500]}
        if status >= 400:
            error_message = ""
            if isinstance(response_data, Mapping):
                error_message = str(response_data.get("message") or response_data.get("code") or response_data)
            raise RuntimeError(f"QQ {operation}失败: HTTP {status} {error_message}")
        if not isinstance(response_data, Mapping):
            raise RuntimeError(f"QQ {operation}响应结构无效: {response_data!r}")
        return response_data

    async def _post_multipart(
        self,
        url: str,
        fields: Mapping[str, Any],
        *,
        file_data: bytes,
        file_name: str,
        operation: str,
    ) -> Mapping[str, Any]:
        """执行带一次 401 刷新重试的 multipart 请求。"""

        await self._get_access_token()
        status, response_text = await self._post_multipart_once(
            url,
            fields,
            file_data=file_data,
            file_name=file_name,
        )
        if status == 401:
            self.ctx.logger.debug(f"QQ {operation}返回 401，刷新 access_token 后重试")
            self._access_token = ""
            await self._get_access_token()
            status, response_text = await self._post_multipart_once(
                url,
                fields,
                file_data=file_data,
                file_name=file_name,
            )

        try:
            response_data: Any = json.loads(response_text) if response_text else {}
        except json.JSONDecodeError:
            response_data = {"raw": response_text[:500]}
        if status >= 400:
            raise RuntimeError(f"QQ {operation}失败: HTTP {status} {response_data}")
        if not isinstance(response_data, Mapping):
            raise RuntimeError(f"QQ {operation}响应结构无效: {response_data!r}")
        return response_data

    async def _post_json_once(
        self,
        url: str,
        body: Mapping[str, Any],
    ) -> Tuple[int, str]:
        """执行一次 POST，并在离开响应上下文后返回状态与正文。"""

        if self._api_session is None or self._api_session.closed:
            raise RuntimeError("QQ 官方 HTTP 客户端尚未就绪")
        async with self._api_session.post(url, json=dict(body)) as response:
            return response.status, await response.text()

    async def _post_multipart_once(
        self,
        url: str,
        fields: Mapping[str, Any],
        *,
        file_data: bytes,
        file_name: str,
    ) -> Tuple[int, str]:
        """执行一次 multipart POST 请求。"""

        if self._api_session is None or self._api_session.closed:
            raise RuntimeError("QQ 官方 HTTP 客户端尚未就绪")
        form = FormData()
        for key, value in fields.items():
            form.add_field(key, str(value))
        form.add_field(
            "file_image",
            file_data,
            filename=file_name,
            content_type="application/octet-stream",
        )
        async with self._api_session.post(url, data=form) as response:
            return response.status, await response.text()

    @staticmethod
    def _parse_send_data(response_data: Mapping[str, Any]) -> str:
        """从发送结果中提取平台消息 ID。"""

        return str(response_data.get("id") or response_data.get("message_id") or "").strip()
