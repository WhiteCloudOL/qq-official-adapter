"""封装 QQ OpenAPI 鉴权与消息发送。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Tuple
from urllib.parse import quote, urlsplit

import base64
import binascii
import json
import math
import time

from aiohttp import ClientResponse, ClientSession, ClientTimeout, FormData

from .constants import (
    ACCESS_TOKEN_REFRESH_MARGIN_SEC,
    API_BASE_PRODUCTION,
    API_BASE_SANDBOX,
    APP_ACCESS_TOKEN_URL,
    DEFAULT_ACTION_TIMEOUT_SEC,
    MAX_API_RESPONSE_BYTES,
    MAX_GROUP_PASSIVE_REPLIES_PER_MESSAGE,
    MAX_OUTBOUND_MARKDOWN_CHARS,
    MAX_OUTBOUND_MEDIA_BYTES,
    MAX_OUTBOUND_TEXT_CHARS,
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
                    await self._ensure_api_session()
                return self._access_token

            settings = self._load_settings()
            await self._ensure_token_session()

            body = {
                "appId": settings.credentials.appid,
                "clientSecret": settings.credentials.app_secret,
            }
            if self._token_session is None:
                raise RuntimeError("QQ 官方鉴权客户端尚未就绪")
            async with self._token_session.post(APP_ACCESS_TOKEN_URL, json=body) as response:
                status = response.status
                response_text = await self._read_response_text(response)
                try:
                    data = json.loads(response_text)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"获取 access_token 响应解析失败: HTTP {status}") from exc
                if not 200 <= status < 300 or not isinstance(data, Mapping):
                    error_message = self._response_error_message(data)
                    raise RuntimeError(f"获取 access_token 失败: HTTP {status} {error_message}")
                raw_access_token = data.get("access_token")
                if not isinstance(raw_access_token, str):
                    raise RuntimeError("access_token 响应缺少有效的 access_token 字段")
                access_token = raw_access_token.strip()
                raw_expires_in = data.get("expires_in")
                if not access_token:
                    raise RuntimeError("access_token 响应缺少 access_token 字段")
                if len(access_token) > 4096 or any(
                    ord(character) < 32 or ord(character) == 127 for character in access_token
                ):
                    raise RuntimeError("access_token 响应包含无效令牌")
                if isinstance(raw_expires_in, bool):
                    raise RuntimeError("access_token 响应的 expires_in 无效")
                try:
                    expires_in = float(raw_expires_in)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(f"access_token 响应的 expires_in 无效: {raw_expires_in!r}") from exc
                if not math.isfinite(expires_in) or expires_in <= 0:
                    raise RuntimeError(f"access_token 响应的 expires_in 无效: {raw_expires_in!r}")

            self._access_token = access_token
            self._access_token_expire_at = now + float(expires_in)
            await self._ensure_api_session()
            self.ctx.logger.debug(
                f"QQ 官方 access_token 已刷新: expires_in={int(expires_in)}s "
                f"expire_at={datetime.fromtimestamp(self._access_token_expire_at, tz=timezone.utc).isoformat()}"
            )
            return access_token

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

    async def _ensure_api_session(self) -> None:
        """确保 QQ OpenAPI 调用 session 存在。"""

        if self._api_session is None or self._api_session.closed:
            self._api_session = ClientSession(
                timeout=ClientTimeout(total=DEFAULT_ACTION_TIMEOUT_SEC),
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
        encoded_target_id = quote(target.target_id, safe="")
        files_url = f"{base_url}/v2/{target_prefix}/{encoded_target_id}/files"
        upload_body: Dict[str, Any] = {"file_type": media.file_type}
        if media.file_data:
            self._decode_media_data(media.file_data)
            upload_body["file_data"] = media.file_data
        elif media.url:
            upload_body["url"] = self._validate_outbound_media_url(media.url)
        else:
            raise ValueError("QQ 富媒体缺少 file_data 或 url")

        upload_data = await self._post_json(
            files_url,
            upload_body,
            operation="上传富媒体",
        )
        raw_file_info = upload_data.get("file_info")
        if not isinstance(raw_file_info, str):
            raise RuntimeError("QQ 上传富媒体响应缺少有效的 file_info")
        file_info = raw_file_info.strip()
        if (
            not file_info
            or len(file_info) > 4096
            or any(ord(character) < 32 or ord(character) == 127 for character in file_info)
        ):
            raise RuntimeError("QQ 上传富媒体响应的 file_info 无效")

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

        if not content:
            raise ValueError("QQ 出站文本内容为空")
        if len(content) > MAX_OUTBOUND_TEXT_CHARS:
            raise ValueError(f"QQ 出站文本超过 {MAX_OUTBOUND_TEXT_CHARS} 字符限制")
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

    async def _send_markdown(
        self,
        settings: QQOfficialAdapterSettings,
        *,
        target: QQMessageTarget,
        content: str,
        reply_msg_id: str,
    ) -> str:
        """向 QQ 群聊或单聊发送自定义 Markdown，返回平台消息 ID。"""

        if target.kind not in {"group", "user"}:
            raise ValueError("QQ Markdown 输出开关仅适用于群聊与单聊")
        if not content:
            raise ValueError("QQ 出站 Markdown 内容为空")
        if len(content) > MAX_OUTBOUND_MARKDOWN_CHARS:
            raise ValueError(f"QQ 出站 Markdown 超过 {MAX_OUTBOUND_MARKDOWN_CHARS} 字符限制")

        body: Dict[str, Any] = {
            "msg_type": 2,
            "markdown": {"content": content},
        }
        await self._add_reply_fields(body, target, reply_msg_id)
        base_url = API_BASE_SANDBOX if settings.credentials.sandbox else API_BASE_PRODUCTION
        response_data = await self._post_json(
            self._message_url(base_url, target),
            body,
            operation="发送 Markdown 消息",
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
            body["image"] = self._validate_outbound_media_url(media.url)
            response_data = await self._post_json(url, body, operation="发送频道图片")
        elif media.file_data:
            response_data = await self._post_multipart(
                url,
                body,
                file_data=self._decode_media_data(media.file_data),
                file_name=self._sanitize_file_name(media.name),
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

        encoded_target_id = quote(target.target_id, safe="")
        if target.kind == "group":
            return f"{base_url}/v2/groups/{encoded_target_id}/messages"
        if target.kind == "user":
            return f"{base_url}/v2/users/{encoded_target_id}/messages"
        if target.kind == "channel":
            return f"{base_url}/channels/{encoded_target_id}/messages"
        return f"{base_url}/dms/{quote(target.guild_id, safe='')}/messages"

    async def _post_json(
        self,
        url: str,
        body: Mapping[str, Any],
        *,
        operation: str,
    ) -> Mapping[str, Any]:
        """执行带一次 401 刷新重试的 QQ OpenAPI POST 请求。"""

        await self._get_access_token()
        request_access_token = self._access_token
        status, response_text = await self._post_json_once(url, body)
        if status == 401:
            self.ctx.logger.debug(f"QQ {operation}返回 401，刷新 access_token 后重试")
            if self._access_token == request_access_token:
                self._access_token = None
            await self._get_access_token()
            status, response_text = await self._post_json_once(url, body)

        response_data: Any = {}
        if response_text:
            try:
                response_data = json.loads(response_text)
            except json.JSONDecodeError:
                response_data = {"raw": response_text[:500]}
        if not 200 <= status < 300:
            error_message = self._response_error_message(response_data)
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
        request_access_token = self._access_token
        status, response_text = await self._post_multipart_once(
            url,
            fields,
            file_data=file_data,
            file_name=file_name,
        )
        if status == 401:
            self.ctx.logger.debug(f"QQ {operation}返回 401，刷新 access_token 后重试")
            if self._access_token == request_access_token:
                self._access_token = None
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
        if not 200 <= status < 300:
            error_message = self._response_error_message(response_data)
            raise RuntimeError(f"QQ {operation}失败: HTTP {status} {error_message}")
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
        async with self._api_session.post(
            url,
            json=dict(body),
            headers=self._authorization_headers(),
        ) as response:
            return response.status, await self._read_response_text(response)

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
        async with self._api_session.post(
            url,
            data=form,
            headers=self._authorization_headers(),
        ) as response:
            return response.status, await self._read_response_text(response)

    @staticmethod
    def _parse_send_data(response_data: Mapping[str, Any]) -> str:
        """从发送结果中提取平台消息 ID。"""

        raw_message_id = response_data.get("id") or response_data.get("message_id")
        if not isinstance(raw_message_id, str):
            raise RuntimeError("QQ 发送消息响应缺少有效的消息 ID")
        message_id = raw_message_id.strip()
        if (
            not message_id
            or len(message_id) > 256
            or any(ord(character) < 32 or ord(character) == 127 for character in message_id)
        ):
            raise RuntimeError("QQ 发送消息响应的消息 ID 无效")
        return message_id

    def _authorization_headers(self) -> Dict[str, str]:
        """构造仅用于 QQ OpenAPI 请求的鉴权头。"""

        if not self._access_token:
            raise RuntimeError("QQ 官方 access_token 尚未就绪")
        return {"Authorization": f"QQBot {self._access_token}"}

    @staticmethod
    async def _read_response_text(response: ClientResponse) -> str:
        """限量读取 QQ HTTP 响应，避免异常响应耗尽内存。"""

        if response.content_length is not None and response.content_length > MAX_API_RESPONSE_BYTES:
            raise RuntimeError(f"QQ API 响应超过 {MAX_API_RESPONSE_BYTES} 字节限制")
        response_bytes = bytearray()
        async for chunk in response.content.iter_chunked(64 * 1024):
            response_bytes.extend(chunk)
            if len(response_bytes) > MAX_API_RESPONSE_BYTES:
                raise RuntimeError(f"QQ API 响应超过 {MAX_API_RESPONSE_BYTES} 字节限制")
        return bytes(response_bytes).decode(response.charset or "utf-8", errors="replace")

    @staticmethod
    def _response_error_message(response_data: Any) -> str:
        """提取不包含令牌等完整响应内容的错误摘要。"""

        if not isinstance(response_data, Mapping):
            return "响应结构无效"
        error_value = response_data.get("message") or response_data.get("code") or "未提供错误详情"
        error_message = str(error_value)[:300]
        return "".join(character if character.isprintable() else " " for character in error_message)

    @staticmethod
    def _decode_media_data(encoded_data: str) -> bytes:
        """验证 Base64 富媒体并限制解码后的大小。"""

        max_encoded_length = ((MAX_OUTBOUND_MEDIA_BYTES + 2) // 3) * 4
        if len(encoded_data) > max_encoded_length:
            raise ValueError(f"QQ 出站媒体超过 {MAX_OUTBOUND_MEDIA_BYTES // 1024 // 1024} MiB 限制")
        try:
            decoded_data = base64.b64decode(encoded_data, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("QQ 出站媒体不是有效的 Base64") from exc
        if not decoded_data:
            raise ValueError("QQ 出站媒体内容为空")
        if len(decoded_data) > MAX_OUTBOUND_MEDIA_BYTES:
            raise ValueError(f"QQ 出站媒体超过 {MAX_OUTBOUND_MEDIA_BYTES // 1024 // 1024} MiB 限制")
        return decoded_data

    @staticmethod
    def _validate_outbound_media_url(url: str) -> str:
        """校验交给 QQ 平台拉取的媒体 URL。"""

        if len(url) > 4096:
            raise ValueError("QQ 出站媒体 URL 过长")
        try:
            parsed_url = urlsplit(url)
            parsed_port = parsed_url.port
        except ValueError as exc:
            raise ValueError("QQ 出站媒体 URL 格式无效") from exc
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
            raise ValueError("QQ 出站媒体 URL 必须是有效的 HTTP(S) 地址")
        if parsed_url.username is not None or parsed_url.password is not None:
            raise ValueError("QQ 出站媒体 URL 不能包含用户凭据")
        if any(ord(character) < 32 or ord(character) == 127 for character in url):
            raise ValueError("QQ 出站媒体 URL 不能包含控制字符")
        if parsed_port is not None and not 1 <= parsed_port <= 65535:
            raise ValueError("QQ 出站媒体 URL 端口无效")
        return url

    @staticmethod
    def _sanitize_file_name(file_name: str) -> str:
        """生成安全、有限长的 multipart 文件名。"""

        sanitized_name = "".join(
            character
            for character in file_name.strip()
            if ord(character) >= 32 and ord(character) != 127 and character not in {"/", "\\"}
        )
        return sanitized_name[:128] or "image.png"
