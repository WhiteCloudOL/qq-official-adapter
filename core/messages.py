"""转换 QQ 与 MaiBot 消息结构。"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Dict, List, Literal, Mapping, Optional, Tuple
from urllib.parse import urlsplit

import asyncio
import base64
import binascii
import ipaddress
import re
import socket
import time

from .constants import (
    EVENT_AT_MESSAGE_CREATE,
    EVENT_DIRECT_MESSAGE_CREATE,
    EVENT_GROUP_AT_MESSAGE_CREATE,
    EVENT_GROUP_MESSAGE_CREATE,
    EVENT_MESSAGE_CREATE,
    GROUP_PASSIVE_REPLY_TTL_SEC,
    INBOUND_MESSAGE_DEDUP_TTL_SEC,
    MAX_INBOUND_MEDIA_BYTES,
    PRIVATE_PASSIVE_REPLY_TTL_SEC,
    TRUSTED_QQ_MEDIA_HOSTS,
)
from .models import OutboundMedia, PassiveReplyContext, QQMessageTarget


_QQ_ATTACHMENT_TAG_PATTERN = re.compile(r"<attachmentType=.*?>", re.IGNORECASE)
_QQ_FACE_TAG_PATTERN = re.compile(r"<faceType=.*?>", re.IGNORECASE)


class QQMessageMixin:
    """提供消息转换、附件接收与回复上下文管理。"""

    def _record_passive_reply_msg_id(self, message_dict: Mapping[str, Any], msg_id: str) -> None:
        """按目标记录被动回复 msg_id，避免跨聊天错误复用。"""

        if not msg_id:
            return
        target = self._target_from_message_dict(message_dict)
        if target is None:
            return
        ttl = PRIVATE_PASSIVE_REPLY_TTL_SEC if target.kind == "user" else GROUP_PASSIVE_REPLY_TTL_SEC
        self._passive_replies[target.route_key] = PassiveReplyContext(
            message_id=msg_id,
            expire_at=time.time() + ttl,
        )

    def _is_duplicate_inbound_message(self, message_id: str) -> bool:
        """记录消息 ID，并判断它是否属于官方重复推送。"""

        self._purge_expired_runtime_state()
        if message_id in self._seen_inbound_message_ids:
            return True
        self._seen_inbound_message_ids[message_id] = time.time() + INBOUND_MESSAGE_DEDUP_TTL_SEC
        return False

    def _purge_expired_runtime_state(self) -> None:
        """清理过期的被动回复、序号和去重状态。"""

        now = time.time()
        expired_routes = [route_key for route_key, context in self._passive_replies.items() if context.expire_at <= now]
        for route_key in expired_routes:
            context = self._passive_replies.pop(route_key)
            self._reply_sequences.pop(context.message_id, None)

        expired_messages = [
            message_id for message_id, expire_at in self._seen_inbound_message_ids.items() if expire_at <= now
        ]
        for message_id in expired_messages:
            self._seen_inbound_message_ids.pop(message_id, None)

    async def _build_inbound_message_dict(
        self,
        event_type: str,
        data: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """将 QQ 官方事件数据转换为 MaiBot 标准消息字典。"""

        msg_id = str(data.get("id") or "").strip()
        if not msg_id:
            raise ValueError(f"{event_type} 事件缺少消息 ID")
        raw_content = self._extract_event_content(data)
        has_inline_emoji = bool(_QQ_FACE_TAG_PATTERN.search(raw_content))
        inline_emoji_only = has_inline_emoji and not self._strip_protocol_tags(raw_content).strip()
        content = self._normalize_event_content(raw_content)

        author = data.get("author") or {}
        if not isinstance(author, Mapping):
            author = {}

        timestamp_seconds = self._parse_event_timestamp(data.get("timestamp"))
        is_qq_group = event_type in {EVENT_GROUP_AT_MESSAGE_CREATE, EVENT_GROUP_MESSAGE_CREATE}
        is_guild = event_type in {EVENT_AT_MESSAGE_CREATE, EVENT_MESSAGE_CREATE}
        is_guild_direct = event_type == EVENT_DIRECT_MESSAGE_CREATE
        is_at = self._is_bot_mentioned(event_type, data, content)
        mention_target_id = ""
        mention_display_name = ""
        if is_at:
            content = self._remove_bot_mention_tokens(content)
            mention_target_id = self._resolve_bot_mention_target_id(data)
            mention_display_name = self._bot_display_name or self._connected_account_name or mention_target_id

        if is_qq_group or is_guild:
            if is_guild:
                group_id = str(data.get("channel_id") or "").strip()
                if not group_id:
                    raise ValueError("频道事件缺少 channel_id")
                target_kind = "channel"
            else:
                group_id = str(data.get("group_openid") or data.get("group_id") or "").strip()
                if not group_id:
                    raise ValueError("群聊事件缺少 group_openid")
                target_kind = "group"
            user_id = str(
                author.get("member_openid")
                or author.get("openid")
                or author.get("id")
                or author.get("user_openid")
                or ""
            ).strip()
            if not user_id:
                raise ValueError("群聊/频道事件缺少发送者 ID")
            user_nickname = str(author.get("username") or author.get("nickname") or user_id).strip() or user_id
            additional_config: Dict[str, Any] = {
                "self_id": self._connected_account_id,
                "platform_io_account_id": self._connected_account_id,
                "qq_official_message_type": "group",
                "qq_official_target_type": target_kind,
                "platform_io_target_group_id": group_id,
                "at_bot": is_at,
                "is_mentioned": is_at,
            }
            if is_guild:
                additional_config["qq_official_channel_id"] = group_id
                additional_config["qq_official_guild_id"] = str(data.get("guild_id") or "").strip()
            additional_config["qq_official_passive_reply_msg_id"] = msg_id
            message_info: Dict[str, Any] = {
                "user_info": {
                    "user_id": user_id,
                    "user_nickname": user_nickname,
                    "user_cardname": None,
                },
                "additional_config": additional_config,
                "group_info": {"group_id": group_id, "group_name": group_id},
            }
        else:
            user_id = str(author.get("user_openid") or author.get("openid") or author.get("id") or "").strip()
            if not user_id:
                raise ValueError("私聊事件缺少发送者 user_openid")
            user_nickname = str(author.get("username") or author.get("nickname") or user_id).strip() or user_id
            target_kind = "direct" if is_guild_direct else "user"
            additional_config = {
                "self_id": self._connected_account_id,
                "platform_io_account_id": self._connected_account_id,
                "qq_official_message_type": "private",
                "qq_official_target_type": target_kind,
                "platform_io_target_user_id": user_id,
                "qq_official_passive_reply_msg_id": msg_id,
                "at_bot": is_at,
                "is_mentioned": is_at,
            }
            if is_guild_direct:
                guild_id = str(data.get("guild_id") or "").strip()
                if not guild_id:
                    raise ValueError("频道私信事件缺少 guild_id")
                additional_config["qq_official_guild_id"] = guild_id
            message_info = {
                "user_info": {
                    "user_id": user_id,
                    "user_nickname": user_nickname,
                    "user_cardname": None,
                },
                "additional_config": additional_config,
            }

        (
            attachment_segments,
            attachment_labels,
            is_picture,
            attachment_contains_emoji,
        ) = await self._build_attachment_segments(data)
        contains_emoji = attachment_contains_emoji or has_inline_emoji
        if attachment_contains_emoji and content in {"[表情]", "[动画表情]"}:
            content = ""

        raw_message: List[Dict[str, Any]] = []
        if is_at and mention_target_id:
            raw_message.append(
                {
                    "type": "at",
                    "data": {
                        "target_user_id": mention_target_id,
                        "target_user_nickname": mention_display_name or None,
                        "target_user_cardname": None,
                    },
                }
            )
        if content:
            raw_message.append({"type": "text", "data": content})
        raw_message.extend(attachment_segments)
        if not raw_message:
            raise ValueError(f"{event_type} 消息既没有文本，也没有可处理的附件")

        mention_text = f"@{mention_display_name}" if is_at and mention_display_name else ""
        processed_parts = [part for part in [mention_text, content, *attachment_labels] if part]
        processed_plain_text = " ".join(processed_parts).strip()
        message_reference = data.get("message_reference")
        reply_to = ""
        if isinstance(message_reference, Mapping):
            reply_to = str(message_reference.get("message_id") or "").strip()

        message_dict: Dict[str, Any] = {
            "message_id": msg_id,
            "timestamp": str(float(timestamp_seconds)),
            "platform": "qq",
            "message_info": message_info,
            "raw_message": raw_message,
            "is_mentioned": is_at,
            "is_at": is_at,
            "is_emoji": inline_emoji_only or (contains_emoji and not content and len(attachment_segments) == 1),
            "is_picture": is_picture,
            "is_command": content.startswith("/"),
            "is_notify": False,
            "session_id": "",
            "processed_plain_text": processed_plain_text,
        }
        if reply_to:
            message_dict["reply_to"] = reply_to
        return message_dict

    @staticmethod
    def _extract_event_content(data: Mapping[str, Any]) -> str:
        """提取标准 content，兼容全量群消息的 msg_elements 文本结构。"""

        content = str(data.get("content") or "").strip()
        if content:
            return content

        msg_elements = data.get("msg_elements")
        if not isinstance(msg_elements, list):
            return ""
        element_texts: List[str] = []
        for element in msg_elements:
            if not isinstance(element, Mapping):
                continue
            for key in ("content", "text"):
                value = element.get(key)
                if isinstance(value, str) and value.strip():
                    element_texts.append(value.strip())
                elif isinstance(value, Mapping):
                    nested_content = value.get("content")
                    if isinstance(nested_content, str) and nested_content.strip():
                        element_texts.append(nested_content.strip())
            text_element = element.get("text_element")
            if isinstance(text_element, Mapping):
                nested_content = text_element.get("content")
                if isinstance(nested_content, str) and nested_content.strip():
                    element_texts.append(nested_content.strip())
        return "".join(element_texts).strip()

    @staticmethod
    def _strip_protocol_tags(content: str) -> str:
        """移除 QQ 仅用于描述附件与表情的协议标签。"""

        content = _QQ_ATTACHMENT_TAG_PATTERN.sub("", content)
        return _QQ_FACE_TAG_PATTERN.sub("", content)

    @staticmethod
    def _normalize_event_content(content: str) -> str:
        """将 QQ 协议标签转换为可读消息内容。"""

        content = _QQ_ATTACHMENT_TAG_PATTERN.sub("", content)
        content = _QQ_FACE_TAG_PATTERN.sub("[表情]", content)
        return content.strip()

    def _is_bot_mentioned(
        self,
        event_type: str,
        data: Mapping[str, Any],
        content: str,
    ) -> bool:
        """结合事件类型、mentions、msg_elements 与文本标记识别 @ 当前机器人。"""

        is_explicit_self_event = event_type in {EVENT_GROUP_AT_MESSAGE_CREATE, EVENT_AT_MESSAGE_CREATE}
        bot_ids = self._known_bot_ids()
        mentions = data.get("mentions")
        if isinstance(mentions, list):
            for mention in mentions:
                if not isinstance(mention, Mapping):
                    continue
                mention_ids = {
                    str(mention.get(key) or "").strip() for key in ("id", "openid", "user_openid", "member_openid")
                }
                if bot_ids.intersection(mention_ids):
                    return True
                if mention.get("bot") is True and self._is_current_bot_identity(mention):
                    self._remember_bot_identity(mention)
                    return True

        msg_elements = data.get("msg_elements")
        if self._mapping_tree_mentions_bot(msg_elements, bot_ids):
            return True

        for bot_id in bot_ids:
            if f"<@{bot_id}>" in content or f"<@!{bot_id}>" in content:
                return True
            if re.search(rf"@(?:机器人)?{re.escape(bot_id)}(?:\s|$)", content):
                return True
        return is_explicit_self_event

    def _known_bot_ids(self) -> set[str]:
        """返回当前连接已确认的机器人身份。"""

        settings = self._load_settings()
        return {
            value
            for value in (
                self._connected_account_id,
                settings.credentials.appid,
                *self._bot_mention_ids,
            )
            if value
        }

    def _is_current_bot_name(self, names: set[str]) -> bool:
        """判断 mention 展示名是否属于当前机器人。"""

        names.discard("")
        if self._connected_account_name and self._connected_account_name in names:
            return True
        appid = self._load_settings().credentials.appid
        return bool(appid and any(appid in name for name in names))

    def _is_current_bot_identity(self, identity: Mapping[str, Any]) -> bool:
        """判断事件中的机器人身份是否属于当前连接。"""

        identity_ids = {
            str(identity.get(key) or "").strip() for key in ("id", "openid", "user_openid", "member_openid")
        }
        if self._known_bot_ids().intersection(identity_ids):
            return True
        identity_names = {str(identity.get(key) or "").strip() for key in ("username", "nickname", "name")}
        return self._is_current_bot_name(identity_names)

    def _remember_bot_identity(self, identity: Mapping[str, Any]) -> None:
        """记录事件中已确认属于当前机器人的场景 OpenID。"""

        for key in ("id", "openid", "user_openid", "member_openid"):
            identity_value = str(identity.get(key) or "").strip()
            if identity_value:
                self._bot_mention_ids.add(identity_value)

    def _resolve_bot_mention_target_id(self, data: Mapping[str, Any]) -> str:
        """提取当前事件中指向本机器人的场景 OpenID。"""

        bot_ids = self._known_bot_ids()
        mentions = data.get("mentions")
        if isinstance(mentions, list):
            for mention in mentions:
                if not isinstance(mention, Mapping) or not self._is_current_bot_identity(mention):
                    continue
                self._remember_bot_identity(mention)
                for key in ("id", "openid", "user_openid", "member_openid"):
                    mention_id = str(mention.get(key) or "").strip()
                    if mention_id:
                        return mention_id

        element_target_id = self._find_bot_mention_target_id(data.get("msg_elements"), bot_ids)
        if element_target_id:
            return element_target_id
        return self._connected_account_id or self._load_settings().credentials.appid

    @classmethod
    def _find_bot_mention_target_id(cls, value: Any, bot_ids: set[str]) -> str:
        """从 msg_elements 树中提取指向当前机器人的 @ 目标。"""

        if isinstance(value, list):
            for item in value:
                target_id = cls._find_bot_mention_target_id(item, bot_ids)
                if target_id:
                    return target_id
            return ""
        if not isinstance(value, Mapping):
            return ""

        element_type = str(value.get("type") or value.get("element_type") or "").lower()
        looks_like_mention = "at" in element_type or "mention" in element_type
        target_keys = ("id", "openid", "user_id", "target_id", "member_openid")
        if looks_like_mention:
            for key in target_keys:
                target_id = str(value.get(key) or "").strip()
                if target_id in bot_ids:
                    return target_id

        for key, raw_value in value.items():
            normalized_key = str(key).lower()
            if ("at" in normalized_key or "mention" in normalized_key) and isinstance(raw_value, Mapping):
                for target_key in target_keys:
                    target_id = str(raw_value.get(target_key) or "").strip()
                    if target_id in bot_ids:
                        return target_id
            if isinstance(raw_value, (list, Mapping)):
                target_id = cls._find_bot_mention_target_id(raw_value, bot_ids)
                if target_id:
                    return target_id
        return ""

    @classmethod
    def _mapping_tree_mentions_bot(cls, value: Any, bot_ids: set[str]) -> bool:
        """递归检查未文档化 msg_elements 中的 @ 目标字段。"""

        if isinstance(value, list):
            return any(cls._mapping_tree_mentions_bot(item, bot_ids) for item in value)
        if not isinstance(value, Mapping):
            return False

        element_type = str(value.get("type") or value.get("element_type") or "").lower()
        looks_like_mention = "at" in element_type or "mention" in element_type
        for key, raw_value in value.items():
            normalized_key = str(key).lower()
            if ("at" in normalized_key or "mention" in normalized_key) and isinstance(raw_value, Mapping):
                target_ids = {
                    str(raw_value.get(target_key) or "").strip()
                    for target_key in (
                        "id",
                        "openid",
                        "user_id",
                        "target_id",
                        "member_openid",
                    )
                }
                if bot_ids.intersection(target_ids):
                    return True
            if ("at" in normalized_key or "mention" in normalized_key) and str(raw_value) in bot_ids:
                return True
            if (
                looks_like_mention
                and normalized_key
                in {
                    "id",
                    "openid",
                    "user_id",
                    "target_id",
                    "member_openid",
                }
                and str(raw_value) in bot_ids
            ):
                return True
            if isinstance(raw_value, (list, Mapping)) and cls._mapping_tree_mentions_bot(raw_value, bot_ids):
                return True
        return False

    def _remove_bot_mention_tokens(self, content: str) -> str:
        """移除频道消息中供协议使用的机器人 mention 标记。"""

        cleaned_content = content
        for bot_id in self._known_bot_ids():
            if not bot_id:
                continue
            cleaned_content = cleaned_content.replace(f"<@{bot_id}>", "")
            cleaned_content = cleaned_content.replace(f"<@!{bot_id}>", "")
            cleaned_content = re.sub(
                rf"@(?:机器人)?{re.escape(bot_id)}(?:\s+|$)",
                "",
                cleaned_content,
            )
        return cleaned_content.strip()

    async def _build_attachment_segments(
        self,
        data: Mapping[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[str], bool, bool]:
        """下载并转换 QQ 附件，保留图片/语音二进制与文件元信息。"""

        attachments = data.get("attachments")
        if not isinstance(attachments, list):
            return [], [], False, False

        segments: List[Dict[str, Any]] = []
        labels: List[str] = []
        is_picture = False
        contains_emoji = False
        content_hint = str(data.get("content") or "").strip()
        has_emoji_element = self._mapping_tree_has_emoji(data.get("msg_elements"))
        has_single_inline_emoji_attachment = len(attachments) == 1 and bool(_QQ_FACE_TAG_PATTERN.search(content_hint))
        for attachment in attachments:
            if not isinstance(attachment, Mapping):
                continue
            content_type = str(attachment.get("content_type") or attachment.get("type") or "").strip().lower()
            filename = str(attachment.get("filename") or "").strip()
            url = str(attachment.get("voice_wav_url") or attachment.get("url") or "").strip()
            attachment_type = str(attachment.get("type") or "").strip().lower()
            is_emoji_attachment = (
                attachment_type in {"emoji", "face", "sticker"}
                or bool(attachment.get("is_emoji"))
                or has_emoji_element
                or has_single_inline_emoji_attachment
                or content_hint in {"[表情]", "[动画表情]"}
            )

            if content_type.startswith("image/") or attachment_type in {"image", "emoji", "face", "sticker"}:
                binary_data = await self._download_attachment(url) if url else b""
                segment_type = "emoji" if is_emoji_attachment else "image"
                if binary_data:
                    segments.append(
                        {
                            "type": segment_type,
                            "data": "",
                            "binary_data_base64": base64.b64encode(binary_data).decode("ascii"),
                            "hash": sha256(binary_data).hexdigest(),
                        }
                    )
                    is_picture = is_picture or not is_emoji_attachment
                elif url or filename:
                    self.ctx.logger.debug(f"QQ 入站图片已降级为媒体摘要: {urlsplit(url).netloc}")
                    segments.append(
                        {
                            "type": segment_type,
                            "data": "[表情]" if is_emoji_attachment else "[图片]",
                        }
                    )
                labels.append("[表情]" if is_emoji_attachment else "[图片]")
                contains_emoji = contains_emoji or is_emoji_attachment
                continue

            if content_type == "voice" or content_type.startswith("audio/") or attachment_type == "voice":
                binary_data = await self._download_attachment(url) if url else b""
                if binary_data:
                    segments.append(
                        {
                            "type": "voice",
                            "data": str(attachment.get("asr_refer_text") or "[语音]").strip(),
                            "binary_data_base64": base64.b64encode(binary_data).decode("ascii"),
                            "hash": sha256(binary_data).hexdigest(),
                        }
                    )
                elif url:
                    segments.append(
                        {
                            "type": "file",
                            "data": self._build_file_payload(attachment, content_type, url),
                        }
                    )
                labels.append(str(attachment.get("asr_refer_text") or "[语音]").strip())
                continue

            if url or filename:
                segments.append(
                    {
                        "type": "file",
                        "data": self._build_file_payload(attachment, content_type, url),
                    }
                )
                labels.append("[视频]" if content_type.startswith("video/") else f"[文件：{filename or '未命名'}]")

        return segments, labels, is_picture, contains_emoji

    @classmethod
    def _mapping_tree_has_emoji(cls, value: Any) -> bool:
        """检查消息元素是否包含平台表情或贴纸。"""

        if isinstance(value, list):
            return any(cls._mapping_tree_has_emoji(item) for item in value)
        if not isinstance(value, Mapping):
            return False
        element_type = str(value.get("type") or value.get("element_type") or "").lower()
        if any(marker in element_type for marker in ("emoji", "face", "sticker")):
            return True
        return any(cls._mapping_tree_has_emoji(item) for item in value.values() if isinstance(item, (list, Mapping)))

    @staticmethod
    def _build_file_payload(
        attachment: Mapping[str, Any],
        content_type: str,
        url: str,
    ) -> Dict[str, Any]:
        """构造 MaiBot FileComponent 所需的稳定字段。"""

        return {
            "name": str(attachment.get("filename") or "").strip(),
            "size": str(attachment.get("size") or "").strip(),
            "url": url,
            "file_id": str(attachment.get("file_uuid") or "").strip(),
            "mime_type": content_type,
        }

    async def _download_attachment(self, url: str) -> bytes:
        """安全下载 QQ 入站附件，返回原始二进制。"""

        try:
            await self._validate_public_media_url(url)
            await self._ensure_media_session()
            assert self._media_session is not None
            async with self._media_session.get(url, allow_redirects=False) as response:
                if 300 <= response.status < 400:
                    raise RuntimeError(f"附件地址返回重定向: HTTP {response.status}")
                if response.status >= 400:
                    raise RuntimeError(f"HTTP {response.status}")
                await self._validate_public_media_url(str(response.url))
                if response.content_length and response.content_length > MAX_INBOUND_MEDIA_BYTES:
                    raise RuntimeError(f"附件超过 {MAX_INBOUND_MEDIA_BYTES // 1024 // 1024} MiB 限制")
                chunks: List[bytes] = []
                total_size = 0
                async for chunk in response.content.iter_chunked(64 * 1024):
                    total_size += len(chunk)
                    if total_size > MAX_INBOUND_MEDIA_BYTES:
                        raise RuntimeError(f"附件超过 {MAX_INBOUND_MEDIA_BYTES // 1024 // 1024} MiB 限制")
                    chunks.append(chunk)
                return b"".join(chunks)
        except Exception as exc:
            self.ctx.logger.warning(f"QQ 入站附件下载失败: host={urlsplit(url).netloc} error={exc}")
            return b""

    @staticmethod
    async def _validate_public_media_url(url: str) -> None:
        """拒绝非 HTTP(S) 和解析到本机/内网的附件 URL。"""

        parsed_url = urlsplit(url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
            raise RuntimeError("QQ 附件 URL 不是有效的 HTTP(S) 地址")
        normalized_hostname = parsed_url.hostname.lower()
        if parsed_url.scheme == "https" and normalized_hostname in TRUSTED_QQ_MEDIA_HOSTS:
            # TUN 代理的 Fake-IP 模式会把 QQ 官方媒体域名映射到保留地址。
            return
        try:
            addresses = [ipaddress.ip_address(normalized_hostname)]
        except ValueError:
            loop = asyncio.get_running_loop()
            try:
                address_info = await loop.getaddrinfo(
                    normalized_hostname,
                    parsed_url.port or (443 if parsed_url.scheme == "https" else 80),
                    type=socket.SOCK_STREAM,
                )
            except socket.gaierror as exc:
                raise RuntimeError(f"无法解析 QQ 附件域名: {normalized_hostname}") from exc
            addresses = [ipaddress.ip_address(item[4][0]) for item in address_info]

        if not addresses or any(
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
            or address.is_reserved
            or address.is_unspecified
            for address in addresses
        ):
            raise RuntimeError("QQ 附件 URL 指向非公网地址，已拒绝下载")

    @staticmethod
    def _parse_event_timestamp(raw_timestamp: Any) -> float:
        """解析 QQ 官方事件时间戳为 unix 秒。"""

        if isinstance(raw_timestamp, (int, float)) and raw_timestamp > 0:
            return float(raw_timestamp)
        if isinstance(raw_timestamp, str) and raw_timestamp:
            try:
                dt = datetime.fromisoformat(raw_timestamp)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except ValueError:
                pass
        return time.time()

    def _is_inbound_message_allowed(self, message_dict: Mapping[str, Any]) -> bool:
        """检查入站消息是否通过聊天黑白名单过滤。"""

        settings = self._load_settings()
        message_info = message_dict.get("message_info", {})
        if not isinstance(message_info, Mapping):
            return False
        additional_config = message_info.get("additional_config", {})
        if not isinstance(additional_config, Mapping):
            additional_config = {}
        user_info = message_info.get("user_info", {})
        if not isinstance(user_info, Mapping):
            user_info = {}

        user_id = str(user_info.get("user_id") or "").strip()
        if not user_id:
            return False

        if user_id in settings.chat.ban_user_id:
            self.ctx.logger.info(f"QQ 官方用户 {user_id} 在全局禁止名单中，消息已丢弃")
            return False

        if not settings.chat.enable_chat_list_filter:
            return True

        message_type = str(additional_config.get("qq_official_message_type") or "").strip()
        if message_type == "group":
            group_info = message_info.get("group_info", {})
            if not isinstance(group_info, Mapping):
                return True
            group_openid = str(group_info.get("group_id") or "").strip()
            allowed = self._is_id_allowed_by_list_policy(
                group_openid, settings.chat.group_list_type, settings.chat.group_list
            )
            if not allowed and settings.chat.show_dropped_chat_list_messages:
                self.ctx.logger.info(f"QQ 官方群聊 {group_openid} 未通过聊天名单过滤，消息已丢弃")
            return allowed

        allowed = self._is_id_allowed_by_list_policy(
            user_id, settings.chat.private_list_type, settings.chat.private_list
        )
        if not allowed and settings.chat.show_dropped_chat_list_messages:
            self.ctx.logger.info(f"QQ 官方私聊用户 {user_id} 未通过聊天名单过滤，消息已丢弃")
        return allowed

    @staticmethod
    def _is_id_allowed_by_list_policy(
        target_id: str, list_type: Literal["whitelist", "blacklist"], configured_ids: List[str]
    ) -> bool:
        """根据白名单或黑名单规则判断目标 ID 是否允许通过。"""

        if list_type == "whitelist":
            return target_id in configured_ids
        return target_id not in configured_ids

    def _build_outbound_target(
        self, message: Mapping[str, Any], route: Mapping[str, Any]
    ) -> Tuple[QQMessageTarget, str]:
        """提取发送目标，并只复用该目标自己的被动回复 msg_id。"""

        target = self._target_from_message_dict(message)
        if target is None:
            target_group_id = str(route.get("group_id") or route.get("platform_io_target_group_id") or "").strip()
            target_user_id = str(route.get("user_id") or route.get("platform_io_target_user_id") or "").strip()
            if target_group_id:
                target = QQMessageTarget(kind="group", target_id=target_group_id)
            elif target_user_id:
                target = QQMessageTarget(kind="user", target_id=target_user_id)
        if target is None:
            raise ValueError("出站消息缺少 QQ 群、用户或频道目标 ID")

        message_info = message.get("message_info", {})
        if not isinstance(message_info, Mapping):
            message_info = {}
        additional_config = message_info.get("additional_config", {})
        if not isinstance(additional_config, Mapping):
            additional_config = {}

        reply_msg_id = str(additional_config.get("qq_official_passive_reply_msg_id") or "").strip()
        self._purge_expired_runtime_state()
        passive_context = self._passive_replies.get(target.route_key)
        if reply_msg_id:
            if passive_context is None or passive_context.message_id != reply_msg_id:
                reply_msg_id = ""
        elif passive_context is not None:
            reply_msg_id = passive_context.message_id

        return target, reply_msg_id

    @staticmethod
    def _target_from_message_dict(message: Mapping[str, Any]) -> Optional[QQMessageTarget]:
        """从入站或出站标准消息中恢复 QQ API 目标类型。"""

        message_info = message.get("message_info", {})
        if not isinstance(message_info, Mapping):
            return None
        additional_config = message_info.get("additional_config", {})
        if not isinstance(additional_config, Mapping):
            additional_config = {}

        target_kind = str(additional_config.get("qq_official_target_type") or "").strip().lower()
        group_id = str(additional_config.get("platform_io_target_group_id") or "").strip()
        user_id = str(additional_config.get("platform_io_target_user_id") or "").strip()
        channel_id = str(additional_config.get("qq_official_channel_id") or group_id).strip()
        guild_id = str(additional_config.get("qq_official_guild_id") or "").strip()

        if target_kind == "channel" and channel_id:
            return QQMessageTarget(kind="channel", target_id=channel_id, guild_id=guild_id)
        if target_kind == "direct" and user_id and guild_id:
            return QQMessageTarget(kind="direct", target_id=user_id, guild_id=guild_id)
        if target_kind == "group" and group_id:
            return QQMessageTarget(kind="group", target_id=group_id)
        if target_kind == "user" and user_id:
            return QQMessageTarget(kind="user", target_id=user_id)
        if group_id:
            return QQMessageTarget(kind="group", target_id=group_id)
        if user_id:
            return QQMessageTarget(kind="user", target_id=user_id)
        return None

    @staticmethod
    def _raw_message_segments(message: Mapping[str, Any]) -> List[Mapping[str, Any]]:
        """返回消息中的标准段列表。"""

        raw_message = message.get("raw_message") or message.get("message_segments")
        if not isinstance(raw_message, list):
            return []
        return [segment for segment in raw_message if isinstance(segment, Mapping)]

    @staticmethod
    def _extract_outbound_text(message: Mapping[str, Any]) -> str:
        """只提取真实文本段，不把图片/表情的上下文摘要发到 QQ。"""

        parts: List[str] = []
        segments = QQMessageMixin._raw_message_segments(message)
        for segment in segments:
            seg_type = str(segment.get("type") or "").strip().lower()
            if seg_type == "text":
                data = segment.get("data")
                if isinstance(data, Mapping):
                    parts.append(str(data.get("text") or data.get("content") or ""))
                else:
                    parts.append(str(data or ""))
            elif seg_type == "at":
                data = segment.get("data")
                if isinstance(data, Mapping):
                    target_user_id = str(data.get("target_user_id") or "").strip()
                else:
                    target_user_id = str(data or "").strip()
                if target_user_id:
                    parts.append(f"<@{target_user_id}>")
        if parts:
            return "".join(parts).strip()
        if segments:
            return ""

        if bool(message.get("is_picture")) or bool(message.get("is_emoji")):
            return ""
        return str(message.get("processed_plain_text") or message.get("plain_text") or "").strip()

    @classmethod
    def _extract_outbound_media(cls, message: Mapping[str, Any]) -> List[OutboundMedia]:
        """提取图片、表情、语音、视频与文件媒体。"""

        media_items: List[OutboundMedia] = []
        for segment in cls._raw_message_segments(message):
            seg_type = str(segment.get("type") or "").strip().lower()
            if seg_type not in {"image", "emoji", "voice", "video", "file"}:
                continue
            file_type = {"image": 1, "emoji": 1, "video": 2, "voice": 3, "file": 4}[seg_type]
            data = segment.get("data")
            binary_data = cls._normalize_base64_data(segment.get("binary_data_base64"))
            url = ""
            name = ""
            if isinstance(data, Mapping):
                binary_data = binary_data or cls._normalize_base64_data(
                    data.get("base64") or data.get("file_data") or data.get("binary_data_base64")
                )
                url = str(data.get("url") or "").strip()
                name = str(data.get("name") or "").strip()
                mime_type = str(data.get("mime_type") or "").strip().lower()
                if seg_type == "file":
                    if mime_type.startswith("image/"):
                        file_type = 1
                    elif mime_type.startswith("video/"):
                        file_type = 2
                    elif mime_type.startswith("audio/") or mime_type == "voice":
                        file_type = 3
            elif isinstance(data, str):
                raw_data = data.strip()
                if raw_data.startswith(("http://", "https://")):
                    url = raw_data
                elif not binary_data:
                    binary_data = cls._normalize_base64_data(raw_data)

            if binary_data or url:
                media_items.append(
                    OutboundMedia(
                        file_type=file_type,
                        file_data=binary_data,
                        url=url,
                        name=name,
                    )
                )
        return media_items

    @staticmethod
    def _normalize_base64_data(value: Any) -> str:
        """验证并规范化 Base64，防止把 hash 或 ``[图片]`` 当作文件上传。"""

        if not isinstance(value, str):
            return ""
        encoded_data = value.strip()
        if not encoded_data:
            return ""
        if encoded_data.startswith("data:"):
            _, separator, encoded_data = encoded_data.partition(",")
            if not separator:
                return ""
        try:
            decoded_data = base64.b64decode(encoded_data, validate=True)
        except (binascii.Error, ValueError):
            return ""
        if not decoded_data:
            return ""
        return base64.b64encode(decoded_data).decode("ascii")

    @classmethod
    def _extract_outbound_rich_payloads(
        cls,
        message: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        """提取插件扩展段中的 Markdown、ARK 与 Embed 消息。"""

        rich_payloads: List[Dict[str, Any]] = []
        for segment in cls._raw_message_segments(message):
            segment_data = segment.get("data")
            if not isinstance(segment_data, Mapping):
                continue
            custom_type = str(segment_data.get("type") or segment.get("type") or "").strip().lower()
            nested_data = segment_data.get("data")
            payload_source = nested_data if isinstance(nested_data, Mapping) else segment_data

            msg_type = 0
            if custom_type in {"qq_markdown", "qq_official_markdown"}:
                msg_type = 2
            elif custom_type in {"qq_ark", "qq_official_ark"}:
                msg_type = 3
            elif custom_type in {"qq_embed", "qq_official_embed"}:
                msg_type = 4
            elif custom_type == "qq_official":
                try:
                    msg_type = int(payload_source.get("msg_type") or 0)
                except (TypeError, ValueError):
                    continue
            else:
                continue
            if msg_type not in {0, 2, 3, 4}:
                continue

            payload: Dict[str, Any] = {"msg_type": msg_type}
            for key in (
                "content",
                "markdown",
                "keyboard",
                "ark",
                "embed",
                "message_reference",
                "event_id",
                "is_wakeup",
            ):
                if key in payload_source:
                    payload[key] = payload_source[key]
            rich_payloads.append(payload)
        return rich_payloads
