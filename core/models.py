"""定义 QQ 消息传输模型。"""

from dataclasses import dataclass
from typing import Literal, Tuple


_MAX_QQ_IDENTIFIER_LENGTH = 256
_QQ_IDENTIFIER_ATTRIBUTE_SENSITIVE_CHARS = frozenset({'"', "'", "<", ">", "&"})


def _validate_qq_identifier(value: str, field_name: str) -> None:
    """校验用于路由和 URL 路径的 QQ 标识符。"""

    if not isinstance(value, str):
        raise ValueError(f"{field_name} 必须是字符串")
    if not value:
        raise ValueError(f"{field_name} 不能为空")
    if len(value) > _MAX_QQ_IDENTIFIER_LENGTH:
        raise ValueError(f"{field_name} 长度不能超过 {_MAX_QQ_IDENTIFIER_LENGTH}")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field_name} 不能包含控制字符")
    if any(character in _QQ_IDENTIFIER_ATTRIBUTE_SENSITIVE_CHARS for character in value):
        raise ValueError(f"{field_name} 不能包含标签属性敏感字符")


@dataclass(frozen=True)
class QQMessageTarget:
    """QQ 消息发送目标。"""

    kind: Literal["group", "user", "channel", "direct"]
    target_id: str
    guild_id: str = ""

    def __post_init__(self) -> None:
        """校验从 Host 或 QQ 事件恢复出的发送目标。"""

        if self.kind not in {"group", "user", "channel", "direct"}:
            raise ValueError(f"不支持的 QQ 消息目标类型: {self.kind}")
        _validate_qq_identifier(self.target_id, "target_id")
        if self.guild_id:
            _validate_qq_identifier(self.guild_id, "guild_id")
        if self.kind == "direct" and not self.guild_id:
            raise ValueError("频道私信目标缺少 guild_id")

    @property
    def route_key(self) -> Tuple[str, str, str]:
        """返回用于隔离被动回复上下文的稳定路由键。"""

        return self.kind, self.target_id, self.guild_id


@dataclass(frozen=True)
class PassiveReplyContext:
    """某个聊天目标最近一次可用于被动回复的消息。"""

    message_id: str
    expire_at: float


@dataclass(frozen=True)
class OutboundMedia:
    """待上传到 QQ 富媒体接口的媒体。"""

    file_type: int
    file_data: str = ""
    url: str = ""
    name: str = ""

    def __post_init__(self) -> None:
        """校验富媒体类型与内容来源。"""

        if isinstance(self.file_type, bool) or self.file_type not in {1, 2, 3, 4}:
            raise ValueError(f"不支持的 QQ 富媒体类型: {self.file_type}")
        if not isinstance(self.file_data, str) or not isinstance(self.url, str) or not isinstance(self.name, str):
            raise ValueError("QQ 富媒体字段必须是字符串")
        if not self.file_data and not self.url:
            raise ValueError("QQ 富媒体缺少 file_data 或 url")
