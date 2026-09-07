"""定义 QQ 官方适配器配置。"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Literal, Optional

from maibot_sdk import Field, PluginConfigBase
from pydantic import field_validator

from .constants import SUPPORTED_CONFIG_VERSION


_MAX_CONFIGURED_IDS = 1000
_MAX_QQ_IDENTIFIER_LENGTH = 256


def _schema_i18n(
    *,
    label_en: str,
    label_ja: str,
    hint_en: Optional[str] = None,
    hint_ja: Optional[str] = None,
    placeholder_en: Optional[str] = None,
    placeholder_ja: Optional[str] = None,
) -> Dict[str, Dict[str, str]]:
    """构造 WebUI 配置项多语言说明。"""

    i18n: Dict[str, Dict[str, str]] = {
        "en_US": {"label": label_en},
        "ja_JP": {"label": label_ja},
    }
    if hint_en is not None:
        i18n["en_US"]["hint"] = hint_en
    if hint_ja is not None:
        i18n["ja_JP"]["hint"] = hint_ja
    if placeholder_en is not None:
        i18n["en_US"]["placeholder"] = placeholder_en
    if placeholder_ja is not None:
        i18n["ja_JP"]["placeholder"] = placeholder_ja
    return i18n


class QQOfficialPluginSection(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__: ClassVar[str] = "插件设置"
    __ui_order__: ClassVar[int] = 0

    enabled: bool = Field(
        default=False,
        description="是否启用 QQ 官方机器人适配器。",
        json_schema_extra={
            "label": "启用适配器",
            "hint": "关闭时插件只注册消息网关，不会连接 QQ WebSocket 网关。",
            "i18n": _schema_i18n(
                label_en="Enable adapter",
                label_ja="アダプターを有効化",
                hint_en="When disabled, the plugin only registers the gateway and does not connect to QQ WebSocket.",
                hint_ja="無効にすると、プラグインはゲートウェイの登録のみを行い、QQ WebSocket に接続しません。",
            ),
            "order": 0,
        },
    )
    config_version: str = Field(
        default=SUPPORTED_CONFIG_VERSION,
        description="当前配置结构版本。",
        json_schema_extra={
            "disabled": True,
            "hidden": True,
            "i18n": _schema_i18n(label_en="Config version", label_ja="設定バージョン"),
            "label": "配置版本",
            "order": 99,
        },
    )


class QQOfficialCredentialsSection(PluginConfigBase):
    """QQ 开放平台凭据配置。"""

    __ui_label__: ClassVar[str] = "机器人凭据"
    __ui_order__: ClassVar[int] = 1

    appid: str = Field(
        default="",
        max_length=128,
        description="QQ 开放平台机器人 AppID。",
        json_schema_extra={
            "label": "AppID",
            "hint": "在 https://q.qq.com 机器人管理页面获取。",
            "i18n": _schema_i18n(
                label_en="AppID",
                label_ja="AppID",
                hint_en="Obtain from the bot management page on https://q.qq.com.",
                hint_ja="https://q.qq.com のボット管理ページで取得してください。",
            ),
            "order": 0,
        },
    )
    app_secret: str = Field(
        default="",
        max_length=512,
        description="QQ 开放平台机器人 AppSecret。",
        json_schema_extra={
            "label": "AppSecret",
            "hint": "在机器人管理页面获取，与 AppID 配对使用，用于获取 access_token。",
            "i18n": _schema_i18n(
                label_en="AppSecret",
                label_ja="AppSecret",
                hint_en="Obtain from the bot management page; paired with AppID to fetch access_token.",
                hint_ja="ボット管理ページで取得。AppID とペアで使用し access_token を取得します。",
            ),
            "input_type": "password",
            "order": 1,
        },
    )
    sandbox: bool = Field(
        default=False,
        description="是否使用沙箱环境（sandbox.api.sgroup.qq.com）。",
        json_schema_extra={
            "hidden": True,
            "label": "沙箱环境",
            "hint": "开启后 WebSocket 网关与发送消息走沙箱 API。",
            "i18n": _schema_i18n(
                label_en="Sandbox",
                label_ja="サンドボックス",
                hint_en="Use the sandbox endpoint for WebSocket gateway and REST API.",
                hint_ja="WebSocket ゲートウェイと REST API にサンドボックスを使用します。",
            ),
            "order": 2,
        },
    )

    @field_validator("appid", "app_secret", mode="before")
    @classmethod
    def _normalize_credentials(cls, value: Any) -> str:
        """规范化凭据文本，并拒绝非字符串及控制字符。"""

        if not isinstance(value, str):
            raise ValueError("QQ 机器人凭据必须是字符串")
        normalized_value = value.strip()
        if any(ord(character) < 32 or ord(character) == 127 for character in normalized_value):
            raise ValueError("QQ 机器人凭据不能包含控制字符")
        return normalized_value


class QQOfficialChatSection(PluginConfigBase):
    """聊天名单过滤配置。"""

    __ui_label__: ClassVar[str] = "聊天过滤"
    __ui_order__: ClassVar[int] = 3

    enable_chat_list_filter: bool = Field(
        default=False,
        description="是否启用群聊与私聊名单过滤。",
        json_schema_extra={
            "hint": "关闭后将忽略 group_list 和 private_list，仅保留 ban_user_id 规则。",
            "i18n": _schema_i18n(
                label_en="Enable chat list filter",
                label_ja="チャットリストフィルターを有効化",
                hint_en="When disabled, group_list and private_list are ignored; only ban_user_id remains.",
                hint_ja="無効にすると group_list と private_list を無視し、ban_user_id のみ適用します。",
            ),
            "label": "启用聊天名单过滤",
            "order": 0,
        },
    )
    show_dropped_chat_list_messages: bool = Field(
        default=False,
        description="是否记录未通过聊天名单过滤而被丢弃的消息。",
        json_schema_extra={
            "hint": "关闭后不记录群聊/私聊名单丢弃日志，默认关闭以减少刷屏。",
            "i18n": _schema_i18n(
                label_en="Show dropped chat-list logs",
                label_ja="チャットリスト破棄ログを表示",
                hint_en="When disabled, dropped group/private chat-list logs are not recorded.",
                hint_ja="無効にするとチャットリストで破棄されたログを記録しません。",
            ),
            "label": "显示聊天名单丢弃日志",
            "order": 1,
        },
    )
    group_list_type: Literal["whitelist", "blacklist"] = Field(
        default="whitelist",
        description="群聊名单模式：whitelist 或 blacklist。",
        json_schema_extra={
            "hint": "白名单模式只接收列表内群聊，黑名单模式则忽略列表内群聊。",
            "i18n": _schema_i18n(
                label_en="Group list mode",
                label_ja="グループリストモード",
                hint_en="Whitelist accepts only listed groups; blacklist ignores listed groups.",
                hint_ja="ホワイトリストはリスト内グループのみ受信、ブラックリストはリスト内グループを無視します。",
            ),
            "label": "群聊名单模式",
            "options": ["whitelist", "blacklist"],
            "order": 2,
        },
    )
    group_list: List[str] = Field(
        default_factory=list,
        max_length=_MAX_CONFIGURED_IDS,
        description="群聊名单中的 group_openid 列表。",
        json_schema_extra={
            "hint": "QQ 官方机器人使用 group_openid，不是群号；列表会自动去重。",
            "i18n": _schema_i18n(
                label_en="Group list",
                label_ja="グループリスト",
                hint_en="QQ official bots use group_openid rather than group number; entries are deduplicated.",
                hint_ja="QQ 公式ボットは group_openid を使用します。エントリは自動的に重複排除されます。",
                placeholder_en="Enter group_openid",
                placeholder_ja="group_openid を入力",
            ),
            "label": "群聊名单",
            "order": 3,
            "placeholder": "请输入 group_openid",
        },
    )
    private_list_type: Literal["whitelist", "blacklist"] = Field(
        default="whitelist",
        description="私聊名单模式：whitelist 或 blacklist。",
        json_schema_extra={
            "hint": "白名单模式只接收列表内私聊，黑名单模式则忽略列表内私聊。",
            "i18n": _schema_i18n(
                label_en="Private list mode",
                label_ja="個人チャットリストモード",
                hint_en="Whitelist accepts only listed private chats; blacklist ignores listed ones.",
                hint_ja="ホワイトリストはリスト内の個人チャットのみ受信、ブラックリストはリスト内を無視します。",
            ),
            "label": "私聊名单模式",
            "options": ["whitelist", "blacklist"],
            "order": 4,
        },
    )
    private_list: List[str] = Field(
        default_factory=list,
        max_length=_MAX_CONFIGURED_IDS,
        description="私聊名单中的 user_openid 列表。",
        json_schema_extra={
            "hint": "QQ 官方机器人使用 user_openid；列表会自动去重。",
            "i18n": _schema_i18n(
                label_en="Private list",
                label_ja="個人チャットリスト",
                hint_en="QQ official bots use user_openid; entries are deduplicated.",
                hint_ja="QQ 公式ボットは user_openid を使用します。エントリは自動的に重複排除されます。",
                placeholder_en="Enter user_openid",
                placeholder_ja="user_openid を入力",
            ),
            "label": "私聊名单",
            "order": 5,
            "placeholder": "请输入 user_openid",
        },
    )
    ban_user_id: List[str] = Field(
        default_factory=list,
        max_length=_MAX_CONFIGURED_IDS,
        description="全局屏蔽的用户 openid 列表。",
        json_schema_extra={
            "hint": "这些用户的消息会在进入 Host 之前被直接丢弃。",
            "i18n": _schema_i18n(
                label_en="Globally blocked users",
                label_ja="全体ブロックユーザー",
                hint_en="Messages from these users are dropped before entering the Host.",
                hint_ja="これらのユーザーからのメッセージは Host に入る前に破棄されます。",
                placeholder_en="Enter user_openid",
                placeholder_ja="user_openid を入力",
            ),
            "label": "全局屏蔽用户",
            "order": 6,
            "placeholder": "请输入 user_openid",
        },
    )

    @field_validator("group_list_type", "private_list_type", mode="before")
    @classmethod
    def _normalize_list_types(cls, value: Any) -> Literal["whitelist", "blacklist"]:
        """规范化名单模式字段。"""

        if not isinstance(value, str):
            raise ValueError("名单模式必须是字符串")
        normalized_value = value.strip().lower()
        if normalized_value not in {"whitelist", "blacklist"}:
            raise ValueError("名单模式只能是 whitelist 或 blacklist")
        if normalized_value == "whitelist":
            return "whitelist"
        return "blacklist"

    @field_validator("group_list", "private_list", "ban_user_id", mode="before")
    @classmethod
    def _normalize_id_lists(cls, value: Any) -> List[str]:
        """规范化 ID 列表字段。"""

        if not isinstance(value, list):
            raise ValueError("QQ OpenID 名单必须是字符串列表")
        if len(value) > _MAX_CONFIGURED_IDS:
            raise ValueError(f"QQ OpenID 名单最多包含 {_MAX_CONFIGURED_IDS} 项")

        normalized_values: List[str] = []
        seen_values: set[str] = set()
        for raw_value in value:
            if not isinstance(raw_value, str):
                raise ValueError("QQ OpenID 名单只能包含字符串")
            normalized_value = raw_value.strip()
            if not normalized_value or normalized_value in seen_values:
                continue
            if len(normalized_value) > _MAX_QQ_IDENTIFIER_LENGTH:
                raise ValueError(f"QQ OpenID 长度不能超过 {_MAX_QQ_IDENTIFIER_LENGTH}")
            if any(ord(character) < 32 or ord(character) == 127 for character in normalized_value):
                raise ValueError("QQ OpenID 不能包含控制字符")
            normalized_values.append(normalized_value)
            seen_values.add(normalized_value)
        return normalized_values


class QQOfficialMessageSection(PluginConfigBase):
    """消息发送行为配置。"""

    __ui_label__: ClassVar[str] = "消息发送"
    __ui_order__: ClassVar[int] = 2

    enable_markdown_output: bool = Field(
        default=False,
        description="群聊与单聊的普通文本回复是否使用 Markdown 格式输出。",
        json_schema_extra={
            "hint": "默认关闭。开启后群聊与单聊的普通文本回复改用 Markdown（msg_type=2）；频道、频道私信和显式结构化消息不受此开关影响。",
            "i18n": _schema_i18n(
                label_en="Enable Markdown output",
                label_ja="Markdown 出力を有効化",
                hint_en="Disabled by default. When enabled, regular group and C2C text replies use Markdown (msg_type=2); guild, direct, and explicit structured messages are unchanged.",
                hint_ja="既定では無効です。有効にすると、グループと C2C の通常テキスト返信は Markdown（msg_type=2）を使用します。チャンネル、チャンネル DM、明示的な構造化メッセージには影響しません。",
            ),
            "label": "启用 Markdown 输出",
            "order": 0,
        },
    )


class QQOfficialAdapterSettings(PluginConfigBase):
    """QQ 官方机器人适配器完整配置。"""

    plugin: QQOfficialPluginSection = Field(default_factory=QQOfficialPluginSection)
    credentials: QQOfficialCredentialsSection = Field(default_factory=QQOfficialCredentialsSection)
    message: QQOfficialMessageSection = Field(default_factory=QQOfficialMessageSection)
    chat: QQOfficialChatSection = Field(default_factory=QQOfficialChatSection)

    def should_connect(self) -> bool:
        """判断当前配置是否应该建立 WebSocket 连接。"""

        return self.plugin.enabled
