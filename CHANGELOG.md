# 更新日志

## v1.2.2

- 补齐 [LICENSE](LICENSE) 许可证全文，明确本项目仅采用 `AGPL-3.0-only`，不授权后续版本。
- 移除不再使用的 `pyproject.toml`；插件清单与依赖文件保留。

**迁移建议：** 新安装或已使用新版 MaiBot 的用户，推荐使用由 MaiBot 团队维护的内置「QQ 官方机器人适配器」（`Mai-with-u.qq-adapter`）。请参阅 [内置适配器配置指南](https://docs.meowyun.cn/qqbot/maibot/adapters/qq-official/)。

## v1.2.1

- 修复 [#7](https://github.com/WhiteCloudOL/qq-official-adapter/issues/7)：重连恢复时只使用已确认的机器人账号，不再把 AppID 登记为 Bot 账号；切换凭据后重新确认身份。
- 调整兼容上限：MaiBot `1.4.99`，MaiBot Plugin SDK `2.23.0`；最低版本不变。
- **迁移建议：** 新安装或已使用新版 MaiBot 的用户，推荐迁移到 MaiBot 团队维护的内置「QQ 官方机器人适配器」（`Mai-with-u.qq-adapter`）。请参阅 [内置适配器配置指南](https://docs.meowyun.cn/qqbot/maibot/adapters/qq-official/)；切换前先停用本插件，使用文字子频道或频道私信的用户请先核对所需能力。
