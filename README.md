# WuwaTappoUID

WuwaTappo（鸣潮桌宠）的 GsCore 插件，支持账号管理、背包图鉴、自动开箱、合成和兑换码，统一使用 `wt` 前缀。

## 安装

需要 Python 3.11+、GsCore 和已连接的 Bot 适配器。在 GsCore 项目目录执行：

```bash
git clone https://github.com/gufei233/WuwaTappoUID.git gsuid_core/plugins/WuwaTappoUID
```

重启 GsCore 加载，依赖由 GsCore 自动安装；关闭自动安装时，请在其环境中安装 `requirements.txt`。

## 常用命令

| 命令 | 功能 |
| --- | --- |
| `wt登录` | 国服网页登录，入口5分钟有效 |
| `wt添加token TOKEN [海外]` | 仅私聊，绑定已有token |
| `wt查看` / `wt切换 [1]` / `wt删除 [1]` | 多账号管理 |
| `wt信息` / `wt次数` | 账号、点击数和任务状态 |
| `wt背包 [条件]` / `wt图鉴 [角色/配件/表情]` | 背包筛选与收集图鉴 |
| `wt宝箱` | 开启或续期自动开箱，默认24小时 |
| `wt暂停` / `wt恢复` / `wt停止` | 管理任务；凭据有效时可恢复登录 |
| `wt订阅` / `wt取消订阅` | 每10次通知 / 结束汇总 |
| `wt合成 [升阶/同阶] [角色/配件/表情]` | 可限定品类，默认全部升阶；跳过收藏，每种保留1件 |
| `wt兑换 CODE` | 为当前账号兑换 |
| `wt开箱统计 [今天/7天/30天/全部]` | 默认最近7天的开箱统计 |
| `wt增加次数 N` | 增加点击数 |
| `wt更新资源` / `wt帮助` | 更新素材 / 查看完整帮助 |

背包组合示例：`wt背包 表情 稀有 角色=今汐 重复`。开箱与合成结果图支持 NEW 角标。

合成组合示例：`wt合成 角色` 仅角色升阶；`wt合成 同阶 角色` 仅角色同阶，参数顺序不限。

## 配置与说明

在 GS 控制台的 **WuwaTappoUID** 中配置登录方式、二维码、点击上限、任务时长及记录保留天数。登录默认发送文字链接，开启“发送登录二维码图片”后发送二维码；本地和外置登录均生效。外置登录需部署 [ww-login](https://github.com/gufei233/ww-login) 并填写地址。

[管理说明](docs/ADMIN.md) · [合成说明](docs/SYNTHESIS.md) · [帮助图](docs/help.png) · [更新记录](CHANGELOG.md)

## 致谢

登录交互参考 [XutheringWavesUID](https://github.com/Loping151/XutheringWavesUID)，图片布局和素材参考 [XWUIDCollection](https://github.com/Loping151/XWUIDCollection)。[素材说明](WuwaTappoUID/assets/tappo/ATTRIBUTION.md)。所有《鸣潮》角色及素材版权归库洛游戏所有。

首次从 0.1.50 升级到 0.1.51 或更新版本后，请完整重启 GsCore 进程；仅热重载插件不能接管旧运行任务。完成这次重启后，后续版本支持热重载交接。

升级到 0.1.53 以修复已有后台告警时，也请完整重启 GsCore；仅热重载会继承旧实例的失败计数。
