# Emby ToolKit (ETK) [旧版已归档]

[![中文文档](https://img.shields.io/badge/Wiki-中文文档-&logoColor=red)](https://hbq0405.github.io/emby-toolkit/zh/)
[![GitHub license](https://img.shields.io/github/license/hbq0405/emby-toolkit.svg)](https://github.com/hbq0405/emby-toolkit/blob/main/LICENSE)
[![Telegram](https://img.shields.io/badge/Telegram-加入群组-2CA5E0?logo=telegram&logoColor=white)](https://t.me/+jd5Y1Loi4bs4MzA1)

> **旧版项目已完结撒花。** 本仓库进入归档状态，后续不再新增功能或修复旧版逻辑；未来只维护新版 [ETKvNext](https://github.com/hbq0405/ETKvNext)。

Emby ToolKit 是 ETK 的旧版实现，曾经长期服务于 115 网盘整理、STRM 入库、元数据补全、追剧订阅、资源共享和日常维护自动化。

它不是 Emby 的替代品，而是帮你把媒体库从“能用”推进到“好用、好管、好扩展”。

## 旧版状态

- **115 网盘整理**：自动识别电影、剧集和季，按规则整理目录，生成 STRM，并支持 302 直链播放。
- **元数据补全**：补齐中文标题、简介、海报、演员、角色名、类型、评分和分级信息。
- **智能追剧**：跟踪连载剧，发现新集，补齐缺集，完结后自动转入常规维护。
- **演员订阅**：订阅演员并持续跟踪历史作品、新作品和相关入库状态。
- **自建合集与虚拟库**：维护 TMDb 合集、自定义合集，并通过反向代理扩展 Emby 展示能力。
- **共享资源中心**：浏览中心资源库，支持秒传、115 转存、求共享、贡献点、一致版季包和资源标签。
- **维护状态**：旧版代码、旧版数据库和旧版配置仅用于继续运行现有实例，不再接受新功能需求。
- **迁移方向**：确认新版 ETKvNext 稳定后，使用独立 Windows 迁移工具把旧版正式数据迁入新版数据库。
- **并行运行**：迁移或切换期间不要让旧版和新版同时处理同一批媒体。

## 新版主线

[ETKvNext](https://github.com/hbq0405/ETKvNext) 是唯一持续维护的版本，负责：

- 保留旧版成熟的整理、刮削、入库、播放、订阅和通知逻辑。
- 使用 FastAPI、PostgreSQL 和批量任务工作流改善并发、数据一致性和故障排查。
- 通过资源中心、订阅中心、任务中心和整理记录降低前端复杂度。
- 提供 AMD64 `dev` 镜像和正式版本发布镜像。

新版代码仓库：[github.com/hbq0405/ETKvNext](https://github.com/hbq0405/ETKvNext)

新版镜像：`hbq0405/etkn:dev`

## 旧版特色能力（历史）

- 电影按 TMDb ID 聚合，资源版本按 SHA1 去重，减少重复卡片。
- 剧集按剧名、季、集组织资源，支持一致版季包和连载状态判断。
- 支持国语、中字、HDR、杜比、帧率、纯净版、短剧等资源标签。
- 成人资源会在共享登记前拦截，避免进入公共共享池。
- 后台任务可自动维护元数据、分享状态、追剧状态和资源可用性。

## 适合谁使用

- 使用 Emby 管理个人媒体库的用户。
- 使用 115 网盘保存电影、剧集、动漫或音乐的用户。
- 希望自动整理、自动补元数据、自动追剧的用户。
- 希望通过共享资源中心互通资源、减少重复上传和手工找资源的用户。

## 旧版使用与迁移

已有旧版实例可以继续运行，但新用户不建议再部署旧版。旧版文档现已更新为 ETKvNext 用户 Wiki，入口保持不变：

- [项目文档](https://hbq0405.github.io/emby-toolkit/zh/)
- [快速开始](https://hbq0405.github.io/emby-toolkit/zh/guide/quick-start)
- [Docker 部署](https://hbq0405.github.io/emby-toolkit/zh/guide/docker)
- [115 网盘配置](https://hbq0405.github.io/emby-toolkit/zh/guide/p115)
- [共享资源中心](https://hbq0405.github.io/emby-toolkit/zh/guide/shared-resource)

数据迁移工具会在新版核心流程和数据库结构稳定后单独发布，旧版数据库保持只读，不会由新版容器自动迁移。

## 交流与反馈

- Telegram 群组：[加入讨论](https://t.me/+jd5Y1Loi4bs4MzA1)
- 旧版问题反馈仅用于历史问题留档：[GitHub Issues](https://github.com/hbq0405/emby-toolkit/issues)
- 新版问题反馈：[ETKvNext Issues](https://github.com/hbq0405/ETKvNext/issues)
