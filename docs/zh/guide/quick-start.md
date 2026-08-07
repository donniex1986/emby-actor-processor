# 快速开始

## 准备

- Docker / Docker Compose 或 Unraid Compose。
- PostgreSQL 数据库，已创建 `etkn` 数据库和可写用户。
- Emby 管理员账号。
- TMDb API Key。
- 持久化目录，例如 `/mnt/user/appdata/etkn`。

## 启动

使用 [Docker 部署](/zh/guide/docker) 中的 Compose，映射 `5257:5257` 和 `8097:8097`，并持久化 `/config`。启动后打开 `http://服务器IP:5257`。

## 首次操作

1. 使用 Emby 管理员登录。
2. 完成 Emby、115、TMDb 和网络代理授权。
3. 在 Emby 页面确认媒体库和媒体库根目录。
4. 在 115 页面选择待整理目录、分类和重命名规则。
5. 放入一部测试电影，去任务中心确认“整理 -> 刮削 -> 入库 -> Emby 校验”完整结束。

不要让旧版 ETK 和 ETKvNext 同时处理同一个待整理目录。
