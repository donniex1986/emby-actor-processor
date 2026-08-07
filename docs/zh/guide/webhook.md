# Emby 与事件

Emby 媒体事件由 ETK MediaInfo Bridge 插件接管，不再使用旧版 `/webhook/emby` 作为 Emby 事件入口。

## Emby 插件

首次管理员授权时，ETKN 会通过映射的 Docker Socket 自动安装或更新插件。插件负责元数据、图片、媒体信息、中文搜索、播放事件和删除事件回传。

## MoviePilot

MoviePilot 仍可通过新版 `/webhook` 上报订阅和转存完成事件。MP 直出模式已经移除，资源转存完成后会进入 ETKN 的资源获取、整理和入库任务链。

## 排查事件

如果资源已转存但没有整理，先在任务中心查看资源获取任务是否生成了后续整理任务；如果 Emby 没有出现媒体，检查 Bridge 插件状态、媒体库路径映射和任务链末尾的 Emby 校验日志。
