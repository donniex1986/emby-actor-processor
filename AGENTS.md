# Agent 工作要求

## 1. Think Before Coding

先想清楚，再动手写。

- 需求不确定时，先停下来询问，不要擅自假设。
- 存在多种理解时，先把可能理解列出来，再确认方向。
- 不要闷头猜测，也不要在关键信息缺失时直接实现。

核心原则：一句话，不准瞎假设。

## 2. Simplicity First

能简单，绝不复杂。

- 只写解决当前问题所需的最少代码。
- 50 行能解决的，不要写成 200 行。
- 避免过度抽象、过度封装和无必要的复杂设计。

核心原则：代码越少、路径越短，越容易验证和维护。

## 3. Surgical Changes

外科手术式改动。

- 用户要求改哪里，就只改哪里。
- 不要顺手优化、重构或清理无关代码。
- 修改范围要可控，避免引入额外风险。

核心原则：让它改哪，它就只改哪。

## 4. Goal-Driven Execution

先定目标，再让代码自己验证。

- 开始前明确本次任务的目标和完成标准。
- 修 Bug 时，优先写出能复现问题的测试或验证步骤。
- 修改后必须通过测试、运行结果或明确检查来证明目标已达成。

核心原则：先定目标，再验证结果。

## 临时测试文件约定

- `tests/` 仅用于本地验证和生产热更新前测试，属于临时目录。
- 测试通过后，必须在提交、推送前删除 `tests/` 及其生成物。
- 禁止将 `tests/` 中的临时测试文件纳入 Git 跟踪或提交。

## 执行检查清单

每次开始编码前，先检查：

- 我是否已经理解用户真正要解决的问题？
- 是否存在需要确认的关键假设？
- 是否可以用更少、更简单的代码完成？
- 修改范围是否只覆盖本次任务需要的部分？
- 完成后是否有测试、命令或步骤可以验证结果？

## AI 上下文文档自动读取

每次处理本项目任务前，应先读取 `docs/ai-context/short-term-context.md`，用于快速接手项目当前约定、关键入口和常用验证方式。

涉及架构判断、模块边界、部署热更新、长期维护、跨模块修改或新功能设计时，还应读取 `docs/ai-context/long-term-knowledge-base.md`，并以其中的项目结构、业务流和维护规则作为背景参考。

读取这些文档不代表可以跳过需求确认；如果用户目标或关键假设仍不清楚，仍需先暂停并澄清。

## Emby 服务器连接备查（别名）

敏感连接值不写入本文件；恢复 Git 跟踪前应确保这里只保留别名和用途。

- 服务别名：`emby-main`
- 用途：本项目默认 Emby 服务器。
- 私密信息：地址、账号、密码保存在本机私密备查或密码管理器中，不提交到仓库。
- 使用方式：需要连接 Emby 时，先解析 `emby-main` 对应的本机私密配置。

## PostgreSQL 数据库连接备查（别名）

敏感连接值不写入本文件；恢复 Git 跟踪前应确保这里只保留别名和用途。

- 数据库别名：`etk-db-main`
- 用途：本项目默认生产 PostgreSQL 数据库。
- 私密信息：地址、端口、库名、账号、密码保存在本机私密备查或密码管理器中，不提交到仓库。
- 使用方式：需要直连生产数据库排查时，先解析 `etk-db-main` 对应的本机私密配置。

## SSH 远程连接
SSH 连接必须使用本机 SSH config 中的别名，不在本文件记录公网 IP、端口、用户名、私钥路径等敏感值。

- 中心服务器别名：`center`
- 生产容器宿主机别名：`unraid`
- 影巢中转服务器别名：`hdhive-relay`
- 连接方式：从本机 Windows 通过 `ssh <alias>` 连接。
- 中心端代码：没有 GitHub 仓库，本地源码备份路径使用别名 `center-source-backup` 管理。
- 影巢中转服务器源码备份路径使用别名 `hdhive-relay-source-backup` 管理。该备份只保存 `app.py`、`hdhive-auth.service`、`requirements.txt` 和脱敏 `.env.example`，不保存 SQLite 数据库、token/state、venv 或真实 `.env`。

如果以上任一项不清楚，应先暂停并澄清，而不是直接编码。

## 生产容器热更新备查

生产容器宿主机走 SSH 别名 `unraid`，容器名固定为 `emby-toolkit`。容器内应用目录是 `/app`，前端静态文件目录是 `/app/static`。生产容器的 `/app` 不是宿主机源码挂载，热更新要用 `docker cp` 覆盖容器内文件。

标准流程：

1. 本地先完成验证。

```powershell
python -m py_compile routes\media.py services\subscribe_assistant\manager.py
Push-Location frontend
npm run build
Pop-Location
```

2. 在本机打热更新临时包。只放本次要更新的后端文件和 `frontend/dist`。

```powershell
$hotfix = Join-Path $env:TEMP 'etk-hotfix'
if ($hotfix -notlike "$env:TEMP*") { throw 'unexpected temp path' }
Remove-Item -LiteralPath $hotfix -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path (Join-Path $hotfix 'routes') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $hotfix 'services\subscribe_assistant') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $hotfix 'static') -Force | Out-Null

Copy-Item -LiteralPath 'routes\media.py' -Destination (Join-Path $hotfix 'routes\media.py')
Copy-Item -LiteralPath 'services\subscribe_assistant\manager.py' -Destination (Join-Path $hotfix 'services\subscribe_assistant\manager.py')
Copy-Item -Path 'frontend\dist\*' -Destination (Join-Path $hotfix 'static') -Recurse -Force
```

3. 上传到生产宿主机。

```powershell
scp -r "$env:TEMP\etk-hotfix" unraid:/tmp/
```

4. 远端备份容器内旧文件，再覆盖新文件。按实际改动增减 `docker cp` 的文件列表；不要无脑覆盖无关目录。

```powershell
ssh unraid 'set -e; ts=$(date +%Y%m%d-%H%M%S); backup=/tmp/etk-hotfix-backup/$ts; mkdir -p "$backup/routes" "$backup/services/subscribe_assistant" "$backup/static"; docker cp emby-toolkit:/app/routes/media.py "$backup/routes/media.py"; docker cp emby-toolkit:/app/services/subscribe_assistant/manager.py "$backup/services/subscribe_assistant/manager.py"; docker cp emby-toolkit:/app/static/. "$backup/static/"; docker cp /tmp/etk-hotfix/routes/media.py emby-toolkit:/app/routes/media.py; docker cp /tmp/etk-hotfix/services/subscribe_assistant/manager.py emby-toolkit:/app/services/subscribe_assistant/manager.py; docker cp /tmp/etk-hotfix/static/. emby-toolkit:/app/static/; echo "$backup"'
```

`docker cp` 可能把新拷入的目录权限置成 `700 root:root`，导致 `/assets/*.js` 被服务端回退成 `index.html`，浏览器报 `Expected a JavaScript-or-Wasm module script but the server responded with a MIME type of "text/html"`。覆盖静态文件后必须修权限：

```powershell
ssh unraid "docker exec emby-toolkit find /app/static -type d -exec chmod 755 {} +"
ssh unraid "docker exec emby-toolkit find /app/static -type f -exec chmod 644 {} +"
```

5. 后端 Python 改动必须重启容器才会生效。前端静态文件覆盖后通常立即可用，但统一重启更稳。

```powershell
ssh unraid "docker restart emby-toolkit"
ssh unraid "docker ps --filter name=emby-toolkit --format '{{.Names}} {{.Status}}'"
ssh unraid "docker exec emby-toolkit curl -fsS http://localhost:5257/api/health"
ssh unraid "docker exec emby-toolkit curl -sI http://localhost:5257/assets/index-BhODbAkL.js"
```

注意事项：

- PowerShell 会抢先解析 `$()`、管道和引号。复杂远端命令优先用外层双引号配合简单命令，或外层单引号把 Bash 片段完整交给远端。
- 不要在容器里执行未确认变量的 `rm -rf /app/static/*`。如果需要清空静态目录，先单独确认命令在远端 Bash 中展开正确。
- `docker cp /tmp/etk-hotfix/static/. emby-toolkit:/app/static/` 会覆盖当前入口引用的静态文件，旧 hash 文件残留通常不影响验证。
- 静态资源验证时，`/assets/index-*.js` 必须返回 `Content-Type: text/javascript`。如果返回 `text/html`，优先检查 `/app/static/assets` 目录权限。
- 如需回退，使用第 4 步输出的 `/tmp/etk-hotfix-backup/<timestamp>` 目录，把备份文件 `docker cp` 回容器后重启。

## GitHub 发版操作备查

ETK 与 ETK MediaInfo Bridge 的 GitHub Release 会触发 Telegram 发版通知。TG 会原样转发 GitHub Release 正文；一旦正式发布，错误通知通常无法通过编辑 Release 自动修复。因此必须先创建草稿、完成正文和资产校验，最后才转为正式发布。

### 强制流程

1. 完成测试、版本号修改、提交、标签和推送。ETK 发版必须同时更新后端 `constants.py` 的 `APP_VERSION` 和前端 `frontend/package.json` 的 `version`，打标签前校验两者与目标标签完全一致；后端版本供更新页判断当前版本，前端版本用于界面展示，漏改任一处都会造成版本状态不一致。
2. 创建 `draft=true` 的草稿 Release。禁止直接创建正式 Release。
3. 使用 UTF-8 字节提交 Release 正文，随后从 GitHub API 回读正文并做逐字校验。
4. 有发布资产时，在草稿状态上传资产，并核对 GitHub `digest`、文件大小、程序集版本和本地 SHA-256。
5. 所有校验通过后，才把 `draft` 改为 `false`。这一步会触发 TG 通知，属于不可逆的最终发布动作。
6. 正式发布后从公开下载地址重新下载一次，复核 SHA-256 和程序集依赖；发现问题必须升新版本，不能只原地替换同版本资产，因为已安装错误版本的用户不会触发自动更新。

### PowerShell 中文编码规则

- 中文正文必须使用单引号 here-string（`@' ... '@`），避免反引号、`$变量` 和 Markdown 内容被 PowerShell 插值或转义。
- `ConvertTo-Json` 的结果必须显式转换为 UTF-8 字节，并使用 `application/json; charset=utf-8` 提交。
- 禁止用 Windows PowerShell 直接执行 `Invoke-RestMethod -Body $json -ContentType 'application/json'` 提交中文正文；该写法可能在发送前把中文替换成 `?`。
- 不在命令输出中打印 GitHub Token。Token 只允许在当前进程内从 Git Credential Manager 或安全环境变量读取。

创建草稿 Release 的安全模板：

```powershell
$body = @'
## 修复

- 中文发布说明。
'@

$payload = @{
  tag_name = $tag
  target_commitish = 'main'
  name = $tag
  body = $body
  draft = $true
  prerelease = $false
  generate_release_notes = $false
} | ConvertTo-Json -Depth 10 -Compress

$release = Invoke-RestMethod `
  -Method Post `
  -Uri "https://api.github.com/repos/$owner/$repo/releases" `
  -Headers $headers `
  -ContentType 'application/json; charset=utf-8' `
  -Body ([Text.Encoding]::UTF8.GetBytes($payload))
```

回读正文时必须比较规范化后的完整文本，不能只看 HTTP 成功：

```powershell
$fresh = Invoke-RestMethod `
  -Uri "https://api.github.com/repos/$owner/$repo/releases/$($release.id)" `
  -Headers $headers

$expected = ($body -replace "`r`n", "`n").Trim()
$actual = ($fresh.body -replace "`r`n", "`n").Trim()
if ($actual -cne $expected) { throw 'Release body UTF-8 verification failed' }
if ($actual -match '[\x00-\x08\x0B\x0C\x0E-\x1F]') {
  throw 'Release body contains control characters'
}
```

上传 DLL 后必须在发布前校验 GitHub 返回的摘要：

```powershell
$asset = Invoke-RestMethod `
  -Method Post `
  -Uri "https://uploads.github.com/repos/$owner/$repo/releases/$($release.id)/assets?name=ETKMediaInfoBridge.dll" `
  -Headers $headers `
  -ContentType 'application/octet-stream' `
  -InFile $dllPath

$localDigest = 'sha256:' + (Get-FileHash -Algorithm SHA256 -LiteralPath $dllPath).Hash.ToLowerInvariant()
if ($asset.state -ne 'uploaded' -or $asset.digest -ne $localDigest) {
  throw 'Release asset digest verification failed'
}
```

插件发布还必须使用目标 Emby 版本的真实服务端程序集执行干净构建，不能使用默认 NuGet 引用代替：

```powershell
& $dotnet build -c Release -t:Rebuild -p:EmbyReferencePath=$embyReferencePath
```

构建后检查 `ETKMediaInfoBridge.dll` 的程序集版本、`MediaBrowser.*` 引用版本和全部类型加载结果。草稿发布完成上述检查后，才用同样的 UTF-8 字节方式提交 `@{ draft = $false }`，转为正式 Release。

## MP-MCP 连接备查

MoviePilot 内置 Streamable HTTP MCP，可直接通过 HTTP JSON-RPC 调用。

- 服务别名：`moviepilot-main`
- MCP 端点别名：`moviepilot-main-mcp`
- 工具列表端点别名：`moviepilot-main-mcp-tools`
- 认证 Header：`X-API-KEY: <MOVIEPILOT_API_KEY>`
- 推荐 Header：`Accept: application/json, text/event-stream`
- 私密信息：实际地址和 API Key 保存在本机私密备查或环境变量中，不提交到仓库。

最小验证命令：

```powershell
$mpMcpEndpoint = $env:MOVIEPILOT_MCP_ENDPOINT
$mpApiKey = $env:MOVIEPILOT_API_KEY
if (-not $mpMcpEndpoint -or -not $mpApiKey) { throw 'Missing MOVIEPILOT_MCP_ENDPOINT or MOVIEPILOT_API_KEY' }

$headers = @{
  "X-API-KEY" = $mpApiKey
  "Content-Type" = "application/json"
  "Accept" = "application/json, text/event-stream"
}

$body = @{
  jsonrpc = "2.0"
  id = 1
  method = "tools/list"
  params = @{}
} | ConvertTo-Json -Depth 5 -Compress

Invoke-RestMethod `
  -Uri $mpMcpEndpoint `
  -Headers $headers `
  -Method POST `
  -Body $body
```

调用工具使用 `tools/call`：

```powershell
$body = @{
  jsonrpc = "2.0"
  id = 2
  method = "tools/call"
  params = @{
    name = "query_subscribes"
    arguments = @{}
  }
} | ConvertTo-Json -Depth 10 -Compress
```
