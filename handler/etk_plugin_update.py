import io
import logging
import re
import tarfile
import threading
import time
from urllib.parse import urlsplit

import docker
import requests

import config_manager
import constants
from handler.emby import configure_etk_plugin_origin, emby_client


logger = logging.getLogger(__name__)

PLUGIN_NAME = 'ETK MediaInfo Bridge'
PLUGIN_DLL_NAME = 'ETKMediaInfoBridge.dll'
PLUGIN_TASK_KEY = 'ETKMediaInfoBridgeUpdate'
LATEST_RELEASE_API = 'https://api.github.com/repos/hbq0405/etk-mediainfo-bridge/releases/latest'
LATEST_RELEASE_DOWNLOAD = 'https://github.com/hbq0405/etk-mediainfo-bridge/releases/latest/download/ETKMediaInfoBridge.dll'
_START_LOCK = threading.Lock()
_STARTED = False


def _version_tuple(value):
    numbers = [int(part) for part in re.findall(r'\d+', str(value or ''))[:4]]
    return tuple((numbers + [0, 0, 0, 0])[:4]) if numbers else None


def _emby_headers(api_key):
    return {'X-Emby-Token': api_key, 'Accept': 'application/json'}


def _emby_get_json(base_url, api_key, path):
    response = emby_client.get(
        f"{base_url.rstrip('/')}{path}",
        headers=_emby_headers(api_key),
        params={'api_key': api_key},
    )
    response.raise_for_status()
    return response.json()


def _latest_plugin_version():
    headers = {
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'EmbyToolKit-PluginUpdater',
    }
    token = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_GITHUB_TOKEN) or '').strip()
    if token:
        headers['Authorization'] = f'Bearer {token}'
    response = requests.get(
        LATEST_RELEASE_API,
        headers=headers,
        timeout=20,
        proxies=config_manager.get_proxies_for_requests(),
    )
    response.raise_for_status()
    tag = str((response.json() or {}).get('tag_name') or '').strip()
    return tag, _version_tuple(tag)


def _find_plugin(plugins):
    for plugin in plugins or []:
        if str(plugin.get('Name') or '').strip() == PLUGIN_NAME:
            return plugin
    return None


def is_etk_plugin_installed(base_url=None, api_key=None):
    base_url = str(base_url or config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or '').strip()
    api_key = str(api_key or config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY) or '').strip()
    if not base_url or not api_key:
        return False
    try:
        return _find_plugin(_emby_get_json(base_url, api_key, '/Plugins')) is not None
    except Exception:
        return False


def _plugin_download_headers():
    headers = {'User-Agent': 'EmbyToolKit-PluginInstaller'}
    token = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_GITHUB_TOKEN) or '').strip()
    if token:
        headers['Authorization'] = f'Bearer {token}'
    return headers


def _download_latest_plugin_dll():
    response = requests.get(
        LATEST_RELEASE_DOWNLOAD,
        headers=_plugin_download_headers(),
        timeout=(15, 300),
        proxies=config_manager.get_proxies_for_requests(),
    )
    response.raise_for_status()
    content = response.content
    if len(content) < 10_000 or not content.startswith(b'MZ'):
        raise RuntimeError('下载结果不是有效的 ETK 插件 DLL')
    return content


def _container_network_values(container):
    attrs = container.attrs or {}
    networks = ((attrs.get('NetworkSettings') or {}).get('Networks') or {})
    ips = set()
    aliases = {str(container.name or '').strip().lower()}
    for network in networks.values():
        ip = str((network or {}).get('IPAddress') or '').strip()
        if ip:
            ips.add(ip)
        for alias in (network or {}).get('Aliases') or []:
            if alias:
                aliases.add(str(alias).strip().lower())
    return ips, aliases


def _is_emby_container(container):
    attrs = container.attrs or {}
    config = attrs.get('Config') or {}
    image = str((config.get('Image')) or '').lower()
    name = str(container.name or '').lower()
    labels = config.get('Labels') or {}
    label_values = ' '.join(str(value or '').lower() for value in labels.values())

    # ETK's own container/image contains "emby-toolkit"; do not count it as an
    # Emby server candidate, especially in host-network deployments.
    haystack = ' '.join([image, name, label_values])
    excluded_markers = (
        'emby-toolkit',
        'etk-mediainfo-bridge',
        'hbq0405/emby-toolkit',
    )
    if any(marker in haystack for marker in excluded_markers):
        return False

    image_repo = image.split(':', 1)[0]
    name_tokens = {token for token in re.split(r'[^a-z0-9]+', name) if token}
    image_tokens = {token for token in re.split(r'[^a-z0-9]+', image_repo) if token}
    return (
        image_repo == 'emby/embyserver'
        or image_repo.endswith('/embyserver')
        or image_repo.endswith('/emby')
        or 'embyserver' in image_tokens
        or 'embyserver' in name_tokens
        or name_tokens == {'emby'}
    )


def _candidate_emby_urls(container):
    attrs = container.attrs or {}
    exposed = ((attrs.get('Config') or {}).get('ExposedPorts') or {})
    ports = {8096}
    for value in exposed:
        text = str(value).split('/', 1)[0]
        if text.isdigit():
            ports.add(int(text))
    ips, _aliases = _container_network_values(container)
    return [f'http://{ip}:{port}' for ip in ips for port in sorted(ports)]


def _container_label(container):
    attrs = container.attrs or {}
    image = str(((attrs.get('Config') or {}).get('Image')) or '').strip()
    return f"{container.name or '<unknown>'} ({image or 'unknown image'})"


def _public_server_id(base_url):
    try:
        response = requests.get(f"{base_url.rstrip('/')}/System/Info/Public", timeout=3)
        response.raise_for_status()
        data = response.json() or {}
        return str(data.get('Id') or data.get('ServerId') or '').strip()
    except Exception:
        return ''


def _resolve_emby_container(client, base_url, api_key):
    parsed = urlsplit(base_url)
    target_host = str(parsed.hostname or '').strip().lower()
    target_port = parsed.port or (443 if parsed.scheme == 'https' else 80)
    containers = [item for item in client.containers.list() if _is_emby_container(item)]
    direct = []
    for container in containers:
        ips, aliases = _container_network_values(container)
        attrs = container.attrs or {}
        port_map = ((attrs.get('NetworkSettings') or {}).get('Ports') or {})
        published_ports = {
            int(binding.get('HostPort'))
            for bindings in port_map.values() if bindings
            for binding in bindings
            if str(binding.get('HostPort') or '').isdigit()
        }
        if target_host in {value.lower() for value in ips} or target_host in aliases or target_port in published_ports:
            direct.append(container)
    if len(direct) == 1:
        return direct[0]

    target_info = _emby_get_json(base_url, api_key, '/System/Info')
    target_server_id = str(target_info.get('Id') or target_info.get('ServerId') or '').strip()
    matched = []
    for container in direct or containers:
        if target_server_id and any(
            _public_server_id(url) == target_server_id
            for url in _candidate_emby_urls(container)
        ):
            matched.append(container)
    if len(matched) == 1:
        return matched[0]
    if not direct and len(containers) == 1:
        return containers[0]
    labels = ', '.join(_container_label(item) for item in containers) or 'none'
    raise RuntimeError(f'无法从 Docker 中唯一定位已授权的 Emby 容器，候选容器: {labels}')


def _resolve_plugins_path(container):
    attrs = container.attrs or {}
    mounts = attrs.get('Mounts') or []
    candidates = []
    for mount in mounts:
        destination = str((mount or {}).get('Destination') or '').rstrip('/')
        if destination == '/config':
            candidates.append('/config/plugins')
        elif destination.endswith('/emby'):
            candidates.append(destination + '/plugins')
    candidates.extend(['/config/plugins', '/var/lib/emby/plugins'])
    for path in dict.fromkeys(candidates):
        result = container.exec_run(['test', '-d', path])
        if int(result.exit_code or 0) == 0:
            return path
    raise RuntimeError('无法确定 Emby 插件目录')


def _put_plugin_dll(container, plugins_path, content):
    target_path = f"{plugins_path.rstrip('/')}/{PLUGIN_DLL_NAME}"
    if int(container.exec_run(['test', '-f', target_path]).exit_code or 0) == 0:
        container.exec_run(['cp', '-f', target_path, target_path + '.bak'])
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w') as tar:
        info = tarfile.TarInfo(PLUGIN_DLL_NAME)
        info.size = len(content)
        info.mode = 0o644
        info.mtime = int(time.time())
        tar.addfile(info, io.BytesIO(content))
    archive.seek(0)
    if not container.put_archive(plugins_path, archive.read()):
        raise RuntimeError('写入 Emby 插件目录失败')


def _wait_for_plugin(base_url, api_key, timeout_seconds=180):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            plugin = _find_plugin(_emby_get_json(base_url, api_key, '/Plugins'))
            if plugin is not None:
                return plugin
        except Exception:
            pass
        time.sleep(2)
    return None


def ensure_etk_plugin_installed(base_url, api_key, etk_url=''):
    """Install the bridge into the authorized Docker Emby, then restart and verify it."""
    base_url = str(base_url or '').strip().rstrip('/')
    api_key = str(api_key or '').strip()
    etk_url = str(etk_url or '').strip().rstrip('/')
    if not base_url or not api_key:
        return {'ok': False, 'status': 'failed', 'message': '缺少 Emby 服务地址或授权 Token'}

    try:
        if is_etk_plugin_installed(base_url, api_key):
            origin_configured = bool(etk_url and configure_etk_plugin_origin(base_url, api_key, etk_url))
            return {
                'ok': True,
                'status': 'already_installed',
                'origin_configured': origin_configured,
                'message': 'ETK MediaInfo Bridge 已安装',
            }

        logger.info('  ➜ [插件自动安装] Emby 尚未安装 ETK MediaInfo Bridge，开始下载安装。')
        client = docker.from_env()
        try:
            container = _resolve_emby_container(client, base_url, api_key)
            plugins_path = _resolve_plugins_path(container)
            content = _download_latest_plugin_dll()
            _put_plugin_dll(container, plugins_path, content)
            logger.info('  ➜ [插件自动安装] 插件已写入 Emby 容器，正在重启使其生效。')
            container.restart(timeout=10)
        finally:
            client.close()

        plugin = _wait_for_plugin(base_url, api_key)
        if plugin is None:
            raise RuntimeError('Emby 重启后未检测到 ETK MediaInfo Bridge')
        origin_configured = bool(etk_url and configure_etk_plugin_origin(base_url, api_key, etk_url))
        version = str(plugin.get('Version') or '').strip()
        logger.info('  ➜ [插件自动安装] ETK MediaInfo Bridge %s 安装完成。', version or '')
        return {
            'ok': True,
            'status': 'installed',
            'version': version,
            'origin_configured': origin_configured,
            'message': f"ETK MediaInfo Bridge {version or ''} 已自动安装并重启 Emby".strip(),
        }
    except Exception as e:
        logger.error('  ➜ [插件自动安装] 自动安装失败: %s', e, exc_info=True)
        return {'ok': False, 'status': 'failed', 'message': str(e)}


def _find_update_task(tasks):
    for task in tasks or []:
        if str(task.get('Key') or '').strip() == PLUGIN_TASK_KEY:
            return task
    return None


def _task_end_time(task):
    return str(((task or {}).get('LastExecutionResult') or {}).get('EndTimeUtc') or '').strip()


def _wait_for_task(base_url, api_key, task_id, previous_end_time, timeout_seconds=420):
    deadline = time.monotonic() + timeout_seconds
    saw_running = False
    while time.monotonic() < deadline:
        tasks = _emby_get_json(base_url, api_key, '/ScheduledTasks')
        task = next((item for item in tasks if str(item.get('Id')) == str(task_id)), None)
        if task is None:
            raise RuntimeError('Emby 插件更新任务已消失')
        if str(task.get('State') or '').lower() == 'running':
            saw_running = True
            time.sleep(2)
            continue

        result = task.get('LastExecutionResult') or {}
        end_time = str(result.get('EndTimeUtc') or '').strip()
        if saw_running or (end_time and end_time != previous_end_time):
            status = str(result.get('Status') or '').strip()
            if status.lower() == 'completed':
                return True
            logger.error(
                "  ➜ [插件联动更新] Emby 更新任务失败: status=%s, error=%s",
                status or 'unknown',
                result.get('ErrorMessage') or result.get('LongErrorMessage') or '',
            )
            return False
        time.sleep(2)
    logger.error("  ➜ [插件联动更新] 等待 Emby 插件更新任务超时。")
    return False


def check_and_update_etk_plugin():
    base_url = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or '').strip()
    api_key = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY) or '').strip()
    if not base_url or not api_key:
        logger.debug("  ➜ [插件联动更新] 未配置 Emby 地址或 API Key，跳过检查。")
        return False

    try:
        plugin = _find_plugin(_emby_get_json(base_url, api_key, '/Plugins'))
        if plugin is None:
            logger.warning("  ➜ [插件联动更新] Emby 未安装 ETK MediaInfo Bridge，跳过自动更新。")
            return False
        current_text = str(plugin.get('Version') or '').strip()
        current_version = _version_tuple(current_text)
        latest_text, latest_version = _latest_plugin_version()
        if current_version is None or latest_version is None:
            logger.warning(
                "  ➜ [插件联动更新] 无法解析插件版本: current=%s, latest=%s",
                current_text or 'unknown',
                latest_text or 'unknown',
            )
            return False
        if current_version >= latest_version:
            logger.debug(
                "  ➜ [插件联动更新] 插件已是最新版: %s",
                current_text,
            )
            return False

        tasks = _emby_get_json(base_url, api_key, '/ScheduledTasks')
        task = _find_update_task(tasks)
        if task is None:
            logger.error("  ➜ [插件联动更新] Emby 中未找到插件更新计划任务。")
            return False

        task_id = str(task.get('Id') or '').strip()
        if not task_id:
            logger.error("  ➜ [插件联动更新] Emby 插件更新任务缺少 ID。")
            return False
        previous_end_time = _task_end_time(task)
        logger.info(
            "  ➜ [插件联动更新] 检测到新版本: %s -> %s，正在触发 Emby 更新任务。",
            current_text,
            latest_text,
        )
        if str(task.get('State') or '').lower() != 'running':
            response = emby_client.post(
                f"{base_url.rstrip('/')}/ScheduledTasks/Running/{task_id}",
                headers=_emby_headers(api_key),
                params={'api_key': api_key},
            )
            response.raise_for_status()

        if not _wait_for_task(base_url, api_key, task_id, previous_end_time):
            return False

        system_info = _emby_get_json(base_url, api_key, '/System/Info')
        if not bool(system_info.get('HasPendingRestart')):
            logger.error("  ➜ [插件联动更新] 更新任务已完成，但 Emby 未报告待重启，拒绝自动重启。")
            return False

        logger.info("  ➜ [插件联动更新] 插件已更新，正在自动重启 Emby 使其生效。")
        response = emby_client.post(
            f"{base_url.rstrip('/')}/System/Restart",
            headers=_emby_headers(api_key),
            params={'api_key': api_key},
        )
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error("  ➜ [插件联动更新] 自动检查或更新失败: %s", e)
        return False


def schedule_etk_plugin_update_check(delay_seconds=30):
    global _STARTED
    with _START_LOCK:
        if _STARTED:
            return False
        _STARTED = True

    def delayed_check():
        time.sleep(max(0, delay_seconds))
        check_and_update_etk_plugin()

    threading.Thread(
        target=delayed_check,
        name='ETKPluginUpdateCheck',
        daemon=True,
    ).start()
    return True
