import os
import ipaddress
from urllib.parse import urlsplit

import docker


def validate_internal_service_url(value):
    url = str(value or "").strip().rstrip("/")
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("地址必须以 http:// 或 https:// 开头")
    try:
        host_ip = ipaddress.ip_address(parsed.hostname)
    except ValueError as e:
        raise ValueError("地址必须使用内网 IP，不能使用域名") from e
    if not host_ip.is_private or host_ip.is_loopback or host_ip.is_link_local or host_ip.is_unspecified:
        raise ValueError("地址不是可用的内网 IP")
    return url


def resolve_etk_service_url(config):
    return _resolve_published_service_url(config, 5257)


def resolve_proxy_discovery_url(config):
    if not config.get("proxy_enabled"):
        raise ValueError("请先启用虚拟库反向代理")

    return _resolve_published_service_url(config, int(config.get("proxy_port") or 0))


def _resolve_published_service_url(config, service_port):
    source_url = str(config.get("emby_server_url") or "").strip()
    parsed = urlsplit(source_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("请先配置有效的 Emby 内网地址")

    validate_internal_service_url(source_url)

    if not 1 <= service_port <= 65535:
        raise ValueError("反向代理端口无效")

    client = docker.from_env()
    try:
        hostname = str(os.environ.get("HOSTNAME") or "").strip()
        container = None
        for identifier in (hostname, "emby-toolkit"):
            if not identifier:
                continue
            try:
                container = client.containers.get(identifier)
                break
            except docker.errors.NotFound:
                continue
        if container is None:
            raise RuntimeError("无法从 Docker 定位当前 ETK 容器")

        attrs = container.attrs or {}
        network_mode = str((attrs.get("HostConfig") or {}).get("NetworkMode") or "").lower()
        direct_address = _direct_container_address(attrs, parsed.hostname)
        host_port = (
            service_port
            if network_mode == "host" or direct_address
            else _published_proxy_port(attrs, service_port, parsed.hostname)
        )
    except docker.errors.DockerException as e:
        raise RuntimeError("无法读取 ETK 容器端口映射，请确认已挂载 Docker socket") from e
    finally:
        client.close()

    host = direct_address or parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{host_port}"


def _direct_container_address(attrs, target_host):
    try:
        target_ip = ipaddress.ip_address(target_host)
    except ValueError:
        return None
    networks = ((attrs.get("NetworkSettings") or {}).get("Networks") or {}).values()
    for network in networks:
        for address, prefix in (
            (network.get("IPAddress"), network.get("IPPrefixLen")),
            (network.get("GlobalIPv6Address"), network.get("GlobalIPv6PrefixLen")),
        ):
            if not address or prefix in (None, ""):
                continue
            try:
                if target_ip in ipaddress.ip_network(f"{address}/{prefix}", strict=False):
                    return str(address)
            except ValueError:
                continue
    return None


def _published_proxy_port(attrs, proxy_port, target_host):
    bindings = ((attrs.get("NetworkSettings") or {}).get("Ports") or {}).get(
        f"{proxy_port}/tcp"
    ) or []
    exact_ports = {
        int(item["HostPort"])
        for item in bindings
        if str(item.get("HostIp") or "") == target_host
        and str(item.get("HostPort") or "").isdigit()
    }
    wildcard_ports = {
        int(item["HostPort"])
        for item in bindings
        if str(item.get("HostIp") or "") in ("", "0.0.0.0", "::")
        and str(item.get("HostPort") or "").isdigit()
    }
    ports = exact_ports or wildcard_ports
    if len(ports) != 1:
        raise RuntimeError(f"无法唯一确定容器端口 {proxy_port}/tcp 的宿主机映射")
    return ports.pop()
