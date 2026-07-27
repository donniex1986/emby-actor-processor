import os
from urllib.parse import urlsplit

import docker


def resolve_proxy_discovery_url(config):
    if not config.get("proxy_enabled"):
        raise ValueError("请先启用虚拟库反向代理")

    source_url = str(config.get("etk_server_url") or "").strip()
    parsed = urlsplit(source_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("请先配置有效的 STRM 链接地址")

    proxy_port = int(config.get("proxy_port") or 0)
    if not 1 <= proxy_port <= 65535:
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
        direct_address = parsed.hostname in _container_network_addresses(attrs)
        host_port = (
            proxy_port
            if network_mode == "host" or direct_address
            else _published_proxy_port(attrs, proxy_port, parsed.hostname)
        )
    except docker.errors.DockerException as e:
        raise RuntimeError("无法读取 ETK 容器端口映射，请确认已挂载 Docker socket") from e
    finally:
        client.close()

    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{host_port}"


def _container_network_addresses(attrs):
    networks = ((attrs.get("NetworkSettings") or {}).get("Networks") or {}).values()
    return {
        address
        for network in networks
        for address in (
            str(network.get("IPAddress") or ""),
            str(network.get("GlobalIPv6Address") or ""),
        )
        if address
    }


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
