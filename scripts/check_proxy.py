from __future__ import annotations

import json
import os
import socket
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.mobileproxy_api import fetch_proxy_ip_with_spam_check
from pipeline.mobileproxy_connection import fetch_mobileproxy_http_proxy
from pipeline.utils import (
    check_proxy_health,
    fetch_exit_ip_via_proxy,
    proxy_cfg_to_url,
    proxy_url_to_cfg,
    requests_proxies_from_proxy_url,
)


def _http_probe_via_proxy(proxy_cfg: dict, url: str, timeout: int = 15) -> tuple[bool, str]:
    import requests

    proxy_url = proxy_cfg_to_url(proxy_cfg)
    proxies = requests_proxies_from_proxy_url(proxy_url) or {}
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            proxies=proxies,
            timeout=timeout,
        )
        body = (resp.text or "")[:160]
        return True, f"HTTP {resp.status_code}, body={body!r}"
    except Exception as exc:  # pragma: no cover - диагностический fallback
        return False, f"{type(exc).__name__}: {exc}"


def _tcp_probe(host: str, port: int, timeout: float = 8.0) -> tuple[bool, str]:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True, "TCP connection established"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def main() -> int:
    print("[1/4] API proxy_ip + spam-check")
    api_data = fetch_proxy_ip_with_spam_check()
    if not api_data:
        print("  FAIL: mobileproxy API недоступен или вернул пустой ответ")
        return 1
    print(f"  OK: {json.dumps(api_data, ensure_ascii=False)}")

    print("[2/4] Получение параметров прокси")
    proxy_cfg = None
    source = "api:get_my_proxy"
    env_proxy = (os.getenv("PROXY") or "").strip()
    if env_proxy:
        proxy_cfg = proxy_url_to_cfg(env_proxy)
        source = "env:PROXY"
        if not proxy_cfg:
            print("  FAIL: PROXY задан, но имеет невалидный формат URL")
            return 2
    else:
        proxy_cfg = fetch_mobileproxy_http_proxy(force_refresh=True, use_cache_on_api_fail=False)
        if not proxy_cfg:
            print("  FAIL: не удалось получить host/port/login/password")
            return 2
        proxy_cfg["scheme"] = "http"
    print(f"  OK: source={source}, cfg={json.dumps(proxy_cfg, ensure_ascii=False)}")

    print("[3/4] Сетевая доступность хоста прокси (DNS + TCP)")
    host = str(proxy_cfg.get("host") or "")
    port = int(proxy_cfg.get("port") or 0)
    if not host or port <= 0:
        print("  FAIL: невалидные host/port")
        return 3
    try:
        resolved = socket.gethostbyname(host)
        print(f"  OK: DNS {host} -> {resolved}")
    except Exception as exc:
        print(f"  FAIL: DNS lookup error: {type(exc).__name__}: {exc}")
        return 3
    tcp_ok, tcp_msg = _tcp_probe(host, port)
    if not tcp_ok:
        print(f"  FAIL: TCP {host}:{port} -> {tcp_msg}")
        return 3
    print(f"  OK: TCP {host}:{port} -> {tcp_msg}")

    print("[4/4] Реальный трафик через прокси (несколько endpoint)")
    probes = [
        ("http://httpbin.org/ip", "HTTP httpbin"),
        ("https://api.ipify.org?format=json", "HTTPS ipify"),
        ("https://ifconfig.me/ip", "HTTPS ifconfig"),
    ]
    all_ok = True
    for url, label in probes:
        ok, msg = _http_probe_via_proxy(proxy_cfg, url, timeout=20)
        mark = "OK" if ok else "FAIL"
        print(f"  {mark}: {label} -> {msg}")
        if not ok:
            all_ok = False

    base_health = check_proxy_health(proxy_cfg)
    print(f"  {'OK' if base_health else 'FAIL'}: built-in health-check")
    exit_ip = fetch_exit_ip_via_proxy(proxy_cfg)
    print(f"  INFO: exit_ip={exit_ip}")

    if not all_ok or not base_health:
        print("  FAIL: прокси частично/полностью недоступен для HTTP(S)-трафика")
        return 4

    exit_ip = fetch_exit_ip_via_proxy(proxy_cfg)
    print(f"  OK: health-check прошел, exit_ip={exit_ip}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
