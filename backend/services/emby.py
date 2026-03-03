"""Emby integration service."""
from __future__ import annotations

from typing import Any

import httpx

from ..config import settings


def _auth_headers() -> dict[str, str]:
    api_key = (settings.emby_api_key or "").strip()
    return {"X-Emby-Token": api_key} if api_key else {}


def list_emby_libraries() -> list[dict[str, Any]]:
    base = (settings.emby_url or "").rstrip("/")
    if not base or not settings.emby_api_key:
        return []

    url = f"{base}/emby/Library/SelectableMediaFolders"
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(url, headers=_auth_headers())
    except Exception:
        return []

    if resp.status_code != 200:
        return []

    payload = resp.json()
    items = payload if isinstance(payload, list) else payload.get("Items", [])
    out = []
    for item in items or []:
        out.append(
            {
                "id": item.get("Id") or item.get("id"),
                "name": item.get("Name") or item.get("name"),
                "collection_type": item.get("CollectionType") or item.get("collectionType") or "",
            }
        )
    return out


def refresh_emby_library() -> dict:
    base = (settings.emby_url or "").rstrip("/")
    if not base or not settings.emby_api_key:
        return {"ok": False, "message": "未配置 Emby URL 或 API Key"}

    ids = [x.strip() for x in str(settings.emby_library_ids or "").split(",") if x.strip()]
    headers = _auth_headers()

    try:
        with httpx.Client(timeout=30) as client:
            if ids:
                for lib_id in ids:
                    url = f"{base}/emby/Items/{lib_id}/Refresh"
                    client.post(url, headers=headers, params={"Recursive": "true", "ImageRefreshMode": "Default"})
            else:
                client.post(f"{base}/emby/Library/Refresh", headers=headers)
    except Exception as e:
        return {"ok": False, "message": f"请求失败: {e}"}

    return {"ok": True, "message": "已发送 Emby 刷新请求"}
