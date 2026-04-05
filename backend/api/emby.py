"""Emby APIs."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..services.emby import list_emby_libraries, refresh_emby_library

router = APIRouter()


@router.get("/libraries")
def list_libraries():
    return {"items": list_emby_libraries()}


@router.post("/refresh")
def refresh_library():
    result = refresh_emby_library()
    if result.get("ok"):
        return result
    detail = str(result.get("message") or "Emby 刷新失败")
    status_code = 400 if "未配置" in detail else 502
    raise HTTPException(status_code=status_code, detail=detail)
