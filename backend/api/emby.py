"""Emby APIs."""
from __future__ import annotations

from fastapi import APIRouter

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
    return result
