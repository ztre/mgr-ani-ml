"""Configuration APIs."""
from __future__ import annotations

import hmac

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx

from ..config import settings

router = APIRouter()


class ConfigResponse(BaseModel):
    bangumi_api_key: str
    tmdb_api_key: str
    emby_url: str
    emby_api_key: str
    emby_library_ids: list[str]
    movie_fallback_strategy: str
    movie_fallback_hints: str
    stats_ignore_specials: bool
    stats_ignore_extras: bool
    stats_ignore_trailers_featurettes: bool
    log_retention_days: int
    log_max_task_files: int
    log_cleanup_interval_seconds: int


class ConfigUpdate(BaseModel):
    bangumi_api_key: str | None = None
    tmdb_api_key: str | None = None
    emby_url: str | None = None
    emby_api_key: str | None = None
    emby_library_ids: list[str] | None = None
    movie_fallback_strategy: str | None = None
    movie_fallback_hints: str | None = None
    stats_ignore_specials: bool | None = None
    stats_ignore_extras: bool | None = None
    stats_ignore_trailers_featurettes: bool | None = None
    log_retention_days: int | None = None
    log_max_task_files: int | None = None
    log_cleanup_interval_seconds: int | None = None


class TestConnectionRequest(BaseModel):
    bangumi_api_key: str | None = None
    tmdb_api_key: str | None = None
    emby_url: str | None = None
    emby_api_key: str | None = None


class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str


@router.get("", response_model=ConfigResponse)
def get_config():
    ids = [x.strip() for x in str(settings.emby_library_ids or "").split(",") if x.strip()]
    return ConfigResponse(
        bangumi_api_key=settings.bangumi_api_key or "",
        tmdb_api_key=settings.tmdb_api_key or "",
        emby_url=settings.emby_url or "",
        emby_api_key=settings.emby_api_key or "",
        emby_library_ids=ids,
        movie_fallback_strategy=settings.movie_fallback_strategy or "auto",
        movie_fallback_hints=settings.movie_fallback_hints or "",
        stats_ignore_specials=bool(settings.stats_ignore_specials),
        stats_ignore_extras=bool(settings.stats_ignore_extras),
        stats_ignore_trailers_featurettes=bool(settings.stats_ignore_trailers_featurettes),
        log_retention_days=int(settings.log_retention_days or 14),
        log_max_task_files=int(settings.log_max_task_files or 200),
        log_cleanup_interval_seconds=int(settings.log_cleanup_interval_seconds or 600),
    )


@router.put("")
def update_config(data: ConfigUpdate):
    if data.bangumi_api_key is not None:
        settings.bangumi_api_key = data.bangumi_api_key
    if data.tmdb_api_key is not None:
        settings.tmdb_api_key = data.tmdb_api_key
    if data.emby_url is not None:
        settings.emby_url = data.emby_url
    if data.emby_api_key is not None:
        settings.emby_api_key = data.emby_api_key
    if data.emby_library_ids is not None:
        settings.emby_library_ids = ",".join([x for x in data.emby_library_ids if x])
    if data.movie_fallback_strategy is not None:
        settings.movie_fallback_strategy = data.movie_fallback_strategy
    if data.movie_fallback_hints is not None:
        settings.movie_fallback_hints = data.movie_fallback_hints
    if data.stats_ignore_specials is not None:
        settings.stats_ignore_specials = data.stats_ignore_specials
    if data.stats_ignore_extras is not None:
        settings.stats_ignore_extras = data.stats_ignore_extras
    if data.stats_ignore_trailers_featurettes is not None:
        settings.stats_ignore_trailers_featurettes = data.stats_ignore_trailers_featurettes
    if data.log_retention_days is not None:
        settings.log_retention_days = max(0, data.log_retention_days)
    if data.log_max_task_files is not None:
        settings.log_max_task_files = max(10, data.log_max_task_files)
    if data.log_cleanup_interval_seconds is not None:
        settings.log_cleanup_interval_seconds = max(60, data.log_cleanup_interval_seconds)

    settings.save_to_env()
    return {"ok": True}


@router.post("/restart")
def restart_service():
    # compatible behavior: frontend expects success message only
    return {"ok": True, "message": "重启请求已接受（请由容器/进程管理器执行重启）"}


@router.post("/test-connection")
def test_connection(data: TestConnectionRequest):
    results = {}

    if data.bangumi_api_key is not None:
        # Deprecated but keep compatible contract for frontend.
        results["bangumi"] = {"ok": True, "message": "Bangumi 已弃用（识别仅使用 TMDB）"}

    if data.tmdb_api_key is not None:
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.get(
                    "https://api.themoviedb.org/3/configuration",
                    params={"api_key": data.tmdb_api_key},
                )
            if resp.status_code == 200:
                results["tmdb"] = {"ok": True, "message": "连接成功"}
            else:
                results["tmdb"] = {"ok": False, "message": f"连接失败: {resp.status_code}"}
        except Exception as e:
            results["tmdb"] = {"ok": False, "message": f"连接异常: {e}"}

    if data.emby_url is not None or data.emby_api_key is not None:
        emby_url = (data.emby_url if data.emby_url is not None else settings.emby_url or "").rstrip("/")
        emby_api_key = data.emby_api_key if data.emby_api_key is not None else settings.emby_api_key
        if not emby_url or not emby_api_key:
            results["emby"] = {"ok": False, "message": "缺少 Emby URL 或 API Key"}
        else:
            try:
                with httpx.Client(timeout=10) as client:
                    resp = client.get(f"{emby_url}/emby/System/Info", headers={"X-Emby-Token": emby_api_key})
                if resp.status_code == 200:
                    results["emby"] = {"ok": True, "message": "连接成功"}
                else:
                    results["emby"] = {"ok": False, "message": f"连接失败: {resp.status_code}"}
            except Exception as e:
                results["emby"] = {"ok": False, "message": f"连接异常: {e}"}

    if not results:
        raise HTTPException(status_code=400, detail="未提供可测试的字段")
    return results


@router.post("/change-password")
def change_password(data: PasswordChangeRequest):
    if not settings.auth_enabled:
        raise HTTPException(status_code=400, detail="当前未启用鉴权，无需修改密码")

    current_password = data.current_password or ""
    new_password = (data.new_password or "").strip()
    if not hmac.compare_digest(current_password, settings.auth_password or ""):
        raise HTTPException(status_code=400, detail="当前密码错误")
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="新密码长度至少 6 位")
    if hmac.compare_digest(new_password, settings.auth_password or ""):
        raise HTTPException(status_code=400, detail="新密码不能与当前密码相同")

    settings.auth_password = new_password
    settings.save_to_env()
    return {"ok": True, "message": "密码修改成功"}
