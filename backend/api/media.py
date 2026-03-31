"""Media record and manual operation APIs."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, func, or_, text
from sqlalchemy.orm import Session

from .logs import append_log, current_task_id
from ..config import settings
from ..database import get_db
from ..models import DirectoryState, InodeRecord, MediaRecord, ScanTask, SyncGroup
from ..services.group_routing import resolve_movie_target_root
from ..services.linker import get_inode
from ..services.parser import parse_movie_filename, parse_tv_filename
from ..services.renamer import compute_movie_target_path, compute_tv_target_path
from ..services.scanner import (
    DirectoryProcessError,
    TaskCancelledError,
    init_scan_cancel_flag,
    pop_scan_cancel_flag,
    reidentify_by_target_dir,
    run_manual_organize,
    tag_task_type_with_issue,
)

router = APIRouter()
VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".mov", ".webm", ".flv")


class PendingOrganizeRequest(BaseModel):
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    media_type: Literal["tv", "movie"] = "tv"
    season: int | None = None
    episode_offset: int | None = None


class BatchDeleteRequest(BaseModel):
    ids: list[int]
    delete_files: bool = False


class ReidentifyRequest(BaseModel):
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    season: int | None = None
    episode: int | None = None
    episode_offset: int | None = None


class ReidentifyDirRequest(BaseModel):
    target_dir: str
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    season: int | None = None
    episode_offset: int | None = None


class PendingLogReviewRequest(BaseModel):
    source_kind: Literal["pending", "unprocessed", "review"]
    source_original_path: str | None = None
    source_reason: str | None = None
    source_timestamp: str | None = None
    resolution_status: Literal["resolved", "skipped", "false_positive", "needs_followup"]
    reviewer: str | None = None
    note: str | None = None
    tmdb_id: int | None = None
    season: int | None = None
    episode: int | None = None
    extra_category: str | None = None
    suggested_target: str | None = None


@router.get("")
def list_media(
    db: Session = Depends(get_db),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=2000),
    search: str | None = None,
    type: str | None = Query(None, pattern="^(tv|movie)?$"),
    category: str = Query("all"),
):
    q = db.query(MediaRecord)
    if type:
        q = q.filter(MediaRecord.type == type)
    if search:
        q = q.filter(
            or_(
                MediaRecord.original_path.contains(search),
                MediaRecord.target_path.contains(search),
            )
        )

    if category == "pending":
        q = q.filter(MediaRecord.status == "pending_manual")
    elif category == "success":
        q = q.filter(MediaRecord.status.in_(["scraped", "manual_fixed"]))

    total = q.count()
    rows = q.order_by(MediaRecord.updated_at.desc()).offset(offset).limit(limit).all()
    items = [_media_to_dict(x) for x in rows]
    return {"total": total, "items": items, "offset": offset, "limit": limit}


@router.get("/pending")
def list_pending(
    db: Session = Depends(get_db),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    search: str | None = None,
):
    q = db.query(MediaRecord).filter(MediaRecord.status == "pending_manual")
    if search:
        q = q.filter(MediaRecord.original_path.contains(search))

    total = q.count()
    rows = q.order_by(MediaRecord.updated_at.desc()).offset(offset).limit(limit).all()
    items = [_media_to_dict(x) for x in rows]
    return {"total": total, "items": items, "offset": offset, "limit": limit}


@router.get("/pending-logs")
def list_pending_logs(
    kind: Literal["pending", "unprocessed", "review"] = Query("pending"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    search: str | None = None,
):
    file_path = _resolve_pending_log_path(kind)
    items = _load_pending_log_items(file_path, kind)
    keyword = str(search or "").strip().lower()
    if keyword:
        items = [item for item in items if _pending_log_matches(item, keyword)]
    total = len(items)
    sliced = items[offset: offset + limit]
    return {
        "kind": kind,
        "path": str(file_path) if file_path else "",
        "items": sliced,
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.post("/pending-logs/review")
def create_pending_log_review(data: PendingLogReviewRequest):
    review_path = _resolve_pending_log_path("review")
    if review_path is None:
        raise HTTPException(status_code=400, detail="未配置 review.jsonl 路径")
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "entry_type": "manual_review",
        "source_kind": data.source_kind,
        "source_original_path": (data.source_original_path or "").strip() or None,
        "source_reason": (data.source_reason or "").strip() or None,
        "source_timestamp": (data.source_timestamp or "").strip() or None,
        "resolution_status": data.resolution_status,
        "reviewer": (data.reviewer or "").strip() or None,
        "note": (data.note or "").strip() or None,
        "tmdb_id": data.tmdb_id,
        "season": data.season,
        "episode": data.episode,
        "extra_category": (data.extra_category or "").strip() or None,
        "suggested_target": (data.suggested_target or "").strip() or None,
    }
    review_path.parent.mkdir(parents=True, exist_ok=True)
    with review_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return {"ok": True, "message": "人工处理登记已写入 review.jsonl"}


@router.get("/stats")
def stats(db: Session = Depends(get_db)):
    q = (
        db.query(MediaRecord)
        .filter(MediaRecord.status != "pending_manual")
        .filter(_video_only_expr())
        .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
        .filter(_main_feature_expr())
    )

    total_media = q.count()
    tv_count = q.filter(MediaRecord.type == "tv").count()
    movie_count = q.filter(MediaRecord.type == "movie").count()
    pending_manual_count = db.query(MediaRecord).filter(MediaRecord.status == "pending_manual").count()
    total_size = q.with_entities(func.sum(MediaRecord.size)).scalar() or 0

    return {
        "total_media": total_media,
        "tv_count": tv_count,
        "movie_count": movie_count,
        "pending_manual_count": pending_manual_count,
        "total_size": int(total_size),
    }


@router.get("/by-target-dir")
def list_media_by_target_dir(
    target_dir: str = Query(..., min_length=1),
    limit: int = Query(200, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    target_dir = target_dir.strip()
    if not target_dir:
        raise HTTPException(status_code=400, detail="target_dir 不能为空")

    normalized = target_dir.replace("\\", "/").rstrip("/")
    q = (
        db.query(MediaRecord)
        .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
        .filter(_video_only_expr())
        .filter(_target_dir_like_expr(normalized))
        .order_by(MediaRecord.updated_at.desc())
        .limit(limit)
    )
    items = [_media_to_dict(x) for x in q.all()]
    return {"items": items, "target_dir": target_dir, "total": len(items)}


def _video_only_expr():
    lower_path = func.lower(func.coalesce(MediaRecord.original_path, ""))
    return or_(*[lower_path.like(f"%{ext}") for ext in VIDEO_EXTS])


def _main_feature_expr():
    lower_target = func.lower(func.coalesce(MediaRecord.target_path, ""))
    return and_(
        ~lower_target.like("%/season 00/%"),
        ~lower_target.like("%\\season 00\\%"),
        ~lower_target.like("%/specials/%"),
        ~lower_target.like("%\\specials\\%"),
        ~lower_target.like("%/extras/%"),
        ~lower_target.like("%\\extras\\%"),
        ~lower_target.like("%/trailers/%"),
        ~lower_target.like("%\\trailers\\%"),
        ~lower_target.like("%/interviews/%"),
        ~lower_target.like("%\\interviews\\%"),
    )


def _target_dir_like_expr(target_dir_norm: str):
    lower_target_path = func.lower(func.replace(func.coalesce(MediaRecord.target_path, ""), "\\", "/"))
    base = target_dir_norm.lower().rstrip("/")
    return or_(lower_target_path == base, lower_target_path.like(f"{base}/%"))


@router.get("/search")
def search_tmdb(
    q: str = Query(..., min_length=1),
    media_type: str = Query("tv", pattern="^(tv|movie)$"),
    limit: int = Query(20, ge=1, le=50),
):
    if not settings.tmdb_api_key:
        raise HTTPException(status_code=400, detail="未配置 TMDB API Key")

    keyword = q.strip()
    if keyword.isdigit():
        item = _tmdb_get_by_id(media_type, int(keyword))
        if not item:
            return {"items": []}
        return {"items": [_format_tmdb_item(media_type, item)]}

    endpoint = "tv" if media_type == "tv" else "movie"
    url = f"https://api.themoviedb.org/3/search/{endpoint}"
    params = {"api_key": settings.tmdb_api_key, "query": keyword, "language": "zh-CN"}

    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(url, params=params)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TMDB 搜索请求失败: {e}")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"TMDB 搜索失败: {resp.status_code}")

    payload = resp.json()
    results = payload.get("results", []) if isinstance(payload, dict) else []
    out = [_format_tmdb_item(media_type, x) for x in results[:limit]]
    return {"items": out}


@router.get("/season-poster")
def get_season_poster(
    tmdb_id: int = Query(..., ge=1),
    season: int = Query(..., ge=0),
):
    if not settings.tmdb_api_key:
        raise HTTPException(status_code=400, detail="未配置 TMDB API Key")

    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season}"
    params = {"api_key": settings.tmdb_api_key, "language": "zh-CN"}
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(url, params=params)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TMDB 季封面请求失败: {e}")
    if resp.status_code == 404:
        return {"poster_path": None}
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"TMDB 季封面获取失败: {resp.status_code}")

    data = resp.json() if resp.content else {}
    if not isinstance(data, dict):
        return {"poster_path": None}
    return {"poster_path": data.get("poster_path")}


@router.post("/{media_id}/reidentify")
def reidentify(media_id: int, data: ReidentifyRequest, db: Session = Depends(get_db)):
    row = db.query(MediaRecord).filter(MediaRecord.id == media_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="记录不存在")
    if row.status == "pending_manual" and Path(row.original_path).is_dir():
        raise HTTPException(status_code=400, detail="目录待办请使用手动整理")

    src = Path(row.original_path)
    if not src.exists() or not src.is_file():
        raise HTTPException(status_code=400, detail="源文件不存在")

    group = db.query(SyncGroup).filter(SyncGroup.id == row.sync_group_id).first()
    if not group:
        raise HTTPException(status_code=400, detail="同步组不存在")

    if row.type == "tv":
        pr = parse_tv_filename(str(src)) or parse_movie_filename(str(src))
    else:
        pr = parse_movie_filename(str(src)) or parse_tv_filename(str(src))
    if not pr:
        raise HTTPException(status_code=400, detail="文件名解析失败")

    title = (data.title or "").strip()
    if title:
        pr = pr._replace(title=title)
    if data.year is not None:
        pr = pr._replace(year=data.year)
    if data.season is not None:
        pr = pr._replace(season=max(0, data.season))
    if data.episode is not None:
        pr = pr._replace(episode=max(0, data.episode))
    if data.episode_offset is not None and row.type == "tv" and pr.episode is not None and pr.extra_category is None:
        adjusted = pr.episode - int(data.episode_offset)
        pr = pr._replace(episode=max(1, adjusted))

    target_root = Path(group.target)
    media_type = row.type
    if media_type == "movie":
        movie_root, reason = resolve_movie_target_root(db, group)
        if movie_root is None:
            raise HTTPException(status_code=400, detail=f"电影目标路径不可决策: {reason}")
        target_root = movie_root

    ext = src.suffix.lower()
    dst = (
        compute_tv_target_path(target_root, pr, data.tmdb_id, ext, src_filename=src.name)
        if media_type == "tv"
        else compute_movie_target_path(target_root, pr, data.tmdb_id, ext)
    )
    _link_or_fail(src, dst)

    row.tmdb_id = data.tmdb_id
    row.target_path = str(dst)
    row.status = "manual_fixed"
    row.updated_at = datetime.now(timezone.utc)

    ino = get_inode(src)
    if ino:
        inode_row = db.query(InodeRecord).filter(InodeRecord.inode == ino).first()
        if inode_row:
            inode_row.target_path = str(dst)
            inode_row.sync_group_id = group.id

    db.commit()
    return {"message": "修正成功", "new_path": str(dst), "tmdb_id": data.tmdb_id}


@router.post("/{media_id}/manual-organize")
def manual_organize(media_id: int, data: PendingOrganizeRequest, db: Session = Depends(get_db)):
    pending = db.query(MediaRecord).filter(MediaRecord.id == media_id).first()
    if not pending:
        raise HTTPException(status_code=404, detail="待办记录不存在")
    if pending.status != "pending_manual":
        raise HTTPException(status_code=400, detail="该记录不是待办状态")

    root_dir = Path(pending.original_path)
    if not root_dir.exists() or not root_dir.is_dir():
        raise HTTPException(status_code=400, detail="待办目录不存在或不是目录")

    group = db.query(SyncGroup).filter(SyncGroup.id == pending.sync_group_id).first()
    if not group:
        raise HTTPException(status_code=400, detail="同步组不存在")

    task = ScanTask(
        type=f"manual:{group.name}",
        target_id=group.id,
        target_name=root_dir.name,
        status="running",
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    init_scan_cancel_flag(task.id)
    token = current_task_id.set(task.id)

    try:
        append_log(
            f"手动整理任务启动: media_id={media_id}, group={group.name}, "
            f"dir={root_dir}, media_type={data.media_type}, tmdb_id={data.tmdb_id}"
        )
        processed, failed, has_issues = run_manual_organize(
            db=db,
            pending=pending,
            group=group,
            media_type=data.media_type,
            tmdb_id=data.tmdb_id,
            title_override=data.title,
            year_override=data.year,
            season_override=data.season,
            episode_offset=data.episode_offset,
        )
        task.status = "completed" if failed == 0 else "failed"
        if has_issues:
            task.type = tag_task_type_with_issue(task.type)
            append_log("手动整理完成但存在问题项（Special 冲突已跳过）")
        task.finished_at = datetime.now(timezone.utc)
        append_log(f"手动整理完成: 成功 {processed}，失败 {failed}")
        db.commit()

        return {
            "message": f"手动整理完成: 成功 {processed}，失败 {failed}",
            "processed": processed,
            "failed": failed,
        }
    except TaskCancelledError as e:
        task.status = "cancelled"
        task.finished_at = datetime.now(timezone.utc)
        append_log(f"手动整理已取消: {e}")
        db.commit()
        raise HTTPException(status_code=409, detail=str(e))
    except DirectoryProcessError as e:
        task.status = "failed"
        task.finished_at = datetime.now(timezone.utc)
        append_log(f"手动整理失败: {e}")
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        pop_scan_cancel_flag(task.id)
        task.log_file = f"task_{task.id}.log"
        try:
            db.commit()
        except Exception:
            db.rollback()
        current_task_id.reset(token)


@router.post("/reidentify-by-target-dir")
def reidentify_by_target_dir_api(data: ReidentifyDirRequest, db: Session = Depends(get_db)):
    target_dir = (data.target_dir or "").strip()
    if not target_dir:
        raise HTTPException(status_code=400, detail="target_dir 不能为空")

    normalized = target_dir.replace("\\", "/").rstrip("/")
    lower_target_path = func.lower(func.replace(func.coalesce(MediaRecord.target_path, ""), "\\", "/"))
    rows = (
        db.query(MediaRecord)
        .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
        .filter(or_(lower_target_path == normalized.lower(), lower_target_path.like(f"{normalized.lower()}/%")))
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail="未找到目标目录记录")

    group_id = rows[0].sync_group_id
    if not group_id:
        raise HTTPException(status_code=400, detail="记录缺少同步组信息")

    if any(r.sync_group_id != group_id for r in rows):
        raise HTTPException(status_code=400, detail="目标目录包含多个同步组记录")

    media_type = rows[0].type
    if any(r.type != media_type for r in rows):
        raise HTTPException(status_code=400, detail="目标目录包含多种媒体类型")

    group = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
    if not group:
        raise HTTPException(status_code=400, detail="同步组不存在")

    processed, failed, has_issues = reidentify_by_target_dir(
        db=db,
        group=group,
        media_type=media_type,
        tmdb_id=data.tmdb_id,
        title_override=data.title,
        year_override=data.year,
        season_override=data.season,
        episode_offset=data.episode_offset,
        records=rows,
    )
    msg = f"整组修正完成: 成功 {processed}，失败 {failed}"
    if has_issues:
        msg += "（存在问题项）"
    return {"message": msg, "processed": processed, "failed": failed}


@router.post("/batch-delete")
def batch_delete(data: BatchDeleteRequest, db: Session = Depends(get_db)):
    ids = [int(x) for x in data.ids if isinstance(x, int) or str(x).isdigit()]
    if not ids:
        raise HTTPException(status_code=400, detail="ids 不能为空")

    rows = db.query(MediaRecord).filter(MediaRecord.id.in_(ids)).all()
    deleted_files = 0

    for row in rows:
        if data.delete_files and row.target_path:
            p = Path(row.target_path)
            try:
                if p.exists() and p.is_file():
                    p.unlink()
                    deleted_files += 1
            except OSError:
                pass
        db.delete(row)

    db.commit()
    return {"deleted_records": len(rows), "deleted_files": deleted_files}


@router.delete("/all")
def delete_all_media(db: Session = Depends(get_db)):
    count = db.query(MediaRecord).delete()
    dir_state_count = db.query(DirectoryState).delete()
    db.commit()

    # 重置时同时清空所有 pending / review JSONL 文件
    _jsonl_paths = [
        getattr(settings, "pending_jsonl_path", None),
        getattr(settings, "unprocessed_items_jsonl_path", None),
        getattr(settings, "review_jsonl_path", None),
    ]
    cleared: list[str] = []
    for raw_path in _jsonl_paths:
        if not raw_path:
            continue
        try:
            p = Path(str(raw_path))
            if p.exists():
                p.write_text("", encoding="utf-8")
                cleared.append(p.name)
        except OSError:
            pass

    return {
        "message": f"已删除 {count} 条媒体记录，清理 {dir_state_count} 条目录状态",
        "cleared_logs": cleared,
    }


@router.post("/deduplicate")
def deduplicate_media(db: Session = Depends(get_db)):
    # keep newest row per (sync_group_id, original_path)
    sql = text(
        """
        DELETE FROM media_records
        WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY COALESCE(sync_group_id, -1), original_path
                           ORDER BY updated_at DESC, id DESC
                       ) AS rn
                FROM media_records
            ) t
            WHERE rn > 1
        )
        """
    )
    result = db.execute(sql)
    db.commit()
    deleted = result.rowcount or 0
    return {"message": f"去重完成，删除 {deleted} 条重复记录", "deleted": deleted}


def _tmdb_get_by_id(media_type: str, tmdb_id: int) -> dict | None:
    if not settings.tmdb_api_key:
        return None
    endpoint = "tv" if media_type == "tv" else "movie"
    url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
    params = {"api_key": settings.tmdb_api_key, "language": "zh-CN"}
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(url, params=params)
    except Exception:
        return None
    if resp.status_code != 200:
        return None
    data = resp.json()
    return data if isinstance(data, dict) else None


def _format_tmdb_item(media_type: str, item: dict) -> dict:
    title = item.get("name") if media_type == "tv" else item.get("title")
    date_value = item.get("first_air_date") if media_type == "tv" else item.get("release_date")
    year = None
    if str(date_value or "")[:4].isdigit():
        year = int(str(date_value)[:4])
    return {
        "tmdb_id": item.get("id"),
        "title": title or "",
        "year": year,
        "overview": item.get("overview") or "",
        "poster_path": item.get("poster_path"),
        "media_type": media_type,
        "popularity": item.get("popularity") or 0,
        "vote_count": item.get("vote_count") or 0,
    }


def _link_or_fail(src: Path, dst: Path) -> None:
    if not src.exists():
        raise RuntimeError("source not found")
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        if get_inode(src) == get_inode(dst):
            return
        raise RuntimeError("target exists with different inode")
    import os

    os.link(str(src), str(dst))


def _resolve_pending_log_path(kind: str) -> Path | None:
    if kind == "pending":
        value = str(getattr(settings, "pending_jsonl_path", "") or "").strip()
    elif kind == "review":
        value = str(getattr(settings, "review_jsonl_path", "") or "").strip()
    else:
        value = str(getattr(settings, "unprocessed_items_jsonl_path", "") or "").strip()
        if not value:
            value = str(getattr(settings, "unhandled_jsonl_path", "") or "").strip()
    return Path(value) if value else None


def _load_pending_log_items(file_path: Path | None, kind: str) -> list[dict]:
    if file_path is None or not file_path.exists() or not file_path.is_file():
        return []
    items: list[dict] = []
    try:
        lines = file_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    for index, raw in enumerate(reversed(lines), start=1):
        text = raw.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = {"raw": text}
        if not isinstance(payload, dict):
            payload = {"raw": text}
        payload["kind"] = kind
        payload["line_no"] = len(lines) - index + 1
        items.append(payload)
    return items


def _pending_log_matches(item: dict, keyword: str) -> bool:
    for key, value in item.items():
        if key in {"line_no", "kind"}:
            continue
        if value is None:
            continue
        if keyword in str(value).lower():
            return True
    return False

def _media_to_dict(row: MediaRecord) -> dict:
    is_dir_pending = False
    if row.status == "pending_manual" and row.original_path:
        try:
            is_dir_pending = Path(row.original_path).is_dir()
        except Exception:
            is_dir_pending = False

    return {
        "id": row.id,
        "sync_group_id": row.sync_group_id,
        "original_path": row.original_path,
        "target_path": row.target_path,
        "type": row.type,
        "tmdb_id": row.tmdb_id,
        "bangumi_id": row.bangumi_id,
        "status": row.status,
        "size": row.size,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "is_directory_pending": is_dir_pending,
    }
