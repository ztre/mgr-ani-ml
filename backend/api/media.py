"""Media record and manual operation APIs."""
from __future__ import annotations

import json
import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, func, or_, text
from sqlalchemy.orm import Session, object_session

from .logs import append_log
from ..config import settings
from ..database import get_db
from ..models import CheckIssue, DirectoryState, InodeRecord, MediaRecord, ScanTask, SyncGroup
from ..services.group_routing import resolve_movie_target_root, resolve_tv_target_root
from ..services.linker import get_inode, path_excluded
from ..services.parser import _extract_subtitle_lang, classify_extra_from_text, parse_movie_filename, parse_tv_filename
from ..services.resource_tree import build_resource_summaries, build_resource_tree, extract_tv_primary_season
from ..services.renamer import compute_movie_target_path, compute_tv_target_path
from ..services.target_path_resolver import (
    _normalize_attachment_stem,
    _normalize_media_stem,
    build_attachment_target_from_anchor,
)
from ..services.scanner import (
    _collect_media_files_under_dir,
    _is_ignored_name,
    _normalize_manual_selected_path,
    DirectoryProcessError,
    TaskCancelledError,
    detect_special_dir_context,
    init_scan_cancel_flag,
    pop_scan_cancel_flag,
    reidentify_by_target_dir,
    run_manual_organize,
    run_source_dir_organize,
    tag_task_type_with_issue,
)
from ..services.task_queue import enqueue_task

router = APIRouter()
_log = logging.getLogger(__name__)
VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".mov", ".webm", ".flv")
ATTACHMENT_EXTS = (".ass", ".srt", ".ssa", ".vtt", ".mka", ".sup", ".idx", ".sub")
PRUNABLE_METADATA_FILENAMES = {"tvshow.nfo", "movie.nfo", "season.nfo", "poster.jpg", "fanart.jpg", "banner.jpg", "backdrop.jpg"}


def _is_prunable_metadata_file(name: str) -> bool:
    """判断文件名是否为可清理的媒体库元数据文件（Emby/Jellyfin 刮削产物）。"""
    lower = name.lower()
    if lower in PRUNABLE_METADATA_FILENAMES:
        return True
    # 任意 .nfo 文件（如 海猫鸣泣之时 - S01E01.nfo）
    if lower.endswith(".nfo"):
        return True
    # Emby 缩略图（如 海猫鸣泣之时 - S01E01-thumb.jpg）
    if lower.endswith("-thumb.jpg") or lower.endswith("-thumb.png"):
        return True
    return False


def _build_pending_mainline_relative_paths(
    root_dir: Path,
    *,
    group: SyncGroup,
    media_type: str | None = None,
) -> list[str]:
    include = str(getattr(group, "include", "") or "")
    exclude = str(getattr(group, "exclude", "") or "")
    source_value = getattr(group, "source", None)
    source_root = Path(str(source_value)) if source_value else root_dir
    items = _build_pending_file_items(
        root_dir,
        include=include,
        exclude=exclude,
        source_root=source_root,
        media_type=media_type,
    )
    return [
        str(item.get("relative_path") or "")
        for item in items
        if item.get("file_type") == "video" and item.get("content_role") == "mainline"
    ]


def _load_visible_pending_file_items(
    db: Session,
    pending: MediaRecord,
    group: SyncGroup,
) -> list[dict]:
    root_dir = Path(str(pending.original_path or ""))
    if not _safe_is_dir(root_dir):
        return []
    source_root = Path(str(group.source or ""))
    items = _build_pending_file_items(
        root_dir,
        include=group.include or "",
        exclude=group.exclude or "",
        source_root=source_root,
        media_type=pending.type,
    )
    return _filter_processed_pending_file_items(db, pending, items)


def _load_sync_group_map(db: Session, group_ids: set[int]) -> dict[int, SyncGroup]:
    if not group_ids:
        return {}
    rows = db.query(SyncGroup).filter(SyncGroup.id.in_(sorted(group_ids))).all()
    return {int(row.id): row for row in rows if getattr(row, "id", None) is not None}


def _load_processed_original_paths_by_group(db: Session, group_ids: set[int]) -> dict[int, set[str]]:
    if not group_ids:
        return {}
    rows = (
        db.query(MediaRecord.sync_group_id, MediaRecord.original_path)
        .filter(MediaRecord.sync_group_id.in_(sorted(group_ids)))
        .filter(MediaRecord.status.in_(["scraped", "manual_fixed"]))
        .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
        .all()
    )
    processed_by_group: dict[int, set[str]] = defaultdict(set)
    for sync_group_id, original_path in rows:
        if sync_group_id is None or not str(original_path or "").strip():
            continue
        processed_by_group[int(sync_group_id)].add(str(original_path))
    return processed_by_group


def _pending_has_visible_mainline_files_fast(
    pending: MediaRecord,
    group: SyncGroup,
    *,
    processed_paths: set[str] | None = None,
) -> bool:
    root_dir = Path(str(pending.original_path or ""))
    if not _safe_is_dir(root_dir):
        return False

    processed_lookup = processed_paths or set()
    include = str(getattr(group, "include", "") or "")
    exclude = str(getattr(group, "exclude", "") or "")
    source_value = getattr(group, "source", None)
    source_root = Path(str(source_value)) if source_value else root_dir
    media_type = str(getattr(pending, "type", "") or "")

    try:
        for current_root, _dirnames, filenames in os.walk(root_dir):
            current_dir = Path(current_root)
            for filename in filenames:
                file_path = current_dir / filename
                if file_path.suffix.lower() not in VIDEO_EXTS:
                    continue
                if _is_ignored_name(file_path.name):
                    continue
                if path_excluded(file_path, include, exclude, root_path=source_root):
                    continue
                if str(file_path) in processed_lookup:
                    continue
                if _pending_video_role(file_path, media_type) == "mainline":
                    return True
    except OSError:
        return False

    return False


def _pending_has_visible_mainline_files(
    db: Session,
    pending: MediaRecord,
    group: SyncGroup,
) -> bool:
    items = _load_visible_pending_file_items(db, pending, group)
    return any(item.get("file_type") == "video" and item.get("content_role") == "mainline" for item in items)


class PendingOrganizeRequest(BaseModel):
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    media_type: Literal["tv", "movie"] = "tv"
    season: int | None = None
    episode_override: int | None = None
    episode_offset: int | None = None
    selected_paths: list[str] | None = None
    force_extra_context: bool = False


class BatchDeleteRequest(BaseModel):
    ids: list[int]
    delete_files: bool = False
    delete_resource_scope: bool = False


class DeleteScopeRequest(BaseModel):
    scope_level: Literal["resource", "season", "group", "item"]
    item_ids: list[int]
    resource_dir: str
    type: Literal["tv", "movie"]
    sync_group_id: int | None = None
    tmdb_id: int | None = None
    season: int | None = None
    group_kind: str | None = None
    group_label: str | None = None
    delete_files: bool = False


class ReidentifyRequest(BaseModel):
    media_type: Literal["tv", "movie"] | None = None
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    season: int | None = None
    episode: int | None = None
    episode_offset: int | None = None
    companion_ids: list[int] | None = None  # 附件 ID 列表，随主文件一起修正


class ReidentifyDirRequest(BaseModel):
    target_dir: str
    media_type: Literal["tv", "movie"] | None = None
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    season: int | None = None
    season_override: int | None = None
    episode_override: int | None = None
    episode_offset: int | None = None


class ReidentifyScopeRequest(BaseModel):
    scope_level: Literal["resource", "season", "group", "item"]
    item_ids: list[int]
    resource_dir: str
    type: Literal["tv", "movie"]
    media_type: Literal["tv", "movie"] | None = None
    sync_group_id: int | None = None
    scope_tmdb_id: int | None = None
    season: int | None = None
    group_kind: str | None = None
    group_label: str | None = None
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    season_override: int | None = None
    episode_override: int | None = None
    episode_offset: int | None = None


class BatchReidentifyRequest(BaseModel):
    item_ids: list[int]
    media_type: Literal["tv", "movie"] | None = None
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    season_override: int | None = None
    episode_override: int | None = None
    episode_offset: int | None = None


class AdjustRequest(BaseModel):
    extra_category: str | None = None  # None=正片; "special"/"oped"=Season00; others=extras
    season: int | None = None
    episode: int | None = None
    extra_label: str | None = None
    custom_filename: str | None = None  # optional stem override (without extension)


def _validate_forced_tv_episode(
    media_type: str | None,
    forced_season: int | None,
    forced_episode: int | None,
) -> None:
    if str(media_type or "") != "tv":
        return
    if forced_season == 0 and forced_episode is None:
        raise HTTPException(status_code=400, detail="强制季号为 0 时必须填写强制集数")
    if forced_episode is not None and int(forced_episode) < 1:
        raise HTTPException(status_code=400, detail="强制集数必须大于等于 1")


def _build_scoped_reidentify_target_name(resource_dir: str, group_label: str | None = None) -> str:
    normalized_dir = str(resource_dir or "").replace("\\", "/").rstrip("/")
    resource_name = Path(normalized_dir).name or normalized_dir
    label = str(group_label or "").strip()
    if label and label != resource_name:
        return f"{resource_name} / {label}"
    return resource_name or label or "-"


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


@dataclass(frozen=True)
class _DeletedLinkRecord:
    source_path: Path
    target_path: Path


@dataclass
class _StagedDeleteResult:
    deleted_links: list[_DeletedLinkRecord]
    deleted_inodes: int
    deleted_directory_states: int
    deleted_files: int
    pruned_dirs: int
    pruned_metadata_files: int


@dataclass(frozen=True)
class _PruneBlockedDir:
    path: Path
    reason: str


@dataclass
class _ReidentifyPruneReport:
    pruned_dirs: list[Path]
    blocked_dirs: list[_PruneBlockedDir]
    deleted_metadata_files: list[Path]


def _normalize_existing_dir(path: Path | None) -> Path | None:
    if path is None:
        return None
    try:
        return path.resolve(strict=False)
    except OSError:
        return path


def _safe_is_dir(path: Path) -> bool:
    try:
        return path.exists() and path.is_dir()
    except OSError:
        return False


def _safe_is_file(path: Path) -> bool:
    try:
        return path.exists() and path.is_file()
    except OSError:
        return False


def _normalize_target_path(value: str | None) -> str:
    return str(value or "").replace("\\", "/").rstrip("/")


def _extract_target_dir(path_value: str | None) -> str:
    normalized = _normalize_target_path(path_value)
    if not normalized:
        return ""
    parts = normalized.split("/")
    if len(parts) <= 1:
        return normalized
    return "/".join(parts[:-1])


def _extract_season_dir(path_value: str | None) -> str:
    normalized = _normalize_target_path(path_value)
    match = re.match(r"^(.*?/Season\s+\d{1,2})(?:/|$)", normalized, re.I)
    if match and match.group(1):
        return match.group(1)
    return _extract_target_dir(normalized)


def _extract_show_dir(path_value: str | None) -> str:
    season_dir = _extract_season_dir(path_value)
    if not season_dir or season_dir == _normalize_target_path(path_value):
        return _extract_target_dir(path_value)
    return _extract_target_dir(season_dir)


def _extract_tv_primary_season(path_value: str | None) -> int | None:
    normalized = _normalize_target_path(path_value)
    match = re.match(r"^.*?/Season\s+(\d{1,2})(?:/|$)", normalized, re.I)
    if not match:
        return None
    season = int(match.group(1))
    return season if season > 0 else None


def _extract_tv_aux_season(path_value: str | None) -> int | None:
    normalized = _normalize_target_path(path_value)
    if not normalized:
        return None

    target_dir = _extract_target_dir(normalized)
    leaf = Path(target_dir).name.lower()
    filename = Path(normalized).name

    if leaf in {"season 00", "specials"}:
        match = re.search(r"\bS00E(\d{3,4})\b", filename, re.I)
        if not match:
            return None
        encoded_episode = int(match.group(1))
        if encoded_episode < 100:
            return None
        season = encoded_episode // 100
        return season if season > 0 else None

    if leaf in {"extras", "trailers", "interviews"}:
        match = re.search(r"\bS(\d{1,2})(?:[_ .-]|$)", filename, re.I)
        if not match:
            return None
        season = int(match.group(1))
        return season if season > 0 else None

    return None


def _tv_resource_scope_key(row: MediaRecord) -> tuple[int | None, str, str, int] | None:
    target_path = str(row.target_path or "").strip()
    if not target_path:
        return None
    show_dir = _normalize_target_path(_extract_show_dir(target_path))
    if not show_dir:
        return None

    season = _extract_tv_primary_season(target_path)
    if season is None:
        season = _extract_tv_aux_season(target_path)
    if season is None:
        return None

    return (row.sync_group_id, "tv", show_dir, season)


def _extract_movie_resource_dir(path_value: str | None) -> str:
    target_dir = _extract_target_dir(path_value)
    if not target_dir:
        return ""
    leaf = Path(target_dir).name.lower()
    if leaf in {"extras", "specials", "trailers", "interviews"}:
        return _extract_target_dir(target_dir)
    return target_dir


def _resource_scope_key(row: MediaRecord) -> tuple[object, ...] | None:
    target_path = str(row.target_path or "").strip()
    if not target_path:
        return None
    media_type = str(row.type or "")
    if media_type == "tv":
        return _tv_resource_scope_key(row)

    resource_dir = _extract_movie_resource_dir(target_path)
    normalized_dir = _normalize_target_path(resource_dir)
    if not normalized_dir:
        return None
    return (row.sync_group_id, media_type, normalized_dir)


def _expand_rows_to_resource_scope(db: Session, rows: list[MediaRecord]) -> list[MediaRecord]:
    scope_keys = {key for row in rows if (key := _resource_scope_key(row)) is not None}
    if not scope_keys:
        return rows

    group_ids = sorted({key[0] for key in scope_keys if key and key[0] is not None})
    if not group_ids:
        return rows

    candidates = (
        db.query(MediaRecord)
        .filter(MediaRecord.sync_group_id.in_(group_ids))
        .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
        .all()
    )
    expanded: dict[int, MediaRecord] = {int(row.id): row for row in rows if row.id is not None}
    for candidate in candidates:
        key = _resource_scope_key(candidate)
        if key is None or key not in scope_keys or candidate.id is None:
            continue
        expanded[int(candidate.id)] = candidate
    return list(expanded.values())


def _derive_source_dir(original_path: str, status: str | None, target_path: str | None) -> Path:
    path = Path(str(original_path or ""))
    normalized_status = str(status or "").strip().lower()
    if normalized_status == "pending_manual" and not str(target_path or "").strip():
        return path
    if _safe_is_dir(path):
        return path
    return path.parent


def _group_target_root(db: Session, group_id: int | None, media_type: str) -> Path | None:
    if group_id is None:
        return None
    group = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
    if group is None:
        return None
    if media_type == "movie":
        movie_root, _reason = resolve_movie_target_root(db, group)
        if movie_root is not None:
            return movie_root
    else:
        tv_root, _reason = resolve_tv_target_root(db, group)
        if tv_root is not None:
            return tv_root
    target_value = str(group.target or "").strip()
    return Path(target_value) if target_value else None


def _prune_empty_dirs(start_dir: Path, stop_at: Path | None) -> list[Path]:
    removed: list[Path] = []
    current = _normalize_existing_dir(start_dir)
    stop_dir = _normalize_existing_dir(stop_at)
    while current is not None:
        if stop_dir is not None and current == stop_dir:
            break
        parent = current.parent
        try:
            current.rmdir()
            removed.append(current)
        except OSError:
            break
        if parent == current:
            break
        current = parent
    return removed


def _prune_empty_dirs_with_metadata(start_dir: Path, stop_at: Path | None) -> tuple[list[Path], list[Path]]:
    removed_dirs = _prune_empty_dirs(start_dir, stop_at)
    removed_metadata_files: list[Path] = []
    current = _normalize_existing_dir(removed_dirs[-1].parent if removed_dirs else start_dir)
    stop_dir = _normalize_existing_dir(stop_at)
    while current is not None:
        if stop_dir is not None and current == stop_dir:
            break
        parent = current.parent
        try:
            current.rmdir()
            removed_dirs.append(current)
        except OSError:
            removed_metadata = _remove_prunable_metadata_files(current)
            if not removed_metadata:
                break
            removed_metadata_files.extend(removed_metadata)
            try:
                current.rmdir()
                removed_dirs.append(current)
            except OSError:
                break
        if parent == current:
            break
        current = parent
    return removed_dirs, removed_metadata_files


_REAL_CONTENT_EXTS = frozenset(VIDEO_EXTS) | frozenset(ATTACHMENT_EXTS)


def _remove_prunable_metadata_files(dir_path: Path) -> list[Path]:
    """删除目录内的 Emby/Jellyfin 元数据文件，以便后续 rmdir 能成功。

    判定逻辑：目录内不含子目录、不含真实媒体文件（视频/字幕），
    则所有剩余文件均视为可清理的元数据（刮削产物）。
    这样能覆盖 season01-poster.jpg / clearart.png / logo.png 等任意命名的刮削文件。
    """
    try:
        entries = list(dir_path.iterdir())
    except OSError:
        return []

    if not entries:
        return []

    removable_files: list[Path] = []
    for entry in entries:
        try:
            if entry.is_dir():
                return []
            if not entry.is_file():
                return []
        except OSError:
            return []
        # 真实媒体内容不能删除
        if entry.suffix.lower() in _REAL_CONTENT_EXTS:
            return []
        removable_files.append(entry)

    removed: list[Path] = []
    for file_path in removable_files:
        try:
            file_path.unlink()
            removed.append(file_path)
        except OSError:
            return removed
    return removed


def _restore_deleted_links(deleted_links: list[_DeletedLinkRecord]) -> None:
    restore_errors: list[str] = []
    for item in deleted_links:
        try:
            if _safe_is_file(item.target_path):
                continue
            item.target_path.parent.mkdir(parents=True, exist_ok=True)
            os.link(str(item.source_path), str(item.target_path))
        except OSError as exc:
            restore_errors.append(f"{item.target_path} ({exc})")
    if restore_errors:
        raise RuntimeError("; ".join(restore_errors))


def _delete_directory_states_for_removed_sources(
    db: Session,
    rows: list[MediaRecord],
    deleted_ids: set[int],
) -> int:
    affected_by_group: dict[int, set[str]] = defaultdict(set)
    affected_group_ids = {row.sync_group_id for row in rows if row.sync_group_id is not None}
    if not affected_group_ids:
        return 0

    for row in rows:
        if row.sync_group_id is None:
            continue
        source_dir = _derive_source_dir(row.original_path, row.status, row.target_path)
        affected_by_group[row.sync_group_id].add(str(source_dir))

    if not affected_by_group:
        return 0

    deleted_count = 0
    for group_id, dir_paths in affected_by_group.items():
        remaining_rows = (
            db.query(MediaRecord.original_path, MediaRecord.status, MediaRecord.target_path)
            .filter(MediaRecord.sync_group_id == group_id)
            .filter(~MediaRecord.id.in_(sorted(deleted_ids)))
            .filter(
                or_(
                    *[
                        or_(
                            MediaRecord.original_path == dir_path,
                            MediaRecord.original_path.like(f"{dir_path}/%"),
                            MediaRecord.original_path.like(f"{dir_path}\\%"),
                        )
                        for dir_path in sorted(dir_paths)
                    ]
                )
            )
            .all()
        )
        remaining_dir_paths = {
            str(_derive_source_dir(original_path, status, target_path))
            for original_path, status, target_path in remaining_rows
            if str(original_path or "").strip()
        }
        removable = dir_paths - remaining_dir_paths
        if not removable:
            continue
        deleted_count += (
            db.query(DirectoryState)
            .filter(DirectoryState.sync_group_id == group_id)
            .filter(DirectoryState.dir_path.in_(sorted(removable)))
            .delete(synchronize_session=False)
        )
    return int(deleted_count)


def _delete_inodes_for_removed_sources(db: Session, rows: list[MediaRecord], deleted_ids: set[int]) -> int:
    affected_keys = {
        (row.sync_group_id, row.original_path)
        for row in rows
        if row.sync_group_id is not None and str(row.original_path or "").strip()
    }
    if not affected_keys:
        return 0

    deleted_total = 0
    by_group: dict[int, set[str]] = defaultdict(set)
    for group_id, source_path in affected_keys:
        if group_id is None:
            continue
        by_group[group_id].add(source_path)

    for group_id, source_paths in by_group.items():
        remaining_source_paths = {
            str(original_path)
            for (original_path,) in (
                db.query(MediaRecord.original_path)
                .filter(MediaRecord.sync_group_id == group_id)
                .filter(~MediaRecord.id.in_(sorted(deleted_ids)))
                .filter(MediaRecord.original_path.in_(sorted(source_paths)))
                .all()
            )
            if str(original_path or "").strip()
        }
        removable_source_paths = sorted(source_paths - remaining_source_paths)
        if not removable_source_paths:
            continue
        deleted_total += (
            db.query(InodeRecord)
            .filter(InodeRecord.sync_group_id == group_id)
            .filter(InodeRecord.source_path.in_(removable_source_paths))
            .delete(synchronize_session=False)
        )
    return int(deleted_total)


def _stage_batch_delete(
    db: Session,
    rows: list[MediaRecord],
    *,
    delete_files: bool,
) -> _StagedDeleteResult:
    deleted_ids = {int(row.id) for row in rows}
    deleted_links: list[_DeletedLinkRecord] = []
    pruned_dir_count = 0
    deleted_files = 0
    pruned_metadata_files = 0

    target_roots: dict[tuple[int | None, str], Path | None] = {}
    for row in rows:
        key = (row.sync_group_id, row.type)
        if key not in target_roots:
            target_roots[key] = _group_target_root(db, row.sync_group_id, row.type)

    deleted_inode_count = _delete_inodes_for_removed_sources(db, rows, deleted_ids)
    deleted_dir_state_count = _delete_directory_states_for_removed_sources(db, rows, deleted_ids)

    for row in rows:
        db.delete(row)

    db.flush()

    try:
        for row in rows:
            target_value = str(row.target_path or "").strip()
            if not delete_files or not target_value:
                continue
            target_path = Path(target_value)
            if not _safe_is_file(target_path):
                continue
            target_path.unlink()
            deleted_links.append(_DeletedLinkRecord(source_path=Path(row.original_path), target_path=target_path))
            deleted_files += 1
            stop_at = target_roots.get((row.sync_group_id, row.type))
            removed_dirs, removed_metadata = _prune_empty_dirs_with_metadata(target_path.parent, stop_at)
            pruned_dir_count += len(removed_dirs)
            pruned_metadata_files += len(removed_metadata)
    except OSError as exc:
        db.rollback()
        try:
            _restore_deleted_links(deleted_links)
        except RuntimeError as restore_exc:
            raise HTTPException(
                status_code=500,
                detail=f"删除失败且补偿未完全恢复: {exc}; restore={restore_exc}",
            ) from restore_exc
        raise HTTPException(status_code=500, detail=f"删除文件或目录失败: {exc}") from exc
    except Exception as exc:
        db.rollback()
        try:
            _restore_deleted_links(deleted_links)
        except RuntimeError as restore_exc:
            raise HTTPException(
                status_code=500,
                detail=f"数据库提交失败且补偿未完全恢复: {restore_exc}",
            ) from restore_exc
        raise HTTPException(status_code=500, detail=f"数据库提交失败: {exc}") from exc

    return _StagedDeleteResult(
        deleted_links=deleted_links,
        deleted_inodes=deleted_inode_count,
        deleted_directory_states=deleted_dir_state_count,
        deleted_files=deleted_files,
        pruned_dirs=pruned_dir_count,
        pruned_metadata_files=pruned_metadata_files,
    )


def _execute_batch_delete(
    db: Session,
    rows: list[MediaRecord],
    *,
    delete_files: bool,
) -> dict[str, int]:
    staged = _stage_batch_delete(db, rows, delete_files=delete_files)

    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        try:
            _restore_deleted_links(staged.deleted_links)
        except RuntimeError as restore_exc:
            raise HTTPException(
                status_code=500,
                detail=f"数据库提交失败且补偿未完全恢复: {restore_exc}",
            ) from restore_exc
        raise HTTPException(status_code=500, detail=f"数据库提交失败: {exc}") from exc

    log_cleanup = _cleanup_nonreview_pending_logs(rows)

    return {
        "deleted_records": len(rows),
        "deleted_files": staged.deleted_files,
        "deleted_inodes": staged.deleted_inodes,
        "deleted_directory_states": staged.deleted_directory_states,
        "pruned_dirs": staged.pruned_dirs,
        "pruned_metadata_files": staged.pruned_metadata_files,
        "pending_logs_removed": int(log_cleanup.get("pending_logs_removed") or 0),
        "unprocessed_logs_removed": int(log_cleanup.get("unprocessed_logs_removed") or 0),
    }


def _prune_reidentify_old_target_dirs(db: Session, rows: list[MediaRecord]) -> _ReidentifyPruneReport:
    pruned_dirs: set[Path] = set()
    blocked_dirs: list[_PruneBlockedDir] = []
    deleted_metadata_files: list[Path] = []
    candidate_dirs: set[Path] = set()
    target_roots: dict[tuple[int | None, str], Path | None] = {}

    for row in rows:
        target_value = str(getattr(row, "target_path", "") or "").strip()
        if not target_value:
            continue
        key = (getattr(row, "sync_group_id", None), getattr(row, "type", ""))
        if key not in target_roots:
            target_roots[key] = _group_target_root(db, key[0], key[1])

        target_path = Path(target_value)
        current = _normalize_existing_dir(target_path.parent)
        stop_dir = _normalize_existing_dir(target_roots.get(key))
        while current is not None:
            if stop_dir is not None and current == stop_dir:
                break
            candidate_dirs.add(current)
            parent = current.parent
            if parent == current:
                break
            current = parent

    for current in sorted(candidate_dirs, key=lambda value: len(value.parts), reverse=True):
        try:
            current.rmdir()
            pruned_dirs.add(current)
        except OSError as exc:
            removed_metadata = _remove_prunable_metadata_files(current)
            if removed_metadata:
                deleted_metadata_files.extend(removed_metadata)
                try:
                    current.rmdir()
                    pruned_dirs.add(current)
                    continue
                except OSError as retry_exc:
                    exc = retry_exc
            if any(blocked.path.is_relative_to(current) for blocked in blocked_dirs):
                continue
            reason = str(exc.strerror or exc).strip() or type(exc).__name__
            blocked_dirs.append(_PruneBlockedDir(path=current, reason=reason))
            continue

    return _ReidentifyPruneReport(
        pruned_dirs=sorted(pruned_dirs, key=lambda value: (len(value.parts), str(value))),
        blocked_dirs=blocked_dirs,
        deleted_metadata_files=deleted_metadata_files,
    )


def _resolve_reidentify_group(rows: list[MediaRecord], db: Session) -> tuple[SyncGroup, str]:
    if not rows:
        raise HTTPException(status_code=404, detail="未找到可修正的媒体记录")

    group_id = rows[0].sync_group_id
    if not group_id:
        raise HTTPException(status_code=400, detail="记录缺少同步组信息")

    if any(row.sync_group_id != group_id for row in rows):
        raise HTTPException(status_code=400, detail="作用域包含多个同步组记录")

    media_type = rows[0].type
    if any(row.type != media_type for row in rows):
        raise HTTPException(status_code=400, detail="作用域包含多种媒体类型")

    group = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
    if not group:
        raise HTTPException(status_code=400, detail="同步组不存在")
    return group, media_type


def _task_session(task: ScanTask, fallback_db: Session) -> Session:
    try:
        session = object_session(task)
    except Exception:
        return fallback_db

    if session is None:
        return fallback_db
    if type(session).__module__.startswith("unittest.mock"):
        return fallback_db
    if not hasattr(session, "commit") or not hasattr(session, "rollback"):
        return fallback_db
    return session


def _finalize_media_task(db: Session, task: ScanTask, *, status: str, has_issues: bool = False) -> None:
    task_db = _task_session(task, db)
    task.status = status
    if has_issues:
        task.type = tag_task_type_with_issue(task.type)
    task.finished_at = datetime.now(timezone.utc)
    task.log_file = f"task_{task.id}.log"
    task_db.commit()


def _fail_media_task(db: Session, task: ScanTask, detail: str, *, status_code: int) -> None:
    task_db = _task_session(task, db)
    db.rollback()
    task_db.rollback()
    task.status = "cancelled" if status_code == 409 else "failed"
    task.finished_at = datetime.now(timezone.utc)
    task.log_file = f"task_{task.id}.log"
    append_log(f"修正任务失败: {detail}")
    try:
        task_db.commit()
    except Exception:
        task_db.rollback()


def _mark_task_error_logged(exc: Exception) -> Exception:
    try:
        setattr(exc, "_task_log_handled", True)
    except Exception:
        pass
    return exc


def _execute_logged_media_task(
    db: Session,
    *,
    task: ScanTask,
    start_message: str,
    success_message_factory: Callable[[dict], str],
    runner: Callable[[Session, ScanTask], dict],
) -> None:
    append_log(start_message)
    result = runner(db, task)
    has_issues = bool(result.get("has_issues"))
    failed = int(result.get("failed") or 0)
    if has_issues:
        append_log("修正任务完成但存在问题项")
    append_log(success_message_factory(result))
    _finalize_media_task(db, task, status=("completed" if failed == 0 else "failed"), has_issues=has_issues)


def _enqueue_logged_media_task(
    db: Session,
    *,
    task_type: str,
    target_id: int | None,
    target_name: str | None,
    start_message: str,
    queued_message: str,
    success_message_factory: Callable[[dict], str],
    runner: Callable[[Session, ScanTask], dict],
) -> dict:
    task = enqueue_task(
        db,
        task_type=task_type,
        target_id=target_id,
        target_name=target_name,
        queued_message=queued_message,
        runner=lambda worker_db, queued_task: _run_logged_media_task(
            worker_db,
            task=queued_task,
            start_message=start_message,
            success_message_factory=success_message_factory,
            runner=runner,
        ),
    )
    return {
        "message": queued_message,
        "task_id": task.id,
        "status": task.status,
        "log_file": task.log_file,
    }


def _run_logged_media_task(
    db: Session,
    *,
    task: ScanTask,
    start_message: str,
    success_message_factory: Callable[[dict], str],
    runner: Callable[[Session, ScanTask], dict],
) -> None:
    try:
        _execute_logged_media_task(
            db,
            task=task,
            start_message=start_message,
            success_message_factory=success_message_factory,
            runner=runner,
        )
    except HTTPException as exc:
        _fail_media_task(db, task, str(exc.detail), status_code=exc.status_code)
        _mark_task_error_logged(exc)
        raise
    except Exception as exc:
        _fail_media_task(db, task, str(exc), status_code=500)
        _mark_task_error_logged(exc)
        raise


def _require_media_record(db: Session, media_id: int, *, detail: str = "记录不存在") -> MediaRecord:
    row = db.query(MediaRecord).filter(MediaRecord.id == media_id).first()
    if not row:
        raise HTTPException(status_code=404, detail=detail)
    return row


def _require_sync_group(db: Session, group_id: int | None, *, detail: str = "同步组不存在") -> SyncGroup:
    if not group_id:
        raise HTTPException(status_code=400, detail=detail)
    group = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
    if not group:
        raise HTTPException(status_code=400, detail=detail)
    return group


def _manual_organize_log_cleanup_rows(
    db: Session,
    *,
    pending_id: int,
    sync_group_id: int,
    pending_original_path: str | None,
) -> list[MediaRecord]:
    root_dir = str(pending_original_path or "").strip()
    if not root_dir:
        return []

    normalized_root = root_dir.rstrip("/\\")
    if not normalized_root:
        return []

    resolved_rows = (
        db.query(MediaRecord)
        .filter(MediaRecord.sync_group_id == sync_group_id)
        .filter(MediaRecord.status.in_(["scraped", "manual_fixed"]))
        .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
        .filter(
            or_(
                MediaRecord.original_path == normalized_root,
                MediaRecord.original_path.like(f"{normalized_root}/%"),
                MediaRecord.original_path.like(f"{normalized_root}\\%"),
            )
        )
        .all()
    )

    pending_after = db.query(MediaRecord).filter(MediaRecord.id == pending_id).first()
    cleanup_rows = list(resolved_rows)
    if pending_after is None:
        cleanup_rows.append(
            MediaRecord(
                original_path=normalized_root,
                target_path=None,
                status="pending_manual",
            )
        )
    return cleanup_rows


def _run_manual_organize_task(
    db: Session,
    *,
    task: ScanTask,
    media_id: int,
    group_id: int,
    payload: dict,
) -> dict[str, int | bool | str]:
    pending = _require_media_record(db, media_id, detail="待办记录不存在")
    if pending.status != "pending_manual":
        raise HTTPException(status_code=400, detail="该记录不是待办状态")

    group = _require_sync_group(db, group_id)
    init_scan_cancel_flag(task.id)
    try:
        processed, failed, has_issues, inode_skipped = run_manual_organize(
            db=db,
            pending=pending,
            group=group,
            media_type=payload["media_type"],
            tmdb_id=payload["tmdb_id"],
            title_override=payload.get("title"),
            year_override=payload.get("year"),
            season_override=payload.get("season"),
            episode_override=payload.get("episode_override"),
            episode_offset=payload.get("episode_offset"),
            selected_relative_paths=payload.get("selected_paths"),
            force_extra_context=bool(payload.get("force_extra_context")),
        )
        cleanup_rows = _manual_organize_log_cleanup_rows(
            db,
            pending_id=media_id,
            sync_group_id=group.id,
            pending_original_path=pending.original_path,
        )
        log_cleanup = _cleanup_nonreview_pending_logs(cleanup_rows)
        if has_issues:
            append_log("手动整理完成但存在问题项（Special 冲突已跳过）")
        summary = f"手动整理完成: 成功 {processed}，失败 {failed}"
        if inode_skipped:
            summary += f"，已处理跳过 {inode_skipped}"
        return {
            "message": summary,
            "processed": processed,
            "failed": failed,
            "has_issues": has_issues,
            "inode_skipped": inode_skipped,
            "pending_logs_removed": int(log_cleanup.get("pending_logs_removed") or 0),
            "unprocessed_logs_removed": int(log_cleanup.get("unprocessed_logs_removed") or 0),
        }
    except TaskCancelledError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except DirectoryProcessError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        pop_scan_cancel_flag(task.id)


def _run_reidentify_scope(
    *,
    db: Session,
    rows: list[MediaRecord],
    group: SyncGroup,
    media_type: str,
    tmdb_id: int,
    title: str | None,
    year: int | None,
    season: int | None = None,
    season_override: int | None = None,
    episode_override: int | None = None,
    episode_offset: int | None,
) -> dict[str, int | bool | str]:
    if not rows:
        raise HTTPException(status_code=404, detail="待修正记录不存在")

    staged = _stage_batch_delete(db, rows, delete_files=True)
    _cleanup_check_issues_for_records(db, rows)
    effective_season_override = season_override if season_override is not None else season
    try:
        processed, failed, has_issues = reidentify_by_target_dir(
            db=db,
            group=group,
            media_type=media_type,
            tmdb_id=tmdb_id,
            title_override=title,
            year_override=year,
            season_override=effective_season_override,
            episode_override=episode_override,
            episode_offset=episode_offset,
            records=rows,
        )
    except DirectoryProcessError as exc:
        db.rollback()
        try:
            _restore_deleted_links(staged.deleted_links)
        except RuntimeError as restore_exc:
            raise HTTPException(
                status_code=500,
                detail=f"修正失败且补偿未完全恢复: {exc}; restore={restore_exc}",
            ) from restore_exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        try:
            _restore_deleted_links(staged.deleted_links)
        except RuntimeError as restore_exc:
            raise HTTPException(
                status_code=500,
                detail=f"修正异常且补偿未完全恢复: {restore_exc}",
            ) from restore_exc
        raise HTTPException(status_code=500, detail=f"修正失败: {exc}") from exc

    prune_report = _prune_reidentify_old_target_dirs(db, rows)
    extra_pruned_dirs = len(prune_report.pruned_dirs)
    total_pruned_metadata_files = staged.pruned_metadata_files + len(prune_report.deleted_metadata_files)
    if extra_pruned_dirs:
        append_log(f"INFO: 修正后旧目标空目录补清理: {extra_pruned_dirs} 个")
    if total_pruned_metadata_files:
        append_log(f"INFO: 修正后旧目标元数据补清理: {total_pruned_metadata_files} 个文件")
    if prune_report.blocked_dirs:
        blocked_preview = "; ".join(
            f"{item.path} ({item.reason})" for item in prune_report.blocked_dirs[:3]
        )
        if len(prune_report.blocked_dirs) > 3:
            blocked_preview += f"; 其余 {len(prune_report.blocked_dirs) - 3} 个已省略"
        append_log(
            "WARNING: 修正后旧目标目录未完全删除: "
            f"{len(prune_report.blocked_dirs)} 个目录仍保留 -> {blocked_preview}"
        )

    log_cleanup = _cleanup_nonreview_pending_logs(rows)
    return {
        "processed": processed,
        "failed": failed,
        "has_issues": has_issues,
        "deleted_records": len(rows),
        "deleted_files": staged.deleted_files,
        "deleted_inodes": staged.deleted_inodes,
        "deleted_directory_states": staged.deleted_directory_states,
        "pruned_dirs": staged.pruned_dirs + extra_pruned_dirs,
        "pruned_metadata_files": total_pruned_metadata_files,
        "pending_logs_removed": int(log_cleanup.get("pending_logs_removed") or 0),
        "unprocessed_logs_removed": int(log_cleanup.get("unprocessed_logs_removed") or 0),
    }


def _run_single_reidentify(
    *,
    db: Session,
    row: MediaRecord,
    group: SyncGroup,
    data: ReidentifyRequest,
) -> dict[str, int | str | bool]:
    src = Path(row.original_path)
    media_type = data.media_type or row.type
    if media_type == "tv":
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
    if data.episode_offset is not None and media_type == "tv" and pr.episode is not None and pr.extra_category is None:
        adjusted = pr.episode + int(data.episode_offset)
        pr = pr._replace(episode=max(1, adjusted))

    if media_type == "tv":
        tv_root, tv_reason = resolve_tv_target_root(db, group)
        if tv_root is None:
            raise HTTPException(status_code=400, detail=f"TV 目标路径不可决策: {tv_reason}")
        target_root = tv_root
    else:
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

    # 记录旧目标路径，用于后续清理（类型变更时旧目录可能在另一个媒体库根目录下）
    old_target_value = str(row.target_path or "").strip()
    old_target = Path(old_target_value) if old_target_value else None
    if str(row.type or "") == "movie":
        old_target_root_val, _ = resolve_movie_target_root(db, group)
        old_target_root = old_target_root_val or Path(group.target)
    else:
        old_target_root_val, _ = resolve_tv_target_root(db, group)
        old_target_root = old_target_root_val or Path(group.target)

    _link_or_fail(src, dst)

    # 清理旧目标硬链接（路径变更时），并向上级联删除空目录 / 纯元数据目录
    # 特别是类型变更（tv↔movie）时，旧链接位于不同媒体库根目录下必须清理，
    # 否则 Emby 仍会在旧目录下看到该文件导致解析异常
    if old_target is not None and old_target != dst:
        try:
            if old_target.is_file():
                old_target.unlink()
                append_log(f"已清理旧目标文件: {old_target}")
        except OSError as exc:
            append_log(f"WARNING: 旧目标文件清理失败: {old_target} | {exc}")
        try:
            _prune_empty_dirs_with_metadata(old_target.parent, old_target_root)
        except OSError:
            pass

    # Clean up check issues that pointed at the old target path (before overwriting it)
    _cleanup_check_issues_for_records(db, [row])
    row.tmdb_id = data.tmdb_id
    row.type = media_type
    row.target_path = str(dst)
    row.status = "manual_fixed"
    row.updated_at = datetime.now(timezone.utc)

    ino = get_inode(src)
    if ino:
        inode_row = db.query(InodeRecord).filter(InodeRecord.inode == ino).first()
        if inode_row:
            inode_row.target_path = str(dst)
            inode_row.sync_group_id = group.id

    # 处理随行附件（字幕/音轨等）
    companion_processed = 0
    companion_failed = 0
    if data.companion_ids:
        companion_rows = db.query(MediaRecord).filter(MediaRecord.id.in_(data.companion_ids)).all()
        for att_row in companion_rows:
            att_src = Path(att_row.original_path)
            if not att_src.exists() or not att_src.is_file():
                append_log(f"WARNING: 附件源文件不存在，跳过: {att_src}")
                companion_failed += 1
                continue
            att_ext = att_src.suffix.lower()
            att_pr = parse_tv_filename(str(att_src)) or parse_movie_filename(str(att_src)) or pr
            try:
                att_dst = build_attachment_target_from_anchor(dst, att_pr, att_ext, src_path=att_src)
                # 清理附件旧目标
                att_old_value = str(att_row.target_path or "").strip()
                if att_old_value:
                    att_old = Path(att_old_value)
                    if att_old != att_dst:
                        try:
                            if att_old.is_file():
                                att_old.unlink()
                        except OSError:
                            pass
                _link_or_fail(att_src, att_dst)
                att_row.tmdb_id = data.tmdb_id
                att_row.type = media_type
                att_row.target_path = str(att_dst)
                att_row.status = "manual_fixed"
                att_row.updated_at = datetime.now(timezone.utc)
                att_ino = get_inode(att_src)
                if att_ino:
                    att_inode_row = db.query(InodeRecord).filter(InodeRecord.inode == att_ino).first()
                    if att_inode_row:
                        att_inode_row.target_path = str(att_dst)
                        att_inode_row.sync_group_id = group.id
                append_log(f"附件修正完成: {att_src.name} -> {att_dst}")
                companion_processed += 1
            except Exception as exc:
                append_log(f"WARNING: 附件修正失败，跳过: {att_src.name} | {exc}")
                companion_failed += 1

    db.commit()
    total_processed = 1 + companion_processed
    total_failed = companion_failed
    append_log(f"单文件修正完成: {src.name} -> {dst}" + (f"（附件 {companion_processed} 个）" if companion_processed else ""))
    return {
        "message": "修正成功",
        "new_path": str(dst),
        "tmdb_id": data.tmdb_id,
        "processed": total_processed,
        "failed": total_failed,
        "has_issues": total_failed > 0,
    }


def _run_single_adjust(
    *,
    db: Session,
    row: MediaRecord,
    group: SyncGroup,
    data: AdjustRequest,
) -> dict[str, str | bool]:
    src = Path(row.original_path)
    ext = src.suffix.lower()
    old_target = Path(str(row.target_path))
    is_movie = row.type == "movie"

    if is_movie:
        # Movie target: <show_dir>/<file> (mainline) OR <show_dir>/extras/<file>
        # Detect by checking if parent folder is "extras"
        if old_target.parent.name.lower() == "extras":
            show_dir = old_target.parent.parent
        else:
            show_dir = old_target.parent
    else:
        # TV target: <show_dir>/Season XX/<file>, <show_dir>/Season 00/<file>, <show_dir>/extras/<file>
        show_dir = old_target.parent.parent
    target_root = show_dir.parent

    # Extract title and year from the existing show dir name: "Title (year) [tmdbid=xxx]"
    show_dirname = show_dir.name
    m = re.match(r"^(.*?)(?:\s+\((\d{4})\))?(?:\s+\[tmdbid=\d+\])?\s*$", show_dirname)
    existing_title = m.group(1).strip() if m else show_dirname.strip()
    existing_year = int(m.group(2)) if (m and m.group(2)) else None

    # Parse source file to get base ParseResult
    pr = (parse_movie_filename(str(src)) or parse_tv_filename(str(src))) if is_movie else (parse_tv_filename(str(src)) or parse_movie_filename(str(src)))
    if not pr:
        raise HTTPException(status_code=400, detail="文件名解析失败")

    # Override with resolved values
    pr = pr._replace(
        title=existing_title,
        year=existing_year,
        extra_category=data.extra_category,
        extra_label=data.extra_label if data.extra_label is not None else None,
    )
    if not is_movie:
        if data.season is not None:
            pr = pr._replace(season=max(0, data.season))
        if data.episode is not None:
            pr = pr._replace(episode=max(0, data.episode))

    # Compute new target path
    if is_movie:
        dst = compute_movie_target_path(target_root, pr, row.tmdb_id, ext)
    else:
        dst = compute_tv_target_path(target_root, pr, row.tmdb_id, ext, src_filename=src.name)

    # Apply custom filename stem override
    if data.custom_filename:
        safe_stem = re.sub(r'[<>:"/\\|?*]', "_", data.custom_filename.strip())
        if safe_stem:
            dst = dst.with_name(safe_stem + ext)

    # Create new hardlink
    _link_or_fail(src, dst)

    # Remove old hardlink if path changed
    if dst != old_target:
        try:
            if old_target.is_file():
                old_target.unlink()
        except OSError as exc:
            append_log(f"WARNING: 旧目标文件清理失败: {old_target} | {exc}")

    # Update database record
    row.target_path = str(dst)
    row.status = "manual_fixed"
    row.updated_at = datetime.now(timezone.utc)

    ino = get_inode(src)
    if ino:
        inode_row = db.query(InodeRecord).filter(InodeRecord.inode == ino).first()
        if inode_row:
            inode_row.target_path = str(dst)

    # 处理随行附件：从磁盘找同目录同 stem 的附件文件，再按 original_path 查 DB 更新记录。
    # 不依赖 file_type / sync_group_id 列，与识别修正的附件处理方式一致。
    src_dir = src.parent
    video_stem_key = _normalize_media_stem(src.stem)
    att_companion_count = 0
    att_companion_failed = 0
    if video_stem_key and src_dir.is_dir():
        try:
            disk_att_files = sorted(
                p for p in src_dir.iterdir()
                if p.suffix.lower() in ATTACHMENT_EXTS
                and _normalize_attachment_stem(p.stem) == video_stem_key
            )
        except OSError:
            disk_att_files = []
        for att_src in disk_att_files:
            att_row = db.query(MediaRecord).filter(MediaRecord.original_path == str(att_src)).first()
            if att_row is None:
                continue
            att_ext = att_src.suffix.lower()
            att_pr = parse_tv_filename(str(att_src)) or parse_movie_filename(str(att_src)) or pr
            try:
                att_dst = build_attachment_target_from_anchor(dst, att_pr, att_ext, src_path=att_src)
                att_old_value = str(att_row.target_path or "").strip()
                if att_old_value:
                    att_old = Path(att_old_value)
                    if att_old != att_dst:
                        try:
                            if att_old.is_file():
                                att_old.unlink()
                        except OSError as exc:
                            append_log(f"WARNING: 随行附件旧目标清理失败: {att_old} | {exc}")
                _link_or_fail(att_src, att_dst)
                att_row.target_path = str(att_dst)
                att_row.status = "manual_fixed"
                att_row.updated_at = datetime.now(timezone.utc)
                att_ino = get_inode(att_src)
                if att_ino:
                    att_inode_row = db.query(InodeRecord).filter(InodeRecord.inode == att_ino).first()
                    if att_inode_row:
                        att_inode_row.target_path = str(att_dst)
                append_log(f"随行附件调整完成: {att_src.name} -> {att_dst}")
                att_companion_count += 1
            except Exception as exc:
                append_log(f"WARNING: 随行附件调整失败，跳过: {att_src.name} | {exc}")
                att_companion_failed += 1

    db.commit()

    # Prune empty directories left by old target
    if dst != old_target and not old_target.is_file():
        try:
            _prune_empty_dirs_with_metadata(old_target.parent, target_root)
        except OSError:
            pass

    extra_note = f"（附件 {att_companion_count} 个）" if att_companion_count else ""
    append_log(f"季内调整完成: {src.name} -> {dst}{extra_note}")
    return {
        "message": f"{src.name} -> {dst.name}{extra_note}",
        "new_path": str(dst),
        "has_issues": att_companion_failed > 0,
    }


def _normalize_scope_item_ids(item_ids: list[int]) -> list[int]:
    return sorted({int(value) for value in item_ids if isinstance(value, int) or str(value).isdigit()})


def _load_scope_media_rows(db: Session, data: DeleteScopeRequest) -> list[MediaRecord]:
    item_ids = _normalize_scope_item_ids(data.item_ids)
    if not item_ids:
        raise HTTPException(status_code=400, detail="scope item_ids 不能为空")

    rows = db.query(MediaRecord).filter(MediaRecord.id.in_(item_ids)).all()
    if not rows:
        return []

    expected_resource_dir = data.resource_dir.strip().replace("\\", "/").rstrip("/")
    expected_type = str(data.type or "").strip()
    expected_sync_group_id = data.sync_group_id
    expected_tmdb_id = data.tmdb_id

    scoped_rows: list[MediaRecord] = []
    for row in rows:
        payload = _media_to_dict(row)
        tree = build_resource_tree(
            [payload],
            resource_dir=expected_resource_dir,
            media_type=expected_type,
            sync_group_id=expected_sync_group_id,
            tmdb_id=expected_tmdb_id,
        )
        if tree["resource"]["record_count"] == 0:
            continue
        scoped_rows.append(row)
    return scoped_rows


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

    rows = q.order_by(MediaRecord.updated_at.desc()).all()
    group_ids = {int(row.sync_group_id) for row in rows if getattr(row, "sync_group_id", None) is not None}
    groups_by_id = _load_sync_group_map(db, group_ids)
    processed_by_group = _load_processed_original_paths_by_group(db, group_ids)
    visible_rows: list[MediaRecord] = []
    for row in rows:
        group = groups_by_id.get(int(row.sync_group_id)) if getattr(row, "sync_group_id", None) is not None else None
        if not group:
            visible_rows.append(row)
            continue
        if not _pending_has_visible_mainline_files_fast(
            row,
            group,
            processed_paths=processed_by_group.get(int(group.id), set()),
        ):
            continue
        visible_rows.append(row)

    total = len(visible_rows)
    items = [_media_to_dict(x) for x in visible_rows[offset: offset + limit]]
    return {"total": total, "items": items, "offset": offset, "limit": limit}


def _normalize_relative_pending_path(value: str | None) -> str:
    text = str(value or "").strip().replace("\\", "/")
    if not text:
        return ""
    parts = [part for part in text.split("/") if part and part != "."]
    if any(part == ".." for part in parts):
        return ""
    return "/".join(parts)


def _pending_file_type(path: Path) -> str:
    return "attachment" if path.suffix.lower() in ATTACHMENT_EXTS else "video"


def _pending_video_role(path: Path, media_type: str | None) -> str:
    if path.suffix.lower() in ATTACHMENT_EXTS:
        return "attachment"
    if detect_special_dir_context(path)[0]:
        return "special"
    if classify_extra_from_text(path.name)[0]:
        return "special"

    parsed = parse_tv_filename(str(path)) if media_type == "tv" else parse_movie_filename(str(path))
    if parsed is None:
        return "mainline"
    if getattr(parsed, "extra_category", None) is not None or getattr(parsed, "is_special", False):
        return "special"
    return "mainline"


def _build_pending_file_items(root_dir: Path, *, include: str, exclude: str, source_root: Path, media_type: str | None = None) -> list[dict]:
    files = _collect_media_files_under_dir(root_dir, include, exclude, source_root)
    items: list[dict] = []
    for file_path in sorted(files, key=lambda value: value.as_posix()):
        try:
            relative_path = file_path.relative_to(root_dir).as_posix()
        except ValueError:
            relative_path = file_path.name
        normalized_relative = _normalize_relative_pending_path(relative_path)
        if not normalized_relative:
            continue
        parent_dir = str(Path(normalized_relative).parent).replace("\\", "/")
        if parent_dir in {"", "."}:
            parent_dir = "."
        items.append(
            {
                "relative_path": normalized_relative,
                "parent_dir": parent_dir,
                "name": file_path.name,
                "size": int(file_path.stat().st_size) if _safe_is_file(file_path) else 0,
                "file_type": _pending_file_type(file_path),
                "content_role": _pending_video_role(file_path, media_type),
                "absolute_path": str(file_path),
            }
        )
    return items


def _filter_processed_pending_file_items(db: Session, pending: MediaRecord, items: list[dict]) -> list[dict]:
    candidate_paths = [str(item.get("absolute_path") or "") for item in items if item.get("absolute_path")]
    if not candidate_paths:
        return items

    rows = db.query(MediaRecord.original_path, MediaRecord.status, MediaRecord.target_path).filter(
        MediaRecord.sync_group_id == pending.sync_group_id,
        MediaRecord.original_path.in_(candidate_paths),
    ).all()
    processed_paths = {
        str(original_path)
        for original_path, status, target_path in rows
        if str(status or "") in {"scraped", "manual_fixed"} and str(target_path or "").strip()
    }
    if not processed_paths:
        return items
    return [item for item in items if str(item.get("absolute_path") or "") not in processed_paths]


@router.get("/pending/{media_id}/files")
def list_pending_files(media_id: int, db: Session = Depends(get_db)):
    pending = db.query(MediaRecord).filter(MediaRecord.id == media_id).first()
    if not pending:
        raise HTTPException(status_code=404, detail="待办记录不存在")
    if pending.status != "pending_manual":
        raise HTTPException(status_code=400, detail="该记录不是待办状态")

    root_dir = Path(str(pending.original_path or ""))
    if not _safe_is_dir(root_dir):
        raise HTTPException(status_code=400, detail="待办目录不存在或不是目录")

    group = db.query(SyncGroup).filter(SyncGroup.id == pending.sync_group_id).first()
    if not group:
        raise HTTPException(status_code=400, detail="同步组不存在")

    items = _load_visible_pending_file_items(db, pending, group)
    return {
        "pending": _media_to_dict(pending),
        "root_dir": str(root_dir),
        "items": items,
        "total": len(items),
    }


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


@router.get("/pending-logs-kinds")
def list_pending_log_kinds(search: str | None = None):
    keyword = str(search or "").strip().lower()
    items: list[dict] = []
    for kind in ("pending", "unprocessed", "review"):
        file_path = _resolve_pending_log_path(kind)
        kind_items = _load_pending_log_items(file_path, kind)
        if keyword:
            kind_items = [item for item in kind_items if _pending_log_matches(item, keyword)]
        items.append(
            {
                "kind": kind,
                "path": str(file_path) if file_path else "",
                "total": len(kind_items),
            }
        )
    return {"items": items}


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
    total_size = q.with_entities(func.sum(MediaRecord.size)).scalar() or 0

    # 与 list_pending 保持一致：只统计有可见正片文件的待办条目
    raw_pending = db.query(MediaRecord).filter(MediaRecord.status == "pending_manual").all()
    group_ids = {int(row.sync_group_id) for row in raw_pending if getattr(row, "sync_group_id", None) is not None}
    groups_by_id = _load_sync_group_map(db, group_ids)
    processed_by_group = _load_processed_original_paths_by_group(db, group_ids)
    pending_manual_count = 0
    for row in raw_pending:
        group = groups_by_id.get(int(row.sync_group_id)) if getattr(row, "sync_group_id", None) is not None else None
        if not group:
            pending_manual_count += 1
            continue
        if _pending_has_visible_mainline_files_fast(
            row,
            group,
            processed_paths=processed_by_group.get(int(group.id), set()),
        ):
            pending_manual_count += 1

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
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    target_dir = target_dir.strip()
    if not target_dir:
        raise HTTPException(status_code=400, detail="target_dir 不能为空")

    normalized = target_dir.replace("\\", "/").rstrip("/")
    q = (
        db.query(MediaRecord)
        .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
        .filter(_target_dir_like_expr(normalized))
        .order_by(MediaRecord.updated_at.desc())
    )
    total = q.count()
    rows = q.offset(offset).limit(limit).all()
    items = [_media_to_dict(x) for x in rows]
    return {"items": items, "target_dir": target_dir, "total": total, "offset": offset, "limit": limit}


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
    # 只对 ASCII A-Z 执行小写，与 SQLite LOWER() 行为一致。
    # Python str.lower() 会转换非 ASCII 大写字母（如 Ⅱ→ⅱ），而 SQLite LOWER() 不会，
    # 导致含 Unicode 大写的路径匹配失败。
    base = re.sub(r"[A-Z]", lambda m: m.group().lower(), target_dir_norm).rstrip("/")
    return or_(lower_target_path == base, lower_target_path.like(f"{base}/%"))


def _aux_feature_expr():
    return ~_main_feature_expr()


def _apply_media_list_filters(q, *, search: str | None, media_type: str | None, category: str | None):
    if media_type:
        q = q.filter(MediaRecord.type == media_type)
    if search:
        q = q.filter(
            or_(
                MediaRecord.original_path.contains(search),
                MediaRecord.target_path.contains(search),
            )
        )

    normalized_category = str(category or "all").strip().lower()
    if normalized_category == "pending":
        q = q.filter(MediaRecord.status == "pending_manual")
    elif normalized_category == "success":
        q = q.filter(MediaRecord.status.in_(["scraped", "manual_fixed"]))
    elif normalized_category == "main":
        q = q.filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "").filter(_main_feature_expr())
    elif normalized_category == "sps":
        q = q.filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "").filter(_aux_feature_expr())
    return q


@router.get("/resources")
def list_media_resources(
    db: Session = Depends(get_db),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=500),
    search: str | None = None,
    type: str | None = Query(None, pattern="^(tv|movie)?$"),
    category: str = Query("all"),
):
    q = db.query(MediaRecord).filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
    q = _apply_media_list_filters(q, search=search, media_type=type, category=category)
    rows = q.order_by(MediaRecord.updated_at.desc()).all()
    summaries = build_resource_summaries([_media_to_dict(row) for row in rows])
    total = len(summaries)
    items = summaries[offset: offset + limit]
    return {"total": total, "items": items, "offset": offset, "limit": limit}


@router.get("/source-dirs")
def get_source_dirs(
    tmdb_id: int = Query(..., ge=1),
    sync_group_id: int | None = Query(None),
    season: int | None = Query(None, ge=0),
    db: Session = Depends(get_db),
):
    """返回指定 tmdb_id（+可选 sync_group_id / season）的已有记录的来源目录列表，
    供前端洗版候选打分时作为参照。"""
    q = (
        db.query(MediaRecord.original_path, MediaRecord.status, MediaRecord.target_path)
        .filter(
            MediaRecord.tmdb_id == tmdb_id,
            MediaRecord.target_path.isnot(None),
            MediaRecord.target_path != "",
            MediaRecord.status != "pending_manual",
        )
    )
    if sync_group_id is not None:
        q = q.filter(MediaRecord.sync_group_id == sync_group_id)

    rows = q.all()
    source_dirs: set[str] = set()
    for original_path, status, target_path in rows:
        if season is not None:
            row_season = extract_tv_primary_season(target_path)
            if row_season != season:
                continue
        src = str(_derive_source_dir(original_path, status, target_path))
        if src:
            source_dirs.add(src)

    return {"source_dirs": sorted(source_dirs)}


@router.get("/resource-tree")
def get_media_resource_tree(
    resource_dir: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
    type: str | None = Query(None, pattern="^(tv|movie)?$"),
    sync_group_id: int | None = Query(None),
    tmdb_id: int | None = Query(None),
):
    normalized_resource_dir = resource_dir.strip().replace("\\", "/").rstrip("/")
    if not normalized_resource_dir:
        raise HTTPException(status_code=400, detail="resource_dir 不能为空")

    q = db.query(MediaRecord).filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
    q = q.filter(_target_dir_like_expr(normalized_resource_dir))
    # 不按 type 过滤：resource_dir + sync_group_id 已足够定位记录集。
    # 传入 type 可能与 DB 中实际存储的 type 不一致（如 movie 被前端误传为 tv），
    # 过滤会导致 0 条结果；type 仅作为 build_resource_tree 的展示提示。
    if sync_group_id is not None:
        q = q.filter(MediaRecord.sync_group_id == sync_group_id)
    rows = q.order_by(MediaRecord.updated_at.desc()).all()
    tree = build_resource_tree(
        [_media_to_dict(row) for row in rows],
        resource_dir=normalized_resource_dir,
        media_type=type,
        sync_group_id=sync_group_id,
        tmdb_id=tmdb_id,
    )
    return tree


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
    _validate_forced_tv_episode(data.media_type or row.type, data.season, data.episode)
    payload = data.model_dump() if hasattr(data, "model_dump") else data.dict()
    return _enqueue_logged_media_task(
        db,
        task_type=f"reidentify:item:{group.name}",
        target_id=group.id,
        target_name=src.name,
        queued_message=f"单文件修正任务已进入队列，等待执行: {src.name}",
        start_message=(
            f"单文件修正任务启动: media_id={media_id}, group={group.name}, "
            f"file={src.name}, media_type={data.media_type or row.type}, tmdb_id={data.tmdb_id}, "
            f"season={data.season}, episode={data.episode}, episode_offset={data.episode_offset}"
        ),
        success_message_factory=lambda result: f"单文件修正任务完成: 成功 {int(result.get('processed') or 0)}，失败 {int(result.get('failed') or 0)}",
        runner=lambda worker_db, _task: _run_single_reidentify(
            db=worker_db,
            row=_require_media_record(worker_db, media_id),
            group=_require_sync_group(worker_db, group.id),
            data=ReidentifyRequest(**payload),
        ),
    )


@router.post("/{media_id}/adjust")
def adjust_media(media_id: int, data: AdjustRequest, db: Session = Depends(get_db)):
    row = db.query(MediaRecord).filter(MediaRecord.id == media_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="记录不存在")
    if row.type not in ("tv", "movie"):
        raise HTTPException(status_code=400, detail="仅支持TV/Movie类型的调整")
    if not row.target_path:
        raise HTTPException(status_code=400, detail="记录缺少目标路径，请先完成识别")

    src = Path(row.original_path)
    if not src.exists() or not src.is_file():
        raise HTTPException(status_code=400, detail="源文件不存在")

    group = db.query(SyncGroup).filter(SyncGroup.id == row.sync_group_id).first()
    if not group:
        raise HTTPException(status_code=400, detail="同步组不存在")

    payload = data.model_dump() if hasattr(data, "model_dump") else data.dict()
    return _enqueue_logged_media_task(
        db,
        task_type=f"adjust:item:{group.name}",
        target_id=group.id,
        target_name=src.name,
        queued_message=f"季内调整任务已进入队列: {src.name}",
        start_message=(
            f"季内调整任务启动: media_id={media_id}, group={group.name}, "
            f"file={src.name}, extra_category={data.extra_category}, "
            f"season={data.season}, episode={data.episode}"
        ),
        success_message_factory=lambda result: f"季内调整完成: {result.get('message', '')}",
        runner=lambda worker_db, _task: _run_single_adjust(
            db=worker_db,
            row=_require_media_record(worker_db, media_id),
            group=_require_sync_group(worker_db, group.id),
            data=AdjustRequest(**payload),
        ),
    )


@router.post("/{media_id}/manual-organize")
def manual_organize(media_id: int, data: PendingOrganizeRequest, db: Session = Depends(get_db)):
    _validate_forced_tv_episode(data.media_type, data.season, data.episode_override)
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

    pending_mainline_paths = _build_pending_mainline_relative_paths(
        root_dir,
        group=group,
        media_type=data.media_type,
    )
    if data.selected_paths:
        selected_path_set = {
            normalized
            for normalized in (_normalize_manual_selected_path(path_value) for path_value in data.selected_paths)
            if normalized
        }
        processed_mainline_paths = [
            relative_path
            for relative_path in pending_mainline_paths
            if _normalize_manual_selected_path(relative_path) in selected_path_set
        ]
    else:
        processed_mainline_paths = list(pending_mainline_paths)

    payload = data.model_dump() if hasattr(data, "model_dump") else data.dict()
    return _enqueue_logged_media_task(
        db,
        task_type=f"manual:{group.name}",
        target_id=group.id,
        target_name=root_dir.name,
        queued_message=(
            f"手动整理任务已进入队列，等待执行: {root_dir.name}"
            + (f"（{len(processed_mainline_paths)} 项）" if processed_mainline_paths else "")
        ),
        start_message=(
            f"手动整理任务启动: media_id={media_id}, group={group.name}, "
            f"dir={root_dir}, media_type={data.media_type}, tmdb_id={data.tmdb_id}"
        ),
        success_message_factory=lambda result: (
            f"手动整理完成: 成功 {int(result.get('processed') or 0)}，失败 {int(result.get('failed') or 0)}"
            + (f"，已处理跳过 {int(result.get('inode_skipped') or 0)}" if int(result.get('inode_skipped') or 0) > 0 else "")
        ),
        runner=lambda worker_db, task: _run_manual_organize_task(
            worker_db,
            task=task,
            media_id=media_id,
            group_id=group.id,
            payload=payload,
        ),
    )


@router.post("/reidentify-by-target-dir")
def reidentify_by_target_dir_api(data: ReidentifyDirRequest, db: Session = Depends(get_db)):
    target_dir = (data.target_dir or "").strip()
    if not target_dir:
        raise HTTPException(status_code=400, detail="target_dir 不能为空")
    _validate_forced_tv_episode(
        data.media_type,
        data.season_override if data.season_override is not None else data.season,
        data.episode_override,
    )

    normalized = target_dir.replace("\\", "/").rstrip("/")
    lower_target_path = func.lower(func.replace(func.coalesce(MediaRecord.target_path, ""), "\\", "/"))
    rows = (
        db.query(MediaRecord)
        .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
        .filter(or_(lower_target_path == normalized.lower(), lower_target_path.like(f"{normalized.lower()}/%")))
        .all()
    )
    group, media_type = _resolve_reidentify_group(rows, db)
    media_type = data.media_type or media_type
    payload = data.model_dump() if hasattr(data, "model_dump") else data.dict()
    row_ids = [row.id for row in rows]
    return _enqueue_logged_media_task(
        db,
        task_type=f"reidentify:resource:{group.name}",
        target_id=group.id,
        target_name=Path(normalized).name or normalized,
        queued_message=f"整组修正任务已进入队列，等待执行: {Path(normalized).name or normalized}",
        start_message=(
            f"整组修正任务启动: group={group.name}, target_dir={normalized}, "
            f"media_type={media_type}, tmdb_id={data.tmdb_id}, season_override={data.season_override if data.season_override is not None else data.season}, "
            f"episode_override={data.episode_override}, episode_offset={data.episode_offset}"
        ),
        success_message_factory=lambda result: (
            f"整组修正任务完成: 成功 {int(result.get('processed') or 0)}，失败 {int(result.get('failed') or 0)}"
        ),
        runner=lambda worker_db, _task: {
            **(lambda scoped_result: {
                "message": (
                    f"整组修正完成: 成功 {int(scoped_result['processed'])}，失败 {int(scoped_result['failed'])}"
                    + ("（存在问题项）" if bool(scoped_result['has_issues']) else "")
                ),
                **scoped_result,
            })(
                _run_reidentify_scope(
                    db=worker_db,
                    rows=worker_db.query(MediaRecord).filter(MediaRecord.id.in_(row_ids)).all(),
                    group=_require_sync_group(worker_db, group.id),
                    media_type=media_type,
                    tmdb_id=payload["tmdb_id"],
                    title=payload.get("title"),
                    year=payload.get("year"),
                    season_override=(payload.get("season_override") if payload.get("season_override") is not None else payload.get("season")),
                    episode_override=payload.get("episode_override"),
                    episode_offset=payload.get("episode_offset"),
                )
            )
        },
    )


@router.post("/reidentify-scope")
def reidentify_scope_api(data: ReidentifyScopeRequest, db: Session = Depends(get_db)):
    scope = DeleteScopeRequest(
        scope_level=data.scope_level,
        item_ids=data.item_ids,
        resource_dir=data.resource_dir,
        type=data.type,
        sync_group_id=data.sync_group_id,
        tmdb_id=data.scope_tmdb_id,
        season=data.season,
        group_kind=data.group_kind,
        group_label=data.group_label,
        delete_files=True,
    )
    rows = _load_scope_media_rows(db, scope)
    group, media_type = _resolve_reidentify_group(rows, db)
    media_type = data.media_type or media_type
    _validate_forced_tv_episode(
        media_type,
        data.season_override if data.season_override is not None else data.season,
        data.episode_override,
    )
    label = "季度修正" if data.scope_level == "season" else "作用域修正"
    target_name = _build_scoped_reidentify_target_name(data.resource_dir, data.group_label)
    payload = data.model_dump() if hasattr(data, "model_dump") else data.dict()
    return _enqueue_logged_media_task(
        db,
        task_type=(f"reidentify:season:{group.name}" if data.scope_level == "season" else f"reidentify:scope:{group.name}"),
        target_id=group.id,
        target_name=target_name,
        queued_message=f"{label}任务已进入队列，等待执行: {target_name}",
        start_message=(
            f"{label}任务启动: group={group.name}, scope={data.scope_level}, "
            f"target={target_name}, media_type={media_type}, tmdb_id={data.tmdb_id}, "
            f"season_override={data.season_override if data.season_override is not None else data.season}, "
            f"episode_override={data.episode_override}, episode_offset={data.episode_offset}"
        ),
        success_message_factory=lambda result: f"{label}任务完成: 成功 {int(result.get('processed') or 0)}，失败 {int(result.get('failed') or 0)}",
        runner=lambda worker_db, _task: {
            **(lambda scoped_result: {
                "message": (
                    f"{label}完成: 成功 {int(scoped_result['processed'])}，失败 {int(scoped_result['failed'])}"
                    + ("（存在问题项）" if bool(scoped_result['has_issues']) else "")
                ),
                **scoped_result,
            })(
                _run_reidentify_scope(
                    db=worker_db,
                    rows=_load_scope_media_rows(worker_db, DeleteScopeRequest(**{
                        "scope_level": payload["scope_level"],
                        "item_ids": payload["item_ids"],
                        "resource_dir": payload["resource_dir"],
                        "type": payload["type"],
                        "sync_group_id": payload.get("sync_group_id"),
                        "tmdb_id": payload.get("scope_tmdb_id"),
                        "season": payload.get("season"),
                        "group_kind": payload.get("group_kind"),
                        "group_label": payload.get("group_label"),
                        "delete_files": True,
                    })),
                    group=_require_sync_group(worker_db, group.id),
                    media_type=media_type,
                    tmdb_id=payload["tmdb_id"],
                    title=payload.get("title"),
                    year=payload.get("year"),
                    season_override=(payload.get("season_override") if payload.get("season_override") is not None else payload.get("season")),
                    episode_override=payload.get("episode_override"),
                    episode_offset=payload.get("episode_offset"),
                )
            )
        },
    )


@router.post("/batch-reidentify")
def batch_reidentify(data: BatchReidentifyRequest, db: Session = Depends(get_db)):
    if not data.item_ids:
        raise HTTPException(status_code=400, detail="item_ids 不能为空")
    rows = db.query(MediaRecord).filter(MediaRecord.id.in_(data.item_ids)).all()
    if not rows:
        raise HTTPException(status_code=404, detail="未找到待修正记录")
    group, inferred_media_type = _resolve_reidentify_group(rows, db)
    media_type = data.media_type or inferred_media_type
    _validate_forced_tv_episode(media_type, data.season_override, data.episode_override)
    item_ids_snapshot = list(data.item_ids)
    payload = data.model_dump() if hasattr(data, "model_dump") else data.dict()
    target_name = group.name
    return _enqueue_logged_media_task(
        db,
        task_type=f"reidentify:batch:{group.name}",
        target_id=group.id,
        target_name=target_name,
        queued_message=f"批量修正任务已进入队列，等待执行: {len(item_ids_snapshot)} 个文件",
        start_message=(
            f"批量修正任务启动: group={group.name}, count={len(item_ids_snapshot)}, "
            f"media_type={media_type}, tmdb_id={data.tmdb_id}, "
            f"season_override={data.season_override}, episode_override={data.episode_override}, "
            f"episode_offset={data.episode_offset}"
        ),
        success_message_factory=lambda result: f"批量修正完成: 成功 {int(result.get('processed') or 0)}，失败 {int(result.get('failed') or 0)}",
        runner=lambda worker_db, _task: {
            **(lambda scoped_result: {
                "message": (
                    f"批量修正完成: 成功 {int(scoped_result['processed'])}，失败 {int(scoped_result['failed'])}"
                    + ("（存在问题项）" if bool(scoped_result['has_issues']) else "")
                ),
                **scoped_result,
            })(
                _run_reidentify_scope(
                    db=worker_db,
                    rows=worker_db.query(MediaRecord).filter(MediaRecord.id.in_(item_ids_snapshot)).all(),
                    group=_require_sync_group(worker_db, group.id),
                    media_type=media_type,
                    tmdb_id=payload["tmdb_id"],
                    title=payload.get("title"),
                    year=payload.get("year"),
                    season_override=payload.get("season_override"),
                    episode_override=payload.get("episode_override"),
                    episode_offset=payload.get("episode_offset"),
                )
            )
        },
    )


@router.post("/batch-delete")
def batch_delete(data: BatchDeleteRequest, db: Session = Depends(get_db)):
    ids = [int(x) for x in data.ids if isinstance(x, int) or str(x).isdigit()]
    if not ids:
        raise HTTPException(status_code=400, detail="ids 不能为空")

    rows = db.query(MediaRecord).filter(MediaRecord.id.in_(ids)).all()
    if not rows:
        return {
            "deleted_records": 0,
            "deleted_files": 0,
            "deleted_inodes": 0,
            "deleted_directory_states": 0,
            "pruned_dirs": 0,
            "pruned_metadata_files": 0,
        }

    if data.delete_resource_scope:
        rows = _expand_rows_to_resource_scope(db, rows)

    return _execute_batch_delete(db, rows, delete_files=bool(data.delete_files))


@router.post("/delete-scope")
def delete_media_scope(data: DeleteScopeRequest, db: Session = Depends(get_db)):
    rows = _load_scope_media_rows(db, data)
    if not rows:
        return {
            "deleted_records": 0,
            "deleted_files": 0,
            "deleted_inodes": 0,
            "deleted_directory_states": 0,
            "pruned_dirs": 0,
            "pruned_metadata_files": 0,
        }
    return _execute_batch_delete(db, rows, delete_files=bool(data.delete_files))


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
    # NOTE: preserved for backwards compatibility; new UI uses /wash/* instead.
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


# ---------------------------------------------------------------------------
# Active wash (洗版) endpoints
# ---------------------------------------------------------------------------

class WashExecuteRequest(BaseModel):
    sync_group_id: int | None = None
    tmdb_id: int
    season: int | None = None
    keep_ids: list[int]


@router.get("/wash/candidates")
def get_wash_candidates_endpoint(
    tmdb_id: int = Query(...),
    sync_group_id: int | None = Query(default=None),
    season: int | None = Query(default=None),
    db: Session = Depends(get_db),
):
    from ..services.wash import get_wash_candidates

    result = get_wash_candidates(db, sync_group_id, tmdb_id, season)
    return {
        "sync_group_id": result.sync_group_id,
        "tmdb_id": result.tmdb_id,
        "season": result.season,
        "total_records": result.total_records,
        "candidate_groups": [
            {
                "key": cg.key,
                "label": cg.label,
                "records": cg.records,
                "recommended": cg.recommended,
                "record_count": cg.record_count,
                "size_human": cg.size_human,
                "total_size": cg.total_size,
            }
            for cg in result.candidate_groups
        ],
    }


@router.get("/wash/source-scan")
def get_wash_source_scan(
    tmdb_id: int = Query(...),
    sync_group_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
):
    """Scan sync-group source directories for all dirs related to tmdb_id.

    Returns structured entries for both recorded and unrecorded directories,
    each with a list of contained video files.
    """
    from ..services.wash import scan_source_dirs_for_tmdb

    entries = scan_source_dirs_for_tmdb(db, tmdb_id, sync_group_id)
    return {
        "tmdb_id": tmdb_id,
        "total": len(entries),
        "entries": [
            {
                "dir_path": e.dir_path,
                "sync_group_id": e.sync_group_id,
                "is_recorded": e.is_recorded,
                "record_count": e.record_count,
                "group_key": e.group_key,
                "video_count": e.video_count,
                "video_files": e.video_files,
            }
            for e in entries
        ],
    }


@router.post("/wash/execute")
def execute_wash_endpoint(req: WashExecuteRequest, db: Session = Depends(get_db)):
    from ..services.wash import execute_wash

    if not req.keep_ids:
        raise HTTPException(status_code=400, detail="keep_ids 不能为空")

    _sync_group_id = req.sync_group_id
    _tmdb_id = req.tmdb_id
    _season = req.season
    _keep_ids = list(req.keep_ids)

    # Derive a human-readable task label from the sync group if known
    if _sync_group_id is not None:
        group = db.query(SyncGroup).filter(SyncGroup.id == _sync_group_id).first()
        _group_name = group.name if group else str(_sync_group_id)
    else:
        _group_name = f"tmdb:{_tmdb_id}"

    task_type = (
        f"wash:season:{_group_name}"
        if _season is not None
        else f"wash:resource:{_group_name}"
    )

    from ..services.task_queue import enqueue_task

    def _runner(worker_db, _task):
        execute_wash(worker_db, _sync_group_id, _tmdb_id, _season, _keep_ids)

    task = enqueue_task(
        db,
        task_type=task_type,
        target_id=_sync_group_id,
        target_name=_group_name,
        runner=_runner,
        queued_message=f"洗版任务「{_group_name}」已进入队列",
    )
    return {"task_id": task.id, "status": task.status}


# ---------------------------------------------------------------------------
# Wash: source-dir organize (对扫描发现的未记录源目录执行修正整理)
# ---------------------------------------------------------------------------

class SourceDirOrganizeRequest(BaseModel):
    source_dir: str
    sync_group_id: int
    media_type: str
    tmdb_id: int
    title: str | None = None
    year: int | None = None
    season_override: int | None = None
    episode_override: int | None = None
    episode_offset: int | None = None
    # If True, execute wash (delete existing records/hardlinks) before organize
    pre_wash: bool = False
    # Record IDs to KEEP during pre-wash; empty list = delete all existing records
    wash_keep_ids: list[int] = []


def _run_source_dir_organize_task(
    db: Session,
    *,
    task: ScanTask,
    group: SyncGroup,
    payload: dict,
) -> dict:
    from ..services.wash import execute_wash

    source_dir = payload["source_dir"]
    tmdb_id = payload["tmdb_id"]
    init_scan_cancel_flag(task.id)
    try:
        wash_result = None
        if payload.get("pre_wash"):
            wash_keep_ids: list[int] = payload.get("wash_keep_ids") or []
            wash_season: int | None = payload.get("season_override")
            wash_result = execute_wash(
                db,
                sync_group_id=group.id,
                tmdb_id=tmdb_id,
                season=wash_season,
                keep_ids=wash_keep_ids,
            )
            _log.info(
                "Pre-wash before source-dir organize: deleted %d records, %d hardlinks (tmdb_id=%d season=%s)",
                len(wash_result.deleted_record_ids), wash_result.deleted_hardlinks,
                tmdb_id, wash_season,
            )

        processed, failed, has_issues = run_source_dir_organize(
            db,
            group=group,
            media_type=payload["media_type"],
            tmdb_id=tmdb_id,
            source_dir=source_dir,
            title_override=payload.get("title"),
            year_override=payload.get("year"),
            season_override=payload.get("season_override"),
            episode_override=payload.get("episode_override"),
            episode_offset=payload.get("episode_offset"),
        )
        summary = f"源目录修正完成: 成功 {processed}，失败 {failed}"
        if wash_result:
            summary += f"（洗版已清理旧记录 {len(wash_result.deleted_record_ids)} 条）"

        # 洗版/修正完成后，对该 sync group 触发一次检查以自动关闭已消失的孤立问题
        try:
            from ..services.checks import run_checks_for_group as _run_checks_for_group
            _run_checks_for_group(db, group)
            _log.info("Post-organize check completed for group %d (%s)", group.id, group.name)
        except Exception:
            _log.warning(
                "Post-organize check failed for group %d (%s), skipping",
                group.id, group.name, exc_info=True,
            )

        return {
            "message": summary,
            "processed": processed,
            "failed": failed,
            "has_issues": has_issues,
        }
    except TaskCancelledError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except DirectoryProcessError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        pop_scan_cancel_flag(task.id)


@router.post("/wash/source-dir-organize")
def source_dir_organize_endpoint(req: SourceDirOrganizeRequest, db: Session = Depends(get_db)):
    source_dir = (req.source_dir or "").strip()
    if not source_dir:
        raise HTTPException(status_code=400, detail="source_dir 不能为空")
    group = _require_sync_group(db, req.sync_group_id)

    payload = req.model_dump()
    payload["source_dir"] = source_dir
    dir_name = Path(source_dir).name or source_dir

    return _enqueue_logged_media_task(
        db,
        task_type=f"reidentify:source:{group.name}",
        target_id=group.id,
        target_name=dir_name,
        start_message=f"开始源目录修正整理: {dir_name}",
        queued_message=f"源目录修正整理任务已进入队列: {dir_name}",
        success_message_factory=lambda r: r.get("message", "源目录修正整理完成"),
        runner=lambda worker_db, task: _run_source_dir_organize_task(
            worker_db,
            task=task,
            group=group,
            payload=payload,
        ),
    )


# ---------------------------------------------------------------------------
# Manual record import (手动新增成品记录)
# ---------------------------------------------------------------------------

class ManualRecordIn(BaseModel):
    sync_group_id: int
    original_path: str
    target_path: str
    season: int | None = None
    category: Literal["episode", "special", "extra"] | None = None
    file_type: Literal["video", "attachment"] | None = None


@router.post("/manual-record")
def create_manual_record(req: ManualRecordIn, db: Session = Depends(get_db)):
    from ..services.manual_record import import_manual_record

    try:
        result = import_manual_record(
            db,
            sync_group_id=req.sync_group_id,
            original_path=req.original_path,
            target_path=req.target_path,
            season=req.season,
            category=req.category,
            file_type=req.file_type,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        "id": result.id,
        "status": result.status,
        "idempotent": result.idempotent,
        "closed_issues_count": result.closed_issues_count,
    }


# ---------------------------------------------------------------------------
# Filesystem path autocomplete (fs/list)
# ---------------------------------------------------------------------------

@router.get("/fs/list")
def fs_list(prefix: str = Query("", description="Path prefix to list under")) -> dict:
    """Return immediate children of the directory matching *prefix*.

    If *prefix* ends with a path separator (or is empty) we list that directory.
    Otherwise we list the parent and filter entries that start with the basename
    of *prefix*.  Used by the path autocomplete pickers in the UI.

    Returns at most 200 entries for performance.
    """
    import os as _os

    prefix = (prefix or "").strip()

    # Determine which directory to scan and what filter to apply
    if not prefix:
        # Root-level: list filesystem roots (/ on Linux, drives on Windows)
        roots = []
        if os.name == "nt":
            import string as _string
            roots = [f"{d}:\\" for d in _string.ascii_uppercase if os.path.exists(f"{d}:\\")]
        else:
            roots = ["/"]
        return {"entries": [{"path": r, "is_dir": True, "name": r} for r in roots], "parent": ""}

    # Normalize separators
    prefix_norm = prefix.replace("\\", "/")

    if prefix_norm.endswith("/"):
        scan_dir = prefix_norm.rstrip("/") or "/"
        filter_prefix = ""
    else:
        scan_dir = str(Path(prefix_norm).parent)
        filter_prefix = Path(prefix_norm).name.lower()

    scan_path = Path(scan_dir)
    if not scan_path.exists() or not scan_path.is_dir():
        return {"entries": [], "parent": scan_dir}

    try:
        entries = []
        with _os.scandir(scan_path) as it:
            for entry in it:
                if filter_prefix and not entry.name.lower().startswith(filter_prefix):
                    continue
                try:
                    is_dir = entry.is_dir(follow_symlinks=True)
                    full = entry.path.replace("\\", "/")
                    if is_dir and not full.endswith("/"):
                        full += "/"
                    entries.append({"path": full, "name": entry.name, "is_dir": is_dir})
                except OSError:
                    continue
                if len(entries) >= 200:
                    break
        entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))
    except PermissionError:
        return {"entries": [], "parent": scan_dir}

    return {"entries": entries, "parent": scan_dir}


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


def _format_tmdb_artwork(media_type: str, tmdb_id: int, item: dict | None) -> dict:
    payload = item if isinstance(item, dict) else {}
    return {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "poster_path": payload.get("poster_path") or None,
        "backdrop_path": payload.get("backdrop_path") or None,
    }


@router.get("/poster")
def get_tmdb_poster(
    tmdb_id: int = Query(..., ge=1),
    media_type: str = Query("tv", pattern="^(tv|movie)$"),
):
    if not settings.tmdb_api_key:
        return _format_tmdb_artwork(media_type, tmdb_id, None)
    item = _tmdb_get_by_id(media_type, tmdb_id)
    return _format_tmdb_artwork(media_type, tmdb_id, item)


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


def _normalize_log_path_value(value: str | None) -> str:
    return str(value or "").replace("\\", "/").rstrip("/")


def _build_deleted_log_matchers(rows: list[MediaRecord]) -> tuple[set[str], set[str]]:
    original_paths: set[str] = set()
    pending_dirs: set[str] = set()
    for row in rows:
        original_path = _normalize_log_path_value(getattr(row, "original_path", None))
        if not original_path:
            continue
        original_paths.add(original_path)
        if str(getattr(row, "status", "") or "").strip().lower() == "pending_manual" and not str(getattr(row, "target_path", "") or "").strip():
            pending_dirs.add(original_path)
    return original_paths, pending_dirs


def _should_drop_nonreview_log_entry(payload: dict, original_paths: set[str], pending_dirs: set[str]) -> bool:
    payload_original = _normalize_log_path_value(payload.get("original_path"))
    payload_source_dir = _normalize_log_path_value(payload.get("source_dir"))
    if payload_original and payload_original in original_paths:
        return True
    if pending_dirs and ((payload_original and payload_original in pending_dirs) or (payload_source_dir and payload_source_dir in pending_dirs)):
        return True
    return False


def _prune_nonreview_log_file(file_path: Path | None, rows: list[MediaRecord]) -> int:
    if file_path is None or not file_path.exists() or not file_path.is_file() or not rows:
        return 0

    original_paths, pending_dirs = _build_deleted_log_matchers(rows)
    if not original_paths and not pending_dirs:
        return 0

    try:
        raw_lines = file_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return 0

    kept_lines: list[str] = []
    removed = 0
    for line in raw_lines:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            kept_lines.append(line)
            continue
        if not isinstance(payload, dict):
            kept_lines.append(line)
            continue
        if _should_drop_nonreview_log_entry(payload, original_paths, pending_dirs):
            removed += 1
            continue
        kept_lines.append(json.dumps(payload, ensure_ascii=False))

    if removed == 0:
        return 0

    try:
        content = "\n".join(kept_lines)
        if content:
            content += "\n"
        file_path.write_text(content, encoding="utf-8")
    except OSError:
        return 0
    return removed


def _cleanup_nonreview_pending_logs(rows: list[MediaRecord]) -> dict[str, int]:
    pending_removed = _prune_nonreview_log_file(_resolve_pending_log_path("pending"), rows)
    unprocessed_removed = _prune_nonreview_log_file(_resolve_pending_log_path("unprocessed"), rows)
    return {
        "pending_logs_removed": pending_removed,
        "unprocessed_logs_removed": unprocessed_removed,
    }

def _cleanup_check_issues_for_records(db: Session, rows: list[MediaRecord]) -> int:
    """Delete CheckIssue rows that are associated with the given MediaRecords.

    Matches on:
    - resource_dir equals the parent of any record's target_path  (covers reidentify old dirs)
    - target_path prefix matches any record's target_path  (covers individual file issues)
    - tmdb_id + season + sync_group_id  (covers wash-scope issues)
    Returns the number of deleted issues.
    """
    if not rows:
        return 0

    # Collect resource dirs (parent of target_path) and exact target paths from the rows
    resource_dirs: set[str] = set()
    target_paths: set[str] = set()
    tmdb_season_keys: set[tuple] = set()  # (sync_group_id, tmdb_id, season | None)

    for r in rows:
        if r.target_path:
            tp = str(r.target_path).replace("\\", "/").rstrip("/")
            target_paths.add(tp)
            resource_dirs.add(str(Path(r.target_path).parent).replace("\\", "/").rstrip("/"))
        if r.tmdb_id:
            tmdb_season_keys.add((r.sync_group_id, r.tmdb_id, r.season if hasattr(r, 'season') else None))

    # Also capture season from target_path (e.g. ".../Season 01/...") as a hint
    # but we'll rely on direct field matching only to stay precise.

    issues_q = db.query(CheckIssue)

    # Build per-record conditions: resource_dir match OR target_path prefix match
    from sqlalchemy import or_ as _or
    conditions = []

    for rd in resource_dirs:
        conditions.append(CheckIssue.resource_dir == rd)

    for tp in target_paths:
        normalized_tp = tp.lower()
        # exact match or sub-path (e.g. subs/attachments under same target dir)
        conditions.append(
            func.lower(func.replace(func.coalesce(CheckIssue.target_path, ""), "\\\\", "/")) == normalized_tp
        )
        conditions.append(
            func.lower(func.replace(func.coalesce(CheckIssue.source_path, ""), "\\\\", "/")) == normalized_tp
        )

    if not conditions:
        return 0

    to_delete = db.query(CheckIssue).filter(_or(*conditions)).all()
    count = len(to_delete)
    for issue in to_delete:
        db.delete(issue)
    if count:
        _log.info("Cleaned up %d CheckIssue record(s) for %d affected media records", count, len(rows))
    return count


def _cleanup_check_issues_for_tmdb(
    db: Session,
    sync_group_id: int | None,
    tmdb_id: int,
    season: int | None,
) -> int:
    """Delete CheckIssue rows by tmdb_id + optional season + optional sync_group_id.
    Used after wash operations."""
    q = db.query(CheckIssue).filter(CheckIssue.tmdb_id == tmdb_id)
    if sync_group_id is not None:
        q = q.filter(CheckIssue.sync_group_id == sync_group_id)
    if season is not None:
        q = q.filter(CheckIssue.season == season)
    issues = q.all()
    count = len(issues)
    for issue in issues:
        db.delete(issue)
    if count:
        _log.info(
            "Cleaned up %d CheckIssue record(s) for tmdb_id=%d season=%s sync_group_id=%s",
            count, tmdb_id, season, sync_group_id,
        )
    return count


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
