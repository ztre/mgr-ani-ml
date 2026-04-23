"""Subtitle batch import service for manual attachment handling.

Flow
----
1. ``preview_subtitle_batch`` – parses each subtitle path, matches it to a
   video MediaRecord by episode number, and returns a preview list that the
   frontend can show in an editable table (user can fix episode/language).

2. ``import_subtitle_batch`` – takes the (possibly user-corrected) list,
   copies each subtitle to the backup directory (``subtitle_backup_root``),
   then creates a hardlink inside the resource's Season directory.  Both
   paths share the same inode so deleting the resource target hardlink later
   leaves the backup file intact.

Constraints
-----------
* ``subtitle_backup_root`` and the sync-group target directory **must** be on
  the same filesystem – ``os.link`` cannot span filesystems.  A clear error
  message is surfaced when this is not the case.
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import uuid as _uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from ..config import settings
from ..models import ManualAttachmentBackup, MediaRecord, SyncGroup
from .library_mutations import upsert_inode_record, upsert_media_record
from .linker import is_same_inode
from .media_content_types import EXTRA_CATEGORIES, SPECIAL_CATEGORIES
from .parser import _extract_subtitle_lang, extract_bracket_episode, parse_tv_filename
from .resource_tree import extract_movie_resource_dir, extract_show_dir

_log = logging.getLogger(__name__)

# Supported subtitle file extensions (lower-case)
SUBTITLE_EXTS: frozenset[str] = frozenset({
    ".ass", ".srt", ".ssa", ".vtt", ".sup", ".idx", ".sub",
})

# Language suffix options exposed to the frontend for the editable dropdown
LANG_OPTIONS: list[str] = [
    ".zh-CN", ".zh-TW", ".ja", ".en", ".zh-CN.ja", ".zh-TW.ja",
]


# ---------------------------------------------------------------------------
# Staging helpers (for browser-uploaded files)
# ---------------------------------------------------------------------------

def stage_subtitle_uploads(files_data: list[tuple[str, bytes]]) -> tuple[str, list[Path]]:
    """Save uploaded subtitle bytes to an isolated staging directory.

    Parameters
    ----------
    files_data:
        List of ``(filename, raw_bytes)`` pairs from the multipart upload.

    Returns
    -------
    (staging_token, staged_paths)
        *staging_token* is a UUID string identifying the directory;
        *staged_paths* are the absolute paths of the saved files.
    """
    staging_token = str(_uuid.uuid4())
    staging_dir = Path(settings.subtitle_backup_root) / ".staging" / staging_token
    try:
        staging_dir.mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError) as exc:
        raise ValueError(
            f"无法创建字幕备份目录 '{settings.subtitle_backup_root}'：{exc}。"
            "请在「系统设置 → 字幕备份根目录」中配置一个可写路径。"
        ) from exc

    staged_paths: list[Path] = []
    for raw_name, content in files_data:
        safe_name = Path(raw_name).name  # strip any directory components from client
        if not safe_name:
            continue
        dest = staging_dir / safe_name
        dest.write_bytes(content)
        staged_paths.append(dest)

    return staging_token, staged_paths


def cleanup_staging(staging_token: str) -> None:
    """Remove the staging directory for *staging_token* (best-effort)."""
    staging_dir = Path(settings.subtitle_backup_root) / ".staging" / staging_token
    if staging_dir.exists():
        shutil.rmtree(staging_dir, ignore_errors=True)
        _log.debug("Removed subtitle staging dir: %s", staging_dir)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SubtitlePreviewItem:
    subtitle_path: str            # filename (used as import key)
    filename: str                 # basename
    parsed_episode: int | None    # auto-parsed episode number (may be None)
    parsed_lang: str | None       # auto-parsed language suffix (e.g. ".zh-CN")
    parsed_category: str | None   # "episode" | "special" | "extra"
    matched_video_id: int | None  # MediaRecord.id of the matched video
    matched_video_stem: str | None  # stem of the matched video's target filename
    proposed_target: str | None   # proposed target path inside resource dir
    proposed_backup: str | None   # proposed path inside subtitle_backup_root
    has_conflict: bool            # True if proposed_target already exists on disk
    match_found: bool             # True if a matching video record was found


@dataclass(frozen=True)
class SubtitleVideoCandidate:
    id: int
    stem: str
    episode: int | None
    category: str
    target_path: str


@dataclass
class SubtitlePreviewResult:
    items: list[SubtitlePreviewItem]
    available_videos: list[SubtitleVideoCandidate]


@dataclass
class SubtitleImportItem:
    subtitle_path: str   # source subtitle file path
    episode: int | None  # TV may carry a corrected episode; movie can be empty
    lang: str            # language suffix (e.g. ".zh-CN")
    matched_video_id: int  # MediaRecord.id of the target video


@dataclass
class SubtitleImportResult:
    imported: int
    errors: list[dict] = field(default_factory=list)  # [{subtitle_path, reason}]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SXEYY_RE = re.compile(r"[sS]\d{1,2}[eE](\d{1,3})")

# Keywords mapped to the same 3 categories used by manual record import.
_SPECIAL_RE = re.compile(
    r"(?i)(\b|[^a-z0-9])(sp|special|ova|oav|ona|oped|nced|ncop)([^a-z0-9]|$)"
)
_EXTRA_RE = re.compile(
    r"(?i)(\b|[^a-z0-9])(extra|pv|cm|preview|trailer|teaser|menu|making|interview|mv|iv)([^a-z0-9]|$)"
)


def _normalize_content_category(category: str | None) -> str:
    raw = str(category or "").strip().lower()
    if not raw or raw == "episode":
        return "episode"
    if raw in SPECIAL_CATEGORIES:
        return "special"
    if raw == "extra" or raw in EXTRA_CATEGORIES:
        return "extra"
    return raw


def _parse_category_from_subtitle(filename: str) -> str | None:
    """Parse subtitle category into the same 3 buckets as manual record import."""
    name = Path(filename).stem  # strip final extension for cleaner matching
    if _SPECIAL_RE.search(name):
        return "special"
    if _EXTRA_RE.search(name):
        return "extra"
    return "episode"


def _parse_episode_from_subtitle(path_str: str) -> int | None:
    """Best-effort episode number extraction from a subtitle filename."""
    name = Path(path_str).name
    # 1) bracket episode: [01], [02], …
    ep, _ = extract_bracket_episode(name)
    if ep is not None:
        return ep
    # 2) full TV parse
    result = parse_tv_filename(name)
    if result is not None and result.episode is not None:
        return result.episode
    return None


def _normalize_path(value: str | None) -> str:
    return str(value or "").replace("\\", "/").rstrip("/")


def _resolve_video_episode(row: MediaRecord) -> int | None:
    for raw_path in (row.target_path, row.original_path):
        name = Path(str(raw_path or "")).name
        if not name:
            continue
        match = _SXEYY_RE.search(name)
        if match:
            return int(match.group(1))
        parsed = parse_tv_filename(name)
        if parsed is not None and parsed.episode is not None:
            return parsed.episode
        bracket_episode, _ = extract_bracket_episode(name)
        if bracket_episode is not None:
            return bracket_episode
    return None


def _load_preview_video_rows(
    db: Session,
    sync_group_id: int,
    media_type: str,
    tmdb_id: int | None,
    season: int | None,
    resource_dir: str | None,
) -> list[MediaRecord]:
    base_query = (
        db.query(MediaRecord)
        .filter(
            MediaRecord.sync_group_id == sync_group_id,
            MediaRecord.type == media_type,
            MediaRecord.file_type == "video",
            MediaRecord.target_path.isnot(None),
            MediaRecord.target_path != "",
        )
    )

    if media_type == "tv":
        if season is None:
            return []
        base_query = base_query.filter(MediaRecord.season == season)

    if tmdb_id is not None:
        by_tmdb = base_query.filter(MediaRecord.tmdb_id == tmdb_id).all()
        if by_tmdb:
            return by_tmdb

    normalized_resource_dir = _normalize_path(resource_dir)
    if normalized_resource_dir:
        like_prefix = f"{normalized_resource_dir}/%"
        by_resource_dir = base_query.filter(MediaRecord.target_path.like(like_prefix)).all()
        if by_resource_dir:
            return by_resource_dir
        return []

    return base_query.all()


def _compute_subtitle_target(video_target_path: str, lang: str, sub_ext: str) -> Path:
    """Return the subtitle's target path alongside the video file."""
    vp = Path(video_target_path)
    return vp.parent / f"{vp.stem}{lang}{sub_ext}"


def _compute_subtitle_backup(
    subtitle_filename: str,
    video_target_path: str,
    group_name: str,
    media_type: str,
) -> Path:
    """Return the subtitle's backup path under ``subtitle_backup_root``."""
    vp = Path(video_target_path)
    backup_root = Path(settings.subtitle_backup_root)
    backup_base = backup_root / _sanitize_segment(group_name)

    normalized_media_type = str(media_type or "").strip().lower()
    if normalized_media_type == "tv":
        resource_dir = Path(extract_show_dir(video_target_path))
        season_dir = vp.parent
        return backup_base / resource_dir.name / season_dir.name / subtitle_filename

    resource_dir = Path(extract_movie_resource_dir(video_target_path))
    backup_path = backup_base / resource_dir.name
    try:
        relative_parent = vp.parent.relative_to(resource_dir)
    except ValueError:
        relative_parent = Path()
    if str(relative_parent) not in {"", "."}:
        backup_path = backup_path / relative_parent
    return backup_path / subtitle_filename


def _sanitize_segment(name: str) -> str:
    """Strip filesystem-unsafe characters from a path segment."""
    for ch in ('/', '\\', ':', '*', '?', '"', '<', '>', '|'):
        name = name.replace(ch, '_')
    return name


def _preview_item_sort_key(item: SubtitlePreviewItem) -> tuple[int, int, str]:
    parsed_episode = item.parsed_episode
    return (
        parsed_episode is None,
        parsed_episode or 0,
        str(item.filename or "").lower(),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def preview_subtitle_batch(
    db: Session,
    sync_group_id: int,
    media_type: str,
    tmdb_id: int | None,
    season: int | None,
    content_category: str | None,
    resource_dir: str | None,
    filenames: list[str],
) -> SubtitlePreviewResult:
    """Parse subtitle filenames and propose target/backup paths.

    Accepts bare filenames (no file upload required at preview stage).
    Returns a list of :class:`SubtitlePreviewItem` — one per input filename.
    The frontend renders these in an editable table so the user can correct
    auto-detected episode numbers and language suffixes before confirming
    the import. Video matching is constrained by the selected content
    category from the dialog context.
    """
    video_rows = _load_preview_video_rows(
        db,
        sync_group_id=sync_group_id,
        media_type=media_type,
        tmdb_id=tmdb_id,
        season=season,
        resource_dir=resource_dir,
    )

    # Build (category, episode) → video record map.
    normalized_media_type = str(media_type or "tv").strip().lower()
    selected_category = _normalize_content_category(content_category)
    cat_ep_to_video: dict[tuple[str | None, int], MediaRecord] = {}
    category_video_rows: list[MediaRecord] = []
    available_videos: list[SubtitleVideoCandidate] = []
    for row in video_rows:
        if row.id is None or not row.target_path:
            continue
        ep = _resolve_video_episode(row) if normalized_media_type == "tv" else None
        cat = _normalize_content_category(row.category)
        if cat != selected_category:
            continue
        category_video_rows.append(row)
        if ep is not None:
            key = (cat, ep)
            if key not in cat_ep_to_video:
                cat_ep_to_video[key] = row
        available_videos.append(SubtitleVideoCandidate(
            id=int(row.id),
            stem=Path(str(row.target_path)).stem,
            episode=ep,
            category=cat,
            target_path=str(row.target_path),
        ))

    # Get group name for backup path construction
    group = db.query(SyncGroup).filter(SyncGroup.id == sync_group_id).first()
    group_name = group.name if group else str(sync_group_id)

    items: list[SubtitlePreviewItem] = []
    for filename in filenames:
        sub_ext = Path(filename).suffix.lower()

        parsed_ep = _parse_episode_from_subtitle(filename)
        parsed_lang = _extract_subtitle_lang(filename)
        parsed_cat = _parse_category_from_subtitle(filename)

        matched_video: MediaRecord | None = None
        if normalized_media_type == "movie":
            if len(category_video_rows) == 1:
                matched_video = category_video_rows[0]
        elif parsed_ep is not None:
            matched_video = cat_ep_to_video.get((selected_category, parsed_ep))

        proposed_target: str | None = None
        proposed_backup: str | None = None
        has_conflict = False

        if matched_video and matched_video.target_path and parsed_lang and sub_ext:
            target_path = _compute_subtitle_target(
                str(matched_video.target_path), parsed_lang, sub_ext
            )
            backup_path = _compute_subtitle_backup(
                target_path.name,
                str(matched_video.target_path),
                group_name,
                str(matched_video.type or normalized_media_type),
            )
            proposed_target = str(target_path)
            proposed_backup = str(backup_path)
            has_conflict = target_path.exists()

        items.append(SubtitlePreviewItem(
            subtitle_path=filename,
            filename=filename,
            parsed_episode=parsed_ep,
            parsed_lang=parsed_lang,
            parsed_category=parsed_cat,
            matched_video_id=(
                int(matched_video.id)
                if matched_video and matched_video.id is not None
                else None
            ),
            matched_video_stem=(
                Path(str(matched_video.target_path)).stem
                if matched_video and matched_video.target_path
                else None
            ),
            proposed_target=proposed_target,
            proposed_backup=proposed_backup,
            has_conflict=has_conflict,
            match_found=matched_video is not None,
        ))


    items.sort(key=_preview_item_sort_key)
    available_videos.sort(key=lambda item: (item.episode is None, item.episode or 0, item.stem.lower()))
    return SubtitlePreviewResult(items=items, available_videos=available_videos)


def import_subtitle_batch(
    db: Session,
    sync_group_id: int,
    items: list[SubtitleImportItem],
) -> SubtitleImportResult:
    """Import subtitle files: copy to backup dir, hardlink into resource dir.

    Each item carries the (possibly user-corrected) episode, language, and the
    matched video's MediaRecord id.  The server re-derives the target and
    backup paths from the video record — client-provided paths are not trusted.
    """
    group = db.query(SyncGroup).filter(SyncGroup.id == sync_group_id).first()
    if not group:
        raise ValueError(f"同步组 {sync_group_id} 不存在")
    group_name = group.name

    imported = 0
    errors: list[dict] = []

    for item in items:
        try:
            did_import = _import_single_subtitle(db, sync_group_id, group_name, item)
            if did_import is not False:
                imported += 1
        except Exception as exc:
            _log.warning("字幕导入失败 %s: %s", item.subtitle_path, exc)
            errors.append({"subtitle_path": item.subtitle_path, "reason": str(exc)})

    if imported:
        db.commit()

    return SubtitleImportResult(imported=imported, errors=errors)


def _import_single_subtitle(
    db: Session,
    sync_group_id: int,
    group_name: str,
    item: SubtitleImportItem,
) -> None:
    """Core import logic for a single subtitle file."""
    # --- validate source file ---
    sub_path = Path(item.subtitle_path)
    if not sub_path.exists() or not sub_path.is_file():
        raise ValueError(f"字幕文件不存在: {item.subtitle_path}")

    # --- validate matched video record ---
    video_row = (
        db.query(MediaRecord)
        .filter(MediaRecord.id == item.matched_video_id)
        .first()
    )
    if not video_row or not video_row.target_path:
        raise ValueError(f"找不到匹配视频记录 id={item.matched_video_id}")
    if video_row.sync_group_id != sync_group_id:
        raise ValueError(
            f"视频记录 id={item.matched_video_id} 不属于同步组 {sync_group_id}"
        )

    sub_ext = sub_path.suffix.lower()
    lang = item.lang if item.lang.startswith(".") else f".{item.lang}"

    # --- compute paths (server-authoritative) ---
    target_path = _compute_subtitle_target(str(video_row.target_path), lang, sub_ext)
    backup_path = _compute_subtitle_backup(
        target_path.name,
        str(video_row.target_path),
        group_name,
        str(video_row.type or "tv"),
    )

    # --- idempotency: if target exists and is already linked to backup ---
    if target_path.exists():
        if backup_path.exists() and is_same_inode(target_path, backup_path):
            _log.info("字幕导入幂等跳过: %s", target_path)
            return False  # skipped, not an error
        raise ValueError(f"目标路径已存在且非本次备份硬链接: {target_path}")

    # --- copy subtitle to backup directory ---
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    if backup_path.exists():
        # Stale backup from a previous partial run — replace it
        backup_path.unlink()
    shutil.copy2(str(sub_path), str(backup_path))

    # --- create hardlink in resource target directory ---
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(str(backup_path), str(target_path))
    except OSError as exc:
        # Clean up the backup file to avoid stale state
        try:
            backup_path.unlink()
        except OSError:
            pass
        raise ValueError(
            f"创建硬链接失败 ({backup_path} → {target_path}): {exc}。"
            "请确认字幕备份目录与目标目录在同一文件系统。"
        ) from exc

    # --- write MediaRecord ---
    record = upsert_media_record(
        db,
        sync_group_id=sync_group_id,
        src_path=backup_path,
        dst_path=target_path,
        media_type=str(video_row.type or "tv"),
        tmdb_id=video_row.tmdb_id,
        status="manual_fixed",
        season=video_row.season,
        episode=video_row.episode,
        category=video_row.category,
        file_type="attachment",
    )
    record.season = video_row.season
    record.episode = video_row.episode
    record.category = video_row.category
    record.file_type = "attachment"
    db.flush()

    # --- write ManualAttachmentBackup row ---
    backup_row = ManualAttachmentBackup(
        media_record_id=record.id,
        backup_path=str(backup_path),
        created_at=datetime.now(timezone.utc),
    )
    db.add(backup_row)

    # --- write InodeRecord (inode shared between backup and target) ---
    upsert_inode_record(
        db,
        sync_group_id=sync_group_id,
        src_path=backup_path,
        dst_path=target_path,
    )

    _log.info(
        "字幕导入成功: %s → target=%s backup=%s",
        item.subtitle_path, target_path, backup_path,
    )
