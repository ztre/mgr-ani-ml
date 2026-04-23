from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .media_content_types import EXTRA_CATEGORIES, SPECIAL_CATEGORIES
from .parser import ParseResult, parse_movie_filename, parse_tv_filename
from .resource_tree import extract_tmdb_id_from_path, is_auxiliary_target_path

VIDEO_EXTS = frozenset({".mkv", ".mp4", ".avi", ".mov", ".webm", ".flv", ".ts", ".m2ts"})
ATTACHMENT_EXTS = frozenset({".ass", ".srt", ".ssa", ".vtt", ".mka", ".sup", ".idx", ".sub"})


@dataclass(frozen=True)
class MediaRecordMetadata:
    season: int | None
    episode: int | None
    category: str | None
    file_type: str | None


@dataclass(frozen=True)
class MainVideoSlot:
    sync_group_id: int
    tmdb_id: int
    season: int
    episode: int


def normalize_category_bucket(category: str | None) -> str | None:
    raw = str(category or "").strip().lower()
    if not raw or raw == "episode":
        return "episode"
    if raw == "special" or raw in SPECIAL_CATEGORIES:
        return "special"
    if raw == "extra" or raw in EXTRA_CATEGORIES:
        return "extra"
    return raw


def infer_media_record_metadata(
    *,
    media_type: str,
    original_path: str | Path | None = None,
    target_path: str | Path | None = None,
    parse_result: ParseResult | None = None,
    season: int | None = None,
    episode: int | None = None,
    category: str | None = None,
    file_type: str | None = None,
    is_attachment: bool | None = None,
) -> MediaRecordMetadata:
    normalized_media_type = str(media_type or "tv").strip().lower()
    file_type_value = _infer_file_type(
        file_type=file_type,
        original_path=original_path,
        target_path=target_path,
        is_attachment=is_attachment,
    )
    parsed = parse_result or _parse_best_effort(normalized_media_type, original_path, target_path)

    season_value = season if season is not None else (parsed.season if normalized_media_type == "tv" and parsed else None)
    episode_value = episode if episode is not None else (parsed.episode if normalized_media_type == "tv" and parsed else None)

    raw_category = str(category or "").strip() or None
    if raw_category is not None:
        category_value = raw_category
    elif parsed is not None and parsed.extra_category:
        category_value = parsed.extra_category
    elif file_type_value == "video":
        category_value = "episode"
    elif episode_value is not None:
        category_value = "episode"
    else:
        category_value = None

    return MediaRecordMetadata(
        season=season_value,
        episode=episode_value,
        category=category_value,
        file_type=file_type_value,
    )


def build_main_video_slot(
    *,
    sync_group_id: int | None,
    tmdb_id: int | None,
    media_type: str,
    original_path: str | Path | None = None,
    target_path: str | Path | None = None,
    parse_result: ParseResult | None = None,
    season: int | None = None,
    episode: int | None = None,
    category: str | None = None,
    file_type: str | None = None,
    is_attachment: bool | None = None,
) -> MainVideoSlot | None:
    if sync_group_id is None or str(media_type or "").strip().lower() != "tv":
        return None
    if is_auxiliary_target_path(target_path):
        return None
    resolved_tmdb_id = tmdb_id
    if resolved_tmdb_id is None:
        resolved_tmdb_id = extract_tmdb_id_from_path(target_path) or extract_tmdb_id_from_path(original_path)
    if resolved_tmdb_id is None:
        return None

    metadata = infer_media_record_metadata(
        media_type=media_type,
        original_path=original_path,
        target_path=target_path,
        parse_result=parse_result,
        season=season,
        episode=episode,
        category=category,
        file_type=file_type,
        is_attachment=is_attachment,
    )
    if metadata.file_type != "video":
        return None
    if normalize_category_bucket(metadata.category) != "episode":
        return None
    if metadata.season is None or metadata.episode is None:
        return None

    return MainVideoSlot(
        sync_group_id=int(sync_group_id),
        tmdb_id=int(resolved_tmdb_id),
        season=int(metadata.season),
        episode=int(metadata.episode),
    )


def build_main_video_slot_from_record(row: Any) -> MainVideoSlot | None:
    return build_main_video_slot(
        sync_group_id=getattr(row, "sync_group_id", None),
        tmdb_id=getattr(row, "tmdb_id", None),
        media_type=getattr(row, "type", None) or "tv",
        original_path=getattr(row, "original_path", None),
        target_path=getattr(row, "target_path", None),
        season=getattr(row, "season", None),
        episode=getattr(row, "episode", None),
        category=getattr(row, "category", None),
        file_type=getattr(row, "file_type", None),
    )


def _infer_file_type(
    *,
    file_type: str | None,
    original_path: str | Path | None,
    target_path: str | Path | None,
    is_attachment: bool | None,
) -> str | None:
    raw = str(file_type or "").strip().lower()
    if raw in {"video", "attachment"}:
        return raw
    if is_attachment is True:
        return "attachment"
    if is_attachment is False:
        return "video"

    for value in (target_path, original_path):
        ext = Path(str(value or "")).suffix.lower()
        if ext in ATTACHMENT_EXTS:
            return "attachment"
        if ext in VIDEO_EXTS:
            return "video"
    return None


def _parse_best_effort(media_type: str, original_path: str | Path | None, target_path: str | Path | None) -> ParseResult | None:
    for value in (original_path, target_path):
        candidate = str(value or "").strip()
        if not candidate:
            continue
        name = Path(candidate).name
        if not name:
            continue
        if media_type == "movie":
            parsed = parse_movie_filename(name) or parse_tv_filename(name)
        else:
            parsed = parse_tv_filename(name) or parse_movie_filename(name)
        if parsed is not None:
            return parsed
    return None
