"""Shared path filters for checks-related subtitle exclusions."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import func, or_

from ...config import settings

SUBTITLE_EXTS = frozenset({".ass", ".srt", ".ssa", ".vtt", ".sup", ".idx", ".sub"})


def _normalize_path_text(path_value: str | Path | None) -> str:
    return str(path_value or "").replace("\\", "/").strip()


def is_subtitle_file_path(path_value: str | Path | None) -> bool:
    normalized = _normalize_path_text(path_value).lower()
    if not normalized:
        return False
    return any(normalized.endswith(ext) for ext in SUBTITLE_EXTS)


def is_subtitle_backup_path(path_value: str | Path | None) -> bool:
    normalized = _normalize_path_text(path_value).rstrip("/").lower()
    backup_root = _normalize_path_text(getattr(settings, "subtitle_backup_root", "")).rstrip("/").lower()
    if not normalized or not backup_root:
        return False
    return normalized == backup_root or normalized.startswith(f"{backup_root}/")


def is_subtitle_related_path(path_value: str | Path | None) -> bool:
    return is_subtitle_file_path(path_value) or is_subtitle_backup_path(path_value)


def is_subtitle_related_issue(
    *,
    source_path: str | Path | None = None,
    target_path: str | Path | None = None,
    resource_dir: str | Path | None = None,
) -> bool:
    return any(
        is_subtitle_related_path(path_value)
        for path_value in (source_path, target_path, resource_dir)
    )


def build_subtitle_issue_filter(model):
    """Return a SQL expression that matches subtitle-related check issues."""

    def _normalized_col(column):
        return func.lower(func.replace(func.coalesce(column, ""), "\\", "/"))

    conditions = []
    for ext in SUBTITLE_EXTS:
        pattern = f"%{ext}"
        conditions.append(_normalized_col(model.source_path).like(pattern))
        conditions.append(_normalized_col(model.target_path).like(pattern))

    backup_root = _normalize_path_text(getattr(settings, "subtitle_backup_root", "")).rstrip("/").lower()
    if backup_root:
        backup_prefix = f"{backup_root}/%"
        conditions.extend([
            _normalized_col(model.source_path) == backup_root,
            _normalized_col(model.source_path).like(backup_prefix),
            _normalized_col(model.target_path) == backup_root,
            _normalized_col(model.target_path).like(backup_prefix),
            _normalized_col(model.resource_dir) == backup_root,
            _normalized_col(model.resource_dir).like(backup_prefix),
        ])

    return or_(*conditions)