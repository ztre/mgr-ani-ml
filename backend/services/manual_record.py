"""Manual record import service.

Allows users to import an already-processed media file (same-inode hardlink
pair) as a `manual_fixed` MediaRecord, bypassing the scan pipeline.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from ..models import CheckIssue, MediaRecord, SyncGroup
from .library_mutations import upsert_inode_record, upsert_media_record
from .linker import get_file_identity

_log = logging.getLogger(__name__)

_TMDB_ID_RE = re.compile(r"\[tmdbid=(\d+)\]", re.IGNORECASE)


def _parse_tmdb_id_from_path(path: str) -> int | None:
    m = _TMDB_ID_RE.search(path)
    return int(m.group(1)) if m else None


@dataclass
class ManualImportResult:
    id: int
    status: str  # "created" | "updated" | "idempotent"
    idempotent: bool
    closed_issues_count: int


def import_manual_record(
    db: Session,
    sync_group_id: int,
    original_path: str,
    target_path: str,
    season: int | None = None,
    episode: int | None = None,
    category: str | None = None,
    file_type: str | None = None,
) -> ManualImportResult:
    """Validate and import a manual_fixed MediaRecord.

    Raises:
        ValueError  with a descriptive message on validation failure.
    """
    src = Path(original_path)
    dst = Path(target_path)

    # --- Validate sync group ---
    group = db.query(SyncGroup).filter(SyncGroup.id == sync_group_id).first()
    if not group:
        raise ValueError(f"同步组 {sync_group_id} 不存在")
    if not group.enabled:
        raise ValueError(f"同步组 {group.name!r} 未启用")

    # --- Validate file existence ---
    if not src.exists():
        raise ValueError(f"original_path 文件不存在: {original_path}")
    if not src.is_file():
        raise ValueError(f"original_path 不是文件: {original_path}")
    if not dst.exists():
        raise ValueError(f"target_path 文件不存在: {target_path}")
    if not dst.is_file():
        raise ValueError(f"target_path 不是文件: {target_path}")

    # --- Validate same inode ---
    src_identity = get_file_identity(src)
    dst_identity = get_file_identity(dst)
    if src_identity is None or dst_identity is None:
        raise ValueError("无法读取文件 inode")
    if src_identity != dst_identity:
        raise ValueError(
            f"original_path 和 target_path 不是同一 inode "
            f"(src={src_identity}, dst={dst_identity})，拒绝导入"
        )

    # --- Determine media type ---
    media_type = group.source_type
    if media_type not in ("tv", "movie"):
        raise ValueError(f"media type 必须是 tv 或 movie，得到: {media_type!r}")

    # --- Determine tmdb_id from target_path ---
    tmdb_id = _parse_tmdb_id_from_path(target_path)

    # --- Idempotency check: exact same triple already exists ---
    existing_exact = (
        db.query(MediaRecord)
        .filter(
            MediaRecord.sync_group_id == sync_group_id,
            MediaRecord.original_path == original_path,
            MediaRecord.target_path == target_path,
        )
        .first()
    )
    if existing_exact:
        _log.info(
            "Manual import idempotent: record id=%d (sync_group=%d orig=%s)",
            existing_exact.id, sync_group_id, original_path,
        )
        return ManualImportResult(
            id=existing_exact.id,
            status="idempotent",
            idempotent=True,
            closed_issues_count=0,
        )

    # --- Conflict check: same original_path but different target ---
    existing_conflict = (
        db.query(MediaRecord)
        .filter(
            MediaRecord.sync_group_id == sync_group_id,
            MediaRecord.original_path == original_path,
            MediaRecord.target_path != target_path,
        )
        .first()
    )
    if existing_conflict:
        raise ValueError(
            f"同一 original_path 已有不同 target_path 的记录 (id={existing_conflict.id}, "
            f"existing_target={existing_conflict.target_path!r})，请先处理冲突"
        )

    # --- Write records ---
    record = upsert_media_record(
        db,
        sync_group_id=sync_group_id,
        src_path=src,
        dst_path=dst,
        media_type=media_type,
        tmdb_id=tmdb_id,
        status="manual_fixed",
        season=season,
        episode=episode,
        category=category,
        file_type=file_type,
    )
    # Set manually-specified metadata fields
    if season is not None:
        record.season = season
    if episode is not None:
        record.episode = episode
    if category is not None:
        record.category = category
    if file_type is not None:
        record.file_type = file_type
    upsert_inode_record(db, sync_group_id=sync_group_id, src_path=src, dst_path=dst)

    # Determine whether this is a create or update
    was_new = record.id is None or record.status == "manual_fixed"
    import_status = "created" if was_new else "updated"

    db.flush()

    # --- Resolve related check issues ---
    closed = _resolve_check_issues_for_import(db, sync_group_id, original_path, target_path)

    db.commit()

    _log.info(
        "Manual import %s: id=%d sync_group=%d orig=%s target=%s closed_issues=%d",
        import_status, record.id, sync_group_id, original_path, target_path, closed,
    )

    return ManualImportResult(
        id=record.id,
        status=import_status,
        idempotent=False,
        closed_issues_count=closed,
    )


def _resolve_check_issues_for_import(
    db: Session,
    sync_group_id: int,
    source_path: str,
    target_path: str,
) -> int:
    """Auto-resolve check issues that are now fixed by this import.

    - source_unrecorded issues with matching sync_group_id + source_path → resolved
    - links_orphans / media_path_sanity issues with matching sync_group_id + target_path → resolved
    """
    now = datetime.now(timezone.utc)
    closed = 0

    # source_unrecorded: source_path match
    src_issues = (
        db.query(CheckIssue)
        .filter(
            CheckIssue.sync_group_id == sync_group_id,
            CheckIssue.checker_code == "source_unrecorded",
            CheckIssue.source_path == source_path,
            CheckIssue.status.in_(("open", "claimed")),
        )
        .all()
    )
    for issue in src_issues:
        issue.status = "resolved"
        issue.resolved_at = now
        issue.updated_at = now
        closed += 1

    # links_orphans + media_path_sanity: target_path match
    tgt_issues = (
        db.query(CheckIssue)
        .filter(
            CheckIssue.sync_group_id == sync_group_id,
            CheckIssue.checker_code.in_(("links_orphans", "media_path_sanity")),
            CheckIssue.target_path == target_path,
            CheckIssue.status.in_(("open", "claimed")),
        )
        .all()
    )
    for issue in tgt_issues:
        issue.status = "resolved"
        issue.resolved_at = now
        issue.updated_at = now
        closed += 1

    return closed
