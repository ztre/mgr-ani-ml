"""Unified library mutation helpers.

All writes that touch MediaRecord / InodeRecord / DirectoryState, plus
filesystem operations (hardlink deletion, empty-dir cleanup), should go
through these functions so the logic is not scattered across callers.

Note: scanner.py still uses its own private _upsert_* helpers internally;
v1 of this module is used exclusively by new features (checks, wash, manual
record).  A future refactor can consolidate them.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from ..models import DirectoryState, InodeRecord, MediaRecord
from .linker import get_file_identity, get_inode
from .media_record_metadata import infer_media_record_metadata

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MediaRecord helpers
# ---------------------------------------------------------------------------

def upsert_media_record(
    db: Session,
    sync_group_id: int,
    src_path: Path | str,
    dst_path: Path | str,
    media_type: str,
    tmdb_id: int | None,
    status: str = "scraped",
    *,
    season: int | None = None,
    episode: int | None = None,
    category: str | None = None,
    file_type: str | None = None,
) -> MediaRecord:
    """Insert or update a MediaRecord; returns the (possibly existing) row."""
    src_path = Path(src_path)
    dst_path = Path(dst_path)

    try:
        src_size = src_path.stat().st_size
    except OSError:
        src_size = 0

    metadata = infer_media_record_metadata(
        media_type=media_type,
        original_path=src_path,
        target_path=dst_path,
        season=season,
        episode=episode,
        category=category,
        file_type=file_type,
    )

    existing = (
        db.query(MediaRecord)
        .filter(
            MediaRecord.sync_group_id == sync_group_id,
            MediaRecord.original_path == str(src_path),
        )
        .first()
    )

    if existing:
        existing.target_path = str(dst_path)
        existing.type = media_type
        existing.tmdb_id = tmdb_id
        existing.status = status
        existing.size = src_size
        existing.season = metadata.season
        existing.episode = metadata.episode
        existing.category = metadata.category
        existing.file_type = metadata.file_type
        existing.updated_at = datetime.now(timezone.utc)
        return existing

    row = MediaRecord(
        sync_group_id=sync_group_id,
        original_path=str(src_path),
        target_path=str(dst_path),
        type=media_type,
        tmdb_id=tmdb_id,
        bangumi_id=None,
        status=status,
        size=src_size,
        season=metadata.season,
        episode=metadata.episode,
        category=metadata.category,
        file_type=metadata.file_type,
    )
    db.add(row)
    db.flush()  # populate row.id without committing
    return row


def delete_media_record(db: Session, record_id: int) -> None:
    """Delete a MediaRecord by primary key (no-op if not found)."""
    row = db.query(MediaRecord).filter(MediaRecord.id == record_id).first()
    if row:
        db.delete(row)


# ---------------------------------------------------------------------------
# InodeRecord helpers
# ---------------------------------------------------------------------------

def find_inode_record_by_identity(
    db: Session,
    *,
    device: int | None,
    inode: int | None,
) -> InodeRecord | None:
    if inode is None:
        return None
    if device is None:
        try:
            return db.query(InodeRecord).filter(InodeRecord.inode == int(inode)).first()
        except Exception:
            return None
    try:
        return (
            db.query(InodeRecord)
            .filter(InodeRecord.device == int(device), InodeRecord.inode == int(inode))
            .first()
        )
    except Exception:
        try:
            return db.query(InodeRecord).filter(InodeRecord.inode == int(inode)).first()
        except Exception:
            return None


def find_inode_record_by_path(db: Session, path: Path | str) -> InodeRecord | None:
    identity = get_file_identity(path)
    if identity is None:
        return None
    return find_inode_record_by_identity(db, device=identity[0], inode=identity[1])


def upsert_inode_record(
    db: Session,
    sync_group_id: int,
    src_path: Path | str,
    dst_path: Path | str,
) -> InodeRecord | None:
    """Insert or update an InodeRecord; returns None if inode cannot be read."""
    src_path = Path(src_path)
    dst_path = Path(dst_path)

    identity = get_file_identity(src_path)
    if identity is None:
        return None
    device, ino = identity

    try:
        src_size = src_path.stat().st_size
    except OSError:
        src_size = 0

    existing = find_inode_record_by_identity(db, device=device, inode=ino)
    if existing:
        existing.device = int(device)
        existing.inode = int(ino)
        existing.source_path = str(src_path)
        existing.target_path = str(dst_path)
        existing.sync_group_id = sync_group_id
        existing.size = src_size
        existing.updated_at = datetime.now(timezone.utc)
        return existing

    row = InodeRecord(
        device=int(device),
        inode=ino,
        source_path=str(src_path),
        target_path=str(dst_path),
        sync_group_id=sync_group_id,
        size=src_size,
    )
    db.add(row)
    db.flush()
    return row


def delete_orphan_inode_records(db: Session, target_paths: list[str]) -> int:
    """Delete InodeRecords whose target_path is in *target_paths* and the file
    no longer exists on the filesystem.  Returns the number of rows deleted."""
    deleted = 0
    if not target_paths:
        return 0
    rows = (
        db.query(InodeRecord)
        .filter(InodeRecord.target_path.in_(target_paths))
        .all()
    )
    for row in rows:
        tp = row.target_path
        if tp and not Path(tp).exists():
            db.delete(row)
            deleted += 1
    return deleted


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def delete_target_hardlink(target_path: str | Path) -> bool:
    """Delete a hardlink at *target_path*.

    Returns True if the file was deleted, False if it did not exist.
    Raises OSError on unexpected failure.
    """
    p = Path(target_path)
    if not p.exists():
        return False
    p.unlink()
    _log.debug("Deleted hardlink: %s", p)
    return True


def cleanup_empty_dirs(start_path: str | Path, stop_at: str | Path | None = None) -> None:
    """Walk upward from *start_path* (or its parent if it is a file) and remove
    empty directories until reaching *stop_at* or the filesystem root."""
    p = Path(start_path)
    if p.is_file():
        p = p.parent
    stop = Path(stop_at) if stop_at else None
    current = p
    while True:
        if stop and current == stop:
            break
        if current == current.parent:
            break  # filesystem root
        try:
            entries = list(current.iterdir())
        except OSError:
            break
        if entries:
            break
        try:
            current.rmdir()
            _log.debug("Removed empty directory: %s", current)
        except OSError:
            break
        current = current.parent


# ---------------------------------------------------------------------------
# DirectoryState helpers
# ---------------------------------------------------------------------------

def cleanup_orphan_directory_states(
    db: Session,
    sync_group_id: int,
    target_dir: str | Path,
) -> None:
    """Remove DirectoryState records under *target_dir* for *sync_group_id*
    when the corresponding directory no longer exists."""
    prefix = str(target_dir).rstrip("/") + "/"
    rows = (
        db.query(DirectoryState)
        .filter(
            DirectoryState.sync_group_id == sync_group_id,
            DirectoryState.dir_path.like(prefix + "%"),
        )
        .all()
    )
    for row in rows:
        if not Path(row.dir_path).exists():
            db.delete(row)
