"""Active wash (洗版) service.

Identifies duplicate MediaRecords for the same tmdb_id coming from DIFFERENT
source directories.  Each distinct parent-directory of original_path represents
one "version" of the media (e.g., a BD-rip vs a Web-DL).

Two-phase operation:
  1. get_wash_candidates()  — dry-run preview (synchronous)
  2. execute_wash()         — destructive execution (called from async task)
"""
from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy.orm import Session

from ..models import CheckIssue, MediaRecord, SyncGroup
from .library_mutations import (
    cleanup_empty_dirs,
    cleanup_orphan_directory_states,
    delete_media_record,
    delete_orphan_inode_records,
    delete_target_hardlink,
)

_log = logging.getLogger(__name__)

# Status ordering: higher = more "complete" / preferred
_STATUS_RANK = {
    "scraped": 3,
    "manual_fixed": 2,
    "pending_manual": 1,
    "pending": 0,
}


def _human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def _source_dir(r: MediaRecord) -> str:
    """Parent directory of original_path (= the source download folder)."""
    return str(Path(r.original_path).parent) if r.original_path else ""


@dataclass
class CandidateGroup:
    """All MediaRecords that share the same source directory."""
    key: str            # stable short key (hex digest of source_dir)
    label: str          # source directory path (human label)
    records: list[dict] # serialised MediaRecord dicts
    recommended: bool   # True for the suggested group to keep
    total_size: int     # sum of record sizes in bytes
    size_human: str     # human-readable total_size
    record_count: int   # len(records)


@dataclass
class WashCandidatesResult:
    tmdb_id: int
    sync_group_id: int | None
    season: int | None
    candidate_groups: list[CandidateGroup] = field(default_factory=list)
    total_records: int = 0


@dataclass
class WashResult:
    kept_ids: list[int]
    deleted_record_ids: list[int]
    deleted_hardlinks: int
    deleted_orphan_inodes: int


def _record_to_dict(r: MediaRecord) -> dict:
    size = r.size or 0
    return {
        "id": r.id,
        "sync_group_id": r.sync_group_id,
        "original_path": r.original_path,
        "target_path": r.target_path,
        "type": r.type,
        "tmdb_id": r.tmdb_id,
        "status": r.status,
        "size": size,
        "size_human": _human_size(size),
        "updated_at": str(r.updated_at) if r.updated_at else None,
    }


def _rank_record(r: MediaRecord) -> tuple:
    """Higher tuple = more preferred record to keep."""
    status_rank = _STATUS_RANK.get(r.status or "", 0)
    size = r.size or 0
    updated_ts = r.updated_at.timestamp() if r.updated_at else 0.0
    return (status_rank, size, updated_ts)


def get_wash_candidates(
    db: Session,
    sync_group_id: int | None,
    tmdb_id: int,
    season: int | None = None,
) -> WashCandidatesResult:
    """Return a dry-run preview of what a wash operation would do.

    Records are grouped by their source directory (parent of original_path).
    Only returns candidates when ≥ 2 distinct source directories exist for the
    same tmdb_id — i.e., there are multiple "versions" of the media.
    """
    result = WashCandidatesResult(
        sync_group_id=sync_group_id,
        tmdb_id=tmdb_id,
        season=season,
    )

    q = db.query(MediaRecord).filter(MediaRecord.tmdb_id == tmdb_id)
    if sync_group_id is not None:
        q = q.filter(MediaRecord.sync_group_id == sync_group_id)
    records = q.all()

    if season is not None:
        season_tag = f"Season {season:02d}"
        records = [r for r in records if r.target_path and season_tag in r.target_path]

    result.total_records = len(records)

    # Group by source directory — each dir = one "version" of the media
    groups: dict[str, list[MediaRecord]] = defaultdict(list)
    for r in records:
        groups[_source_dir(r)].append(r)

    # No candidates if only one (or zero) source dirs
    if len(groups) <= 1:
        return result

    # Build CandidateGroup per source dir; rank groups by their best record
    def _group_rank(grp: list[MediaRecord]) -> tuple:
        return max(_rank_record(r) for r in grp)

    ranked = sorted(groups.items(), key=lambda kv: _group_rank(kv[1]), reverse=True)
    best_src_dir = ranked[0][0]

    for src_dir, grp_records in groups.items():
        key = hashlib.md5(src_dir.encode()).hexdigest()[:12]
        total_size = sum(r.size or 0 for r in grp_records)
        cg = CandidateGroup(
            key=key,
            label=src_dir,
            records=[_record_to_dict(r) for r in grp_records],
            recommended=(src_dir == best_src_dir),
            total_size=total_size,
            size_human=_human_size(total_size),
            record_count=len(grp_records),
        )
        result.candidate_groups.append(cg)

    return result


def execute_wash(
    db: Session,
    sync_group_id: int | None,
    tmdb_id: int,
    season: int | None,
    keep_ids: list[int],
) -> WashResult:
    """Execute the wash: delete non-kept records, their hardlinks, and orphan inodes.

    keep_ids: IDs of ALL records in the chosen source-dir group (they are kept).
    All other records for the same tmdb_id (within the same sync_group if given)
    will have their hardlinks and DB entries deleted.

    Does NOT touch source files.
    """
    keep_ids_set = set(keep_ids)

    q = db.query(MediaRecord).filter(MediaRecord.tmdb_id == tmdb_id)
    if sync_group_id is not None:
        q = q.filter(MediaRecord.sync_group_id == sync_group_id)
    all_records = q.all()

    if season is not None:
        season_tag = f"Season {season:02d}"
        all_records = [r for r in all_records if r.target_path and season_tag in r.target_path]

    to_delete = [r for r in all_records if r.id not in keep_ids_set]
    to_keep = [r for r in all_records if r.id in keep_ids_set]

    # Determine target root dirs per sync group for cleanup
    group_targets: dict[int, Path] = {}
    for rec in to_keep + to_delete:
        gid = rec.sync_group_id
        if gid is not None and gid not in group_targets:
            g = db.query(SyncGroup).filter(SyncGroup.id == gid).first()
            if g and g.target:
                group_targets[gid] = Path(g.target)

    deleted_hardlinks = 0
    deleted_target_paths: list[str] = []

    # 1. Delete hardlinks for discarded records first
    for rec in to_delete:
        if rec.target_path:
            try:
                if delete_target_hardlink(rec.target_path):
                    deleted_hardlinks += 1
                deleted_target_paths.append(rec.target_path)
            except OSError:
                _log.exception("Failed to delete hardlink %s", rec.target_path)

    # 2. Clean up empty dirs per sync group target root
    cleanup_dirs: set[Path] = set()
    for tp in deleted_target_paths:
        cleanup_dirs.add(Path(tp).parent)
    for d in cleanup_dirs:
        # Find which group owns this dir
        for gid, target_root in group_targets.items():
            try:
                d.relative_to(target_root)
                cleanup_empty_dirs(d, stop_at=target_root)
                break
            except ValueError:
                pass

    # 3. Delete orphan inode records
    deleted_orphan_inodes = delete_orphan_inode_records(db, deleted_target_paths)

    # 4. Clean up orphan directory states
    for d in cleanup_dirs:
        for gid in group_targets:
            cleanup_orphan_directory_states(db, gid, d)

    # 5. Clean up CheckIssue records that reference the deleted target paths / resource dirs
    deleted_resource_dirs: set[str] = set()
    deleted_target_path_set: set[str] = set()
    for rec in to_delete:
        if rec.target_path:
            tp = str(rec.target_path).replace("\\", "/").rstrip("/")
            deleted_target_path_set.add(tp.lower())
            deleted_resource_dirs.add(str(Path(rec.target_path).parent).replace("\\", "/").rstrip("/"))

    if deleted_resource_dirs or deleted_target_path_set:
        from sqlalchemy import func as _func, or_ as _or
        issue_conditions = []
        for rd in deleted_resource_dirs:
            issue_conditions.append(CheckIssue.resource_dir == rd)
        for tp in deleted_target_path_set:
            issue_conditions.append(
                _func.lower(_func.replace(_func.coalesce(CheckIssue.target_path, ""), "\\", "/")) == tp
            )
            issue_conditions.append(
                _func.lower(_func.replace(_func.coalesce(CheckIssue.source_path, ""), "\\", "/")) == tp
            )
        if issue_conditions:
            issues_to_delete = db.query(CheckIssue).filter(_or(*issue_conditions)).all()
            for issue in issues_to_delete:
                db.delete(issue)
            if issues_to_delete:
                _log.info("Wash: cleaned up %d CheckIssue record(s)", len(issues_to_delete))

    # Also clean up by tmdb_id scope (catches issues not tied to specific target paths)
    tmdb_issues_q = db.query(CheckIssue).filter(CheckIssue.tmdb_id == tmdb_id)
    if sync_group_id is not None:
        tmdb_issues_q = tmdb_issues_q.filter(CheckIssue.sync_group_id == sync_group_id)
    if season is not None:
        tmdb_issues_q = tmdb_issues_q.filter(CheckIssue.season == season)
    tmdb_issues = tmdb_issues_q.all()
    for issue in tmdb_issues:
        db.delete(issue)
    if tmdb_issues:
        _log.info("Wash: cleaned up %d tmdb-scoped CheckIssue record(s) (tmdb_id=%d)", len(tmdb_issues), tmdb_id)

    # 6. Delete MediaRecords
    deleted_record_ids: list[int] = []
    for rec in to_delete:
        deleted_record_ids.append(rec.id)
        delete_media_record(db, rec.id)

    db.commit()

    _log.info(
        "Wash complete: tmdb_id=%d season=%s kept_ids=%s deleted_records=%d "
        "deleted_hardlinks=%d deleted_orphan_inodes=%d",
        tmdb_id, season,
        [r.id for r in to_keep],
        len(deleted_record_ids), deleted_hardlinks, deleted_orphan_inodes,
    )

    return WashResult(
        kept_ids=list(keep_ids_set),
        deleted_record_ids=deleted_record_ids,
        deleted_hardlinks=deleted_hardlinks,
        deleted_orphan_inodes=deleted_orphan_inodes,
    )


# ---------------------------------------------------------------------------
# Source directory scanner (for wash dialog preview)
# ---------------------------------------------------------------------------

_VIDEO_EXTS = frozenset({".mkv", ".mp4", ".avi", ".mov", ".webm", ".flv", ".ts", ".m2ts"})
_IGNORED_TOKENS = frozenset({"bdmv", "menu", "sample", "scan", "disc", "iso", "font"})


def _is_ignored(name: str) -> bool:
    lower = name.lower()
    return any(t in lower for t in _IGNORED_TOKENS)


def _list_video_files(directory: Path) -> list[dict]:
    """Return all video files (recursively) under directory, sorted by name."""
    results: list[dict] = []
    try:
        for entry in sorted(directory.rglob("*")):
            if entry.is_file() and entry.suffix.lower() in _VIDEO_EXTS:
                try:
                    size = entry.stat().st_size
                except OSError:
                    size = 0
                results.append({
                    "filename": entry.name,
                    "rel_path": str(entry.relative_to(directory)),
                    "size": size,
                    "size_human": _human_size(size),
                })
    except OSError:
        pass
    return results


def _collect_video_leaf_dirs(root: Path, max_depth: int = 8) -> list[Path]:
    """DFS: return directories that directly contain at least one video file."""
    found: list[Path] = []
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        current, depth = stack.pop()
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.name)
        except OSError:
            continue
        has_video = False
        subdirs: list[Path] = []
        for entry in entries:
            if entry.is_dir():
                if not _is_ignored(entry.name) and depth < max_depth:
                    subdirs.append(entry)
            elif entry.is_file() and entry.suffix.lower() in _VIDEO_EXTS:
                has_video = True
        if has_video:
            found.append(current)
        else:
            for d in reversed(subdirs):
                stack.append((d, depth + 1))
    return found


@dataclass
class SourceDirEntry:
    dir_path: str
    sync_group_id: int | None
    is_recorded: bool           # any file in this dir is a MediaRecord.original_path
    record_count: int           # number of MediaRecords whose original_path is under this dir
    group_key: str | None       # matches CandidateGroup.key when is_recorded=True
    video_files: list[dict]     # [{filename, rel_path, size, size_human}]
    video_count: int


def scan_source_dirs_for_tmdb(
    db: Session,
    tmdb_id: int,
    sync_group_id: int | None = None,
) -> list[SourceDirEntry]:
    """Scan sync-group source directories for all video leaf directories.

    Returns ALL directories that contain video files under sync group sources,
    marking each as recorded or unrecorded relative to tmdb_id.  The user
    decides which directories are relevant.
    """
    # Load sync groups to search
    q = db.query(SyncGroup)
    if sync_group_id is not None:
        q = q.filter(SyncGroup.id == sync_group_id)
    groups = q.all()

    # Build set of recorded original_paths for this tmdb_id
    rq = db.query(MediaRecord.original_path, MediaRecord.sync_group_id).filter(
        MediaRecord.tmdb_id == tmdb_id
    )
    if sync_group_id is not None:
        rq = rq.filter(MediaRecord.sync_group_id == sync_group_id)

    # Map: source_dir_path → (record_count, sync_group_id)
    recorded_dir_counts: dict[str, tuple[int, int | None]] = defaultdict(lambda: [0, None])
    for orig_path, sgid in rq.all():
        if orig_path:
            src_dir = str(Path(orig_path).parent)
            recorded_dir_counts[src_dir][0] += 1
            if recorded_dir_counts[src_dir][1] is None:
                recorded_dir_counts[src_dir][1] = sgid

    # Scan roots: only use the sync group source directories from config
    candidate_roots: set[tuple[Path, int | None]] = set()
    for g in groups:
        src = Path(g.source)
        if src.exists():
            candidate_roots.add((src, g.id))

    # Scan for all matching dirs
    seen_paths: set[str] = set()
    entries: list[SourceDirEntry] = []

    for root, sgid in candidate_roots:
        tmdb_dirs = _collect_video_leaf_dirs(root)
        for d in tmdb_dirs:
            d_str = str(d)
            if d_str in seen_paths:
                continue
            seen_paths.add(d_str)

            video_files = _list_video_files(d)
            if not video_files:
                continue  # skip dirs with no video content

            is_rec = d_str in recorded_dir_counts
            rec_count = recorded_dir_counts[d_str][0] if is_rec else 0
            rec_sgid = recorded_dir_counts[d_str][1] if is_rec else sgid

            # Compute group_key to match CandidateGroup.key
            group_key = hashlib.md5(d_str.encode()).hexdigest()[:12] if is_rec else None

            entries.append(SourceDirEntry(
                dir_path=d_str,
                sync_group_id=rec_sgid,
                is_recorded=is_rec,
                record_count=rec_count,
                group_key=group_key,
                video_files=video_files,
                video_count=len(video_files),
            ))

    # Sort: recorded first, then by path
    entries.sort(key=lambda e: (not e.is_recorded, e.dir_path))
    return entries

