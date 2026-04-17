"""links_orphans checker.

Mirrors the logic in tdocs/check_links_orphans.sh:
  Scan the target (links) directory for each sync group and report:
  - orphan_file  : file exists in FS but no DB record has that target_path
  - broken_link  : DB record exists but the target file is missing from FS

DFS leaf-dir traversal (mirrors scanner._collect_video_leaf_dirs).
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from ...models import MediaRecord, SyncGroup
from .base import CheckerBase, IssueData

VIDEO_EXTS = frozenset({".mkv", ".mp4", ".avi", ".mov", ".webm", ".flv"})
ATTACHMENT_EXTS = frozenset({".ass", ".srt", ".ssa", ".vtt", ".mka", ".sup", ".idx", ".sub"})
IGNORED_TOKENS = frozenset({"bdmv", "menu", "sample", "scan", "disc", "iso", "font"})


def _is_ignored_name(name: str) -> bool:
    lower = name.lower()
    return any(tok in lower for tok in IGNORED_TOKENS)


def _collect_leaf_dirs(root: Path, max_depth: int = 8) -> list[Path]:
    """Return leaf directories containing at least one video file (DFS)."""
    if not root.exists():
        return []
    leaf_dirs: list[Path] = []
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        current, depth = stack.pop()
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.as_posix())
        except OSError:
            continue
        has_video = False
        subdirs = []
        for entry in entries:
            if entry.is_dir():
                if not _is_ignored_name(entry.name) and depth < max_depth:
                    subdirs.append(entry)
                continue
            if entry.is_file() and entry.suffix.lower() in VIDEO_EXTS:
                has_video = True
        if has_video:
            leaf_dirs.append(current)
        else:
            for d in reversed(subdirs):
                stack.append((d, depth + 1))
    return leaf_dirs


class LinksOrphansChecker(CheckerBase):
    checker_code = "links_orphans"

    def run(self, db: Session, groups: list[SyncGroup]) -> list[IssueData]:
        issues: list[IssueData] = []

        # 全局所有已记录 target_path 合并为一个大集合，避免跨组目录共享时误报孤立文件
        recorded_targets: set[str] = {
            row[0]
            for row in db.query(MediaRecord.target_path)
            .filter(MediaRecord.target_path.isnot(None))
            .all()
        }

        # --- orphan check: FS files not in DB ---
        seen_target_roots: set[str] = set()
        for group in groups:
            target_root = Path(group.target)
            if not target_root.exists() or str(target_root) in seen_target_roots:
                continue
            seen_target_roots.add(str(target_root))
            for leaf_dir in _collect_leaf_dirs(target_root):
                try:
                    entries = sorted(leaf_dir.iterdir(), key=lambda p: p.name)
                except OSError:
                    continue
                all_files: list[Path] = []
                orphan_files: list[Path] = []
                for entry in entries:
                    if not entry.is_file():
                        continue
                    ext = entry.suffix.lower()
                    if ext not in VIDEO_EXTS and ext not in ATTACHMENT_EXTS:
                        continue
                    all_files.append(entry)
                    if str(entry) not in recorded_targets:
                        orphan_files.append(entry)
                if not orphan_files:
                    continue
                if len(orphan_files) == len(all_files):
                    issues.append(
                        IssueData(
                            checker_code="target_dir_no_source",
                            issue_code="dir_orphan_target",
                            severity="warning",
                            sync_group_id=None,
                            resource_dir=str(leaf_dir),
                            payload={"group_name": group.name, "file_count": len(orphan_files)},
                        )
                    )
                else:
                    for entry in orphan_files:
                        issues.append(
                            IssueData(
                                checker_code=self.checker_code,
                                issue_code="orphan_file",
                                severity="warning",
                                sync_group_id=None,
                                target_path=str(entry),
                                resource_dir=str(leaf_dir),
                                payload={"group_name": group.name},
                            )
                        )

        # --- broken link check: DB records whose FS file is missing ---
        db_records = (
            db.query(MediaRecord.id, MediaRecord.original_path, MediaRecord.target_path, MediaRecord.tmdb_id)
            .filter(MediaRecord.target_path.isnot(None))
            .all()
        )
        for rec_id, orig_path, tgt_path, tmdb_id in db_records:
            if tgt_path and not Path(tgt_path).exists():
                issues.append(
                    IssueData(
                        checker_code=self.checker_code,
                        issue_code="broken_link",
                        severity="error",
                        sync_group_id=None,
                        source_path=orig_path,
                        target_path=tgt_path,
                        tmdb_id=tmdb_id,
                        payload={"media_record_id": rec_id},
                    )
                )
        return issues
