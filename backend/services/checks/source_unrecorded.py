"""source_unrecorded checker.

Mirrors the logic in tdocs/check_source_unrecorded.sh:
  Deep-first traversal of sync_group.source to find video leaf directories,
  then report any media file (video + attachment) not present in
  media_records.original_path for that group.

Ignored directory tokens: bdmv, menu, sample, scan, disc, iso, font.
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


def _collect_video_leaf_dirs(source: Path, max_depth: int = 10) -> list[Path]:
    """Return directories that directly contain at least one video file (DFS)."""
    if not source.exists():
        return []
    leaf_dirs: list[Path] = []
    stack: list[tuple[Path, int]] = [(source, 0)]
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


class SourceUnrecordedChecker(CheckerBase):
    checker_code = "source_unrecorded"

    def run(self, db: Session, groups: list[SyncGroup]) -> list[IssueData]:
        issues: list[IssueData] = []
        for group in groups:
            group_issues = self._check_group(db, group)
            issues.extend(group_issues)
        return issues

    def _check_group(self, db: Session, group: SyncGroup) -> list[IssueData]:
        source_root = Path(group.source)
        if not source_root.exists():
            return []

        # Load all recorded original_paths for this group into a set
        recorded: set[str] = {
            row[0]
            for row in db.query(MediaRecord.original_path)
            .filter(MediaRecord.sync_group_id == group.id)
            .all()
        }

        issues: list[IssueData] = []
        leaf_dirs = _collect_video_leaf_dirs(source_root)
        for leaf_dir in leaf_dirs:
            try:
                entries = sorted(leaf_dir.iterdir(), key=lambda p: p.name)
            except OSError:
                continue
            for entry in entries:
                if not entry.is_file():
                    continue
                ext = entry.suffix.lower()
                if ext not in VIDEO_EXTS and ext not in ATTACHMENT_EXTS:
                    continue
                if str(entry) not in recorded:
                    issues.append(
                        IssueData(
                            checker_code=self.checker_code,
                            issue_code="file_not_recorded",
                            severity="warning",
                            sync_group_id=group.id,
                            source_path=str(entry),
                            resource_dir=str(leaf_dir),
                            payload={"group_name": group.name},
                        )
                    )
        return issues
