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
        if has_video and current != source:
            leaf_dirs.append(current)
        else:
            # 源目录本身直接含视频时，继续遍历子目录，不将源目录加入检查列表
            for d in reversed(subdirs):
                stack.append((d, depth + 1))
    return leaf_dirs


class SourceUnrecordedChecker(CheckerBase):
    checker_code = "source_unrecorded"

    def run(self, db: Session, groups: list[SyncGroup]) -> list[IssueData]:
        # 全局检查：将所有同步组的已记录路径合并为一个大集合，避免跨组目录误报
        recorded: set[str] = {
            row[0]
            for row in db.query(MediaRecord.original_path).all()
        }

        issues: list[IssueData] = []
        seen_sources: set[str] = set()
        for group in groups:
            source_root = Path(group.source)
            if not source_root.exists() or str(source_root) in seen_sources:
                continue
            seen_sources.add(str(source_root))

            leaf_dirs = _collect_video_leaf_dirs(source_root)
            for leaf_dir in leaf_dirs:
                try:
                    entries = sorted(leaf_dir.iterdir(), key=lambda p: p.name)
                except OSError:
                    continue

                media_files: list[Path] = []
                for entry in entries:
                    if not entry.is_file():
                        continue
                    ext = entry.suffix.lower()
                    if ext in VIDEO_EXTS or ext in ATTACHMENT_EXTS:
                        media_files.append(entry)

                unrecorded = [f for f in media_files if str(f) not in recorded]

                if not unrecorded:
                    continue

                if len(unrecorded) == len(media_files):
                    # All files in this directory are unrecorded → report as directory-level issue
                    issues.append(
                        IssueData(
                            checker_code="source_dir_unrecorded",
                            issue_code="dir_not_recorded",
                            severity="warning",
                            sync_group_id=None,
                            resource_dir=str(leaf_dir),
                            payload={"group_name": group.name, "file_count": len(media_files)},
                        )
                    )
                else:
                    # Only some files unrecorded → report per-file
                    for entry in unrecorded:
                        issues.append(
                            IssueData(
                                checker_code=self.checker_code,
                                issue_code="file_not_recorded",
                                severity="warning",
                                sync_group_id=None,
                                source_path=str(entry),
                                resource_dir=str(leaf_dir),
                                payload={"group_name": group.name},
                            )
                        )
        return issues
