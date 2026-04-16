"""target_no_source checker.

Reports DB records where target_path exists on the filesystem but
original_path (source file) no longer exists.

This indicates a hardlink whose source was moved or deleted after linking,
leaving an "orphaned" target with no traceable origin.

If all files from a source directory have missing sources, the whole
directory is reported as target_dir_no_source (directory-level issue)
instead of expanding into individual target_no_source records.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from sqlalchemy.orm import Session

from ...models import MediaRecord, SyncGroup
from .base import CheckerBase, IssueData


class TargetNoSourceChecker(CheckerBase):
    checker_code = "target_no_source"

    def run(self, db: Session, groups: list[SyncGroup]) -> list[IssueData]:
        issues: list[IssueData] = []
        for group in groups:
            issues.extend(self._check_group(db, group))
        return issues

    def _check_group(self, db: Session, group: SyncGroup) -> list[IssueData]:
        records = (
            db.query(
                MediaRecord.id,
                MediaRecord.original_path,
                MediaRecord.target_path,
                MediaRecord.tmdb_id,
            )
            .filter(
                MediaRecord.sync_group_id == group.id,
                MediaRecord.target_path.isnot(None),
                MediaRecord.original_path.isnot(None),
            )
            .all()
        )

        # Group all records by source directory (check FS once per record)
        # dir_all: all records per source dir
        # dir_missing: records where target exists but source is gone
        dir_all: dict[str, list[tuple]] = defaultdict(list)
        dir_missing: dict[str, list[tuple]] = defaultdict(list)

        for rec_id, orig_path, tgt_path, tmdb_id in records:
            source_dir = str(Path(orig_path).parent)
            dir_all[source_dir].append((rec_id, orig_path, tgt_path, tmdb_id))
            if tgt_path and Path(tgt_path).exists() and not Path(orig_path).exists():
                dir_missing[source_dir].append((rec_id, orig_path, tgt_path, tmdb_id))

        issues: list[IssueData] = []
        for source_dir, missing in dir_missing.items():
            if not missing:
                continue

            if len(missing) == len(dir_all[source_dir]):
                # All files from this source directory have no source → directory-level issue
                issues.append(
                    IssueData(
                        checker_code="target_dir_no_source",
                        issue_code="dir_source_missing",
                        severity="warning",
                        sync_group_id=group.id,
                        resource_dir=source_dir,
                        payload={
                            "group_name": group.name,
                            "file_count": len(missing),
                        },
                    )
                )
            else:
                # Only some files missing source → report per-file
                for rec_id, orig_path, tgt_path, tmdb_id in missing:
                    issues.append(
                        IssueData(
                            checker_code=self.checker_code,
                            issue_code="source_missing",
                            severity="warning",
                            sync_group_id=group.id,
                            source_path=orig_path,
                            target_path=tgt_path,
                            tmdb_id=tmdb_id,
                            payload={"media_record_id": rec_id, "group_name": group.name},
                        )
                    )
        return issues
