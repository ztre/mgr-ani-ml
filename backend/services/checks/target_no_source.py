"""target_no_source checker.

Reports DB records where target_path exists on the filesystem but
original_path (source file) no longer exists.

This indicates a hardlink whose source was moved or deleted after linking,
leaving an "orphaned" target with no traceable origin.
"""
from __future__ import annotations

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
        issues: list[IssueData] = []
        for rec_id, orig_path, tgt_path, tmdb_id in records:
            # Only report when target exists but source is gone
            if tgt_path and Path(tgt_path).exists() and not Path(orig_path).exists():
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
