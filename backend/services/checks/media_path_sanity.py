"""media_path_sanity checker.

Mirrors the logic in tdocs/check_media_path_sanity.sh:
  For each MediaRecord with a target_path:
  1. target_missing  : target_path is set but file does not exist on FS

Note: type_mismatch (record.type vs sync_group.source_type) check has been
removed because source/target paths can be shared across sync groups, making
group-scoped type comparisons unreliable (cross-group records would always
mismatch their original group's source_type).
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from ...models import MediaRecord
from .base import CheckerBase, IssueData


class MediaPathSanityChecker(CheckerBase):
    checker_code = "media_path_sanity"

    def run(self, db: Session, groups: list[SyncGroup]) -> list[IssueData]:
        issues: list[IssueData] = []

        records = (
            db.query(
                MediaRecord.id,
                MediaRecord.original_path,
                MediaRecord.target_path,
                MediaRecord.tmdb_id,
            )
            .filter(MediaRecord.target_path.isnot(None))
            .all()
        )

        for rec_id, orig_path, tgt_path, tmdb_id in records:
            if not tgt_path:
                continue

            # Check: target file existence
            if not Path(tgt_path).exists():
                issues.append(
                    IssueData(
                        checker_code=self.checker_code,
                        issue_code="target_missing",
                        severity="error",
                        sync_group_id=None,
                        source_path=orig_path,
                        target_path=tgt_path,
                        tmdb_id=tmdb_id,
                        payload={"media_record_id": rec_id},
                    )
                )

        return issues
