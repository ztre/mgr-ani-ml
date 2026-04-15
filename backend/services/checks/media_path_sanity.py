"""media_path_sanity checker.

Mirrors the logic in tdocs/check_media_path_sanity.sh:
  For each MediaRecord in the group:
  1. target_missing  : target_path is set but file does not exist on FS
  2. type_mismatch   : the record's `type` (tv/movie) does not match the
                       library root directory for this group
                       (e.g. a `tv` record found under a `movie`-type group)
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from ...models import MediaRecord, SyncGroup
from .base import CheckerBase, IssueData


class MediaPathSanityChecker(CheckerBase):
    checker_code = "media_path_sanity"

    def run(self, db: Session, groups: list[SyncGroup]) -> list[IssueData]:
        issues: list[IssueData] = []
        for group in groups:
            issues.extend(self._check_group(db, group))
        return issues

    def _check_group(self, db: Session, group: SyncGroup) -> list[IssueData]:
        issues: list[IssueData] = []
        group_source_type = group.source_type  # "tv" or "movie"
        target_root = str(group.target).rstrip("/")

        records = (
            db.query(
                MediaRecord.id,
                MediaRecord.original_path,
                MediaRecord.target_path,
                MediaRecord.type,
                MediaRecord.tmdb_id,
            )
            .filter(
                MediaRecord.sync_group_id == group.id,
                MediaRecord.target_path.isnot(None),
            )
            .all()
        )

        for rec_id, orig_path, tgt_path, rec_type, tmdb_id in records:
            if not tgt_path:
                continue

            # Check 1: target file existence
            if not Path(tgt_path).exists():
                issues.append(
                    IssueData(
                        checker_code=self.checker_code,
                        issue_code="target_missing",
                        severity="error",
                        sync_group_id=group.id,
                        source_path=orig_path,
                        target_path=tgt_path,
                        tmdb_id=tmdb_id,
                        payload={"media_record_id": rec_id, "group_name": group.name},
                    )
                )

            # Check 2: type / library-root mismatch
            # The record's type should match the group's configured source_type.
            # A mismatch means the record was possibly written under the wrong group.
            if rec_type and group_source_type and rec_type != group_source_type:
                issues.append(
                    IssueData(
                        checker_code=self.checker_code,
                        issue_code="type_mismatch",
                        severity="warning",
                        sync_group_id=group.id,
                        source_path=orig_path,
                        target_path=tgt_path,
                        tmdb_id=tmdb_id,
                        payload={
                            "media_record_id": rec_id,
                            "record_type": rec_type,
                            "group_source_type": group_source_type,
                            "group_name": group.name,
                        },
                    )
                )

        return issues
