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
        records = (
            db.query(
                MediaRecord.id,
                MediaRecord.original_path,
                MediaRecord.target_path,
                MediaRecord.tmdb_id,
                MediaRecord.sync_group_id,
            )
            .filter(
                MediaRecord.target_path.isnot(None),
                MediaRecord.original_path.isnot(None),
            )
            .all()
        )

        # 构建 group_id -> group_name 映射用于 payload
        group_names: dict[int | None, str] = {g.id: g.name for g in groups}

        dir_all: dict[str, list[tuple]] = defaultdict(list)
        dir_missing: dict[str, list[tuple]] = defaultdict(list)

        for rec_id, orig_path, tgt_path, tmdb_id, sg_id in records:
            source_dir = str(Path(orig_path).parent)
            dir_all[source_dir].append((rec_id, orig_path, tgt_path, tmdb_id, sg_id))
            if tgt_path and Path(tgt_path).exists() and not Path(orig_path).exists():
                dir_missing[source_dir].append((rec_id, orig_path, tgt_path, tmdb_id, sg_id))

        issues: list[IssueData] = []
        for source_dir, missing in dir_missing.items():
            if not missing:
                continue

            if len(missing) == len(dir_all[source_dir]):
                issues.append(
                    IssueData(
                        checker_code="target_dir_no_source",
                        issue_code="dir_source_missing",
                        severity="warning",
                        sync_group_id=None,
                        resource_dir=source_dir,
                        payload={"file_count": len(missing)},
                    )
                )
            else:
                for rec_id, orig_path, tgt_path, tmdb_id, sg_id in missing:
                    issues.append(
                        IssueData(
                            checker_code=self.checker_code,
                            issue_code="source_missing",
                            severity="warning",
                            sync_group_id=None,
                            source_path=orig_path,
                            target_path=tgt_path,
                            tmdb_id=tmdb_id,
                            payload={"media_record_id": rec_id},
                        )
                    )
        return issues
