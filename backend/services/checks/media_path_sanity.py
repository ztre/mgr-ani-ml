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

from collections import defaultdict
from pathlib import Path

from sqlalchemy.orm import Session

from ...models import MediaRecord, SyncGroup
from ..media_record_metadata import MainVideoSlot, build_main_video_slot_from_record
from ..resource_tree import extract_show_dir
from .base import CheckerBase, IssueData
from .path_filters import is_subtitle_related_issue


class MediaPathSanityChecker(CheckerBase):
    checker_code = "media_path_sanity"

    def run(self, db: Session, groups: list[SyncGroup]) -> list[IssueData]:
        issues: list[IssueData] = []
        duplicate_slots: dict[MainVideoSlot, list[MediaRecord]] = defaultdict(list)

        records = db.query(MediaRecord).filter(MediaRecord.target_path.isnot(None)).all()

        for row in records:
            rec_id = row.id
            orig_path = row.original_path
            tgt_path = row.target_path
            tmdb_id = row.tmdb_id
            if not tgt_path:
                continue
            if is_subtitle_related_issue(source_path=orig_path, target_path=tgt_path):
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
            slot = build_main_video_slot_from_record(row)
            if slot is not None:
                duplicate_slots[slot].append(row)

        for slot, slot_rows in duplicate_slots.items():
            original_paths = sorted({
                str(getattr(row, "original_path", "") or "").strip()
                for row in slot_rows
                if str(getattr(row, "original_path", "") or "").strip()
            })
            if len(original_paths) <= 1:
                continue
            target_paths = sorted({
                str(getattr(row, "target_path", "") or "").strip()
                for row in slot_rows
                if str(getattr(row, "target_path", "") or "").strip()
            })
            sample_target = target_paths[0] if target_paths else None
            issues.append(
                IssueData(
                    checker_code=self.checker_code,
                    issue_code="duplicate_main_video_slot",
                    severity="warning",
                    sync_group_id=slot.sync_group_id,
                    source_path=original_paths[0] if original_paths else None,
                    target_path=sample_target,
                    resource_dir=extract_show_dir(sample_target),
                    tmdb_id=slot.tmdb_id,
                    season=slot.season,
                    episode=slot.episode,
                    payload={
                        "media_record_ids": [int(getattr(row, "id")) for row in slot_rows],
                        "original_paths": original_paths,
                        "target_paths": target_paths,
                    },
                )
            )

        return issues
