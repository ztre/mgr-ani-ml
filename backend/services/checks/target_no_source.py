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

import logging
import os
from collections import defaultdict
from pathlib import Path

from sqlalchemy.orm import Session

from ...models import InodeRecord, MediaRecord, SyncGroup
from .base import CheckerBase, IssueData
from .path_filters import is_subtitle_related_issue

_log = logging.getLogger(__name__)


def _try_fix_stale_original_path(
    db: Session,
    rec_id: int,
    orig_path: str,
    tgt_path: str,
) -> bool:
    """尝试通过 inode 追踪找到 original_path 的新位置并修复 DB。

    策略（优先级递减）：
    1. InodeRecord 优先：查 InodeRecord.target_path == tgt_path，取 source_path 验证文件存在
    2. 目录扫描兜底：遍历 Path(orig_path).parent 目录，逐文件比对 inode

    返回 True 表示已定位新路径并修复；返回 False 表示无法定位（仍跳过上报，
    因为 st_nlink >= 2 说明 inode 未被彻底删除）。
    """
    # --- 策略 1：InodeRecord 查找 ---
    inode_row = (
        db.query(InodeRecord)
        .filter(InodeRecord.target_path == tgt_path)
        .first()
    )
    if inode_row is not None:
        candidate = str(inode_row.source_path or "").strip()
        if candidate and candidate != orig_path and Path(candidate).exists():
            _apply_path_fix(db, rec_id, orig_path, candidate, inode_row)
            return True
        # candidate == orig_path 或不存在：InodeRecord 与 MediaRecord 一致但文件不在，
        # inode 仍健康（nlink>=2），意味着文件在另一个未记录路径，无法精确定位，静默跳过
        if candidate == orig_path:
            return False

    # --- 策略 2：目录扫描兜底 ---
    try:
        tgt_stat = os.stat(tgt_path)
    except OSError:
        return False

    orig_dir = Path(orig_path).parent
    try:
        candidates = list(orig_dir.iterdir())
    except OSError:
        return False

    for entry in candidates:
        if not entry.is_file():
            continue
        try:
            entry_stat = os.stat(entry)
        except OSError:
            continue
        if (entry_stat.st_dev, entry_stat.st_ino) == (tgt_stat.st_dev, tgt_stat.st_ino):
            new_path = str(entry)
            if new_path != orig_path:
                _apply_path_fix(db, rec_id, orig_path, new_path, inode_row)
                return True

    _log.info(
        "stale original_path but healthy inode (nlink>=2), cannot locate new path: "
        "rec_id=%d orig=%s tgt=%s",
        rec_id, orig_path, tgt_path,
    )
    return False


def _apply_path_fix(
    db: Session,
    rec_id: int,
    old_orig_path: str,
    new_orig_path: str,
    inode_row: InodeRecord | None,
) -> None:
    """将 MediaRecord.original_path 和 InodeRecord.source_path 同步更新为新路径。"""
    record = db.query(MediaRecord).filter(MediaRecord.id == rec_id).first()
    if record is not None:
        record.original_path = new_orig_path
    if inode_row is not None:
        inode_row.source_path = new_orig_path
    db.flush()
    _log.info(
        "Auto-fixed stale original_path: rec_id=%d %s -> %s",
        rec_id, old_orig_path, new_orig_path,
    )


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
            if is_subtitle_related_issue(source_path=orig_path, target_path=tgt_path):
                continue
            source_dir = str(Path(orig_path).parent)
            dir_all[source_dir].append((rec_id, orig_path, tgt_path, tmdb_id, sg_id))
            if tgt_path and Path(tgt_path).exists() and not Path(orig_path).exists():
                # 区分「真正孤立硬链接」与「源路径 DB 过期但 inode 仍健康」：
                # st_nlink >= 2 说明目标文件 inode 还有其他路径存活（源文件被改名/移动），
                # 不是孤立硬链接，尝试自动修复 original_path 后跳过上报。
                try:
                    nlink = os.stat(tgt_path).st_nlink
                except OSError:
                    nlink = 0

                if nlink >= 2:
                    _try_fix_stale_original_path(db, rec_id, orig_path, tgt_path)
                    # 无论修复成功与否：inode 健康，不上报为孤立硬链接
                    continue

                # nlink == 1：真正孤立，target 是 inode 唯一路径，源文件已彻底删除
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
