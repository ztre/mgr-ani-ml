"""Check runner: orchestrates checkers, persists results with fingerprint dedup.

Rules:
- Same fingerprint, status='open'  → update updated_at only (not a new issue)
- Same fingerprint, status='ignored'|'resolved' and issue reappears → reopen
- New fingerprint → create new CheckIssue
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from ...models import CheckIssue, CheckRun, SyncGroup
from .base import IssueData
from .links_orphans import LinksOrphansChecker
from .media_path_sanity import MediaPathSanityChecker
from .source_unrecorded import SourceUnrecordedChecker
from .target_no_source import TargetNoSourceChecker

if TYPE_CHECKING:
    pass

_log = logging.getLogger(__name__)

_ALL_CHECKER_CODES = ["source_unrecorded", "links_orphans", "media_path_sanity", "target_no_source"]
_CHECKER_MAP = {
    "source_unrecorded": SourceUnrecordedChecker(),
    "links_orphans": LinksOrphansChecker(),
    "media_path_sanity": MediaPathSanityChecker(),
    "target_no_source": TargetNoSourceChecker(),
}


def _compute_fingerprint(issue: IssueData) -> str:
    """Stable SHA-256 fingerprint for deduplication."""
    key_path = issue.source_path or issue.target_path or issue.resource_dir or ""
    raw = "|".join([
        issue.checker_code,
        issue.issue_code,
        str(issue.sync_group_id or ""),
        key_path,
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _persist_issues(
    db: Session,
    check_run_id: int,
    raw_issues: list[IssueData],
) -> tuple[int, int, int]:
    """Persist issues; return (found, opened, reopened)."""
    found = len(raw_issues)
    opened = 0
    reopened = 0
    now = datetime.now(timezone.utc)

    for item in raw_issues:
        fp = _compute_fingerprint(item)
        existing = db.query(CheckIssue).filter(CheckIssue.fingerprint == fp).first()

        if existing is None:
            # Brand new issue
            db.add(CheckIssue(
                check_run_id=check_run_id,
                checker_code=item.checker_code,
                issue_code=item.issue_code,
                severity=item.severity,
                sync_group_id=item.sync_group_id,
                source_path=item.source_path,
                target_path=item.target_path,
                resource_dir=item.resource_dir,
                tmdb_id=item.tmdb_id,
                season=item.season,
                episode=item.episode,
                payload_json=json.dumps(item.payload) if item.payload else None,
                status="open",
                fingerprint=fp,
                created_at=now,
                updated_at=now,
            ))
            opened += 1

        elif existing.status == "open" or existing.status == "claimed":
            # Still present — just refresh timestamp
            existing.updated_at = now
            existing.check_run_id = check_run_id

        else:
            # Was ignored or resolved, but issue reappeared → reopen
            _log.info(
                "Reopening check issue id=%d fingerprint=%.16s (was %s)",
                existing.id, fp, existing.status,
            )
            existing.status = "open"
            existing.updated_at = now
            existing.resolved_at = None
            existing.check_run_id = check_run_id
            reopened += 1

    return found, opened, reopened


def _auto_resolve_vanished(
    db: Session,
    checked_scopes: set[tuple[str, int | None]],
    found_fingerprints: set[str],
) -> int:
    """Auto-resolve open/claimed issues not found in the current run.

    Only operates within (checker_code, sync_group_id) scopes that were
    actually executed successfully.  Issues with status 'ignored' are
    left untouched.
    """
    now = datetime.now(timezone.utc)
    resolved = 0

    for checker_code, sync_group_id in checked_scopes:
        q = db.query(CheckIssue).filter(
            CheckIssue.checker_code == checker_code,
            CheckIssue.status.in_(("open", "claimed")),
        )
        if sync_group_id is not None:
            q = q.filter(CheckIssue.sync_group_id == sync_group_id)

        for issue in q.all():
            if issue.fingerprint not in found_fingerprints:
                _log.info(
                    "Auto-resolving vanished issue id=%d fp=%.16s checker=%s group=%s",
                    issue.id, issue.fingerprint, checker_code, sync_group_id,
                )
                issue.status = "resolved"
                issue.resolved_at = now
                issue.updated_at = now
                resolved += 1

    return resolved


def _run_checks(db: Session, groups: list[SyncGroup], check_run: CheckRun) -> None:
    """Execute all enabled checkers for the given groups and persist results."""
    all_issues: list[IssueData] = []
    checked_scopes: set[tuple[str, int | None]] = set()

    for group in groups:
        enabled = group.get_enabled_checks()
        for code in enabled:
            checker = _CHECKER_MAP.get(code)
            if checker is None:
                _log.warning("Unknown checker code %r for group %r — skipping", code, group.name)
                continue
            try:
                issues = checker.run(db, [group])
                all_issues.extend(issues)
                # Only mark scope as checked on success; failures leave issues untouched
                checked_scopes.add((code, group.id))
            except Exception:
                _log.exception("Checker %r failed for group %r", code, group.name)

    found, opened, reopened = _persist_issues(db, check_run.id, all_issues)

    found_fingerprints = {_compute_fingerprint(issue) for issue in all_issues}
    resolved = _auto_resolve_vanished(db, checked_scopes, found_fingerprints)

    check_run.status = "completed"
    check_run.finished_at = datetime.now(timezone.utc)
    check_run.summary_json = json.dumps({
        "found": found,
        "opened": opened,
        "reopened": reopened,
        "resolved": resolved,
    })
    db.commit()
    _log.info(
        "Check run %d completed: found=%d opened=%d reopened=%d resolved=%d",
        check_run.id, found, opened, reopened, resolved,
    )


def run_checks_full(db: Session) -> CheckRun:
    """Run all enabled checkers across all enabled sync groups."""
    groups = (
        db.query(SyncGroup)
        .filter(SyncGroup.enabled.is_(True))
        .order_by(SyncGroup.id)
        .all()
    )
    now = datetime.now(timezone.utc)
    check_run = CheckRun(
        sync_group_id=None,
        status="running",
        started_at=now,
    )
    db.add(check_run)
    db.flush()

    try:
        _run_checks(db, groups, check_run)
    except Exception:
        check_run.status = "failed"
        check_run.finished_at = datetime.now(timezone.utc)
        db.commit()
        raise

    return check_run


def run_checks_for_group(db: Session, group: SyncGroup) -> CheckRun:
    """Run all enabled checkers for a single sync group."""
    now = datetime.now(timezone.utc)
    check_run = CheckRun(
        sync_group_id=group.id,
        status="running",
        started_at=now,
    )
    db.add(check_run)
    db.flush()

    try:
        _run_checks(db, [group], check_run)
    except Exception:
        check_run.status = "failed"
        check_run.finished_at = datetime.now(timezone.utc)
        db.commit()
        raise

    return check_run
