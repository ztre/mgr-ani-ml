"""Checks center API."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import CheckIssue, CheckRun, SyncGroup
from ..services.checks import run_checks_full
from ..services.checks.path_filters import build_subtitle_issue_filter
from ..services.task_queue import enqueue_task

router = APIRouter()


# ---------------------------------------------------------------------------
# Trigger checks
# ---------------------------------------------------------------------------

@router.post("/run")
def run_full_check(db: Session = Depends(get_db)):
    """Enqueue a full check across all enabled sync groups."""
    def _runner(worker_db, _task):
        run_checks_full(worker_db)

    task = enqueue_task(
        db,
        task_type="check:full",
        target_id=None,
        target_name="全量检查",
        runner=_runner,
        queued_message="全量检查任务已进入队列",
    )
    return {"task_id": task.id, "status": task.status}


# ---------------------------------------------------------------------------
# Bulk delete (used by system reset)
# ---------------------------------------------------------------------------

@router.delete("/all")
def delete_all_checks(db: Session = Depends(get_db)):
    """Delete all CheckIssue and CheckRun records."""
    db.query(CheckIssue).delete()
    db.query(CheckRun).delete()
    db.commit()
    return {"message": "检查中心数据已清空"}


# ---------------------------------------------------------------------------
# Check runs history
# ---------------------------------------------------------------------------

@router.get("/runs")
def list_check_runs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    q = db.query(CheckRun).order_by(CheckRun.id.desc())
    total = q.count()
    runs = q.offset((page - 1) * page_size).limit(page_size).all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_run_to_dict(r) for r in runs],
    }


def _run_to_dict(run: CheckRun) -> dict:
    return {
        "id": run.id,
        "sync_group_id": run.sync_group_id,
        "status": run.status,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "summary_json": run.summary_json,
    }


# ---------------------------------------------------------------------------
# Check issues
# ---------------------------------------------------------------------------

@router.get("/issues")
def list_check_issues(
    status: str | None = Query(default=None),
    checker_code: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(CheckIssue).order_by(CheckIssue.updated_at.desc())
    q = q.filter(~build_subtitle_issue_filter(CheckIssue))
    if status:
        q = q.filter(CheckIssue.status == status)
    if checker_code:
        q = q.filter(CheckIssue.checker_code == checker_code)
    total = q.count()
    items = q.offset((page - 1) * page_size).limit(page_size).all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_issue_to_dict(i) for i in items],
    }


def _issue_to_dict(issue: CheckIssue) -> dict:
    return {
        "id": issue.id,
        "check_run_id": issue.check_run_id,
        "checker_code": issue.checker_code,
        "issue_code": issue.issue_code,
        "severity": issue.severity,
        "sync_group_id": issue.sync_group_id,
        "source_path": issue.source_path,
        "target_path": issue.target_path,
        "resource_dir": issue.resource_dir,
        "tmdb_id": issue.tmdb_id,
        "season": issue.season,
        "episode": issue.episode,
        "payload_json": issue.payload_json,
        "status": issue.status,
        "fingerprint": issue.fingerprint,
        "created_at": issue.created_at,
        "updated_at": issue.updated_at,
        "resolved_at": issue.resolved_at,
    }


# ---------------------------------------------------------------------------
# Issue actions
# ---------------------------------------------------------------------------

def _get_issue_or_404(issue_id: int, db: Session) -> CheckIssue:
    issue = db.query(CheckIssue).filter(CheckIssue.id == issue_id).first()
    if not issue:
        raise HTTPException(status_code=404, detail="检查问题不存在")
    return issue


@router.post("/issues/{issue_id}/ignore")
def ignore_issue(issue_id: int, db: Session = Depends(get_db)):
    issue = _get_issue_or_404(issue_id, db)
    if issue.status == "resolved":
        raise HTTPException(status_code=400, detail="已解决的问题不可忽略")
    issue.status = "ignored"
    issue.updated_at = datetime.now(timezone.utc)
    db.commit()
    return _issue_to_dict(issue)


@router.post("/issues/{issue_id}/resolve")
def resolve_issue(issue_id: int, db: Session = Depends(get_db)):
    issue = _get_issue_or_404(issue_id, db)
    now = datetime.now(timezone.utc)
    issue.status = "resolved"
    issue.updated_at = now
    issue.resolved_at = now
    db.commit()
    return _issue_to_dict(issue)


@router.post("/issues/{issue_id}/reopen")
def reopen_issue(issue_id: int, db: Session = Depends(get_db)):
    issue = _get_issue_or_404(issue_id, db)
    if issue.status not in ("ignored", "resolved"):
        raise HTTPException(status_code=400, detail=f"当前状态 {issue.status!r} 不可转为待处理（仅 ignored/resolved 可操作）")
    issue.status = "open"
    issue.resolved_at = None
    issue.updated_at = datetime.now(timezone.utc)
    db.commit()
    return _issue_to_dict(issue)


# ---------------------------------------------------------------------------
# Batch issue actions
# ---------------------------------------------------------------------------

class BatchIssueActionRequest(BaseModel):
    ids: list[int]
    action: Literal["resolve", "ignore", "reopen"]


@router.post("/issues/batch-action")
def batch_issue_action(req: BatchIssueActionRequest, db: Session = Depends(get_db)):
    """Bulk resolve / ignore / reopen a list of issue IDs."""
    if not req.ids:
        return {"updated": 0}
    now = datetime.now(timezone.utc)
    issues = db.query(CheckIssue).filter(CheckIssue.id.in_(req.ids)).all()
    updated = 0
    for issue in issues:
        if req.action == "resolve":
            issue.status = "resolved"
            issue.resolved_at = now
        elif req.action == "ignore":
            if issue.status != "resolved":
                issue.status = "ignored"
        elif req.action == "reopen":
            if issue.status in ("ignored", "resolved"):
                issue.status = "open"
                issue.resolved_at = None
        issue.updated_at = now
        updated += 1
    db.commit()
    return {"updated": updated}
