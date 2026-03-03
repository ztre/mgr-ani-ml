"""Scan trigger APIs."""
from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks

from ..database import SessionLocal
from ..services.scanner import run_scan

router = APIRouter()


def _run_scan(group_id: int | None = None):
    db = SessionLocal()
    try:
        run_scan(db, group_id=group_id)
    finally:
        db.close()


@router.post("/run")
def trigger_scan(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_scan)
    return {"message": "扫描任务已启动"}


@router.post("/run/{group_id}")
def trigger_group_scan(group_id: int, background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_scan, group_id)
    return {"message": f"同步组 {group_id} 扫描任务已启动"}
