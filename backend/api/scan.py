"""Scan trigger APIs."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..database import get_db
from ..models import SyncGroup
from ..services.scanner import run_scan
from ..services.task_queue import enqueue_task

router = APIRouter()


@router.post("/run")
def trigger_scan(db=Depends(get_db)):
    task = enqueue_task(
        db,
        task_type="full",
        target_id=None,
        target_name="全量扫描",
        queued_message="全量扫描任务已进入队列，等待执行",
        runner=lambda worker_db, queued_task: run_scan(worker_db, task=queued_task),
    )
    return {"message": "全量扫描任务已进入队列", "task_id": task.id, "status": task.status}


@router.post("/run/{group_id}")
def trigger_group_scan(group_id: int, db=Depends(get_db)):
    group = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
    if not group:
        raise HTTPException(status_code=404, detail="同步组不存在")

    task = enqueue_task(
        db,
        task_type="group",
        target_id=group_id,
        target_name=group.name,
        queued_message=f"同步组 {group.name} 扫描任务已进入队列，等待执行",
        runner=lambda worker_db, queued_task: run_scan(worker_db, group_id=group_id, task=queued_task),
    )
    return {"message": f"同步组 {group.name} 扫描任务已进入队列", "task_id": task.id, "status": task.status}
