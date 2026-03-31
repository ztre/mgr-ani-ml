"""Task history APIs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import ScanTask
from ..services.scanner import request_scan_cancel
from .logs import LOG_DIR, _tail_lines, append_log, cleanup_logs_if_needed, current_task_id

router = APIRouter()
DEFAULT_DISPLAY_TZ = timezone(timedelta(hours=8))


def _to_default_display_time(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.astimezone(DEFAULT_DISPLAY_TZ).replace(tzinfo=None)


def _normalize_task_type(task_type: str | None) -> str:
    raw = str(task_type or "").strip()
    return raw[len("issue_sp:"):] if raw.startswith("issue_sp:") else raw


def _is_cancelable_scan_task(task: ScanTask) -> bool:
    task_type = _normalize_task_type(task.type)
    return (
        task_type == "full"
        or task_type == "group"
        or task_type.startswith("webhook_scan:")
        or task_type.startswith("manual:")
    )


class ScanTaskResponse(BaseModel):
    id: int
    type: str
    target_id: int | None
    target_name: str | None
    status: str
    created_at: datetime
    finished_at: datetime | None

    class Config:
        from_attributes = True


@router.get("", response_model=list[ScanTaskResponse])
def list_tasks(db: Session = Depends(get_db), limit: int = Query(10, ge=1, le=200), offset: int = Query(0, ge=0)):
    cleanup_logs_if_needed()
    tasks = db.query(ScanTask).order_by(ScanTask.created_at.desc()).offset(offset).limit(limit).all()
    return [
        ScanTaskResponse(
            id=task.id,
            type=task.type,
            target_id=task.target_id,
            target_name=task.target_name,
            status=task.status,
            created_at=_to_default_display_time(task.created_at),
            finished_at=_to_default_display_time(task.finished_at),
        )
        for task in tasks
    ]


@router.get("/{task_id}/logs")
def get_task_logs(task_id: int, limit: int = Query(400, ge=1, le=5000), db: Session = Depends(get_db)):
    cleanup_logs_if_needed()
    task = db.query(ScanTask).filter(ScanTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    path = LOG_DIR / f"task_{task_id}.log"
    if not path.exists():
        return {"logs": ["暂无日志"]}
    return {"logs": _tail_lines(path, limit)}


@router.post("/{task_id}/cancel")
def cancel_task(task_id: int, db: Session = Depends(get_db)):
    cleanup_logs_if_needed()
    task = db.query(ScanTask).filter(ScanTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    if not _is_cancelable_scan_task(task):
        raise HTTPException(status_code=409, detail="该任务类型暂不支持中断")
    if task.status == "cancelled":
        return {"message": "任务已取消"}
    if task.status not in {"running", "cancelling"}:
        raise HTTPException(status_code=409, detail="只有运行中的扫描任务可以中断")

    if not request_scan_cancel(task.id):
        raise HTTPException(status_code=409, detail="任务未处于可中断状态")

    if task.status != "cancelling":
        task.status = "cancelling"
        db.commit()

    token = current_task_id.set(task.id)
    try:
        append_log("INFO: 用户请求中断任务，等待当前处理单元安全停止")
    finally:
        current_task_id.reset(token)

    return {"message": "已发送中断请求，等待当前处理单元停止"}


@router.delete("/all")
def delete_all_tasks(db: Session = Depends(get_db)):
    count = db.query(ScanTask).delete()
    db.commit()
    for p in LOG_DIR.glob("task_*.log"):
        try:
            p.unlink()
        except OSError:
            pass
    return {"message": f"已删除 {count} 条任务记录并清理日志"}
