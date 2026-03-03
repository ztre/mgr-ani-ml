"""Task history APIs."""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import ScanTask
from .logs import LOG_DIR, _tail_lines, cleanup_logs_if_needed

router = APIRouter()


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
    return db.query(ScanTask).order_by(ScanTask.created_at.desc()).offset(offset).limit(limit).all()


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
