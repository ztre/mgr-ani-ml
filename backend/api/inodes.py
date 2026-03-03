"""Inode management APIs."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import InodeRecord

router = APIRouter()


@router.get("")
def list_inodes(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=2000),
    search: str | None = None,
    sync_group_id: int | None = Query(None),
    has_target: bool | None = Query(None),
):
    q = db.query(InodeRecord)
    if search:
        q = q.filter(or_(InodeRecord.source_path.contains(search), InodeRecord.target_path.contains(search)))
    if sync_group_id is not None:
        q = q.filter(InodeRecord.sync_group_id == sync_group_id)
    if has_target is True:
        q = q.filter(InodeRecord.target_path.isnot(None), InodeRecord.target_path != "")
    elif has_target is False:
        q = q.filter(or_(InodeRecord.target_path.is_(None), InodeRecord.target_path == ""))

    total = q.count()
    items = q.order_by(InodeRecord.updated_at.desc()).offset(skip).limit(limit).all()
    return {"total": total, "items": items, "skip": skip, "limit": limit}


@router.delete("/cleanup")
def cleanup_inodes(db: Session = Depends(get_db)):
    deleted = 0
    ids: list[int] = []
    rows = db.query(InodeRecord.id, InodeRecord.source_path).yield_per(1000)
    for inode_id, source in rows:
        if not source or not Path(source).exists():
            ids.append(inode_id)
            if len(ids) >= 500:
                deleted += db.query(InodeRecord).filter(InodeRecord.id.in_(ids)).delete(synchronize_session=False)
                ids.clear()
    if ids:
        deleted += db.query(InodeRecord).filter(InodeRecord.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
    return {"message": f"已清理 {deleted} 条无效记录"}


@router.delete("/all")
def delete_all_inodes(db: Session = Depends(get_db)):
    count = db.query(InodeRecord).delete()
    db.commit()
    return {"message": f"已删除 {count} 条记录"}


@router.delete("/{inode_id}")
def delete_inode(inode_id: int, db: Session = Depends(get_db)):
    row = db.query(InodeRecord).filter(InodeRecord.id == inode_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="记录不存在")
    db.delete(row)
    db.commit()
    return {"ok": True}
