"""Inode management APIs."""
from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import InodeRecord
from ..services.resource_tree import build_resource_summaries, build_resource_tree

router = APIRouter()


class InodeBatchDeleteRequest(BaseModel):
    ids: list[int]


class InodeDeleteScopeRequest(BaseModel):
    scope_level: str
    item_ids: list[int]
    resource_dir: str
    type: str
    sync_group_id: int | None = None
    tmdb_id: int | None = None
    season: int | None = None
    group_kind: str | None = None
    group_label: str | None = None


def _inode_to_dict(row: InodeRecord) -> dict:
    return {
        "id": row.id,
        "inode": row.inode,
        "source_path": row.source_path,
        "original_path": row.source_path,
        "target_path": row.target_path,
        "sync_group_id": row.sync_group_id,
        "size": row.size,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _target_dir_like_expr(target_dir_norm: str):
    lower_target_path = func.lower(func.replace(InodeRecord.target_path, "\\", "/"))
    # 只对 ASCII A-Z 做小写化，与 SQLite LOWER() 行为一致；
    # Python str.lower() 会转换非 ASCII 大写字符（如 Ⅱ→ⅱ），SQLite 不会，导致匹配失败。
    base = re.sub(r"[A-Z]", lambda m: m.group().lower(), target_dir_norm).rstrip("/")
    return or_(
        lower_target_path == base,
        lower_target_path.like(f"{base}/%"),
    )


def _normalize_scope_item_ids(item_ids: list[int]) -> list[int]:
    return sorted({int(value) for value in item_ids if isinstance(value, int) or str(value).isdigit()})


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


@router.get("/resources")
def list_inode_resources(
    db: Session = Depends(get_db),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=500),
    search: str | None = None,
    sync_group_id: int | None = Query(None),
):
    q = db.query(InodeRecord).filter(InodeRecord.target_path.isnot(None), InodeRecord.target_path != "")
    if search:
        q = q.filter(or_(InodeRecord.source_path.contains(search), InodeRecord.target_path.contains(search)))
    if sync_group_id is not None:
        q = q.filter(InodeRecord.sync_group_id == sync_group_id)

    rows = q.order_by(InodeRecord.updated_at.desc()).all()
    summaries = build_resource_summaries([_inode_to_dict(row) for row in rows])
    total = len(summaries)
    items = summaries[offset: offset + limit]
    return {"total": total, "items": items, "offset": offset, "limit": limit}


@router.get("/resource-tree")
def get_inode_resource_tree(
    resource_dir: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
    sync_group_id: int | None = Query(None),
):
    normalized_resource_dir = resource_dir.strip().replace("\\", "/").rstrip("/")
    if not normalized_resource_dir:
        raise HTTPException(status_code=400, detail="resource_dir 不能为空")

    q = db.query(InodeRecord).filter(InodeRecord.target_path.isnot(None), InodeRecord.target_path != "")
    q = q.filter(_target_dir_like_expr(normalized_resource_dir))
    if sync_group_id is not None:
        q = q.filter(InodeRecord.sync_group_id == sync_group_id)
    rows = q.order_by(InodeRecord.updated_at.desc()).all()
    tree = build_resource_tree(
        [_inode_to_dict(row) for row in rows],
        resource_dir=normalized_resource_dir,
        sync_group_id=sync_group_id,
    )
    return tree


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


@router.post("/batch-delete")
def batch_delete_inodes(data: InodeBatchDeleteRequest, db: Session = Depends(get_db)):
    ids = [int(x) for x in data.ids if isinstance(x, int) or str(x).isdigit()]
    if not ids:
        raise HTTPException(status_code=400, detail="ids 不能为空")

    deleted = db.query(InodeRecord).filter(InodeRecord.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": int(deleted or 0)}


@router.post("/delete-scope")
def delete_inode_scope(data: InodeDeleteScopeRequest, db: Session = Depends(get_db)):
    ids = _normalize_scope_item_ids(data.item_ids)
    if not ids:
        raise HTTPException(status_code=400, detail="scope item_ids 不能为空")

    deleted = db.query(InodeRecord).filter(InodeRecord.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": int(deleted or 0)}
