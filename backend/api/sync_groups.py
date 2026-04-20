"""Sync group CRUD APIs."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..config import settings
from ..database import get_db
from ..models import SyncGroup

router = APIRouter()


class SyncGroupIn(BaseModel):
    name: str | None = None
    source: str | None = None
    source_type: str | None = None
    target: str | None = None
    include: str | None = ""
    exclude: str | None = ""
    enabled: bool | None = True
    enabled_checks: list[str] | None = None  # None = keep existing / use defaults


class SyncGroupSettingsResponse(BaseModel):
    subtitle_backup_root: str


class SyncGroupSettingsUpdate(BaseModel):
    subtitle_backup_root: str


@router.get("/settings", response_model=SyncGroupSettingsResponse)
def get_sync_group_settings():
    return SyncGroupSettingsResponse(
        subtitle_backup_root=str(settings.subtitle_backup_root or "/app/subtitle_backup"),
    )


@router.put("/settings")
def update_sync_group_settings(data: SyncGroupSettingsUpdate):
    subtitle_backup_root = (data.subtitle_backup_root or "").strip()
    if not subtitle_backup_root:
        raise HTTPException(status_code=400, detail="字幕备份根目录不能为空")
    settings.subtitle_backup_root = subtitle_backup_root
    settings.save_to_env()
    return {"ok": True, "subtitle_backup_root": subtitle_backup_root}


@router.get("")
def list_sync_groups(db: Session = Depends(get_db)):
    return db.query(SyncGroup).order_by(SyncGroup.id.asc()).all()


@router.get("/{group_id}")
def get_sync_group(group_id: int, db: Session = Depends(get_db)):
    row = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="同步组不存在")
    return row


@router.post("")
def create_sync_group(data: SyncGroupIn, db: Session = Depends(get_db)):
    if not (data.name and data.source and data.target and data.source_type in {"tv", "movie"}):
        raise HTTPException(status_code=400, detail="name/source/target/source_type 必填")

    row = SyncGroup(
        name=data.name.strip(),
        source=data.source.strip(),
        source_type=data.source_type,
        target=data.target.strip(),
        include=data.include or "",
        exclude=data.exclude or "",
        enabled=bool(data.enabled),
        enabled_checks=json.dumps(data.enabled_checks) if data.enabled_checks is not None else None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.put("/{group_id}")
def update_sync_group(group_id: int, data: SyncGroupIn, db: Session = Depends(get_db)):
    row = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="同步组不存在")

    if data.name is not None:
        row.name = data.name
    if data.source is not None:
        row.source = data.source
    if data.source_type is not None:
        if data.source_type not in {"tv", "movie"}:
            raise HTTPException(status_code=400, detail="source_type 必须是 tv 或 movie")
        row.source_type = data.source_type
    if data.target is not None:
        row.target = data.target
    if data.include is not None:
        row.include = data.include
    if data.exclude is not None:
        row.exclude = data.exclude
    if data.enabled is not None:
        row.enabled = bool(data.enabled)
    if data.enabled_checks is not None:
        row.enabled_checks = json.dumps(data.enabled_checks)
    row.updated_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(row)
    return row


@router.delete("/{group_id}")
def delete_sync_group(group_id: int, db: Session = Depends(get_db)):
    row = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="同步组不存在")
    db.delete(row)
    db.commit()
    return {"ok": True}
