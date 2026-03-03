"""SQLAlchemy models."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class SyncGroup(Base):
    __tablename__ = "sync_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    source = Column(String(1024), nullable=False)
    source_type = Column(String(20), nullable=False)  # tv | movie
    target = Column(String(1024), nullable=False)
    include = Column(Text, default="")
    exclude = Column(Text, default="")
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class MediaRecord(Base):
    __tablename__ = "media_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sync_group_id = Column(Integer, nullable=True)
    original_path = Column(String(2048), nullable=False, index=True)
    target_path = Column(String(2048), nullable=True)
    type = Column(String(20), nullable=False)  # tv | movie
    tmdb_id = Column(Integer, nullable=True)
    bangumi_id = Column(Integer, nullable=True)
    status = Column(String(50), default="pending")
    size = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class InodeRecord(Base):
    __tablename__ = "inode_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    inode = Column(Integer, nullable=False, unique=True, index=True)
    source_path = Column(String(2048), nullable=False)
    target_path = Column(String(2048), nullable=True)
    sync_group_id = Column(Integer, nullable=True)
    size = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ScanTask(Base):
    __tablename__ = "scan_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(40), default="full")
    target_id = Column(Integer, nullable=True)
    target_name = Column(String(255), nullable=True)
    status = Column(String(20), default="running")
    log_file = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)


class DirectoryState(Base):
    __tablename__ = "directory_states"
    __table_args__ = (UniqueConstraint("sync_group_id", "dir_path", name="uq_dir_state"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    sync_group_id = Column(Integer, nullable=False, index=True)
    dir_path = Column(String(2048), nullable=False, index=True)
    signature = Column(String(128), nullable=False, default="")
    status = Column(String(50), nullable=False, default="SCANNED")
    last_error = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
