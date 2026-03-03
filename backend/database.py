"""Database engine/session setup."""
from __future__ import annotations

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .config import settings
from .models import Base


def _sqlite_file_path(database_url: str) -> Path | None:
  if not database_url.startswith('sqlite:///'):
    return None
  raw = database_url.replace('sqlite:///', '', 1)
  # sqlite:////abs/path.db => /abs/path.db
  raw = '/' + raw.lstrip('/')
  return Path(raw)


engine = create_engine(
  settings.database_url,
  connect_args={'check_same_thread': False} if 'sqlite' in settings.database_url else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
  sqlite_path = _sqlite_file_path(settings.database_url)
  if sqlite_path is not None:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if not sqlite_path.exists():
      sqlite_path.touch()
  Base.metadata.create_all(bind=engine)


def get_db():
  db = SessionLocal()
  try:
    yield db
  finally:
    db.close()
