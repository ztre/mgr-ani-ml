"""Database engine/session setup."""
from __future__ import annotations

from pathlib import Path
from sqlalchemy import create_engine, text
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
  if sqlite_path is not None:
    _ensure_directory_states_unique_index()


def _ensure_directory_states_unique_index() -> None:
  # Ensure legacy SQLite DB has the UNIQUE index required by ON CONFLICT.
  with engine.connect() as conn:
    rows = conn.execute(text("PRAGMA index_list('directory_states')")).fetchall()
    for row in rows:
      if str(row[1]).lower() == "uq_directory_states_sync_group_dir_path":
        return

    dupes = conn.execute(
      text(
        "SELECT sync_group_id, dir_path, COUNT(*) "
        "FROM directory_states GROUP BY sync_group_id, dir_path HAVING COUNT(*) > 1"
      )
    ).fetchall()
    if dupes:
      conn.execute(
        text(
          "DELETE FROM directory_states "
          "WHERE rowid NOT IN ("
          "  SELECT MIN(rowid) FROM directory_states "
          "  GROUP BY sync_group_id, dir_path"
          ")"
        )
      )
      conn.commit()

    conn.execute(
      text(
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "uq_directory_states_sync_group_dir_path "
        "ON directory_states (sync_group_id, dir_path)"
      )
    )
    conn.commit()


def get_db():
  db = SessionLocal()
  try:
    yield db
  finally:
    db.close()
