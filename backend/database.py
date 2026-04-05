"""Database engine/session setup."""
from __future__ import annotations

from pathlib import Path
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from .config import settings
from .models import Base, ScanTask


_REQUEST_DB_TIMEOUT_SECONDS = 1.0


def _sqlite_connect_args(timeout_seconds: float) -> dict:
  return {'check_same_thread': False, 'timeout': float(timeout_seconds)}


def _sqlite_busy_timeout_ms(timeout_seconds: float) -> int:
  return max(1, int(float(timeout_seconds) * 1000))


def _sqlite_file_path(database_url: str) -> Path | None:
  if not database_url.startswith('sqlite:///'):
    return None
  raw = database_url.replace('sqlite:///', '', 1)
  # sqlite:////abs/path.db => /abs/path.db
  raw = '/' + raw.lstrip('/')
  return Path(raw)


def _task_database_url(database_url: str) -> str:
  sqlite_path = _sqlite_file_path(database_url)
  if sqlite_path is None:
    return database_url
  task_path = sqlite_path.with_name(f"{sqlite_path.stem}.tasks{sqlite_path.suffix or '.db'}")
  return f"sqlite:///{task_path}"


_IS_SQLITE = 'sqlite' in settings.database_url
engine = create_engine(
  settings.database_url,
  connect_args=_sqlite_connect_args(30) if _IS_SQLITE else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
request_engine = create_engine(
  settings.database_url,
  connect_args=_sqlite_connect_args(_REQUEST_DB_TIMEOUT_SECONDS) if _IS_SQLITE else {},
)
RequestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=request_engine)

_TASK_DATABASE_URL = _task_database_url(settings.database_url)
_IS_TASK_SQLITE = 'sqlite' in _TASK_DATABASE_URL
task_engine = create_engine(
  _TASK_DATABASE_URL,
  connect_args=_sqlite_connect_args(30) if _IS_TASK_SQLITE else {},
)
TaskSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=task_engine)
request_task_engine = create_engine(
  _TASK_DATABASE_URL,
  connect_args=_sqlite_connect_args(_REQUEST_DB_TIMEOUT_SECONDS) if _IS_TASK_SQLITE else {},
)
RequestTaskSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=request_task_engine)


if _IS_SQLITE:
  @event.listens_for(engine, "connect")
  def _set_sqlite_pragmas(dbapi_connection, connection_record) -> None:
    cursor = dbapi_connection.cursor()
    try:
      cursor.execute("PRAGMA journal_mode=WAL")
      cursor.execute("PRAGMA synchronous=NORMAL")
      cursor.execute("PRAGMA busy_timeout=30000")
    finally:
      cursor.close()


if _IS_SQLITE:
  @event.listens_for(request_engine, "connect")
  def _set_request_sqlite_pragmas(dbapi_connection, connection_record) -> None:
    cursor = dbapi_connection.cursor()
    try:
      cursor.execute("PRAGMA journal_mode=WAL")
      cursor.execute("PRAGMA synchronous=NORMAL")
      cursor.execute(f"PRAGMA busy_timeout={_sqlite_busy_timeout_ms(_REQUEST_DB_TIMEOUT_SECONDS)}")
    finally:
      cursor.close()


if _IS_TASK_SQLITE:
  @event.listens_for(task_engine, "connect")
  def _set_task_sqlite_pragmas(dbapi_connection, connection_record) -> None:
    cursor = dbapi_connection.cursor()
    try:
      cursor.execute("PRAGMA journal_mode=WAL")
      cursor.execute("PRAGMA synchronous=NORMAL")
      cursor.execute("PRAGMA busy_timeout=30000")
    finally:
      cursor.close()


if _IS_TASK_SQLITE:
  @event.listens_for(request_task_engine, "connect")
  def _set_request_task_sqlite_pragmas(dbapi_connection, connection_record) -> None:
    cursor = dbapi_connection.cursor()
    try:
      cursor.execute("PRAGMA journal_mode=WAL")
      cursor.execute("PRAGMA synchronous=NORMAL")
      cursor.execute(f"PRAGMA busy_timeout={_sqlite_busy_timeout_ms(_REQUEST_DB_TIMEOUT_SECONDS)}")
    finally:
      cursor.close()


def init_db() -> None:
  sqlite_path = _sqlite_file_path(settings.database_url)
  task_sqlite_path = _sqlite_file_path(_TASK_DATABASE_URL)
  if sqlite_path is not None:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if not sqlite_path.exists():
      sqlite_path.touch()
    _apply_sqlite_pragmas()
  if task_sqlite_path is not None:
    task_sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if not task_sqlite_path.exists():
      task_sqlite_path.touch()
    _apply_task_sqlite_pragmas()
  Base.metadata.create_all(bind=engine)
  ScanTask.__table__.create(bind=task_engine, checkfirst=True)
  _migrate_scan_tasks_to_task_db()
  if sqlite_path is not None:
    _ensure_directory_states_unique_index()
    _ensure_resource_lookup_indexes()


def _apply_sqlite_pragmas() -> None:
  with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL"))
    conn.execute(text("PRAGMA synchronous=NORMAL"))
    conn.execute(text("PRAGMA busy_timeout=30000"))


def _apply_task_sqlite_pragmas() -> None:
  with task_engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL"))
    conn.execute(text("PRAGMA synchronous=NORMAL"))
    conn.execute(text("PRAGMA busy_timeout=30000"))


def _migrate_scan_tasks_to_task_db() -> None:
  if _TASK_DATABASE_URL == settings.database_url:
    return

  with engine.connect() as source_conn:
    source_rows = source_conn.execute(
      text(
        "SELECT id, type, target_id, target_name, status, log_file, created_at, finished_at "
        "FROM scan_tasks ORDER BY id"
      )
    ).mappings().all()

  if not source_rows:
    return

  with task_engine.begin() as task_conn:
    existing_ids = {
      int(row[0])
      for row in task_conn.execute(text("SELECT id FROM scan_tasks")).fetchall()
      if row and row[0] is not None
    }
    for row in source_rows:
      row_id = row.get("id")
      if row_id is None or int(row_id) in existing_ids:
        continue
      task_conn.execute(
        text(
          "INSERT INTO scan_tasks (id, type, target_id, target_name, status, log_file, created_at, finished_at) "
          "VALUES (:id, :type, :target_id, :target_name, :status, :log_file, :created_at, :finished_at)"
        ),
        dict(row),
      )


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


def _ensure_resource_lookup_indexes() -> None:
  with engine.connect() as conn:
    statements = [
      "CREATE INDEX IF NOT EXISTS idx_media_records_target_path ON media_records (target_path)",
      "CREATE INDEX IF NOT EXISTS idx_media_records_sync_group_target_path ON media_records (sync_group_id, target_path)",
      "CREATE INDEX IF NOT EXISTS idx_inode_records_target_path ON inode_records (target_path)",
      "CREATE INDEX IF NOT EXISTS idx_inode_records_sync_group_target_path ON inode_records (sync_group_id, target_path)",
    ]
    for statement in statements:
      conn.execute(text(statement))
    conn.commit()


def get_db():
  db = RequestSessionLocal()
  try:
    yield db
  finally:
    db.close()


def get_task_db():
  db = RequestTaskSessionLocal()
  try:
    yield db
  finally:
    db.close()
