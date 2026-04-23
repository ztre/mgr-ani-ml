"""Database engine/session setup."""
from __future__ import annotations

import logging
from pathlib import Path
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from .config import settings
from .models import Base, ScanTask

_log = logging.getLogger(__name__)


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
    _migrate_schema()
    _ensure_inode_identity_indexes()
    _ensure_directory_states_unique_index()
    _ensure_resource_lookup_indexes()
    _backfill_media_record_metadata()
    _backfill_attachment_backup_sources_and_inodes()
    _backfill_inode_record_devices()


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


def _migrate_schema() -> None:
  """Incremental schema migration for existing SQLite databases.

  All DDL changes are guarded by structural inspection (PRAGMA) so they are
  idempotent and safe to run against both fresh and legacy DBs.  Any error
  is logged and re-raised so startup fails visibly rather than silently.
  """
  try:
    with engine.connect() as conn:
      _migrate_sync_groups_enabled_checks(conn)
      _migrate_check_runs_columns(conn)
      _migrate_check_issues_columns(conn)
      _migrate_check_issues_fingerprint_index(conn)
      _migrate_media_records_columns(conn)
      _migrate_inode_records_columns(conn)
      conn.commit()
  except Exception:
    _log.exception("Schema migration failed — database may be in an inconsistent state")
    raise


def _table_columns(conn, table_name: str) -> set[str]:
  """Return the set of column names for a table via PRAGMA table_info."""
  rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
  return {row[1] for row in rows}  # column index 1 = name


def _table_exists(conn, table_name: str) -> bool:
  row = conn.execute(
    text("SELECT name FROM sqlite_master WHERE type='table' AND name=:n"),
    {"n": table_name},
  ).fetchone()
  return row is not None


def _migrate_sync_groups_enabled_checks(conn) -> None:
  cols = _table_columns(conn, "sync_groups")
  if "enabled_checks" not in cols:
    _log.info("Migration: adding column sync_groups.enabled_checks")
    conn.execute(text("ALTER TABLE sync_groups ADD COLUMN enabled_checks TEXT"))


def _migrate_check_runs_columns(conn) -> None:
  if not _table_exists(conn, "check_runs"):
    return  # create_all already created it; nothing to migrate
  expected = {
    "id": "INTEGER",
    "sync_group_id": "INTEGER",
    "status": "VARCHAR(20)",
    "started_at": "DATETIME",
    "finished_at": "DATETIME",
    "summary_json": "TEXT",
  }
  cols = _table_columns(conn, "check_runs")
  for col_name, col_type in expected.items():
    if col_name not in cols:
      _log.info("Migration: adding column check_runs.%s", col_name)
      conn.execute(text(f"ALTER TABLE check_runs ADD COLUMN {col_name} {col_type}"))


def _migrate_check_issues_columns(conn) -> None:
  if not _table_exists(conn, "check_issues"):
    return  # create_all already created it; nothing to migrate
  expected = {
    "id": "INTEGER",
    "check_run_id": "INTEGER",
    "checker_code": "VARCHAR(64)",
    "issue_code": "VARCHAR(64)",
    "severity": "VARCHAR(20)",
    "sync_group_id": "INTEGER",
    "source_path": "VARCHAR(2048)",
    "target_path": "VARCHAR(2048)",
    "resource_dir": "VARCHAR(2048)",
    "tmdb_id": "INTEGER",
    "season": "INTEGER",
    "episode": "INTEGER",
    "payload_json": "TEXT",
    "status": "VARCHAR(20)",
    "fingerprint": "VARCHAR(128)",
    "created_at": "DATETIME",
    "updated_at": "DATETIME",
    "resolved_at": "DATETIME",
  }
  cols = _table_columns(conn, "check_issues")
  for col_name, col_type in expected.items():
    if col_name not in cols:
      _log.info("Migration: adding column check_issues.%s", col_name)
      conn.execute(text(f"ALTER TABLE check_issues ADD COLUMN {col_name} {col_type}"))


def _migrate_check_issues_fingerprint_index(conn) -> None:
  if not _table_exists(conn, "check_issues"):
    return
  rows = conn.execute(text("PRAGMA index_list('check_issues')")).fetchall()
  existing_indexes = {str(row[1]).lower() for row in rows}  # index 1 = name
  if "uq_check_issue_fingerprint" not in existing_indexes:
    _log.info("Migration: creating unique index uq_check_issue_fingerprint on check_issues")
    conn.execute(
      text(
        "CREATE UNIQUE INDEX uq_check_issue_fingerprint "
        "ON check_issues (fingerprint)"
      )
    )


def _migrate_media_records_columns(conn) -> None:
  new_cols = [
    ("season", "INTEGER"),
    ("episode", "INTEGER"),
    ("category", "VARCHAR(50)"),
    ("file_type", "VARCHAR(20)"),
  ]
  cols = _table_columns(conn, "media_records")
  for col_name, col_type in new_cols:
    if col_name not in cols:
      _log.info("Migration: adding column media_records.%s", col_name)
      conn.execute(text(f"ALTER TABLE media_records ADD COLUMN {col_name} {col_type}"))


def _migrate_inode_records_columns(conn) -> None:
  if not _table_exists(conn, "inode_records"):
    return
  cols = _table_columns(conn, "inode_records")
  if "device" not in cols:
    _log.info("Migration: adding column inode_records.device")
    conn.execute(text("ALTER TABLE inode_records ADD COLUMN device INTEGER"))


def _migrate_scan_tasks_to_task_db() -> None:
  if _TASK_DATABASE_URL == settings.database_url:
    return

  try:
    with engine.connect() as source_conn:
      if not _table_exists(source_conn, "scan_tasks"):
        return
      available_cols = _table_columns(source_conn, "scan_tasks")

    # 仅迁移目标表已有的列，旧 schema 缺列时用 None 补全
    _TASK_COLS = ["id", "type", "target_id", "target_name", "status", "log_file", "created_at", "finished_at"]
    select_cols = [c for c in _TASK_COLS if c in available_cols]
    if not select_cols or "id" not in select_cols:
      return

    with engine.connect() as source_conn:
      source_rows = source_conn.execute(
        text(f"SELECT {', '.join(select_cols)} FROM scan_tasks ORDER BY id")
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
        insert_data = {c: row.get(c) for c in _TASK_COLS}
        task_conn.execute(
          text(
            "INSERT INTO scan_tasks (id, type, target_id, target_name, status, log_file, created_at, finished_at) "
            "VALUES (:id, :type, :target_id, :target_name, :status, :log_file, :created_at, :finished_at)"
          ),
          insert_data,
        )
  except Exception:
    _log.warning("scan_tasks 迁移到 task_db 失败，跳过迁移（已有记录不受影响）", exc_info=True)


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


def _ensure_inode_identity_indexes() -> None:
  with engine.connect() as conn:
    if not _table_exists(conn, "inode_records"):
      return
    rows = conn.execute(text("PRAGMA index_list('inode_records')")).fetchall()
    for row in rows:
      index_name = str(row[1] or "")
      if index_name.lower() == "ix_inode_records_inode":
        _log.info("Migration: dropping legacy unique index ix_inode_records_inode")
        conn.execute(text("DROP INDEX IF EXISTS ix_inode_records_inode"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_inode_records_inode ON inode_records (inode)"))
    conn.execute(
      text(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_inode_records_device_inode "
        "ON inode_records (device, inode)"
      )
    )
    conn.commit()


def _backfill_media_record_metadata() -> None:
  from .models import MediaRecord
  from .services.media_record_metadata import infer_media_record_metadata

  db = SessionLocal()
  updated = 0
  try:
    rows = db.query(MediaRecord).yield_per(500)
    for row in rows:
      metadata = infer_media_record_metadata(
        media_type=str(getattr(row, "type", "") or "tv"),
        original_path=getattr(row, "original_path", None),
        target_path=getattr(row, "target_path", None),
        season=getattr(row, "season", None),
        episode=getattr(row, "episode", None),
        category=getattr(row, "category", None),
        file_type=getattr(row, "file_type", None),
      )
      changed = False
      if getattr(row, "season", None) != metadata.season:
        row.season = metadata.season
        changed = True
      if getattr(row, "episode", None) != metadata.episode:
        row.episode = metadata.episode
        changed = True
      if getattr(row, "category", None) != metadata.category:
        row.category = metadata.category
        changed = True
      if getattr(row, "file_type", None) != metadata.file_type:
        row.file_type = metadata.file_type
        changed = True
      if changed:
        updated += 1
    if updated:
      _log.info("Migration: backfilled structured media metadata for %d rows", updated)
      db.commit()
    else:
      db.rollback()
  except Exception:
    db.rollback()
    _log.exception("MediaRecord metadata backfill failed")
    raise
  finally:
    db.close()


def _backfill_attachment_backup_sources_and_inodes() -> None:
  from .models import InodeRecord, ManualAttachmentBackup, MediaRecord
  from .services.library_mutations import upsert_inode_record
  from .services.linker import get_file_identity

  db = SessionLocal()
  media_updates = 0
  inode_updates = 0
  inode_creates = 0
  try:
    attachment_rows = (
      db.query(MediaRecord, ManualAttachmentBackup)
      .join(ManualAttachmentBackup, ManualAttachmentBackup.media_record_id == MediaRecord.id)
      .filter(MediaRecord.file_type == "attachment")
      .all()
    )
    backup_by_target: dict[str, str] = {}
    for media_row, backup_row in attachment_rows:
      backup_path = str(getattr(backup_row, "backup_path", "") or "").strip()
      if not backup_path:
        continue
      target_path = str(getattr(media_row, "target_path", "") or "").strip()
      if target_path:
        backup_by_target[target_path] = backup_path
      if str(getattr(media_row, "original_path", "") or "").strip() != backup_path:
        media_row.original_path = backup_path
        media_updates += 1

    inode_rows = db.query(InodeRecord).yield_per(500)
    for inode_row in inode_rows:
      target_path = str(getattr(inode_row, "target_path", "") or "").strip()
      backup_path = backup_by_target.get(target_path)
      if backup_path and str(getattr(inode_row, "source_path", "") or "").strip() != backup_path:
        inode_row.source_path = backup_path
        inode_updates += 1
      source_value = str(getattr(inode_row, "source_path", "") or "").strip()
      identity = get_file_identity(source_value) if source_value else None
      if identity is None and target_path:
        identity = get_file_identity(target_path)
      if identity is None:
        continue
      device, inode = identity
      if getattr(inode_row, "device", None) != int(device):
        inode_row.device = int(device)
        inode_updates += 1
      if getattr(inode_row, "inode", None) != int(inode):
        inode_row.inode = int(inode)
        inode_updates += 1

    existing_targets = {
      str(value[0])
      for value in db.query(InodeRecord.target_path)
      .filter(InodeRecord.target_path.isnot(None), InodeRecord.target_path != "")
      .all()
      if value and str(value[0] or "").strip()
    }
    for media_row, backup_row in attachment_rows:
      target_path = str(getattr(media_row, "target_path", "") or "").strip()
      backup_path = str(getattr(backup_row, "backup_path", "") or "").strip()
      if not target_path or not backup_path or target_path in existing_targets:
        continue
      created = upsert_inode_record(
        db,
        sync_group_id=getattr(media_row, "sync_group_id", None),
        src_path=backup_path,
        dst_path=target_path,
      )
      if created is not None:
        existing_targets.add(target_path)
        inode_creates += 1

    if media_updates or inode_updates or inode_creates:
      _log.info(
        "Migration: fixed attachment backup sources (media=%d inode_updates=%d inode_creates=%d)",
        media_updates,
        inode_updates,
        inode_creates,
      )
      db.commit()
    else:
      db.rollback()
  except Exception:
    db.rollback()
    _log.exception("Attachment backup/inode backfill failed")
    raise
  finally:
    db.close()


def _backfill_inode_record_devices() -> None:
  from .models import InodeRecord
  from .services.linker import get_file_identity

  db = SessionLocal()
  updated = 0
  try:
    rows = db.query(InodeRecord).yield_per(500)
    for row in rows:
      source_value = str(getattr(row, "source_path", "") or "").strip()
      target_value = str(getattr(row, "target_path", "") or "").strip()
      identity = get_file_identity(source_value) if source_value else None
      if identity is None and target_value:
        identity = get_file_identity(target_value)
      if identity is None:
        continue
      device, inode = identity
      changed = False
      if getattr(row, "device", None) != int(device):
        row.device = int(device)
        changed = True
      if getattr(row, "inode", None) != int(inode):
        row.inode = int(inode)
        changed = True
      if changed:
        updated += 1
    if updated:
      _log.info("Migration: backfilled inode device identity for %d rows", updated)
      db.commit()
    else:
      db.rollback()
  except Exception:
    db.rollback()
    _log.exception("InodeRecord device backfill failed")
    raise
  finally:
    db.close()


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
