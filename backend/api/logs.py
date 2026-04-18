"""Task log file utilities and APIs."""
from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re

from fastapi import APIRouter, HTTPException, Query

from ..config import settings

router = APIRouter()

LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

current_task_id: ContextVar[int | None] = ContextVar("current_task_id", default=None)
_last_cleanup_at: datetime | None = None
APP_LOG_FILENAMES: tuple[str, ...] = tuple(["app.log", *(f"app.log.{index}" for index in range(1, 6))])


def append_task_log(task_id: int | None, message: str) -> None:
    if not task_id:
        return

    msg = str(message or "").strip()
    if not re.match(r"^(INFO|WARNING|ERROR):\s", msg):
        msg = f"INFO: {msg}"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] {msg}\n"
    path = LOG_DIR / f"task_{task_id}.log"
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def append_log(message: str) -> None:
    append_task_log(current_task_id.get(), message)


def _tail_lines(path: Path, limit: int) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return lines[-limit:]
    except Exception:
        return ["读取日志失败"]


def _build_log_file_meta(path: Path) -> dict:
    return {
        "name": path.name,
        "size": path.stat().st_size if path.exists() else 0,
        "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat() if path.exists() else None,
    }


def _resolve_app_log_name(file_name: str | None) -> str:
    raw_value = file_name
    if raw_value is not None and not isinstance(raw_value, str):
        raw_value = getattr(raw_value, "default", None)
    normalized = str(raw_value or "app.log").strip()
    if normalized not in APP_LOG_FILENAMES:
        raise HTTPException(status_code=400, detail="仅支持查看 app.log 到 app.log.5")
    return normalized


def _list_app_log_files() -> list[dict]:
    out = []
    for name in APP_LOG_FILENAMES:
        path = LOG_DIR / name
        item = _build_log_file_meta(path)
        item["exists"] = path.exists()
        out.append(item)
    return out


def cleanup_logs_if_needed(force: bool = False) -> None:
    global _last_cleanup_at
    now = datetime.now(timezone.utc)
    interval = max(60, int(settings.log_cleanup_interval_seconds or 600))
    if not force and _last_cleanup_at and (now - _last_cleanup_at).total_seconds() < interval:
        return
    _last_cleanup_at = now

    retention_days = max(0, int(settings.log_retention_days or 14))
    max_files = max(10, int(settings.log_max_task_files or 200))

    files = sorted(LOG_DIR.glob("task_*.log"), key=lambda p: p.stat().st_mtime if p.exists() else 0)

    if retention_days > 0:
        cutoff = now - timedelta(days=retention_days)
        for p in files:
            try:
                mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            except Exception:
                continue
            if mtime < cutoff:
                try:
                    p.unlink()
                except OSError:
                    pass

    files = sorted(LOG_DIR.glob("task_*.log"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
    overflow = len(files) - max_files
    if overflow > 0:
        for p in files[:overflow]:
            try:
                p.unlink()
            except OSError:
                pass


@router.get("")
def list_logs(limit: int = Query(200, ge=1, le=5000)):
    cleanup_logs_if_needed()
    files = sorted(LOG_DIR.glob("task_*.log"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    out = []
    for p in files[:limit]:
        out.append(_build_log_file_meta(p))
    return {"items": out}


@router.get("/app")
def get_app_logs(
    file_name: str = Query("app.log"),
    limit: int = Query(400, ge=1, le=5000),
):
    cleanup_logs_if_needed()
    selected_name = _resolve_app_log_name(file_name)
    path = LOG_DIR / selected_name
    files = _list_app_log_files()
    if not path.exists():
        return {
            "name": path.name,
            "size": 0,
            "updated_at": None,
            "logs": ["暂无日志"],
            "files": files,
        }
    result = _build_log_file_meta(path)
    result["logs"] = _tail_lines(path, limit)
    result["files"] = files
    return result
