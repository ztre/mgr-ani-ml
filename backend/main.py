"""FastAPI app entrypoint."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import OperationalError

from .api import auth, checks, config as config_api, emby, inodes, logs, media, scan, sync_groups, tasks
from .api.logs import cleanup_logs_if_needed
from .config import settings
from .database import RequestSessionLocal, SessionLocal, init_db
from .models import MediaRecord, SyncGroup
from .security import require_auth
from .services.scanner import run_scan
from .services.task_queue import enqueue_task, ensure_task_queue_worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.auth_enabled:
        if not (settings.auth_secret or "").strip():
            raise RuntimeError("AUTH_SECRET 未配置，已拒绝启动（鉴权启用时必须设置强随机密钥）")
        if (settings.auth_username or "").strip() == "admin" and (settings.auth_password or "") == "admin123":
            raise RuntimeError("检测到默认账号口令 admin/admin123，请修改后再启动")
    init_db()
    cleanup_logs_if_needed(force=True)
    ensure_task_queue_worker()
    yield


app = FastAPI(
    title="Anime Media Manager",
    description="Anime recognition/organization backend",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(sync_groups.router, prefix="/api/sync-groups", tags=["sync-groups"], dependencies=[Depends(require_auth)])
app.include_router(media.router, prefix="/api/media", tags=["media"], dependencies=[Depends(require_auth)])
app.include_router(scan.router, prefix="/api/scan", tags=["scan"], dependencies=[Depends(require_auth)])
app.include_router(config_api.router, prefix="/api/config", tags=["config"], dependencies=[Depends(require_auth)])
app.include_router(emby.router, prefix="/api/emby", tags=["emby"], dependencies=[Depends(require_auth)])
app.include_router(logs.router, prefix="/api/logs", tags=["logs"], dependencies=[Depends(require_auth)])
app.include_router(tasks.router, prefix="/api/tasks", tags=["tasks"], dependencies=[Depends(require_auth)])
app.include_router(inodes.router, prefix="/api/inodes", tags=["inodes"], dependencies=[Depends(require_auth)])
app.include_router(checks.router, prefix="/api/checks", tags=["checks"], dependencies=[Depends(require_auth)])


@app.get("/api/health")
def health():
    return {"status": "ok"}


def _is_sqlite_busy_error(exc: OperationalError) -> bool:
    raw = str(getattr(exc, "orig", exc) or "").lower()
    return "database is locked" in raw or "database table is locked" in raw


@app.exception_handler(OperationalError)
async def handle_operational_error(request, exc: OperationalError):
    if _is_sqlite_busy_error(exc):
        return JSONResponse(status_code=409, content={"detail": "数据库正忙，请稍后重试"})
    return JSONResponse(status_code=500, content={"detail": "数据库操作失败"})


def _run_scan_group_task(
    group_id: int,
    task_type_override: str | None = None,
    target_name_override: str | None = None,
    target_dir_override: str | None = None,
):
    db = SessionLocal()
    try:
        run_scan(
            db,
            group_id=group_id,
            task_type_override=task_type_override,
            target_name_override=target_name_override,
            target_dir_override=target_dir_override,
        )
    finally:
        db.close()


@app.post("/sendTask")
def send_task(dirname: str = Form(...), group: str = Form(...), _username: str = Depends(require_auth)):
    dirname = (dirname or "").strip()
    group = (group or "").strip()
    if not dirname or not group:
        raise HTTPException(status_code=400, detail="dirname 和 group 不能为空")

    db = RequestSessionLocal()
    try:
        sync_group = db.query(SyncGroup).filter(SyncGroup.name == group).first()
        if not sync_group:
            raise HTTPException(status_code=404, detail=f"未找到同步组: {group}")

        # 优先匹配已有 pending_manual 记录（精确目录名）
        pending_rows = (
            db.query(MediaRecord)
            .filter(MediaRecord.sync_group_id == sync_group.id, MediaRecord.status == "pending_manual")
            .all()
        )
        matched = [row for row in pending_rows if Path(row.original_path or "").name == dirname]
        if len(matched) > 1:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": f"存在多个同名待办目录: {dirname}",
                    "candidates": [row.original_path for row in matched],
                },
            )

        # 如果没有 pending 记录，则使用同步组 source 目录拼接 dirname 作为目标路径
        if matched:
            target_dir = matched[0].original_path
            source = "pending_record"
        else:
            target_dir = str(Path(sync_group.source) / dirname)
            source = "source_dir"

        task = enqueue_task(
            db,
            task_type=f"webhook_scan:{sync_group.name}",
            target_id=sync_group.id,
            target_name=dirname,
            queued_message=f"webhook 扫描整理任务已进入队列，等待执行: {dirname}",
            runner=lambda worker_db, queued_task: run_scan(
                worker_db,
                group_id=sync_group.id,
                task=queued_task,
                task_type_override=f"webhook_scan:{sync_group.name}",
                target_name_override=dirname,
                target_dir_override=target_dir,
            ),
        )
        return {
            "ok": True,
            "message": "webhook 扫描整理任务已进入队列",
            "task_id": task.id,
            "status": task.status,
            "dirname": dirname,
            "group": group,
            "group_id": sync_group.id,
            "target_dir": target_dir,
            "source": source,
        }
    finally:
        db.close()


frontend_dist = Path(__file__).parent / "frontend" / "dist"
if not frontend_dist.exists():
    frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"


@app.exception_handler(404)
async def spa_fallback(request, exc):
    if request.url.path.startswith("/api") or request.method != "GET":
        detail = getattr(exc, "detail", "Not Found")
        return JSONResponse(status_code=404, content={"detail": detail})
    index = frontend_dist / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return JSONResponse(status_code=404, content={"detail": "Not Found"})


if frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="static")
