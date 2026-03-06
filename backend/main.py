"""FastAPI app entrypoint."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .api import auth, config as config_api, emby, inodes, logs, media, scan, sync_groups, tasks
from .api.logs import cleanup_logs_if_needed
from .config import settings
from .database import SessionLocal, init_db
from .models import MediaRecord, SyncGroup
from .security import require_auth
from .services.scanner import run_scan


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.auth_enabled:
        if not (settings.auth_secret or "").strip():
            raise RuntimeError("AUTH_SECRET 未配置，已拒绝启动（鉴权启用时必须设置强随机密钥）")
        if (settings.auth_username or "").strip() == "admin" and (settings.auth_password or "") == "admin123":
            raise RuntimeError("检测到默认账号口令 admin/admin123，请修改后再启动")
    init_db()
    cleanup_logs_if_needed(force=True)
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


@app.get("/api/health")
def health():
    return {"status": "ok"}


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
def send_task(
    background_tasks: BackgroundTasks,
    dirname: str = Form(...),
    group: str = Form(...),
    _username: str = Depends(require_auth),
):
    dirname = (dirname or "").strip()
    group = (group or "").strip()
    if not dirname or not group:
        raise HTTPException(status_code=400, detail="dirname 和 group 不能为空")

    db = SessionLocal()
    try:
        sync_group = db.query(SyncGroup).filter(SyncGroup.name == group).first()
        if not sync_group:
            raise HTTPException(status_code=404, detail=f"未找到同步组: {group}")

        pending_rows = (
            db.query(MediaRecord)
            .filter(MediaRecord.sync_group_id == sync_group.id, MediaRecord.status == "pending_manual")
            .all()
        )
        matched = [row for row in pending_rows if Path(row.original_path or "").name == dirname]
        if not matched:
            raise HTTPException(status_code=404, detail=f"未找到匹配待办目录: {dirname}")
        if len(matched) > 1:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": f"存在多个同名待办目录: {dirname}",
                    "candidates": [row.original_path for row in matched],
                },
            )

        background_tasks.add_task(
            _run_scan_group_task,
            sync_group.id,
            f"webhook_scan:{sync_group.name}",
            dirname,
            matched[0].original_path,
        )
        return {
            "ok": True,
            "message": "已提交 webhook 扫描整理任务",
            "dirname": dirname,
            "group": group,
            "group_id": sync_group.id,
        }
    finally:
        db.close()


frontend_dist = Path(__file__).parent / "frontend" / "dist"
if not frontend_dist.exists():
    frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"


@app.exception_handler(404)
async def spa_fallback(request, _exc):
    if request.url.path.startswith("/api"):
        return JSONResponse(status_code=404, content={"detail": "Not Found"})
    index = frontend_dist / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return JSONResponse(status_code=404, content={"detail": "Not Found"})


if frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="static")
