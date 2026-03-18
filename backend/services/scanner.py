"""扫描任务编排（v2）：目录驱动、统一识别、事务执行与回滚。"""
import os
import fcntl
import re
import time
import queue
import atexit
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
from contextlib import nullcontext

import httpx
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from ..api.logs import append_log, current_task_id
from ..config import settings
from ..database import SessionLocal
from ..models import DirectoryState, InodeRecord, MediaRecord, ScanTask, SyncGroup
from .emby import refresh_emby_library
from .group_routing import resolve_movie_target_root
from .linker import get_inode, is_same_inode, path_excluded
from .metadata import scrape_movie_metadata, scrape_tv_metadata
from .parser import (
    ParseResult,
    SPECIAL_TYPE_MAP,
    classify_extra_from_text,
    get_tmdb_tv_details_sync,
    parse_movie_filename,
    parse_tv_filename,
)
from .recognition_flow import (
    LocalParseSnapshot,
    parse_structure_locally,
    recognize_directory_with_fallback,
    recognize_directory_with_season_hint_trace,
    resolve_season as resolve_season_by_tmdb,
)
from .renamer import compute_movie_target_path, compute_tv_target_path
from .allocator import (
    allocate_indices_for_batch,
    mark_pending,
    scan_existing_special_indices,
    should_fallback_to_pending,
)

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".webm", ".flv", ".ts", ".m2ts"}
ATTACHMENT_EXTS = {".ass", ".srt", ".ssa", ".vtt", ".mka", ".sup", ".idx", ".sub"}
MEDIA_EXTS = VIDEO_EXTS | ATTACHMENT_EXTS
SCAN_LOCK = Lock()
MEDIA_TASK_QUEUE: queue.Queue = queue.Queue()
_WORKER_THREAD = None
_WORKER_LOCK = Lock()
_WORKER_LOCK_FD = None

IGNORED_TOKENS = {"bdmv", "menu", "sample", "scan", "disc", "iso", "font"}
EXTRAS_CATEGORIES = {
    "pv",
    "cm",
    "preview",
    "trailer",
    "teaser",
    "character_pv",
    "iv",
    "mv",
    "making",
    "interview",
    "bdextra",
}


@dataclass
class OperationLog:
    created_dirs: set[Path] = field(default_factory=set)
    created_links: set[Path] = field(default_factory=set)
    created_files: set[Path] = field(default_factory=set)


@dataclass
class MediaTask:
    path: Path
    media_type: str | None
    parsed: LocalParseSnapshot
    created_at: datetime
    sync_group_id: int
    task_id: int | None


class DirectoryProcessError(Exception):
    pass


def tag_task_type_with_issue(task_type: str) -> str:
    base = str(task_type or "").strip() or "scan"
    return base if base.startswith("issue_sp:") else f"issue_sp:{base}"


def _start_media_worker() -> None:
    global _WORKER_THREAD
    global _WORKER_LOCK_FD
    with _WORKER_LOCK:
        if not bool(getattr(settings, "worker_autostart", True)):
            return
        if _WORKER_THREAD is not None and _WORKER_THREAD.is_alive():
            return
        if _WORKER_LOCK_FD is None:
            # 单机锁：同一主机内尽量避免多个进程同时启动 worker
            lock_path = Path("/tmp/amm_media_worker.lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            _WORKER_LOCK_FD = lock_path.open("a+")
            try:
                fcntl.flock(_WORKER_LOCK_FD.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                append_log("WARNING: Worker lock is held by another process; continuing with local queue worker.")
        _WORKER_THREAD = Thread(target=_media_task_worker, daemon=True)
        _WORKER_THREAD.start()
        append_log("INFO: Media worker started")


def _stop_media_worker() -> None:
    global _WORKER_THREAD
    global _WORKER_LOCK_FD
    with _WORKER_LOCK:
        if _WORKER_THREAD is None:
            return
        MEDIA_TASK_QUEUE.put(None)
        # 优雅退出：最多等待 3 秒，避免阻塞退出流程
        _WORKER_THREAD.join(timeout=3.0)
        if _WORKER_THREAD.is_alive():
            append_log("WARNING: Media worker did not exit in time; continuing shutdown")
        _WORKER_THREAD = None
        if _WORKER_LOCK_FD is not None:
            try:
                fcntl.flock(_WORKER_LOCK_FD.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            _WORKER_LOCK_FD.close()
            _WORKER_LOCK_FD = None


def _media_task_worker() -> None:
    while True:
        task = MEDIA_TASK_QUEUE.get()
        if task is None:
            MEDIA_TASK_QUEUE.task_done()
            break
        # 单消费者：所有 DB 写入与文件落地都在此线程完成
        token = None
        if task.task_id:
            token = current_task_id.set(task.task_id)
        append_log(f"INFO: MediaTask start: {task.path}")
        retries = 0
        while retries < 3:
            local_db = SessionLocal()
            try:
                _handle_media_task(local_db, task)
                local_db.commit()
                append_log(f"INFO: MediaTask done: {task.path}")
                break
            except Exception as e:
                local_db.rollback()
                retries += 1
                backoff = 2 ** retries
                # 失败重试：指数退避，最多 3 次
                append_log(f"WARNING: MediaTask failed ({retries}/3): {task.path} | {e} | backoff={backoff}s")
                time.sleep(backoff)
                if retries >= 3:
                    append_log(f"WARNING: MediaTask permanently failed: {task.path}")
            finally:
                local_db.close()
        if token is not None:
            current_task_id.reset(token)
        MEDIA_TASK_QUEUE.task_done()


atexit.register(_stop_media_worker)
_start_media_worker()


def run_scan(
    db: Session,
    group_id: int | None = None,
    task_type_override: str | None = None,
    target_name_override: str | None = None,
    target_dir_override: str | None = None,
):
    """执行全量或单组扫描。"""
    task = ScanTask(
        type=task_type_override or ("group" if group_id else "full"),
        target_id=group_id,
        status="running",
        target_name=None,
    )
    if target_name_override:
        task.target_name = target_name_override
    elif group_id:
        group = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
        task.target_name = group.name if group else None
    else:
        task.target_name = "全量扫描"

    db.add(task)
    db.commit()
    db.refresh(task)

    from ..api.logs import LOG_DIR

    log_file = LOG_DIR / f"task_{task.id}.log"
    if log_file.exists():
        try:
            log_file.unlink()
        except Exception:
            pass

    token = current_task_id.set(task.id)
    acquired = SCAN_LOCK.acquire(blocking=False)
    try:
        if not acquired:
            append_log("已有扫描任务在运行，本次任务被拒绝以避免并发冲突")
            task.status = "failed"
            task.finished_at = datetime.utcnow()
            db.commit()
            return

        append_log(f"任务启动: ID={task.id}, 类型={task.type}")

        if not settings.tmdb_api_key:
            append_log("错误: 未配置 TMDB API Key，无法进行识别与刮削。")
            task.status = "failed"
            task.finished_at = datetime.utcnow()
            db.commit()
            return

        q = db.query(SyncGroup).filter(SyncGroup.enabled == True)
        if group_id is not None:
            q = q.filter(SyncGroup.id == group_id)
        groups = q.all()
        append_log(f"开始扫描，共 {len(groups)} 个同步组")

        first_scanned_dir_name = None
        has_issues = False
        try:
            _start_media_worker()
            for group in groups:
                append_log(f"处理同步组: {group.name}")
                group_first_dir, group_has_issues = _process_sync_group(
                    db,
                    group,
                    target_dir_override=target_dir_override,
                )
                if first_scanned_dir_name is None and group_first_dir:
                    first_scanned_dir_name = group_first_dir
                if group_has_issues:
                    has_issues = True

            MEDIA_TASK_QUEUE.join()

            if first_scanned_dir_name and not target_name_override and task.type in {"group", "full"}:
                task.target_name = first_scanned_dir_name
            if has_issues:
                append_log("扫描完成但存在问题项（已打标记）")
                task.type = tag_task_type_with_issue(task.type)

            append_log("扫描完成，刷新 Emby...")
            refresh_emby_library()
            append_log("完成")
            task.status = "completed"
        except Exception as e:
            import traceback

            append_log(f"扫描任务异常: {e}\n{traceback.format_exc()}")
            db.rollback()
            task.status = "failed"
    finally:
        task.finished_at = datetime.utcnow()
        task.log_file = f"task_{task.id}.log"
        try:
            db.commit()
        except Exception as e:
            append_log(f"无法更新任务状态: {e}")
            db.rollback()
        if acquired:
            SCAN_LOCK.release()
        current_task_id.reset(token)


def run_manual_organize(
    db: Session,
    pending: MediaRecord,
    group: SyncGroup,
    media_type: str,
    tmdb_id: int,
    title_override: str | None = None,
    year_override: int | None = None,
    season_override: int | None = None,
    episode_offset: int | None = None,
) -> tuple[int, int, bool]:
    root_dir = Path(pending.original_path or "")
    if not root_dir.exists() or not root_dir.is_dir():
        raise DirectoryProcessError("待办目录不存在或不是目录")

    if media_type not in {"tv", "movie"}:
        raise DirectoryProcessError("media_type 必须是 tv 或 movie")

    if not settings.tmdb_api_key:
        raise DirectoryProcessError("未配置 TMDB API Key")

    tmdb_data = _get_tmdb_item_by_id(media_type, tmdb_id)
    if not tmdb_data:
        raise DirectoryProcessError(f"TMDB 条目不存在: {tmdb_id}")

    tv_target_root = Path(group.target)
    movie_target_root, movie_route_reason = resolve_movie_target_root(db, group, source_path=root_dir)
    if media_type == "movie" and movie_target_root is None:
        raise DirectoryProcessError(f"电影目标路径不可决策: {movie_route_reason}")

    target_root = tv_target_root if media_type == "tv" else movie_target_root
    year_from_tmdb = _extract_year_from_tmdb_item(media_type, tmdb_data)
    year = year_override if year_override is not None else year_from_tmdb
    fallback_title = str(tmdb_data.get("name") or tmdb_data.get("title") or "Unknown")
    resolved_title = (title_override or "").strip() or _resolve_chinese_title_by_tmdb(
        media_type=media_type,
        tmdb_id=tmdb_id,
        fallback_title=fallback_title,
    )

    context = {
        "media_type": media_type,
        "tmdb_id": tmdb_id,
        "tmdb_data": tmdb_data,
        "title": resolved_title,
        "year": year,
        "target_root": str(target_root),
        "score": 1.0,
        "fallback_round": 0,
        "season_hint": max(1, int(season_override)) if (season_override is not None and season_override > 0) else None,
        "special_hint": False,
        "record_status": "manual_fixed",
        "_has_issues": False,
        "episode_offset": int(episode_offset) if episode_offset is not None else None,
    }

    if media_type == "tv":
        stable_ok, stable_reason = _stabilize_directory_context(root_dir, context)
        if not stable_ok:
            raise DirectoryProcessError(f"目录稳定决策失败: {stable_reason}")

    include = group.include or ""
    exclude = group.exclude or ""
    source_root = Path(group.source)
    media_files = _collect_media_files_under_dir(root_dir, include, exclude, source_root)
    if not media_files:
        raise DirectoryProcessError("待办目录中没有可处理媒体")

    video_files = sorted([p for p in media_files if p.suffix.lower() in VIDEO_EXTS], key=lambda p: p.as_posix())
    attachment_files = sorted([p for p in media_files if p.suffix.lower() in ATTACHMENT_EXTS], key=lambda p: p.as_posix())
    seen_targets: dict[Path, Path] = {}
    op_log = OperationLog()
    dir_runtime: dict = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
    }

    append_log(
        f"手动整理进入扫描主流程: group={group.name}, dir={root_dir}, media_type={media_type}, "
        f"tmdb_id={tmdb_id}, title={resolved_title}, files={len(media_files)}"
    )

    processed = 0
    try:
        for src_path in video_files:
            _process_file(
                db=db,
                src_path=src_path,
                sync_group_id=group.id,
                context=context,
                seen_targets=seen_targets,
                op_log=op_log,
                dir_runtime=dir_runtime,
            )
            processed += 1

        for src_path in attachment_files:
            _process_file(
                db=db,
                src_path=src_path,
                sync_group_id=group.id,
                context=context,
                seen_targets=seen_targets,
                op_log=op_log,
                dir_runtime=dir_runtime,
            )
            processed += 1

        db.delete(pending)
        db.commit()
        return processed, 0, bool(context.get("_has_issues"))
    except DirectoryProcessError:
        db.rollback()
        _rollback_operations(op_log)
        raise


def reidentify_by_target_dir(
    db: Session,
    group: SyncGroup,
    media_type: str,
    tmdb_id: int,
    title_override: str | None,
    year_override: int | None,
    season_override: int | None,
    episode_offset: int | None,
    records: list[MediaRecord],
) -> tuple[int, int, bool]:
    if media_type not in {"tv", "movie"}:
        raise DirectoryProcessError("media_type 必须是 tv 或 movie")

    if not settings.tmdb_api_key:
        raise DirectoryProcessError("未配置 TMDB API Key")

    if not records:
        raise DirectoryProcessError("未找到可修正的媒体记录")

    tmdb_data = _get_tmdb_item_by_id(media_type, tmdb_id)
    if not tmdb_data:
        raise DirectoryProcessError(f"TMDB 条目不存在: {tmdb_id}")

    tv_target_root = Path(group.target)
    movie_target_root, movie_route_reason = resolve_movie_target_root(db, group)
    if media_type == "movie" and movie_target_root is None:
        raise DirectoryProcessError(f"电影目标路径不可决策: {movie_route_reason}")

    target_root = tv_target_root if media_type == "tv" else movie_target_root
    year_from_tmdb = _extract_year_from_tmdb_item(media_type, tmdb_data)
    year = year_override if year_override is not None else year_from_tmdb
    fallback_title = str(tmdb_data.get("name") or tmdb_data.get("title") or "Unknown")
    resolved_title = (title_override or "").strip() or _resolve_chinese_title_by_tmdb(
        media_type=media_type,
        tmdb_id=tmdb_id,
        fallback_title=fallback_title,
    )

    context = {
        "media_type": media_type,
        "tmdb_id": tmdb_id,
        "tmdb_data": tmdb_data,
        "title": resolved_title,
        "year": year,
        "target_root": str(target_root),
        "score": 1.0,
        "fallback_round": 0,
        "season_hint": max(1, int(season_override)) if (season_override is not None and season_override > 0) else None,
        "special_hint": False,
        "record_status": "manual_fixed",
        "_has_issues": False,
        "episode_offset": int(episode_offset) if episode_offset is not None else None,
    }

    media_files: list[Path] = []
    for row in records:
        if not row.original_path:
            continue
        p = Path(row.original_path)
        if not p.exists() or not p.is_file():
            continue
        if p.suffix.lower() not in MEDIA_EXTS:
            continue
        media_files.append(p)

    if not media_files:
        raise DirectoryProcessError("没有可处理的媒体文件")

    video_files = sorted([p for p in media_files if p.suffix.lower() in VIDEO_EXTS], key=lambda p: p.as_posix())
    attachment_files = sorted([p for p in media_files if p.suffix.lower() in ATTACHMENT_EXTS], key=lambda p: p.as_posix())
    seen_targets: dict[Path, Path] = {}
    op_log = OperationLog()
    dir_runtime: dict = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
    }

    append_log(
        f"整组修正: group={group.name}, media_type={media_type}, tmdb_id={tmdb_id}, "
        f"title={resolved_title}, files={len(media_files)}"
    )

    processed = 0
    try:
        for src_path in video_files:
            _process_file(
                db=db,
                src_path=src_path,
                sync_group_id=group.id,
                context=context,
                seen_targets=seen_targets,
                op_log=op_log,
                dir_runtime=dir_runtime,
            )
            processed += 1

        for src_path in attachment_files:
            _process_file(
                db=db,
                src_path=src_path,
                sync_group_id=group.id,
                context=context,
                seen_targets=seen_targets,
                op_log=op_log,
                dir_runtime=dir_runtime,
            )
            processed += 1

        db.commit()
        return processed, 0, bool(context.get("_has_issues"))
    except DirectoryProcessError:
        db.rollback()
        _rollback_operations(op_log)
        raise


def _process_sync_group(
    db: Session,
    group: SyncGroup,
    target_dir_override: str | None = None,
) -> tuple[str | None, bool]:
    """处理单个同步组。"""
    source = Path(group.source)
    if not source.exists():
        append_log(f"源目录不存在: {source}")
        return None, False

    include = group.include or ""
    exclude = group.exclude or ""

    media_dirs = _collect_video_leaf_dirs(source, include, exclude)
    if target_dir_override:
        target_dir_path = Path(target_dir_override)
        filtered_dirs: list[Path] = []
        for d in media_dirs:
            if d == target_dir_path:
                filtered_dirs = [d]
                break
            if d.name == target_dir_path.name:
                filtered_dirs.append(d)
        media_dirs = filtered_dirs
    append_log(f"找到 {len(media_dirs)} 个待处理目录")
    first_dir_name = _extract_first_dir_name(media_dirs, source)

    task_id = current_task_id.get()
    for media_dir in media_dirs:
        structure_hint = _detect_media_type_from_structure(media_dir)
        parsed = parse_structure_locally(media_dir, structure_hint=structure_hint)
        # 入队：扫描线程只负责构建任务，不直接写 DB / 落地文件
        task = MediaTask(
            path=media_dir,
            media_type=structure_hint,
            parsed=parsed,
            created_at=datetime.utcnow(),
            sync_group_id=group.id,
            task_id=task_id,
        )
        MEDIA_TASK_QUEUE.put(task)

    append_log(f"INFO: 已入队任务: {len(media_dirs)}")
    return first_dir_name, False


def _handle_media_task(db: Session, task: MediaTask) -> None:
    group = db.query(SyncGroup).filter(SyncGroup.id == task.sync_group_id).first()
    if not group:
        return
    source = Path(group.source)
    tv_target_root = Path(group.target)
    movie_target_root, movie_route_reason = resolve_movie_target_root(db, group)
    if movie_route_reason:
        append_log(f"TV转移电影目标路径路由: {movie_route_reason}: {movie_target_root}")

    include = group.include or ""
    exclude = group.exclude or ""
    media_dir = task.path

    blocked_dirs: set[Path] = _load_existing_recorded_dirs(db, group.id, source)
    if _is_under_pending_dir(media_dir, blocked_dirs):
        append_log(f"INFO: 跳过已在媒体记录/待办中的目录: {media_dir}")
        return

    signature = _build_dir_signature(media_dir, source, include, exclude)
    if _can_skip_dir_by_signature(db, group.id, media_dir, signature):
        append_log(f"INFO: 跳过未变化成功目录: {media_dir}")
        return

    _upsert_dir_state(db, group.id, media_dir, signature, "SCANNED", None)

    structure_hint = task.media_type or _detect_media_type_from_structure(media_dir)
    best, snapshot, fallback_round = recognize_directory_with_fallback(
        media_dir,
        group.source_type,
        structure_hint=structure_hint,
    )
    if best is None:
        reason = f"目录级识别失败或低置信度: {media_dir.name}"
        _mark_dir_pending(db, media_dir, source, group.id, group.source_type, reason)
        _upsert_dir_state(db, group.id, media_dir, signature, "LOW_CONFIDENCE", reason)
        return

    recognized_type = best.media_type
    target_type = recognized_type
    if target_type == "movie" and movie_target_root is None:
        reason = "识别为电影但未找到可用电影目标路径"
        _mark_dir_pending(db, media_dir, source, group.id, group.source_type, reason)
        _upsert_dir_state(db, group.id, media_dir, signature, "FAILED", reason)
        return

    _upsert_dir_state(db, group.id, media_dir, signature, "IDENTIFIED", None)

    media_files = _collect_media_files_under_dir(media_dir, include, exclude, source)
    video_files = sorted([p for p in media_files if p.suffix.lower() in VIDEO_EXTS], key=lambda p: p.as_posix())
    attachment_files = sorted([p for p in media_files if p.suffix.lower() in ATTACHMENT_EXTS], key=lambda p: p.as_posix())

    target_root = tv_target_root if target_type == "tv" else movie_target_root

    resolved_title = _resolve_chinese_title_by_tmdb(
        media_type=target_type,
        tmdb_id=best.tmdb_id,
        fallback_title=best.title or snapshot.main_title,
        strict=True,
    )
    if not resolved_title:
        reason = "TMDB zh-CN 标题缺失"
        _mark_dir_pending(db, media_dir, source, group.id, group.source_type, reason)
        _upsert_dir_state(db, group.id, media_dir, signature, "LOW_CONFIDENCE", reason)
        append_log(f"WARNING: 转待办目录: {media_dir} | 原因: {reason}")
        return

    context = {
        "media_type": target_type,
        "tmdb_id": best.tmdb_id,
        "tmdb_data": _get_tmdb_item_by_id(target_type, best.tmdb_id) or best.tmdb_data,
        "title": resolved_title,
        "year": best.year or snapshot.year_hint,
        "target_root": str(target_root),
        "score": best.score,
        "fallback_round": fallback_round,
        "season_hint": snapshot.season_hint,
        "special_hint": snapshot.special_hint,
        "final_hint": snapshot.final_hint,
        "_has_issues": False,
    }

    stable_ok, stable_reason = _stabilize_directory_context(media_dir, context)
    if not stable_ok:
        reason = f"目录稳定决策失败: {stable_reason}"
        _mark_dir_pending(db, media_dir, source, group.id, group.source_type, reason)
        _upsert_dir_state(db, group.id, media_dir, signature, "LOW_CONFIDENCE", reason)
        return

    append_log(
        f"INFO: 目录识别成功: {media_dir.name} -> {target_type}:{context['title']} "
        f"(score={best.score:.3f}, fallback_round={fallback_round})"
    )

    op_log = OperationLog()
    seen_targets: dict[Path, Path] = {}
    dir_runtime: dict = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "pending_count": 0,
        "skipped_count": 0,
    }

    show_dir = _resolve_show_dir(target_root, context["title"], context.get("year"), context.get("tmdb_id"))
    use_lock = bool(getattr(settings, "use_file_lock", True))
    # show 级别文件锁：保证同一剧集的索引分配与落地原子性（单机有效）
    lock_ctx = _ShowLock(show_dir / ".assign.lock") if use_lock else nullcontext()

    with lock_ctx:
        # 批量预扫描 + 批量分配：消除 N×M 重排
        existing_cache = scan_existing_special_indices(target_root, context.get("tmdb_id"))
        append_log(f"INFO: scan existing specials: {existing_cache}")
        all_files = video_files + attachment_files
        items, item_map = _build_allocation_items(all_files, context, target_type)
        assignments = allocate_indices_for_batch(
            items,
            existing_cache,
            preserve_original_index=bool(getattr(settings, "preserve_original_index", True)),
        )
        context["allocator_assignments"] = assignments
        context["allocator_items"] = item_map

        for src_path in video_files:
            if _is_under_pending_dir(src_path, blocked_dirs):
                continue
            _process_file(
                db=db,
                src_path=src_path,
                sync_group_id=group.id,
                context=context,
                seen_targets=seen_targets,
                op_log=op_log,
                dir_runtime=dir_runtime,
            )

        for src_path in attachment_files:
            if _is_under_pending_dir(src_path, blocked_dirs):
                continue
            _process_file(
                db=db,
                src_path=src_path,
                sync_group_id=group.id,
                context=context,
                seen_targets=seen_targets,
                op_log=op_log,
                dir_runtime=dir_runtime,
            )

    append_log(
        "INFO: specials summary: assigned=%d, pending=%d, skipped=%d"
        % (len(context.get("allocator_assignments") or {}), dir_runtime.get("pending_count", 0), dir_runtime.get("skipped_count", 0))
    )
    _upsert_dir_state(db, group.id, media_dir, signature, "SUCCESS", None)


def _process_file(
    db: Session,
    src_path: Path,
    sync_group_id: int,
    context: dict,
    seen_targets: dict[Path, Path],
    op_log: OperationLog,
    dir_runtime: dict | None = None,
) -> None:
    ext = src_path.suffix.lower()
    if ext not in MEDIA_EXTS:
        return

    ino = get_inode(src_path)
    if ino:
        # 幂等保护：已处理文件直接跳过
        existing = db.query(InodeRecord).filter(InodeRecord.inode == ino).first()
        if existing:
            append_log(f"INFO: 已处理文件跳过: {src_path}")
            return

    if _should_ignore_fractional_episode(src_path.name):
        append_log(f"跳过半集文件: {src_path.name}")
        return

    media_type = context["media_type"]
    tmdb_id = context.get("tmdb_id")
    tmdb_data = context.get("tmdb_data") or {}
    target_root = Path(context["target_root"])
    is_attachment = ext in ATTACHMENT_EXTS
    record_status = str(context.get("record_status") or "scraped")

    parse_result = parse_tv_filename(str(src_path)) if media_type == "tv" else parse_movie_filename(str(src_path))
    if parse_result is None:
        fallback_parse = parse_movie_filename(str(src_path)) if media_type == "tv" else parse_tv_filename(str(src_path))
        if fallback_parse is None:
            raise DirectoryProcessError(f"文件解析失败: {src_path.name}")
        parse_result = fallback_parse

    parse_result = parse_result._replace(
        title=context.get("title") or parse_result.title,
        year=context.get("year") if context.get("year") else parse_result.year,
    )

    menu_token = _extract_menu_token(src_path.name)
    if menu_token:
        append_log(f"INFO: Special 忽略: {menu_token}")
        return

    # 附件跟随正片：优先复用同目录(同集)视频的目标路径。
    if is_attachment:
        anchor_dst = _resolve_attachment_follow_target(src_path, parse_result, dir_runtime)
        if anchor_dst is not None:
            dst_path = _build_attachment_target_from_anchor(anchor_dst, parse_result, ext)
            try:
                _execute_transactional_outputs(
                    src_path=src_path,
                    dst_path=dst_path,
                    media_type=media_type,
                    parse_result=parse_result,
                    tmdb_data=tmdb_data,
                    should_scrape=False,
                    op_log=op_log,
                )
            except DirectoryProcessError as e:
                if "目标路径已被其他文件占用" in str(e):
                    context["_has_issues"] = True
                    append_log(f"WARNING: 附件跟随目标冲突已跳过: {src_path.name} -> {dst_path}")
                    return
                raise
            _upsert_media_record(db, sync_group_id, src_path, dst_path, media_type, tmdb_id, status=record_status)
            _upsert_inode_record(db, sync_group_id, src_path, dst_path)
            append_log(f"INFO: 附件跟随正片: {src_path.name} -> {dst_path}")
            return
        if parse_result.extra_category is None:
            append_log(f"WARNING: 附件未匹配到视频已跳过: {src_path.name}")
            dir_runtime["skipped_count"] = int(dir_runtime.get("skipped_count") or 0) + 1
            return

    # 强制：特殊目录下文件必须进入 Special/Extras 流程
    if is_under_special_folder(src_path) and parse_result.extra_category is None:
        extra_category, extra_label, _from_bracket = classify_extra_from_text(src_path.name)
        if extra_category:
            parse_result = parse_result._replace(
                extra_category=extra_category,
                extra_label=extra_label,
                is_special=extra_category in {"special", "oped"},
            )
        else:
            bracket_label = _extract_bracket_label(src_path.name)
            parse_result = parse_result._replace(
                extra_category="making",
                extra_label=bracket_label or "Extra",
                is_special=False,
            )
        _log_special_classification(parse_result.extra_category, parse_result.extra_label)
        special_logged = True
    else:
        special_logged = False

    if parse_result.extra_category is None and is_under_special_folder(src_path):
        extra_category, extra_label, _from_bracket = classify_extra_from_text(src_path.name)
        if extra_category:
            parse_result = parse_result._replace(
                extra_category=extra_category,
                extra_label=extra_label,
                is_special=extra_category in {"special", "oped"},
            )
            _log_special_classification(extra_category, extra_label)
            special_logged = True
        else:
            bracket_label = _extract_bracket_label(src_path.name)
            parse_result = parse_result._replace(
                extra_category="making",
                extra_label=bracket_label or "Extra",
                is_special=False,
            )
            append_log(f"WARNING: Special 未识别，按 Extras 处理: {src_path.name}")
            special_logged = True

    if parse_result.extra_category is not None and not special_logged:
        _log_special_classification(parse_result.extra_category, parse_result.extra_label)

    assignments = context.get("allocator_assignments") or {}
    item_map = context.get("allocator_items") or {}
    assigned_by_allocator = False
    item = item_map.get(str(src_path))
    assigned_index = assignments.get(str(src_path))
    if item and assigned_index is not None:
        prefix = item.get("prefix") or _special_label_prefix(parse_result.extra_category, parse_result.extra_label)
        preferred = item.get("preferred")
        if preferred and int(preferred) != int(assigned_index):
            append_log(f"INFO: Special remap: {prefix}{int(preferred):02d} -> {prefix}{int(assigned_index):02d} for {src_path.name}")
        parse_result = parse_result._replace(extra_label=_format_prefix_number(prefix, int(assigned_index)))
        if parse_result.extra_category in {"special", "oped"}:
            parse_result = parse_result._replace(episode=int(assigned_index))
        assigned_by_allocator = True
    else:
        parse_result = _apply_special_indexing(parse_result, src_path, dir_runtime)

    if media_type == "tv" and parse_result.extra_category is None and _should_ignore_zero_episode(src_path.name):
        append_log(f"INFO: 跳过第0集/00集文件: {src_path.name}")
        return

    if media_type == "tv":
        if parse_result.episode is None and parse_result.extra_category is None:
            ep = _extract_episode_from_filename_loose(src_path.name)
            if ep is not None:
                parse_result = parse_result._replace(episode=ep)

        resolved_season = context.get("resolved_season")
        season_from_path = _extract_season_from_path(src_path)
        has_explicit = _has_explicit_season_token(src_path.name)
        if season_from_path is not None:
            season = season_from_path
        elif not has_explicit and resolved_season:
            season = resolved_season
        else:
            season = parse_result.season or resolved_season or 1
        parse_result = parse_result._replace(season=season)

        offset = context.get("episode_offset")
        if offset is not None and parse_result.episode is not None and parse_result.extra_category is None:
            adjusted = parse_result.episode - int(offset)
            parse_result = parse_result._replace(episode=max(1, adjusted))

        dst_path = compute_tv_target_path(target_root, parse_result, tmdb_id, ext, src_filename=src_path.name)
    else:
        dst_path = compute_movie_target_path(target_root, parse_result, tmdb_id, ext)

    if parse_result.extra_category in {"special", "oped"} | EXTRAS_CATEGORIES:
        attempts = 0
        while True:
            conflict = False
            if dst_path in seen_targets:
                conflict = True
            elif dst_path.exists() and not is_same_inode(src_path, dst_path):
                conflict = True
            if not conflict:
                break
            if assigned_by_allocator:
                break
            parse_result = _apply_special_indexing(parse_result, src_path, dir_runtime, force_next=True)
            if media_type == "tv":
                dst_path = compute_tv_target_path(target_root, parse_result, tmdb_id, ext, src_filename=src_path.name)
            else:
                dst_path = compute_movie_target_path(target_root, parse_result, tmdb_id, ext)
            attempts += 1
            if attempts > 50:
                raise DirectoryProcessError(f"多个源文件映射到同一目标: {src_path.name}")

    if assigned_by_allocator and item:
        item["suggested_target"] = str(dst_path)
        pending_path_value = str(getattr(settings, "pending_jsonl_path", "") or "").strip()
        pending_path = Path(pending_path_value) if pending_path_value else None
        pending_cfg = {
            "max_auto_remap_attempts": getattr(settings, "max_auto_remap_attempts", 3),
        }
        if dst_path.exists():
            existing_rec = (
                db.query(MediaRecord)
                .filter(MediaRecord.target_path == str(dst_path))
                .order_by(MediaRecord.id.desc())
                .first()
            )
            if existing_rec and existing_rec.original_path:
                item["owner_dir"] = str(Path(existing_rec.original_path).parent)
        if dst_path in seen_targets:
            if pending_path:
                mark_pending(item, "batch target conflict", pending_path)
            append_log(f"WARNING: Pending: {src_path.name} | reason: batch target conflict")
            dir_runtime["pending_count"] = int(dir_runtime.get("pending_count") or 0) + 1
            context["_has_issues"] = True
            return
        fallback, reason = should_fallback_to_pending(item, dst_path, {}, pending_cfg)
        if fallback:
            if pending_path:
                mark_pending(item, reason, pending_path)
            append_log(f"WARNING: Pending: {src_path.name} | reason: {reason}")
            dir_runtime["pending_count"] = int(dir_runtime.get("pending_count") or 0) + 1
            context["_has_issues"] = True
            return

    _deduplicate_target_or_raise(seen_targets, src_path, dst_path)

    should_scrape = (not is_attachment) and _should_scrape_for_target(media_type, dst_path, parse_result)
    _execute_transactional_outputs(
        src_path=src_path,
        dst_path=dst_path,
        media_type=media_type,
        parse_result=parse_result,
        tmdb_data=tmdb_data,
        should_scrape=should_scrape,
        op_log=op_log,
    )

    _upsert_media_record(db, sync_group_id, src_path, dst_path, media_type, tmdb_id, status=record_status)
    _upsert_inode_record(db, sync_group_id, src_path, dst_path)
    if ext in VIDEO_EXTS:
        _register_video_anchor(src_path, dst_path, parse_result, dir_runtime)

    append_log(f"INFO: 处理成功: {src_path.name} -> {dst_path}")


def resolve_season(tmdb_id: int, season_hint: int | None, final_season: bool = False) -> int | None:
    return resolve_season_by_tmdb(None, tmdb_id, season_hint, final_hint=final_season)


def _extract_season_from_path(src_path: Path) -> int | None:
    for part in src_path.parts:
        m1 = re.match(r"^season\s*(\d{1,2})$", part, re.I)
        if m1:
            return int(m1.group(1))
        m2 = re.match(r"^s(\d{1,2})$", part, re.I)
        if m2:
            return int(m2.group(1))
        m3 = re.match(r"^第\s*(\d{1,2})\s*季$", part)
        if m3:
            return int(m3.group(1))
    return None


def _has_explicit_season_token(name: str) -> bool:
    """判断文件名是否显式标注季号（用于避免误用 resolved_season 覆盖）。"""
    s = str(name or "")
    if re.search(r"\bS\d{1,2}\b", s, re.I):
        return True
    if re.search(r"\bSeason\s*\d{1,2}\b", s, re.I):
        return True
    if re.search(r"\b\d{1,2}(st|nd|rd|th)\s+Season\b", s, re.I):
        return True
    return False


def is_under_special_folder(file_path: Path) -> bool:
    for parent in file_path.parents:
        name = parent.name.lower()
        if name in {"sps", "special", "specials", "extras", "sp"}:
            return True
    return False


def _extract_episode_from_filename_loose(filename: str) -> int | None:
    stem = Path(filename).stem
    patterns = [
        r"\b\d{1,2}\s*[xX]\s*(\d{1,3})\b",
        r"\[(\d{2,3})\]",
        r"\((\d{1,3})\)",
        r"\bEP?(\d{2,3})\b",
        r"\bEP[_\-\s]*(\d{1,3})\b",
        r"\bEpisode\s*(\d{1,3})\b",
        r"\bEp\s*(\d{1,3})\b",
        r"\bE(\d{1,3})\b",
        r"第\s*(\d{1,3})\s*[集话話]",
        r"(?<!\d)(\d{2})(?!\d)",
        r"(?:^|[\s\-_\.])(\d{1,3})(?:$|[\s\-_\.])",
    ]
    for p in patterns:
        m = re.search(p, stem, re.I)
        if not m:
            continue
        ep = int(m.group(1))
        if 1 <= ep <= 999 and ep not in {2160, 1080, 720, 480, 265, 264}:
            return ep
    return None


def _register_video_anchor(src_path: Path, dst_path: Path, parse_result: ParseResult, dir_runtime: dict | None) -> None:
    if dir_runtime is None:
        return
    by_parent = dir_runtime.setdefault("video_anchor_by_parent", {})
    by_parent_ep = dir_runtime.setdefault("video_anchor_by_parent_episode", {})
    by_parent_stem = dir_runtime.setdefault("video_anchor_by_parent_stem", {})
    parent_key = str(src_path.parent)
    by_parent[parent_key] = dst_path
    ep_key = _normalized_episode_key(parse_result, src_path, src_path.name)
    if ep_key is not None:
        by_parent_ep[(parent_key, int(ep_key))] = dst_path
    stem_key = _normalize_media_stem(src_path.stem)
    if stem_key:
        by_parent_stem[(parent_key, stem_key)] = dst_path


def _resolve_attachment_follow_target(src_path: Path, parse_result: ParseResult, dir_runtime: dict | None) -> Path | None:
    if dir_runtime is None:
        return None
    by_parent = dir_runtime.get("video_anchor_by_parent", {})
    by_parent_ep = dir_runtime.get("video_anchor_by_parent_episode", {})
    by_parent_stem = dir_runtime.get("video_anchor_by_parent_stem", {})
    parent_key = str(src_path.parent)

    ep_key = _normalized_episode_key(parse_result, src_path, src_path.name)
    if ep_key is not None:
        candidate = by_parent_ep.get((parent_key, int(ep_key)))
        if candidate:
            return candidate
    stem_key = _normalize_media_stem(src_path.stem)
    if stem_key:
        candidate = by_parent_stem.get((parent_key, stem_key))
        if candidate:
            return candidate
    return by_parent.get(parent_key)


def _build_attachment_target_from_anchor(anchor_dst: Path, parse_result: ParseResult, ext: str) -> Path:
    base = anchor_dst.stem
    lang = parse_result.subtitle_lang or ""
    return anchor_dst.with_name(f"{base}{lang}{ext}")


def _execute_transactional_outputs(
    src_path: Path,
    dst_path: Path,
    media_type: str,
    parse_result: ParseResult,
    tmdb_data: dict,
    should_scrape: bool,
    op_log: OperationLog,
) -> None:
    _create_hardlink_with_tracking(src_path, dst_path, op_log)

    if not should_scrape:
        return

    if media_type == "tv":
        show_dir = _resolve_tv_show_dir_for_scrape(dst_path)
        before_files = set(_list_files_safe(show_dir))
        scrape_tv_metadata(show_dir, tmdb_data)
        _track_new_files(show_dir, before_files, op_log)
        if not (show_dir / "tvshow.nfo").exists():
            raise DirectoryProcessError(f"TV 元数据校验失败: {show_dir}")
    else:
        show_dir = dst_path.parent
        before_files = set(_list_files_safe(show_dir))
        scrape_movie_metadata(show_dir, tmdb_data)
        _track_new_files(show_dir, before_files, op_log)
        if not (show_dir / "movie.nfo").exists():
            raise DirectoryProcessError(f"Movie 元数据校验失败: {show_dir}")


def _create_hardlink_with_tracking(src_path: Path, dst_path: Path, op_log: OperationLog) -> None:
    if not src_path.exists():
        raise DirectoryProcessError(f"源文件不存在: {src_path}")

    if dst_path.exists():
        if is_same_inode(src_path, dst_path):
            return
        raise DirectoryProcessError(f"目标路径已被其他文件占用: {dst_path}")

    created_dirs = _ensure_dir_tree(dst_path.parent)
    op_log.created_dirs.update(created_dirs)

    try:
        os.link(str(src_path), str(dst_path))
    except OSError as e:
        raise DirectoryProcessError(f"创建硬链接失败: {dst_path} ({e})") from e

    op_log.created_links.add(dst_path)


def _ensure_dir_tree(path: Path) -> list[Path]:
    created: list[Path] = []
    cursor = Path(path)
    stack = []
    while not cursor.exists() and cursor.parent != cursor:
        stack.append(cursor)
        cursor = cursor.parent

    for d in reversed(stack):
        d.mkdir(exist_ok=True)
        created.append(d)

    return created


def _track_new_files(base_dir: Path, before_files: set[Path], op_log: OperationLog) -> None:
    after = set(_list_files_safe(base_dir))
    for f in (after - before_files):
        op_log.created_files.add(f)


def _rollback_operations(op_log: OperationLog) -> None:
    for f in sorted(op_log.created_files, key=lambda p: len(p.parts), reverse=True):
        try:
            if f.exists() and f.is_file():
                f.unlink()
        except OSError:
            pass

    for link in sorted(op_log.created_links, key=lambda p: len(p.parts), reverse=True):
        try:
            if link.exists() and link.is_file():
                link.unlink()
        except OSError:
            pass

    for d in sorted(op_log.created_dirs, key=lambda p: len(p.parts), reverse=True):
        try:
            if d.exists() and d.is_dir() and not any(d.iterdir()):
                d.rmdir()
        except OSError:
            pass


def _deduplicate_target_or_raise(
    seen_targets: dict[Path, Path],
    src_path: Path,
    dst_path: Path,
) -> None:
    prev = seen_targets.get(dst_path)
    if prev is None:
        seen_targets[dst_path] = src_path
        return
    if prev != src_path:
        raise DirectoryProcessError(f"多个源文件映射到同一目标: {prev.name} / {src_path.name}")


def _should_scrape_for_target(media_type: str, dst_path: Path, parse_result: ParseResult) -> bool:
    if media_type == "movie" and parse_result.extra_category and dst_path.parent.name.lower() == "extras":
        return False
    return True


def _upsert_media_record(
    db: Session,
    sync_group_id: int,
    src_path: Path,
    dst_path: Path,
    media_type: str,
    tmdb_id: int | None,
    status: str = "scraped",
) -> None:
    src_size = _safe_file_size(src_path)
    existing = db.query(MediaRecord).filter(
        MediaRecord.sync_group_id == sync_group_id,
        MediaRecord.original_path == str(src_path),
    ).first()

    if existing:
        existing.target_path = str(dst_path)
        existing.type = media_type
        existing.tmdb_id = tmdb_id
        existing.status = status
        existing.size = src_size
        return

    db.add(
        MediaRecord(
            sync_group_id=sync_group_id,
            original_path=str(src_path),
            target_path=str(dst_path),
            type=media_type,
            tmdb_id=tmdb_id,
            bangumi_id=None,
            status=status,
            size=src_size,
        )
    )


def _get_tmdb_item_by_id(media_type: str, tmdb_id: int) -> dict | None:
    if not settings.tmdb_api_key:
        return None
    endpoint = "tv" if media_type == "tv" else "movie"
    url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
    params = {"api_key": settings.tmdb_api_key, "language": "zh-CN"}
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(url, params=params)
    except Exception:
        return None
    if resp.status_code != 200:
        return None
    data = resp.json() if resp.content else {}
    return data if isinstance(data, dict) else None


def _extract_year_from_tmdb_item(media_type: str, item: dict) -> int | None:
    date_value = item.get("first_air_date") if media_type == "tv" else item.get("release_date")
    s = str(date_value or "")
    return int(s[:4]) if s[:4].isdigit() else None


def _extract_year(text: str) -> int | None:
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text or "")
    return int(m.group(1)) if m else None


def _upsert_inode_record(db: Session, sync_group_id: int, src_path: Path, dst_path: Path) -> None:
    ino = get_inode(src_path)
    if not ino:
        return

    src_size = _safe_file_size(src_path)
    existing = db.query(InodeRecord).filter(InodeRecord.inode == ino).first()
    if existing:
        existing.source_path = str(src_path)
        existing.target_path = str(dst_path)
        existing.sync_group_id = sync_group_id
        existing.size = src_size
        return

    db.add(
        InodeRecord(
            inode=ino,
            source_path=str(src_path),
            target_path=str(dst_path),
            sync_group_id=sync_group_id,
            size=src_size,
        )
    )


def _collect_video_leaf_dirs(source: Path, include: str, exclude: str) -> list[Path]:
    """
    递归遍历 source，返回“直接包含视频文件”的目录。
    命中后不再深入该目录子级。
    """
    dirs: set[Path] = set()
    if not source.exists():
        return []

    stack = [source]
    while stack:
        current = stack.pop()
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.as_posix())
        except OSError:
            continue

        has_video = False
        subdirs = []
        for entry in entries:
            if entry.is_dir():
                if _is_ignored_name(entry.name):
                    continue
                subdirs.append(entry)
                continue
            if not entry.is_file() or entry.suffix.lower() not in VIDEO_EXTS:
                continue
            if _is_ignored_name(entry.name):
                continue
            if path_excluded(entry, include, exclude, root_path=source):
                continue
            has_video = True
            break

        if has_video:
            dirs.add(current)
        else:
            stack.extend(reversed(subdirs))

    return sorted(dirs, key=lambda p: p.as_posix())


def _collect_media_files_under_dir(media_dir: Path, include: str, exclude: str, source_root: Path) -> list[Path]:
    files: list[Path] = []
    try:
        for entry in media_dir.rglob("*"):
            if not entry.is_file():
                continue
            if entry.suffix.lower() not in MEDIA_EXTS:
                continue
            if _is_ignored_name(entry.name):
                continue
            if path_excluded(entry, include, exclude, root_path=source_root):
                continue
            files.append(entry)
    except OSError:
        return files
    return files


def _extract_first_dir_name(dirs: list[Path], source_root: Path) -> str | None:
    if not dirs:
        return None
    first = sorted(dirs, key=lambda p: p.as_posix())[0]
    try:
        relative = first.relative_to(source_root)
        if relative.parts:
            return relative.parts[0]
    except Exception:
        pass
    return first.name


def _load_existing_pending_dirs(db: Session, sync_group_id: int) -> set[Path]:
    rows = (
        db.query(MediaRecord.original_path)
        .filter(
            MediaRecord.sync_group_id == sync_group_id,
            MediaRecord.status == "pending_manual",
        )
        .all()
    )
    out: set[Path] = set()
    for (path_str,) in rows:
        if path_str:
            out.add(Path(path_str))
    return out


def _load_existing_recorded_dirs(db: Session, sync_group_id: int, source_root: Path) -> set[Path]:
    """
    收集该同步组已存在于媒体记录中的目录（含待办目录），用于目录级快速跳过。
    - pending_manual: original_path 本身是目录，直接加入
    - 其它状态: original_path 通常是文件路径，取 parent 目录加入
    """
    rows = (
        db.query(MediaRecord.original_path, MediaRecord.status)
        .filter(MediaRecord.sync_group_id == sync_group_id)
        .all()
    )
    out: set[Path] = set()
    source_norm = _normalize_path(source_root)

    for original_path, status in rows:
        if not original_path:
            continue
        p = Path(original_path)
        if status == "pending_manual":
            candidate = p
        else:
            candidate = p if p.suffix == "" else p.parent

        cand_norm = _normalize_path(candidate)
        if not cand_norm or not (cand_norm == source_norm or cand_norm.startswith(source_norm + "/")):
            continue
        out.add(candidate)

    return out


def _mark_dir_pending(
    db: Session,
    src_path: Path,
    source_root: Path,
    sync_group_id: int,
    source_type: str,
    reason: str,
) -> Path:
    pending_dir = _resolve_manual_scope(src_path, source_root)
    append_log(f"WARNING: 转待办目录: {pending_dir} | 原因: {reason}")

    existing = db.query(MediaRecord).filter(
        MediaRecord.original_path == str(pending_dir),
        MediaRecord.sync_group_id == sync_group_id,
        MediaRecord.status == "pending_manual",
    ).first()
    if existing is None:
        record = MediaRecord(
            sync_group_id=sync_group_id,
            original_path=str(pending_dir),
            target_path=None,
            type=source_type,
            tmdb_id=None,
            bangumi_id=None,
            status="pending_manual",
            size=0,
        )
        _commit_with_retry(db, add_record=record)
    else:
        _commit_with_retry(db)
    return pending_dir


def _resolve_manual_scope(src_path: Path, source_root: Path) -> Path:
    scope = src_path if src_path.is_dir() else src_path.parent
    if scope == source_root:
        return scope

    aux_names = {"sps", "specials", "extras", "scans", "menus", "menu", "cds", "cd"}
    if scope.name.lower() in aux_names and scope.parent != source_root:
        return scope.parent
    return scope


def _is_under_pending_dir(src_path: Path, pending_dirs: set[Path]) -> bool:
    src_norm = _normalize_path(src_path)
    for pending_dir in pending_dirs:
        base = _normalize_path(pending_dir)
        if src_norm == base or src_norm.startswith(base + "/"):
            return True
    return False


def _build_dir_signature(media_dir: Path, source_root: Path, include: str, exclude: str) -> str:
    file_count = 0
    latest_mtime = 0.0
    try:
        for entry in media_dir.rglob("*"):
            if not entry.is_file():
                continue
            if entry.suffix.lower() not in MEDIA_EXTS:
                continue
            if _is_ignored_name(entry.name):
                continue
            if path_excluded(entry, include, exclude, root_path=source_root):
                continue
            file_count += 1
            try:
                latest_mtime = max(latest_mtime, entry.stat().st_mtime)
            except OSError:
                continue
    except OSError:
        pass
    return f"{file_count}:{int(latest_mtime)}"


def _can_skip_dir_by_signature(db: Session, sync_group_id: int, media_dir: Path, signature: str) -> bool:
    state = db.query(DirectoryState).filter(
        DirectoryState.sync_group_id == sync_group_id,
        DirectoryState.dir_path == str(media_dir),
    ).first()
    if not state:
        return False
    return state.status == "SUCCESS" and state.signature == signature


def _upsert_dir_state(
    db: Session,
    sync_group_id: int,
    media_dir: Path,
    signature: str,
    status: str,
    last_error: str | None,
) -> None:
    dir_path = str(media_dir)

    bind = db.get_bind()
    if bind is not None and bind.dialect.name == "sqlite":
        now = datetime.utcnow()
        stmt = sqlite_insert(DirectoryState).values(
            sync_group_id=sync_group_id,
            dir_path=dir_path,
            signature=signature,
            status=status,
            last_error=last_error,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[DirectoryState.sync_group_id, DirectoryState.dir_path],
            set_={
                "signature": signature,
                "status": status,
                "last_error": last_error,
                "updated_at": now,
            },
        )
        try:
            db.execute(stmt)
            return
        except OperationalError:
            # Legacy DB may miss unique constraint; fallback to manual upsert.
            pass

    state = db.query(DirectoryState).filter(
        DirectoryState.sync_group_id == sync_group_id,
        DirectoryState.dir_path == dir_path,
    ).first()
    if state is None:
        record = DirectoryState(
            sync_group_id=sync_group_id,
            dir_path=dir_path,
            signature=signature,
            status=status,
            last_error=last_error,
        )
        try:
            db.add(record)
            return
        except IntegrityError:
            db.rollback()
    else:
        state.signature = signature
        state.status = status
        state.last_error = last_error


def _commit_with_retry(db: Session, add_record: object | None = None) -> None:
    delays = [0.05, 0.1, 0.2]
    for attempt, delay in enumerate(delays, 1):
        try:
            if add_record is not None:
                db.add(add_record)
            db.commit()
            return
        except OperationalError as e:
            db.rollback()
            append_log(f"WARNING: DB write retry {attempt}: {e}")
            time.sleep(delay)
        except IntegrityError as e:
            db.rollback()
            append_log(f"WARNING: DB write retry {attempt}: {e}")
            time.sleep(delay)
    db.commit()


def _resolve_tv_show_dir_for_scrape(dst_path: Path) -> Path:
    current = dst_path.parent
    while True:
        if _is_season_like_dir(current):
            return current.parent
        if current.parent == current:
            break
        current = current.parent
    return dst_path.parent.parent


def _is_season_like_dir(path: Path) -> bool:
    name = path.name.strip().lower()
    if name == "specials":
        return True
    return re.match(r"^season\s+\d{1,2}$", name, re.I) is not None


def _should_ignore_fractional_episode(filename: str) -> bool:
    stem = Path(filename).stem
    if re.search(r"\d{1,3}\.5\b", stem, re.I):
        return True
    return re.search(r"(?:^|[\[\(\s\-_])\d{1,3}\.5(?:$|[\]\)\s\-_])", stem, re.I) is not None


def _should_ignore_zero_episode(filename: str) -> bool:
    stem = Path(filename).stem
    patterns = [
        r"[\[\(]\s*0{1,2}\s*[\]\)]",   # [00] / (0)
        r"\bEP?\s*0+\b",               # EP0 / E00
        r"第\s*0+\s*[集话話]",          # 第0集
        r"(?:^|[\s._-])0{1,2}(?:$|[\s._-])",  # token 0 / 00
    ]
    return any(re.search(p, stem, re.I) is not None for p in patterns)


def _should_ignore_menu_file(filename: str) -> bool:
    return _extract_menu_token(filename) is not None


def _extract_menu_token(filename: str) -> str | None:
    stem = Path(filename).stem
    m = re.search(r"\b(BD\s*Menu|Top\s*Menu|PopUp\s*Menu|Menu)\b", stem, re.I)
    if not m:
        return None
    token = re.sub(r"\s+", " ", m.group(1)).strip()
    return token or "Menu"


def _is_ignored_name(name: str) -> bool:
    lowered = str(name or "").lower()
    if not lowered:
        return False
    return any(token in lowered for token in IGNORED_TOKENS)


def _normalize_media_stem(stem: str) -> str:
    s = str(stem or "")
    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.sub(r"\([^\)]*\)", " ", s)
    s = re.sub(r"\{[^\}]*\}", " ", s)
    s = re.sub(r"\b(?:2160p|1080p|720p|480p|4k)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:x264|x265|h264|h265|hevc|av1|hi10p|ma10p)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:flac|aac|ac3|dts|ddp\d?\.\d?)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:webrip|web-dl|bdrip|bluray|remux)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:chs|cht|sc|tc|gb|big5|jpn|jp|ja|eng|en)\b", " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s.replace("_", " ").replace(".", " ")).strip()
    return s.lower()


def _safe_name(text: str) -> str:
    bad = '<>:"/\\|?*'
    s = "".join("_" if c in bad else c for c in (text or "Unknown"))
    return s.strip() or "Unknown"


def _resolve_show_dir(target_root: Path, title: str, year: int | None, tmdb_id: int | None) -> Path:
    year_part = f" ({year})" if year else ""
    tmdb_part = f" [tmdbid={tmdb_id}]" if tmdb_id else ""
    return target_root / f"{_safe_name(title)}{year_part}{tmdb_part}"


class _ShowLock:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self.handle = None

    def __enter__(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.lock_path.open("a+")
        # 注意：文件锁只对单机多进程有效，分布式需集中锁/单写服务
        # Single-host advisory lock only. Use centralized locks for multi-host deployments.
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.handle is None:
            return False
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()
        return False




def _detect_media_type_from_structure(media_dir: Path) -> str | None:
    video_files = [p for p in media_dir.iterdir() if p.is_file() and p.suffix.lower() in VIDEO_EXTS and not _is_ignored_name(p.name)]
    if not video_files:
        return None
    if len(video_files) == 1:
        return "movie"
    episode_hits = 0
    for p in video_files:
        if _extract_episode_from_filename_loose(p.name) is not None or re.search(r"\bEP\s*\d{1,3}\b", p.stem, re.I):
            episode_hits += 1
    if episode_hits >= 2:
        return "tv"
    return None


def _extract_bracket_label(filename: str) -> str | None:
    tokens = re.findall(r"\[([^\]]+)\]", filename or "")
    if not tokens:
        return None
    for raw in tokens:
        text = re.sub(r"\s+", " ", raw).strip()
        if not text:
            continue
        if _is_ignored_name(text):
            continue
        return text[:60]
    return None


def _format_prefix_number(prefix: str, idx: int) -> str:
    return f"{prefix}{int(idx):02d}"


def _special_label_prefix(category: str | None, label: str | None) -> str:
    label = str(label or "").strip()
    if category == "oped":
        upper = label.upper()
        if upper.startswith("NCED") or upper.startswith("ED"):
            return "ED"
        if upper.startswith("NCOP") or upper.startswith("OP"):
            return "OP"
        for token in ("NCOP", "NCED", "OP", "ED"):
            if re.search(rf"\b{token}\b", label, re.I):
                return "ED" if token == "NCED" else ("OP" if token == "NCOP" else token)
        return "OP"
    if category == "special":
        for token in ("OVA", "OAD", "SP", "SPECIAL"):
            if re.search(rf"\b{token}\b", label, re.I):
                return "SP" if token == "SPECIAL" else token
        return "SP"
    if category == "pv":
        return "PV"
    if category == "cm":
        return "CM"
    if category == "preview":
        if re.search(r"\bWEBPREVIEW\b", label, re.I):
            return "WebPreview"
        return "Preview"
    if category == "character_pv":
        return "CharacterPV"
    if category == "trailer":
        return "Trailer"
    if category == "teaser":
        return "Teaser"
    if category == "iv":
        return "IV"
    if category == "mv":
        return "MV"
    if category == "interview":
        return "Interview"
    if category == "making":
        return "Making"
    if category == "bdextra":
        return "BDExtra"
    return "Extra"


def _resolve_prefix_for_parse(parse_result: ParseResult) -> str | None:
    if not parse_result.extra_category:
        return None
    label = str(parse_result.extra_label or "")
    upper = label.upper()
    for token, (_bucket, prefix) in SPECIAL_TYPE_MAP.items():
        if token.upper() in upper:
            return prefix
    return _special_label_prefix(parse_result.extra_category, label)


def _build_allocation_items(
    media_files: list[Path],
    context: dict,
    media_type: str,
) -> tuple[list[dict], dict[str, dict]]:
    items: list[dict] = []
    item_map: dict[str, dict] = {}
    for src_path in media_files:
        ext = src_path.suffix.lower()
        if ext not in MEDIA_EXTS:
            continue
        parse_result = parse_tv_filename(str(src_path)) if media_type == "tv" else parse_movie_filename(str(src_path))
        if parse_result is None:
            continue
        if media_type == "tv":
            # 显式季号优先；若文件名未显式标注季号，则使用解析到的 resolved_season
            season_from_path = _extract_season_from_path(src_path)
            has_explicit = _has_explicit_season_token(src_path.name)
            resolved_season = context.get("resolved_season")
            if season_from_path is not None:
                season = season_from_path
            elif not has_explicit and resolved_season:
                season = resolved_season
            else:
                season = parse_result.season or resolved_season or 1
            parse_result = parse_result._replace(season=season)
        prefix = _resolve_prefix_for_parse(parse_result)
        if not prefix:
            continue
        if parse_result.extra_category not in {"special", "oped"} | EXTRAS_CATEGORIES:
            continue
        preferred = _extract_label_index(parse_result.extra_label) or parse_result.episode
        item = {
            "file_path": str(src_path),
            "source_dir": str(src_path.parent),
            "tmdbid": context.get("tmdb_id"),
            "season_key": parse_result.season if media_type == "tv" else None,
            "prefix": prefix,
            "preferred": preferred,
            "is_attachment": ext in ATTACHMENT_EXTS,
            "lang": parse_result.subtitle_lang,
            "final_hint": context.get("final_hint"),
            "final_resolved_season": context.get("final_resolved_season"),
        }
        items.append(item)
        item_map[str(src_path)] = item
    return items, item_map


def _extract_label_index(label: str | None) -> int | None:
    if not label:
        return None
    m = re.search(r"(\d{1,3})", label)
    if not m:
        return None
    val = int(m.group(1))
    return val if 1 <= val <= 999 else None


def _apply_special_indexing(
    parse_result: ParseResult,
    src_path: Path,
    dir_runtime: dict | None,
    force_next: bool = False,
) -> ParseResult:
    if dir_runtime is None:
        return parse_result
    category = parse_result.extra_category
    if category not in {"special", "oped"} | EXTRAS_CATEGORIES:
        return parse_result

    label = parse_result.extra_label or ""
    prefix = _special_label_prefix(category, label)
    used = dir_runtime.setdefault("special_used", {}).setdefault((category, prefix), set())
    idx = None if force_next else _extract_label_index(label)

    version_suffix = ""
    m_ver = re.search(r"\b(?:ver\.?\s*\d+|v\d+)\b", label, re.I)
    if m_ver:
        version_suffix = m_ver.group(0).strip()

    if idx is None or force_next:
        next_idx = 1
        while next_idx in used:
            next_idx += 1
        if force_next:
            append_log(f"INFO: Special remap: {prefix}{next_idx:02d} for {src_path.name}")
        idx = next_idx

    used.add(idx)
    new_label = _format_prefix_number(prefix, idx)
    if version_suffix and version_suffix not in new_label:
        new_label = f"{new_label} {version_suffix}"
    updated = parse_result._replace(extra_label=new_label)
    if category in {"special", "oped"}:
        updated = updated._replace(episode=idx)
    return updated


def _log_special_classification(category: str | None, label: str | None) -> None:
    if not category:
        return
    display = {
        "oped": "OPED",
        "pv": "PV",
        "cm": "CM",
        "preview": "Preview",
        "character_pv": "CharacterPV",
        "trailer": "Trailer",
        "teaser": "Teaser",
        "iv": "IV",
        "mv": "MV",
        "making": "Making",
        "interview": "Interview",
        "bdextra": "BDExtra",
        "special": "Specials",
    }.get(category, category)
    if category == "oped":
        upper = str(label or "").upper()
        if upper.startswith("OP"):
            display = "OP"
        elif upper.startswith("ED"):
            display = "ED"
    if label:
        append_log(f"Special 识别: {label} -> {display}")
    else:
        append_log(f"Special 分类: {display}")


def _normalized_episode_key(parse_result: ParseResult, src_path: Path, filename: str) -> int | None:
    if parse_result.extra_category in {"special", "oped"}:
        season = parse_result.season or _extract_season_from_path(src_path) or 1
        index = parse_result.episode
        if index is None:
            index = _extract_episode_from_filename_loose(filename)
        index = index or 1
        if parse_result.extra_category == "oped":
            label = str(parse_result.extra_label or "").upper()
            if label.startswith("ED"):
                return season * 100 + int(index * 2)
            return season * 100 + int(index * 2 - 1)
        return season * 100 + int(index)
    ep = parse_result.episode
    if ep is None:
        ep = _extract_episode_from_filename_loose(filename)
    return int(ep) if ep is not None else None


def _list_files_safe(path: Path) -> list[Path]:
    if not path.exists():
        return []
    try:
        return [x for x in path.iterdir() if x.is_file()]
    except OSError:
        return []


def _normalize_path(path: Path) -> str:
    return str(path).replace("\\", "/").rstrip("/")


def _safe_file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _resolve_chinese_title_by_tmdb(
    media_type: str,
    tmdb_id: int | None,
    fallback_title: str,
    strict: bool = False,
) -> str | None:
    if not tmdb_id or not settings.tmdb_api_key:
        return None if strict else fallback_title

    endpoint = "tv" if media_type == "tv" else "movie"
    url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
    params = {"api_key": settings.tmdb_api_key, "language": "zh-CN"}

    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url, params=params)
        if resp.status_code != 200:
            return None if strict else fallback_title
        data = resp.json() if resp.content else {}
        if not isinstance(data, dict):
            return None if strict else fallback_title
        if media_type == "tv":
            title = str(data.get("name") or "").strip()
        else:
            title = str(data.get("title") or "").strip()
        if title:
            return title
        return None if strict else fallback_title
    except Exception:
        return None if strict else fallback_title


def _is_final_season_title(name: str) -> bool:
    text = str(name or "")
    return re.search(r"\b(?:the\s+)?final\s+season(?:\s+part\s*\d+)?\b", text, re.I) is not None


def _stabilize_directory_context(media_dir: Path, context: dict) -> tuple[bool, str | None]:
    media_type = str(context.get("media_type") or "")
    tmdb_id = context.get("tmdb_id")
    title = str(context.get("title") or "").strip()

    if media_type not in {"tv", "movie"}:
        return False, "识别类型不明确"
    if not tmdb_id:
        return False, "tmdb_id 缺失"
    if not title:
        return False, "标题缺失"

    if media_type == "movie":
        return True, None

    # TV: 目录识别后再做一次季号稳定决策。
    season_from_path = _extract_season_from_path(media_dir)
    season_hint = context.get("season_hint")
    if isinstance(season_hint, int) and season_hint <= 0:
        season_hint = None

    try:
        details = get_tmdb_tv_details_sync(int(tmdb_id))
    except Exception:
        details = None
    if not details:
        return False, "无法获取 TMDB 季信息"

    valid_seasons = sorted(
        {
            int(x.get("season_number"))
            for x in details.get("seasons", [])
            if x.get("season_number") is not None and int(x.get("season_number")) > 0
        }
    )

    chosen = season_from_path or season_hint
    is_final = _is_final_season_title(media_dir.name) or bool(context.get("final_hint"))

    # 无稳定季号时：多季作品默认优先第一季（仅在显式季号存在时才做严格校验）
    if not chosen and len(valid_seasons) > 1 and not is_final:
        append_log("INFO: 多季作品且无稳定季号信息，默认使用第一季")
        chosen = 1
    if chosen and valid_seasons and chosen not in valid_seasons:
        append_log(
            f"INFO: chosen season {chosen} not found in TMDB seasons for candidate tmdbid {tmdb_id} - re-search attempted"
        )
        if not bool(getattr(settings, "season_aware_research_enabled", True)):
            return False, f"季号不匹配 TMDB: season_hint={chosen}"

        best, tried_queries = recognize_directory_with_season_hint_trace(media_dir, "tv", chosen, structure_hint="tv")
        if tried_queries:
            append_log(f"INFO: re-search tried: {tried_queries}")
        if best and best.tmdb_id and int(best.tmdb_id) != int(tmdb_id):
            context["tmdb_id"] = int(best.tmdb_id)
            context["tmdb_data"] = _get_tmdb_item_by_id("tv", best.tmdb_id) or best.tmdb_data
            new_title = _resolve_chinese_title_by_tmdb("tv", int(best.tmdb_id), context.get("title", ""), strict=True)
            if new_title:
                context["title"] = new_title
            details = get_tmdb_tv_details_sync(int(best.tmdb_id))
            if details:
                valid_seasons = sorted(
                    {
                        int(x.get("season_number"))
                        for x in details.get("seasons", [])
                        if x.get("season_number") is not None and int(x.get("season_number")) > 0
                    }
                )
                if chosen in valid_seasons:
                    tmdb_id = int(best.tmdb_id)
                    append_log(
                        f"INFO: resolved by season-aware re-search -> tmdbid={tmdb_id}, season={chosen}"
                    )
                else:
                    return False, f"季号不匹配 TMDB: season_hint={chosen}, tried={tried_queries}"
            else:
                return False, "无法获取 TMDB 季信息"
        elif best and best.tmdb_id and int(best.tmdb_id) == int(tmdb_id):
            details = get_tmdb_tv_details_sync(int(best.tmdb_id))
            valid_seasons = sorted(
                {
                    int(x.get("season_number"))
                    for x in (details.get("seasons", []) if isinstance(details, dict) else [])
                    if x.get("season_number") is not None and int(x.get("season_number")) > 0
                }
            )
            if chosen in valid_seasons:
                append_log(
                    f"INFO: resolved by season-aware re-search -> tmdbid={best.tmdb_id}, season={chosen}"
                )
            else:
                return False, f"季号不匹配 TMDB: season_hint={chosen}, tried={tried_queries}"
        else:
            return False, f"季号不匹配 TMDB: season_hint={chosen}, tried={tried_queries}"

    resolved = resolve_season_by_tmdb(None, int(tmdb_id), chosen, final_hint=is_final)
    if resolved is None:
        return False, "Final Season 解析失败或 TMDB 不可用"

    context["season_hint"] = chosen
    context["resolved_season"] = resolved
    context["final_resolved_season"] = resolved if is_final else None
    return True, None
