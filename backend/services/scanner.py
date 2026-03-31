"""扫描任务编排（v2）：目录驱动、统一识别、事务执行与回滚。"""
import os
import fcntl
import re
import time
import hashlib
import json
import queue
import atexit
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread
from contextlib import nullcontext
from typing import Literal

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
    extract_bang_season,
    extract_strong_extra_fallback_label,
    get_tmdb_tv_details_sync,
    is_title_number_safe,
    parse_movie_filename,
    parse_tv_filename,
    _is_nc_ver_skip_file,
)
from .recognition_flow import (
    LocalParseSnapshot,
    infer_season_from_tmdb_seasons,
    parse_structure_locally,
    recognize_directory_with_fallback,
    recognize_directory_with_season_hint_trace,
    resolve_season as resolve_season_by_tmdb,
)
from .allocator import (
    allocate_indices_for_batch,
    mark_pending,
    scan_existing_special_indices,
    should_fallback_to_pending,
)
from .media_content_types import ALL_EXTRA_LIKE_CATEGORIES, EXTRA_CATEGORIES, SPECIAL_CATEGORIES, classify_content_type
from .target_path_resolver import (
    apply_readable_suffix_for_unnumbered_extra as resolver_apply_readable_suffix_for_unnumbered_extra,
    build_attachment_target_from_anchor as resolver_build_attachment_target_from_anchor,
    deduplicate_target_or_raise as resolver_deduplicate_target_or_raise,
    resolve_attachment_follow_target as resolver_resolve_attachment_follow_target,
    resolve_final_target,
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
EXTRAS_CATEGORIES = set(EXTRA_CATEGORIES)
SPECIAL_ANCHOR_CATEGORIES = {"special", "oped", "making", "trailer", "preview", "pv", "cm", "teaser", "character_pv"}
SPECIAL_DIR_TOKENS = {
    "sps",
    "special",
    "specials",
    "extras",
    "extra",
    "sp",
    "bonus",
    "bonus disc",
    "special disc",
}
STRONG_SPECIAL_DIR_TOKENS = {
    "sps",
    "special",
    "specials",
    "extras",
    "extra",
    "sp",
    "bonus",
    "bonus disc",
    "special disc",
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


def _classify_media_task_error(exc: Exception) -> Literal["deterministic_conflict", "transient", "other"]:
    text = str(exc or "")
    lowered = text.lower()
    deterministic_keywords = (
        "多个源文件映射到同一目标",
        "目标路径已被其他文件占用",
        "batch target conflict",
        "target conflict",
        "mapped to same target",
    )
    if any(token in text for token in deterministic_keywords) or "deterministic conflict" in lowered:
        return "deterministic_conflict"
    if isinstance(exc, (OperationalError, TimeoutError, httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout)):
        return "transient"
    transient_keywords = (
        "database is locked",
        "database table is locked",
        "timeout",
        "timed out",
        "temporarily unavailable",
        "deadlock",
        "try again",
    )
    if any(token in lowered for token in transient_keywords):
        return "transient"
    return "other"


def _resolve_unprocessed_jsonl_path() -> Path | None:
    path_value = str(getattr(settings, "unprocessed_items_jsonl_path", "") or "").strip()
    if path_value:
        return Path(path_value)
    legacy = str(getattr(settings, "unhandled_jsonl_path", "") or "").strip()
    if not legacy:
        return None
    return Path(legacy)


def _resolve_review_jsonl_path() -> Path | None:
    path_value = str(getattr(settings, "review_jsonl_path", "") or "").strip()
    if not path_value:
        return None
    return Path(path_value)


def _append_jsonl_record(path: Path, payload: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        return


def _record_unhandled_item(
    *,
    original_path: Path,
    reason: str,
    file_type: str,
    sync_group_id: int | None = None,
    tmdb_id: int | None = None,
    season: int | None = None,
    episode: int | None = None,
    extra_category: str | None = None,
    suggested_target: Path | None = None,
) -> None:
    unprocessed_path = _resolve_unprocessed_jsonl_path()
    if unprocessed_path is None:
        return

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "original_path": str(original_path),
        "source_dir": str(original_path.parent),
        "sync_group_id": sync_group_id,
        "file_type": file_type,
        "reason": str(reason or "").strip(),
        "tmdb_id": tmdb_id,
        "season": season,
        "episode": episode,
        "extra_category": extra_category,
        "suggested_target": str(suggested_target) if suggested_target else None,
    }
    payload["codeai_brief"] = (
        f"path={payload['original_path']} | reason={payload['reason']} | type={payload['file_type']} | "
        f"group={payload['sync_group_id']} | tmdb={payload['tmdb_id']} | "
        f"s{payload['season']}e{payload['episode']} | extra={payload['extra_category']} | "
        f"suggested={payload['suggested_target']}"
    )
    _append_jsonl_record(unprocessed_path, payload)


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
                append_log("WARNING: Worker 进程锁被其他进程持有，继续使用本地队列 Worker。")
        _WORKER_THREAD = Thread(target=_media_task_worker, daemon=True)
        _WORKER_THREAD.start()
        append_log("INFO: 媒体处理 Worker 已启动")


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
            append_log("WARNING: 媒体 Worker 未及时退出，继续关闭")
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
        append_log(f"INFO: 媒体任务开始: {task.path}")
        retries = 0
        recompute_retry_used = False
        while True:
            local_db = SessionLocal()
            try:
                _handle_media_task(local_db, task, force_recompute_names=recompute_retry_used)
                local_db.commit()
                append_log(f"INFO: 媒体任务完成: {task.path}")
                break
            except Exception as e:
                local_db.rollback()
                error_kind = _classify_media_task_error(e)
                if error_kind == "deterministic_conflict" and not recompute_retry_used:
                    recompute_retry_used = True
                    append_log(
                        f"WARNING: MediaTask 确定性冲突: {task.path} | {e} | trigger target-name recompute retry"
                    )
                    continue
                if error_kind == "deterministic_conflict" and recompute_retry_used:
                    group = local_db.query(SyncGroup).filter(SyncGroup.id == task.sync_group_id).first()
                    if group:
                        reason = f"deterministic conflict after recompute: {e}"
                        _mark_dir_pending(
                            local_db,
                            task.path,
                            Path(group.source),
                            group.id,
                            group.source_type,
                            reason,
                        )
                        append_log(
                            f"WARNING: MediaTask 确定性冲突未解决，已移至 pending_manual: {task.path}"
                        )
                    else:
                        append_log(
                            f"WARNING: MediaTask 确定性冲突未解决 (组缺失): {task.path} | {e}"
                        )
                    break
                if error_kind == "transient" and retries < 3:
                    retries += 1
                    backoff = 2 ** retries
                    append_log(
                        f"WARNING: MediaTask 暂时性失败 ({retries}/3): {task.path} | {e} | backoff={backoff}s"
                    )
                    time.sleep(backoff)
                    continue
                append_log(
                    f"WARNING: MediaTask 暂时性失败 ({error_kind}): {task.path} | {e}"
                )
                break
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
        "sync_group_id": group.id,
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
        "extra_context": bool(detect_special_dir_context(root_dir)[0]),
        "strong_extra_context": bool(detect_strong_special_dir_context(root_dir)[0]),
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

    video_files = sorted([p for p in media_files if p.suffix.lower() in VIDEO_EXTS], key=_video_sort_key)
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
            if src_path.parent in dir_runtime.get("rolled_back_dirs", set()):
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
            processed += 1

        for src_path in attachment_files:
            if src_path.parent in dir_runtime.get("rolled_back_dirs", set()):
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
        "sync_group_id": group.id,
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
        "extra_context": False,
        "strong_extra_context": False,
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
    context["extra_context"] = bool(any(detect_special_dir_context(p)[0] for p in media_files))
    context["strong_extra_context"] = bool(any(detect_strong_special_dir_context(p)[0] for p in media_files))

    video_files = sorted([p for p in media_files if p.suffix.lower() in VIDEO_EXTS], key=_video_sort_key)
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
            if src_path.parent in dir_runtime.get("rolled_back_dirs", set()):
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
            processed += 1

        for src_path in attachment_files:
            if src_path.parent in dir_runtime.get("rolled_back_dirs", set()):
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


def _handle_media_task(db: Session, task: MediaTask, force_recompute_names: bool = False) -> None:
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

    dir_special_context, dir_special_token = detect_special_dir_context(media_dir)
    structure_hint = task.media_type or _detect_media_type_from_structure(media_dir)
    if dir_special_context:
        structure_hint = "tv"
    # 多季合并目录检测：目录名含 "I+II" / "1+2" / "I&II" 等格式时强制 TV 类型
    # 例：Chihayafuru I+II → 两季正片合并在一个目录中，不应被识别为电影
    _multi_season_merge_re = re.compile(
        r"\b(?:[IVX]{1,4}|[1-9])\s*[+&]\s*(?:[IVX]{1,4}|[1-9])\b", re.I
    )
    if structure_hint != "tv" and _multi_season_merge_re.search(media_dir.name):
        append_log(
            f"INFO: 目录名含多季合并格式（I+II/1+2等），强制 structure_hint=tv: {media_dir.name!r}"
        )
        structure_hint = "tv"
    best, snapshot, fallback_round = recognize_directory_with_fallback(
        media_dir,
        group.source_type,
        structure_hint=structure_hint,
    )
    if best is None:
        reason = f"目录级识别失败或低置信度: {media_dir.name}"
        _finalize_dir_to_pending(
            db,
            media_dir=media_dir,
            source_root=source,
            sync_group_id=group.id,
            source_type=group.source_type,
            signature=signature,
            state="LOW_CONFIDENCE",
            reason=reason,
            emit_jsonl=False,
        )
        return

    # 多集特征否决电影类型：若识别结果为电影，但目录内存在 ≥2 个不同集数的视频文件，
    # 强制改为 TV 识别并重试。这修复了 Kaguya-sama First Kiss wa Owaranai 被误判为电影的问题。
    if best.media_type == "movie" and structure_hint != "movie":
        _dir_video_files = [
            p for p in media_dir.rglob("*")
            if p.is_file() and p.suffix.lower() in VIDEO_EXTS and not _is_ignored_name(p.name)
        ]
        _ep_nums: set[int] = set()
        for _vf in _dir_video_files:
            _ep = _extract_episode_from_filename_loose(_vf.name)
            if _ep is not None and 1 <= _ep <= 999:
                _ep_nums.add(_ep)
        if len(_ep_nums) >= 2:
            append_log(
                f"INFO: 电影结论被多集特征否决: tmdbid={best.tmdb_id} 检测到 {len(_ep_nums)} 个不同集号 {sorted(_ep_nums)[:5]!r}，"
                f"重试 TV-only 识别"
            )
            _tv_best, _tv_snapshot, _tv_fallback_round = recognize_directory_with_fallback(
                media_dir,
                group.source_type,
                structure_hint="tv",
            )
            if _tv_best is not None and _tv_best.media_type == "tv":
                best, snapshot, fallback_round = _tv_best, _tv_snapshot, _tv_fallback_round
                append_log(
                    f"INFO: TV 重识别成功: tmdbid={best.tmdb_id}, score={best.score:.3f}"
                )

    recognized_type = best.media_type
    target_type = recognized_type
    if target_type == "movie" and movie_target_root is None:
        reason = "识别为电影但未找到可用电影目标路径"
        _finalize_dir_to_pending(
            db,
            media_dir=media_dir,
            source_root=source,
            sync_group_id=group.id,
            source_type=group.source_type,
            signature=signature,
            state="FAILED",
            reason=reason,
        )
        return

    _upsert_dir_state(db, group.id, media_dir, signature, "IDENTIFIED", None)

    media_files = _collect_media_files_under_dir(media_dir, include, exclude, source)
    video_files = sorted([p for p in media_files if p.suffix.lower() in VIDEO_EXTS], key=_video_sort_key)
    attachment_files = sorted([p for p in media_files if p.suffix.lower() in ATTACHMENT_EXTS], key=lambda p: p.as_posix())
    extra_context, extra_context_token = detect_special_dir_context(media_dir)
    strong_extra_context, strong_extra_token = detect_strong_special_dir_context(media_dir)
    if not extra_context and dir_special_context:
        extra_context, extra_context_token = True, dir_special_token
    if not strong_extra_context and dir_special_context and dir_special_token in STRONG_SPECIAL_DIR_TOKENS:
        strong_extra_context, strong_extra_token = True, dir_special_token

    target_root = tv_target_root if target_type == "tv" else movie_target_root

    resolved_title = _resolve_chinese_title_by_tmdb(
        media_type=target_type,
        tmdb_id=best.tmdb_id,
        fallback_title=best.title or snapshot.main_title,
        strict=True,
    )
    if not resolved_title:
        reason = "TMDB zh-CN 标题缺失"
        _finalize_dir_to_pending(
            db,
            media_dir=media_dir,
            source_root=source,
            sync_group_id=group.id,
            source_type=group.source_type,
            signature=signature,
            state="LOW_CONFIDENCE",
            reason=reason,
        )
        return

    context = {
        "media_type": target_type,
        "sync_group_id": group.id,
        "tmdb_id": best.tmdb_id,
        "tmdb_data": _get_tmdb_item_by_id(target_type, best.tmdb_id) or best.tmdb_data,
        "title": resolved_title,
        "year": best.year or snapshot.year_hint,
        "target_root": str(target_root),
        "score": best.score,
        "fallback_round": fallback_round,
        "season_hint": snapshot.season_hint,
        "season_hint_confidence": snapshot.season_hint_confidence,
        "season_hint_source": snapshot.season_hint_source,
        "season_hint_raw": snapshot.season_hint_raw,
        "special_hint": snapshot.special_hint,
        "final_hint": snapshot.final_hint,
        "season_aware_done": snapshot.season_aware_done,
        "season_aware_had_candidates": snapshot.season_aware_had_candidates,
        "season_aware_tried_queries": list(snapshot.season_aware_tried_queries or []),
        "recompute_target_names": bool(force_recompute_names),
        "extra_context": bool(extra_context),
        "extra_context_token": extra_context_token,
        "strong_extra_context": bool(strong_extra_context),
        "strong_extra_context_token": strong_extra_token,
        "_has_issues": False,
    }

    stable_ok, stable_reason = _stabilize_directory_context(media_dir, context)
    if not stable_ok:
        reason = f"目录稳定决策失败: {stable_reason}"
        _finalize_dir_to_pending(
            db,
            media_dir=media_dir,
            source_root=source,
            sync_group_id=group.id,
            source_type=group.source_type,
            signature=signature,
            state="LOW_CONFIDENCE",
            reason=reason,
        )
        return

    append_log(
        f"INFO: 目录识别成功: {media_dir.name} -> {target_type}:{context['title']} "
        f"(score={best.score:.3f}, fallback_round={fallback_round})"
    )

    # AI 剧集映射（外挂功能，仅 tv 类型且 ai_enabled=True 时生效）
    if target_type == "tv" and settings.ai_enabled:
        try:
            from .ai_service import analyze_episode_mapping as _ai_analyze
            _series_details = context.get("_tmdb_series_details") or {}
            _ai_mapping = _ai_analyze(_series_details, media_dir, video_files)
            if _ai_mapping:
                context["ai_episode_mapping"] = _ai_mapping
        except Exception as _ai_err:
            append_log(f"WARNING: [AI识别] 调用失败，跳过 AI 映射: {_ai_err}")

    op_log = OperationLog()
    seen_targets: dict[Path, Path] = {}
    dir_runtime: dict = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "video_anchor_by_parent_special": {},
        "video_anchor_recent_by_parent": {},
        "pending_count": 0,
        "skipped_count": 0,
    }

    show_dir = _resolve_show_dir(target_root, context["title"], context.get("year"), context.get("tmdb_id"))
    use_lock = bool(getattr(settings, "use_file_lock", True))
    # show 级别文件锁：保证同一剧集的索引分配与落地原子性（单机有效）
    lock_ctx = _ShowLock(show_dir / ".assign.lock") if use_lock else nullcontext()
    try:
        with lock_ctx:
            # 批量预扫描 + 批量分配：消除 N×M 重排
            existing_cache = scan_existing_special_indices(target_root, context.get("tmdb_id"))
            append_log(f"INFO: 扫描已有特典索引: {existing_cache}")
            all_files = video_files + attachment_files
            if force_recompute_names:
                append_log("INFO: 重算模式已启用：跳过分配器分配并重新计算目标文件名")
                items, item_map, assignments = [], {}, {}
            else:
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
                if src_path.parent in dir_runtime.get("rolled_back_dirs", set()):
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
                if src_path.parent in dir_runtime.get("rolled_back_dirs", set()):
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
    except DirectoryProcessError as e:
        reason = str(e)
        error_kind = _classify_media_task_error(e)
        if error_kind == "transient":
            _rollback_operations(op_log)
            _upsert_dir_state(db, group.id, media_dir, signature, "FAILED", reason)
            raise
        if error_kind == "deterministic_conflict" and not force_recompute_names:
            _rollback_operations(op_log)
            _upsert_dir_state(db, group.id, media_dir, signature, "FAILED", reason)
            raise
        _finalize_dir_to_pending(
            db,
            media_dir=media_dir,
            source_root=source,
            sync_group_id=group.id,
            source_type=group.source_type,
            signature=signature,
            state="FAILED",
            reason=reason,
            op_log=op_log,
        )
        if error_kind == "deterministic_conflict" and force_recompute_names:
            return
        return


def _rollback_dir_context(
    conflict_dir: "Path",
    reason: str,
    seen_targets: "dict[Path, Path]",
    dir_runtime: dict,
    op_log: "OperationLog | None" = None,
) -> None:
    """目录级回滚：清除该目录在本批次产生的所有 seen_targets 条目、anchor 缓存及物理硬链接。"""
    rolled = dir_runtime.setdefault("rolled_back_dirs", set())
    if conflict_dir in rolled:
        return
    rolled.add(conflict_dir)
    parent_key = str(conflict_dir)

    # 物理文件回滚：删除该目录已创建的硬链接及空目录
    if op_log is not None:
        conflict_dst_paths = {dst for dst, src in seen_targets.items() if src.parent == conflict_dir}
        links_to_remove = conflict_dst_paths & op_log.created_links
        removed_count = 0
        dirs_to_check: set[Path] = set()
        for link in sorted(links_to_remove, key=lambda p: len(p.parts), reverse=True):
            try:
                if link.exists() and link.is_file():
                    link.unlink()
                    removed_count += 1
                dirs_to_check.add(link.parent)
            except OSError as _e:
                append_log(f"WARNING: 无法删除硬链接残留: {link} ({_e})")
            op_log.created_links.discard(link)
        for d in sorted(dirs_to_check, key=lambda p: len(p.parts), reverse=True):
            if d in op_log.created_dirs:
                try:
                    if d.exists() and d.is_dir() and not any(d.iterdir()):
                        d.rmdir()
                        op_log.created_dirs.discard(d)
                except OSError:
                    pass
        if removed_count:
            append_log(f"INFO: 目录级物理回滚: {removed_count} 个硬链接已删除 ({conflict_dir.name})")

    stale = [dst for dst, src in seen_targets.items() if src.parent == conflict_dir]
    for dst in stale:
        del seen_targets[dst]
    for anchor_key in (
        "video_anchor_by_parent",
        "video_anchor_by_parent_episode",
        "video_anchor_by_parent_stem",
        "video_anchor_by_parent_special",
        "video_anchor_by_parent_special_prefix",
        "video_anchor_recent_by_parent",
    ):
        dir_runtime.get(anchor_key, {}).pop(parent_key, None)
    append_log(f"INFO: 目录级回滚: {conflict_dir.name} | 原因: {reason}")


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

    tmdb_id = context.get("tmdb_id")
    if _should_ignore_fractional_episode(src_path.name):
        append_log(f"跳过半集文件: {src_path.name}")
        _record_unhandled_item(
            original_path=src_path,
            reason="fractional episode skipped",
            file_type="video" if ext in VIDEO_EXTS else "attachment",
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
        )
        return

    if _is_nc_ver_skip_file(src_path.name):
        append_log(f"INFO: NC Ver 文件已跳过: {src_path.name}")
        _record_unhandled_item(
            original_path=src_path,
            reason="NC Ver skip",
            file_type="video" if ext in VIDEO_EXTS else "attachment",
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
        )
        return

    media_type = context["media_type"]
    tmdb_data = context.get("tmdb_data") or {}
    target_root = Path(context["target_root"])
    is_attachment = ext in ATTACHMENT_EXTS
    record_status = str(context.get("record_status") or "scraped")
    file_special_context, _special_token = detect_special_dir_context(src_path)
    file_strong_special_context, _strong_special_token = detect_strong_special_dir_context(src_path)
    strong_extra_context = bool(context.get("strong_extra_context")) or file_strong_special_context
    extra_context = bool(context.get("extra_context")) or file_special_context or strong_extra_context

    parse_result = parse_tv_filename(str(src_path)) if media_type == "tv" else parse_movie_filename(str(src_path))
    if parse_result is None:
        fallback_parse = parse_movie_filename(str(src_path)) if media_type == "tv" else parse_tv_filename(str(src_path))
        if fallback_parse is None:
            if strong_extra_context:
                parse_result = ParseResult(
                    title=str(context.get("title") or src_path.stem),
                    year=context.get("year") if context.get("year") else None,
                    season=1,
                    episode=None,
                    is_special=False,
                    quality=None,
                    extra_category=None,
                    extra_label=None,
                )
            elif is_attachment:
                # 附件无法解析 → 跳过，不中断目录处理
                _record_unhandled_item(
                    original_path=src_path,
                    reason="parse failed",
                    file_type="attachment",
                    sync_group_id=sync_group_id,
                    tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                )
                return
            else:
                # 视频文件解析失败 → 降级为 bdextra extra，不中断目录处理
                append_log(f"WARNING: 文件解析失败，降级为 bdextra extra: {src_path.name}")
                parse_result = ParseResult(
                    title=str(context.get("title") or src_path.stem),
                    year=context.get("year") if context.get("year") else None,
                    season=1,
                    episode=None,
                    is_special=False,
                    quality=None,
                    extra_category="bdextra",
                    extra_label=None,
                )
        else:
            parse_result = fallback_parse

    parse_result = parse_result._replace(
        title=context.get("title") or parse_result.title,
        year=context.get("year") if context.get("year") else parse_result.year,
    )

    if extra_context:
        forced_category, forced_label, _from_bracket = classify_extra_from_text(src_path.name)
        if forced_category:
            parse_result = parse_result._replace(
                extra_category=forced_category,
                extra_label=forced_label,
                is_special=forced_category in SPECIAL_CATEGORIES,
            )
        elif strong_extra_context and ext in VIDEO_EXTS:
            fallback_label = extract_strong_extra_fallback_label(src_path.name)
            if fallback_label:
                parse_result = parse_result._replace(
                    extra_category="bdextra",
                    extra_label=fallback_label,
                    is_special=False,
                    episode=None,
                )
            else:
                append_log(f"WARNING: 特典目录文件无法稳定分类，已跳过: {src_path.name}")
                _record_unhandled_item(
                    original_path=src_path,
                    reason="extra category unresolved in special dir",
                    file_type="extra",
                    sync_group_id=sync_group_id,
                    tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                    season=parse_result.season,
                    episode=parse_result.episode,
                    extra_category=parse_result.extra_category,
                )
                if dir_runtime is not None:
                    dir_runtime["skipped_count"] = int(dir_runtime.get("skipped_count") or 0) + 1
                return
        elif ext in VIDEO_EXTS:
            append_log(f"WARNING: 特典目录文件无法稳定分类，已跳过: {src_path.name}")
            _record_unhandled_item(
                original_path=src_path,
                reason="extra category unresolved in special dir",
                file_type="extra",
                sync_group_id=sync_group_id,
                tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                season=parse_result.season,
                episode=parse_result.episode,
                extra_category=parse_result.extra_category,
            )
            if dir_runtime is not None:
                dir_runtime["skipped_count"] = int(dir_runtime.get("skipped_count") or 0) + 1
            return
    if strong_extra_context and ext in VIDEO_EXTS and parse_result.extra_category is None:
        append_log(f"WARNING: 特典目录文件无法稳定分类，已跳过: {src_path.name}")
        _record_unhandled_item(
            original_path=src_path,
            reason="extra category unresolved in special dir",
            file_type="extra",
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
            season=parse_result.season,
            episode=parse_result.episode,
            extra_category=parse_result.extra_category,
        )
        if dir_runtime is not None:
            dir_runtime["skipped_count"] = int(dir_runtime.get("skipped_count") or 0) + 1
        return
    if extra_context and parse_result.extra_category is not None and parse_result.extra_category not in SPECIAL_CATEGORIES:
        parse_result = parse_result._replace(episode=None)
    parse_result = _apply_parallel_variant_suffix(parse_result, src_path)

    # 「括号 variant 降级」：正片解析结果无特典类别，但文件名括号内含有明确的特典变体关键词时，
    # 强制将其降级为 special 类。这修复了 [Preview01_1] / [Mystery Camp] / [On Air Ver.] 等
    # variant 文件先于正片占据主视频目标槽位的问题。
    if ext in VIDEO_EXTS and parse_result.extra_category is None and parse_result.episode is not None:
        _variant_category, _variant_label, _from_bracket = _detect_bracket_variant_as_special(src_path.name)
        if _variant_category is not None:
            append_log(
                f"INFO: 括号variant检测: {src_path.name!r} 含 variant 括号 → "
                f"降级为 {_variant_category} label={_variant_label!r}"
            )
            parse_result = parse_result._replace(
                extra_category=_variant_category,
                extra_label=_variant_label,
                is_special=_variant_category in SPECIAL_CATEGORIES,
                episode=None,
            )

    menu_token = _extract_menu_token(src_path.name)
    if menu_token:
        append_log(f"INFO: Special 忽略: {menu_token}")
        _record_unhandled_item(
            original_path=src_path,
            reason=f"menu token ignored: {menu_token}",
            file_type="special",
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
            season=parse_result.season,
            episode=parse_result.episode,
            extra_category=parse_result.extra_category,
        )
        return

    if is_attachment:
        anchor_dst = resolver_resolve_attachment_follow_target(src_path, parse_result, dir_runtime)
        if anchor_dst is not None:
            dst_path = resolver_build_attachment_target_from_anchor(anchor_dst, parse_result, ext, src_path=src_path)
            if dst_path in seen_targets or (dst_path.exists() and not is_same_inode(src_path, dst_path)):
                context["_has_issues"] = True
                append_log(
                    "WARNING: 附件跟随目标冲突已跳过: %s -> %s%s"
                    % (
                        src_path.name,
                        dst_path,
                        _format_conflict_diff_suffix(merged_tags=_extract_distinguish_source_tags(src_path.stem)),
                    )
                )
                _record_unhandled_item(
                    original_path=src_path,
                    reason="attachment follow target occupied",
                    file_type="attachment",
                    sync_group_id=sync_group_id,
                    tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                    season=parse_result.season,
                    episode=parse_result.episode,
                    extra_category=parse_result.extra_category,
                    suggested_target=dst_path,
                )
                return
            try:
                resolver_deduplicate_target_or_raise(seen_targets, src_path, dst_path)
            except ValueError:
                context["_has_issues"] = True
                append_log(f"WARNING: 附件跟随目标冲突已跳过: {src_path.name} -> {dst_path}")
                _record_unhandled_item(
                    original_path=src_path,
                    reason="attachment follow target occupied",
                    file_type="attachment",
                    sync_group_id=sync_group_id,
                    tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                    season=parse_result.season,
                    episode=parse_result.episode,
                    extra_category=parse_result.extra_category,
                    suggested_target=dst_path,
                )
                return
            _execute_transactional_outputs(
                src_path=src_path,
                dst_path=dst_path,
                media_type=media_type,
                parse_result=parse_result,
                tmdb_data=tmdb_data,
                should_scrape=False,
                display_title=context.get("title"),
                op_log=op_log,
            )
            _upsert_media_record(db, sync_group_id, src_path, dst_path, media_type, tmdb_id, status=record_status)
            _upsert_inode_record(db, sync_group_id, src_path, dst_path)
            append_log(f"INFO: 附件跟随正片: {src_path.name} -> {dst_path}")
            return
        if parse_result.extra_category in SPECIAL_ANCHOR_CATEGORIES:
            append_log(f"WARNING: 特典附件未能匹配任何视频，已跳过: {src_path.name}")
            dir_runtime["skipped_count"] = int(dir_runtime.get("skipped_count") or 0) + 1
            reason = "special attachment raw-label unmatched"
            raw_key = _build_special_raw_label_key(str(src_path.parent), parse_result.extra_label)
            raw_value = "__missing__"
            if raw_key is not None:
                raw_value = dir_runtime.get("special_target_by_raw_label", {}).get(raw_key, "__missing__")
            if raw_value is None:
                reason = "special attachment raw-label conflict"
            fine_key = _build_special_fine_label_key(
                str(src_path.parent),
                raw_label=parse_result.extra_label,
                source_text=src_path.stem,
                category=parse_result.extra_category,
            )
            if fine_key is not None:
                fine_value = dir_runtime.get("special_target_by_fine_label", {}).get(fine_key, "__missing__")
                if fine_value is None:
                    reason = "special attachment raw-label conflict"
            _record_unhandled_item(
                original_path=src_path,
                reason=reason,
                file_type="attachment",
                sync_group_id=sync_group_id,
                tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                season=parse_result.season,
                episode=parse_result.episode,
                extra_category=parse_result.extra_category,
            )
            return
        if parse_result.extra_category is None:
            append_log(f"WARNING: 附件未匹配到视频已跳过: {src_path.name}")
            dir_runtime["skipped_count"] = int(dir_runtime.get("skipped_count") or 0) + 1
            _record_unhandled_item(
                original_path=src_path,
                reason="attachment unmatched",
                file_type="attachment",
                sync_group_id=sync_group_id,
                tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                season=parse_result.season,
                episode=parse_result.episode,
                extra_category=parse_result.extra_category,
            )
            return

    if parse_result.extra_category is not None:
        _log_special_classification(parse_result.extra_category, parse_result.extra_label)

    assignments = context.get("allocator_assignments") or {}
    item_map = context.get("allocator_items") or {}

    if media_type == "tv" and parse_result.extra_category is None and _is_missing_or_invalid_episode(parse_result.episode):
        parse_result = parse_result._replace(episode=None)

    if (
        media_type == "tv"
        and (not extra_context)
        and parse_result.extra_category is None
        and _is_missing_or_invalid_episode(parse_result.episode)
        and _should_ignore_zero_episode(
            src_path.name,
            parse_result=parse_result,
            context_title=str(context.get("title") or ""),
        )
    ):
        append_log(f"INFO: 跳过第0集/00集文件: {src_path.name}")
        _record_unhandled_item(
            original_path=src_path,
            reason="zero episode skipped",
            file_type="video",
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
            season=parse_result.season,
            episode=parse_result.episode,
            extra_category=parse_result.extra_category,
        )
        return

    if media_type == "tv" and (not extra_context):
        if _is_missing_or_invalid_episode(parse_result.episode) and parse_result.extra_category is None:
            ep = _extract_episode_from_filename_loose(src_path.name)
            if ep is not None:
                parse_result = parse_result._replace(episode=ep)
        if parse_result.extra_category is None and _is_missing_or_invalid_episode(parse_result.episode):
            # 无法提取集号 → 降级为 bdextra extra，继续处理目录中其余文件
            append_log(f"WARNING: 主视频缺少稳定集号，降级为 bdextra extra: {src_path.name}")
            parse_result = parse_result._replace(
                extra_category="bdextra",
                is_special=False,
                episode=None,
            )

        resolved_season = context.get("resolved_season")
        season_from_path = _extract_season_from_path(src_path)
        has_explicit = _has_explicit_season_token(src_path.name)
        if season_from_path is not None:
            season = season_from_path
        elif not has_explicit and resolved_season:
            season = resolved_season
        else:
            # !! 弱季号提示：有 resolved_season 时优先使用，不信任 bang 派生的季号
            if getattr(parse_result, "season_hint_strength", None) == "bang" and resolved_season:
                append_log(
                    f"INFO: [season] bang 候选覆盖: file={src_path.name!r}, "
                    f"candidate={parse_result.season} → resolved={resolved_season} (source=resolved_season)"
                )
                season = resolved_season
            else:
                season = parse_result.season or resolved_season or 1
        parse_result = parse_result._replace(season=season)

        # AI 剧集映射覆盖（外挂功能）：仅覆盖普通剧集（extra_category 为 None）
        _ai_ep_mapping = context.get("ai_episode_mapping") or {}
        if str(src_path) in _ai_ep_mapping and parse_result.extra_category is None:
            _ai_season, _ai_episode = _ai_ep_mapping[str(src_path)]
            append_log(
                f"INFO: [AI识别] 覆盖集数映射: {src_path.name} "
                f"→ S{_ai_season:02d}E{_ai_episode:02d}"
            )
            parse_result = parse_result._replace(season=_ai_season, episode=_ai_episode)

        offset = context.get("episode_offset")
        if offset is not None and parse_result.episode is not None and parse_result.extra_category is None:
            adjusted = parse_result.episode - int(offset)
            parse_result = parse_result._replace(episode=max(1, adjusted))
        parse_result = resolver_apply_readable_suffix_for_unnumbered_extra(parse_result, src_path, dir_runtime)

    # Phase 2: 特典继承目录已解析季号（仅当文件无显式季号标记时）
    if media_type == "tv" and parse_result.extra_category is not None:
        _resolved_season_for_extra = context.get("resolved_season")
        if _resolved_season_for_extra and not _has_explicit_season_token(src_path.name):
            if parse_result.season in (None, 0, 1):
                parse_result = parse_result._replace(season=_resolved_season_for_extra)

    decision = resolve_final_target(
        src_path=src_path,
        parse_result=parse_result,
        media_type=media_type,
        target_root=target_root,
        tmdb_id=tmdb_id,
        ext=ext,
        seen_targets=seen_targets,
        dir_runtime=dir_runtime,
        assignments=assignments,
        item_map=item_map,
        is_attachment=is_attachment,
    )
    if decision.status == "pending":
        if decision.reason == "main video target conflict unresolved":
            _rollback_dir_context(
                conflict_dir=src_path.parent,
                reason=decision.reason,
                seen_targets=seen_targets,
                dir_runtime=dir_runtime or {},
                op_log=op_log,
            )
            conflict_reason = (
                f"main video target conflict: {decision.dst_path} | links_rolled_back=True"
            )
            append_log(f"WARNING: 主视频目标冲突已回滚并转待办: {src_path.name} | 目标: {decision.dst_path}")
            _record_unhandled_item(
                original_path=src_path,
                reason=conflict_reason,
                file_type=decision.file_type or "video",
                sync_group_id=sync_group_id,
                tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                season=decision.parse_result.season,
                episode=decision.parse_result.episode,
                extra_category=decision.parse_result.extra_category,
                suggested_target=decision.dst_path,
            )
            context["_has_issues"] = True
            raise DirectoryProcessError(f"主视频目标冲突: {src_path.name} -> {decision.dst_path}")
        else:
            append_log(f"WARNING: 转待办: {src_path.name} | 原因: {decision.reason}")
        _record_unhandled_item(
            original_path=src_path,
            reason=decision.reason or "target occupied by other source",
            file_type=decision.file_type or "video",
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
            season=decision.parse_result.season,
            episode=decision.parse_result.episode,
            extra_category=decision.parse_result.extra_category,
            suggested_target=decision.dst_path,
        )
        context["_has_issues"] = True
        return
    if decision.status == "skip":
        if decision.reason and decision.reason.startswith("attachment"):
            append_log(
                "WARNING: 附件跟随目标冲突已跳过: %s -> %s%s"
                % (
                    src_path.name,
                    decision.dst_path,
                    _format_conflict_diff_suffix(decision.merged_tags, decision.dropped_tags),
                )
            )
        else:
            append_log(
                "WARNING: 特典目标冲突，已跳过: %s%s"
                % (
                    src_path.name,
                    _format_conflict_diff_suffix(decision.merged_tags, decision.dropped_tags),
                )
            )
        _record_unhandled_item(
            original_path=src_path,
            reason=decision.reason or "extra target conflict after remap attempts",
            file_type=decision.file_type or ("attachment" if is_attachment else "extra"),
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
            season=decision.parse_result.season,
            episode=decision.parse_result.episode,
            extra_category=decision.parse_result.extra_category,
            suggested_target=decision.dst_path,
        )
        if dir_runtime is not None:
            dir_runtime["skipped_count"] = int(dir_runtime.get("skipped_count") or 0) + 1
        return

    parse_result = decision.parse_result
    dst_path = decision.dst_path
    remapped = decision.remapped
    assigned_by_allocator = decision.assigned_by_allocator
    item = decision.item
    original_special_label = decision.original_special_label
    if decision.used_conflict_enrichment and (decision.merged_tags or decision.dropped_tags):
        append_log(
            "INFO: target-name conflict enriched: %s%s"
            % (
                src_path.name,
                _format_conflict_diff_suffix(decision.merged_tags, decision.dropped_tags),
            )
        )
    if parse_result.extra_category in ALL_EXTRA_LIKE_CATEGORIES:
        _log_special_resolution(src_path, parse_result, original_special_label, remapped)
    content_info = classify_content_type(parse_result, is_attachment)
    file_type_for_result = (
        "attachment"
        if content_info.content_type == "ATTACHMENT"
        else ("special" if content_info.content_type == "SPECIAL" else ("extra" if content_info.content_type == "EXTRA" else "video"))
    )

    if assigned_by_allocator and item:
        item["suggested_target"] = str(dst_path)
        pending_path_value = str(getattr(settings, "pending_jsonl_path", "") or "").strip()
        pending_path = Path(pending_path_value) if pending_path_value else None
        pending_cfg = {
            "max_auto_remap_attempts": getattr(settings, "max_auto_remap_attempts", 3),
        }
        fallback, reason = should_fallback_to_pending(item, dst_path, {}, pending_cfg)
        if fallback:
            if pending_path:
                mark_pending(item, reason, pending_path)
            append_log(f"WARNING: 转待办: {src_path.name} | 原因: {reason}")
            _record_unhandled_item(
                original_path=src_path,
                reason=reason,
                file_type=file_type_for_result,
                sync_group_id=sync_group_id,
                tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                season=parse_result.season,
                episode=parse_result.episode,
                extra_category=parse_result.extra_category,
                suggested_target=dst_path,
            )
            dir_runtime["pending_count"] = int(dir_runtime.get("pending_count") or 0) + 1
            context["_has_issues"] = True
            return

    should_scrape = (not is_attachment) and _should_scrape_for_target(media_type, dst_path, parse_result)
    try:
        _execute_transactional_outputs(
            src_path=src_path,
            dst_path=dst_path,
            media_type=media_type,
            parse_result=parse_result,
            tmdb_data=tmdb_data,
            should_scrape=should_scrape,
            display_title=context.get("title"),
            op_log=op_log,
        )
    except DirectoryProcessError as e:
        if extra_context and ("目标路径已被其他文件占用" in str(e) or "多个源文件映射到同一目标" in str(e)):
            append_log(f"WARNING: 特典写入冲突，已跳过: {src_path.name}")
            _record_unhandled_item(
                original_path=src_path,
                reason="extra transactional output conflict",
                file_type=file_type_for_result,
                sync_group_id=sync_group_id,
                tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
                season=parse_result.season,
                episode=parse_result.episode,
                extra_category=parse_result.extra_category,
                suggested_target=dst_path,
            )
            if dir_runtime is not None:
                dir_runtime["skipped_count"] = int(dir_runtime.get("skipped_count") or 0) + 1
            return
        raise

    _upsert_media_record(db, sync_group_id, src_path, dst_path, media_type, tmdb_id, status=record_status)
    _upsert_inode_record(db, sync_group_id, src_path, dst_path)
    if ext in VIDEO_EXTS:
        _register_video_anchor(src_path, dst_path, parse_result, dir_runtime, raw_label=original_special_label)

    append_log(f"INFO: 处理成功: {src_path.name} -> {dst_path}")


def resolve_season(tmdb_id: int, season_hint: int | None, final_season: bool = False) -> int | None:
    return resolve_season_by_tmdb(None, tmdb_id, season_hint, final_hint=final_season)


def _extract_season_from_path(src_path: Path) -> int | None:
    _roman = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10}
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
        # 罗马数字 Season 目录：Season II, Season III 等
        m4 = re.match(r"^season\s+(I{1,3}|IV|VI{0,3}|IX|X)$", part, re.I)
        if m4:
            val = _roman.get(m4.group(1).upper())
            if val:
                return val
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


def detect_special_dir_context(path: Path) -> tuple[bool, str | None]:
    nodes = [path] + list(path.parents)
    for node in nodes:
        raw = str(node.name or "").strip().lower()
        if not raw:
            continue
        normalized = re.sub(r"[\s._\-]+", " ", raw).strip()
        if normalized in SPECIAL_DIR_TOKENS:
            return True, normalized
    return False, None


def detect_strong_special_dir_context(path: Path) -> tuple[bool, str | None]:
    nodes = [path] + list(path.parents)
    for node in nodes:
        raw = str(node.name or "").strip().lower()
        if not raw:
            continue
        normalized = re.sub(r"[\s._\-]+", " ", raw).strip()
        if normalized in STRONG_SPECIAL_DIR_TOKENS:
            return True, normalized
    return False, None


def is_under_special_folder(file_path: Path) -> bool:
    return detect_special_dir_context(file_path)[0]


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
    ]
    for p in patterns:
        m = re.search(p, stem, re.I)
        if not m:
            continue
        ep = int(m.group(1))
        if 1 <= ep <= 999 and ep not in {2160, 1080, 720, 480, 265, 264}:
            return ep
    return None


def _register_video_anchor(
    src_path: Path,
    dst_path: Path,
    parse_result: ParseResult,
    dir_runtime: dict | None,
    raw_label: str | None = None,
) -> None:
    if dir_runtime is None:
        return
    by_parent = dir_runtime.setdefault("video_anchor_by_parent", {})
    by_parent_ep = dir_runtime.setdefault("video_anchor_by_parent_episode", {})
    by_parent_stem = dir_runtime.setdefault("video_anchor_by_parent_stem", {})
    by_parent_special = dir_runtime.setdefault("video_anchor_by_parent_special", {})
    by_parent_special_prefix = dir_runtime.setdefault("video_anchor_by_parent_special_prefix", {})
    by_parent_recent = dir_runtime.setdefault("video_anchor_recent_by_parent", {})
    special_by_raw = dir_runtime.setdefault("special_target_by_raw_label", {})
    special_by_raw_multi = dir_runtime.setdefault("special_targets_by_raw_label_multi", {})
    special_by_fine = dir_runtime.setdefault("special_target_by_fine_label", {})
    parent_key = str(src_path.parent)
    by_parent[parent_key] = dst_path
    by_parent_recent[parent_key] = dst_path
    special_key = _build_special_anchor_key(parse_result, src_path, original_label=parse_result.extra_label)
    if special_key is not None:
        bucket = by_parent_special.setdefault(special_key, [])
        if dst_path not in bucket:
            bucket.append(dst_path)
    if parse_result.extra_category in SPECIAL_CATEGORIES:
        prefix = _special_label_prefix(parse_result.extra_category, parse_result.extra_label or raw_label)
        if prefix:
            by_parent_special_prefix[(parent_key, parse_result.extra_category, prefix.upper())] = dst_path
    ep_key = _normalized_episode_key(parse_result, src_path, src_path.name)
    if ep_key is not None:
        by_parent_ep[(parent_key, int(ep_key))] = dst_path
    stem_key = _normalize_media_stem(src_path.stem)
    if stem_key:
        by_parent_stem[(parent_key, stem_key)] = dst_path
    raw_key = _build_special_raw_label_key(parent_key, raw_label)
    if raw_key is not None:
        multi_bucket = special_by_raw_multi.setdefault(raw_key, [])
        if dst_path not in multi_bucket:
            multi_bucket.append(dst_path)
        existing = special_by_raw.get(raw_key)
        if existing is None:
            special_by_raw[raw_key] = dst_path
        elif existing != dst_path:
            special_by_raw[raw_key] = None
    fine_key = _build_special_fine_label_key(
        parent_key,
        raw_label=raw_label or parse_result.extra_label,
        source_text=src_path.stem,
        category=parse_result.extra_category,
    )
    if fine_key is not None:
        existing_fine = special_by_fine.get(fine_key)
        if existing_fine is None:
            special_by_fine[fine_key] = dst_path
        elif existing_fine != dst_path:
            special_by_fine[fine_key] = None


def _build_special_raw_label_key(parent_key: str, raw_label: str | None) -> tuple[str, str] | None:
    label = re.sub(r"\s+", " ", str(raw_label or "")).strip()
    if not label:
        return None
    return (parent_key, label.upper())


def _build_special_fine_label_key(
    parent_key: str,
    raw_label: str | None,
    source_text: str,
    category: str | None,
) -> tuple[str, str, str] | None:
    coarse = _build_special_raw_label_key(parent_key, raw_label)
    if coarse is None:
        return None
    token = _build_source_diff_token(source_text, category=category)
    if not token:
        return None
    return (coarse[0], coarse[1], token)


def _build_source_diff_token(source_text: str, category: str | None) -> str:
    tags = _extract_distinguish_source_tags(source_text)
    token_parts: list[str] = []
    if tags:
        token_parts.extend(tags)
    scene = _extract_scene_fragment(source_text)
    if scene:
        normalized_scene = _normalize_special_anchor_token(scene, category)
        if normalized_scene:
            token_parts.append(normalized_scene)
    if not token_parts:
        normalized = _normalize_special_anchor_token(source_text, category)
        if normalized:
            token_parts.append(normalized[:24])
    if not token_parts:
        return ""
    joined = "|".join(x for x in token_parts if x)
    return joined[:96]


def _build_special_anchor_key(
    parse_result: ParseResult,
    src_path: Path,
    original_label: str | None = None,
) -> tuple[str, str, str, str] | None:
    category = str(parse_result.extra_category or "")
    if category not in SPECIAL_ANCHOR_CATEGORIES:
        return None
    parent_key = str(src_path.parent)
    stem_key = _normalize_media_stem(src_path.stem)
    if not stem_key:
        return None
    token = _special_anchor_token(parse_result, src_path, original_label=original_label)
    if not token:
        return None
    return (parent_key, stem_key, category, token)


def _special_anchor_token(parse_result: ParseResult, src_path: Path, original_label: str | None = None) -> str:
    label = str(original_label or parse_result.extra_label or "").strip()
    if not label:
        label = _extract_scene_fragment(src_path.stem) or _normalize_suffix_text(src_path.stem)
    return _normalize_special_anchor_token(label, parse_result.extra_category)


def _normalize_special_anchor_token(text: str, category: str | None) -> str:
    s = str(text or "")
    s = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", s)
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"[^\w\u4e00-\u9fff\- ]+", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip().lower()
    if not s:
        return ""
    cat = str(category or "")
    if cat == "making":
        s = re.sub(r"^making\s*\d{0,3}\b", "", s, flags=re.I)
    elif cat == "special":
        s = re.sub(r"^sp\s*\d{0,3}\b", "", s, flags=re.I)
    elif cat == "oped":
        s = re.sub(r"^(?:op|ed)\s*\d{0,3}\b", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -_")
    return s


def _extract_scene_fragment(text: str) -> str | None:
    s = str(text or "")
    if not s:
        return None
    m = re.search(r"\b(Scene)\s*[-_ ]*([0-9]{1,3}(?:[A-Za-z]{1,8})?)\b", s, re.I)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return None


def _execute_transactional_outputs(
    src_path: Path,
    dst_path: Path,
    media_type: str,
    parse_result: ParseResult,
    tmdb_data: dict,
    should_scrape: bool,
    op_log: OperationLog,
    display_title: str | None = None,
) -> None:
    _create_hardlink_with_tracking(src_path, dst_path, op_log)

    if not should_scrape:
        return

    if media_type == "tv":
        show_dir = _resolve_tv_show_dir_for_scrape(dst_path)
        before_files = set(_list_files_safe(show_dir))
        scrape_tv_metadata(show_dir, tmdb_data, display_title=display_title)
        _track_new_files(show_dir, before_files, op_log)
        if not (show_dir / "tvshow.nfo").exists():
            raise DirectoryProcessError(f"TV 元数据校验失败: {show_dir}")
    else:
        show_dir = dst_path.parent
        before_files = set(_list_files_safe(show_dir))
        scrape_movie_metadata(show_dir, tmdb_data, display_title=display_title)
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
    *,
    emit_log: bool = True,
    emit_jsonl: bool = True,
) -> Path:
    pending_dir = _resolve_manual_scope(src_path, source_root)
    if emit_jsonl:
        _record_unhandled_item(
            original_path=pending_dir,
            reason=reason,
            file_type="video",
            sync_group_id=sync_group_id,
        )
    if emit_log:
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


def _finalize_dir_to_pending(
    db: Session,
    *,
    media_dir: Path,
    source_root: Path,
    sync_group_id: int,
    source_type: str,
    signature: str,
    state: str,
    reason: str,
    op_log: OperationLog | None = None,
    emit_jsonl: bool = True,
) -> None:
    if op_log is not None:
        _rollback_operations(op_log)
    pending_dir = _mark_dir_pending(
        db,
        media_dir,
        source_root,
        sync_group_id,
        source_type,
        reason,
        emit_log=False,
        emit_jsonl=emit_jsonl,
    )
    _upsert_dir_state(db, sync_group_id, media_dir, signature, state, reason)
    append_log(f"WARNING: 目录处理失败并转待办: {pending_dir} | 原因: {reason}")


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
            # 旧版数据库可能缺少唯一约束，降级为手动 upsert
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
            append_log(f"WARNING: 数据库写入重试第 {attempt} 次: {e}")
            time.sleep(delay)
        except IntegrityError as e:
            db.rollback()
            append_log(f"WARNING: 数据库写入重试第 {attempt} 次: {e}")
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


def _should_ignore_zero_episode(
    filename: str,
    *,
    parse_result: ParseResult | None = None,
    context_title: str | None = None,
) -> bool:
    stem = Path(filename).stem
    if _has_explicit_nonzero_episode_signal(stem):
        return False
    if _is_title_number_protected_context(filename, parse_result=parse_result, context_title=context_title):
        return False
    patterns = [
        r"[\[\(]\s*0{1,2}\s*[\]\)]",   # [00] / (0)
        r"\bEP?\s*0+\b",               # EP0 / E00
        r"第\s*0+\s*[集话話]",          # 第0集
    ]
    if any(re.search(p, stem, re.I) is not None for p in patterns):
        return True
    normalized = re.sub(r"[\s._\-]+", " ", stem).strip().lower()
    if re.fullmatch(r"(?:0|00|ep0+|e0+|sp0+|ova0+)", normalized):
        return True
    return False


def _has_explicit_nonzero_episode_signal(stem: str) -> bool:
    if _extract_episode_from_filename_loose(stem) is not None:
        return True
    checks = [
        r"\bEP?\s*0*[1-9]\d{0,2}\b",
        r"\bE\s*0*[1-9]\d{0,2}\b",
        r"第\s*0*[1-9]\d{0,2}\s*[集话話]",
        r"[\[\(]\s*0*[1-9]\d{0,2}\s*[\]\)]",
    ]
    return any(re.search(p, stem, re.I) is not None for p in checks)


def _is_missing_or_invalid_episode(episode: int | None) -> bool:
    if episode is None:
        return True
    try:
        return int(episode) <= 0
    except (TypeError, ValueError):
        return True


def _is_title_number_protected_context(
    filename: str,
    *,
    parse_result: ParseResult | None = None,
    context_title: str | None = None,
) -> bool:
    candidates = [
        str(context_title or "").strip(),
        str(parse_result.title if parse_result else "").strip(),
        Path(filename).stem,
        str(Path(filename).parent.name or "").strip(),
    ]
    for cand in candidates:
        if not cand:
            continue
        normalized = re.sub(r"[\[\]\(\)\{\}_\-]+", " ", cand)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if normalized and not is_title_number_safe(normalized):
            return True
        if re.search(r"(?:tokyo|東京).{0,12}magnitude.{0,8}8(?:[\s.]?0)\b", normalized, re.I):
            return True
        if re.search(r"(?:東京マグニチュード)\s*8(?:[\s.]?0)\b", normalized, re.I):
            return True
    return False


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


# 括号内 variant 关键词模式：用于文件排序时将 variant 文件排在同集正片之后
_VARIANT_BRACKET_SORT_RE = re.compile(
    r"\([^)]*(?:On\s*Air\s*Ver|Staff\s+Credit\s+Ver|Credit\s+Ver|Mystery\s+Camp|Camp\b)[^)]*\)",
    re.I,
)


def _video_sort_key(p: Path) -> tuple[int, str]:
    """排序 key：先按路径正序，但含 variant 括号后缀的文件排在同名正片之后。
    这保证干净的正片文件优先占据目标槽，variant 文件遇到冲突时才转为特典。
    """
    has_variant = bool(_VARIANT_BRACKET_SORT_RE.search(p.name))
    return (1 if has_variant else 0, p.as_posix())


def _normalize_media_stem(stem: str) -> str:
    s = str(stem or "")
    s = re.sub(r"[\[\]\(\)\{\}]", " ", s)
    s = s.replace("_", " ").replace(".", " ")
    s = re.sub(r"\b(?:2160p|1080p|720p|480p|4k)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:x264|x265|h264|h265|hevc|av1|hi10p|ma10p)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:flac|aac|ac3|dts|ddp\d?\.\d?)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:webrip|web-dl|bdrip|bluray|remux)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:chs|cht|sc|tc|gb|big5|jpn|jp|ja|eng|en|zh[-_ ]?(?:cn|tw))\b", " ", s, flags=re.I)
    s = re.sub(r"[^\w\u4e00-\u9fff\- ]+", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip()
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
        # 注意：文件锁只对单机多进程有效，分布式需使用集中锁或单写服务
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
    special_ctx, _token = detect_special_dir_context(media_dir)
    if special_ctx:
        return "tv"
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
        upper = label.upper()
        if upper.startswith("OVA"):
            return "OVA"
        if upper.startswith("OAD"):
            return "OAD"
        if upper.startswith("SP") or upper.startswith("SPECIAL"):
            return "SP"
        for token in ("OVA", "OAD", "SP", "SPECIAL"):
            if re.search(rf"\b{token}\s*0*\d{{0,3}}\b", label, re.I):
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
        special_ctx, _token = detect_special_dir_context(src_path)
        strong_special_ctx, _strong_token = detect_strong_special_dir_context(src_path)
        parse_result = parse_tv_filename(str(src_path)) if media_type == "tv" else parse_movie_filename(str(src_path))
        if parse_result is None:
            fallback_parse = parse_movie_filename(str(src_path)) if media_type == "tv" else parse_tv_filename(str(src_path))
            if fallback_parse is not None:
                parse_result = fallback_parse
            elif strong_special_ctx:
                parse_result = ParseResult(
                    title=str(context.get("title") or src_path.stem),
                    year=context.get("year") if context.get("year") else None,
                    season=1,
                    episode=None,
                    is_special=False,
                    quality=None,
                    extra_category=None,
                    extra_label=None,
                )
            else:
                continue
        if special_ctx:
            forced_category, forced_label, _from_bracket = classify_extra_from_text(src_path.name)
            if forced_category:
                parse_result = parse_result._replace(
                    extra_category=forced_category,
                    extra_label=forced_label,
                    is_special=forced_category in SPECIAL_CATEGORIES,
                )
            elif strong_special_ctx and ext in VIDEO_EXTS:
                fallback_label = extract_strong_extra_fallback_label(src_path.name)
                if fallback_label:
                    parse_result = parse_result._replace(
                        extra_category="bdextra",
                        extra_label=fallback_label,
                        is_special=False,
                        episode=None,
                    )
                else:
                    continue
            elif media_type == "tv":
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
                # !! 弱季号提示：有 resolved_season 时优先使用，不信任 bang 派生的季号
                if getattr(parse_result, "season_hint_strength", None) == "bang" and resolved_season:
                    append_log(
                        f"INFO: [season] bang 候选覆盖: file={src_path.name!r}, "
                        f"candidate={parse_result.season} → resolved={resolved_season} (source=resolved_season)"
                    )
                    season = resolved_season
                else:
                    season = parse_result.season or resolved_season or 1
            parse_result = parse_result._replace(season=season)
        parse_result = _apply_parallel_variant_suffix(parse_result, src_path)
        prefix = _resolve_prefix_for_parse(parse_result)
        if not prefix:
            continue
        if parse_result.extra_category not in ALL_EXTRA_LIKE_CATEGORIES:
            continue
        preferred = _preferred_index_for_extra(parse_result)
        item = {
            "file_path": str(src_path),
            "source_dir": str(src_path.parent),
            "sync_group_id": context.get("sync_group_id"),
            "tmdbid": context.get("tmdb_id"),
            "season_key": parse_result.season if media_type == "tv" else None,
            "episode": parse_result.episode,
            "extra_category": parse_result.extra_category,
            "file_type": "attachment" if ext in ATTACHMENT_EXTS else ("special" if parse_result.extra_category in SPECIAL_CATEGORIES else "extra"),
            "prefix": prefix,
            "preferred": preferred,
            "final_label_seed": parse_result.extra_label,
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


def _preferred_index_for_extra(parse_result: ParseResult) -> int | None:
    if parse_result.extra_category == "making":
        scene_idx = _extract_scene_index(parse_result.extra_label)
        if scene_idx is not None:
            return scene_idx
    return _extract_label_index(parse_result.extra_label) or parse_result.episode


def _extract_scene_index(label: str | None) -> int | None:
    s = str(label or "")
    if not s:
        return None
    m = re.search(r"\bScene\s*[-_ ]*0*(\d{1,3})(?:[A-Za-z]{1,8})?\b", s, re.I)
    if not m:
        return None
    val = int(m.group(1))
    return val if 1 <= val <= 999 else None


def _extract_stable_label_index(label: str | None) -> int | None:
    s = re.sub(r"\s+", " ", str(label or "")).strip()
    if not s:
        return None
    patterns = [
        r"\b(?:SP|OVA|OAD|OAV|SPECIAL)\s*0*(\d{1,3})\b",
        r"\b(?:OP|ED|NCOP|NCED)\s*0*(\d{1,3})\b",
        r"\b(?:PV|CM|Preview|Trailer|Teaser|CharacterPV|WebPreview|IV|MV|Interview|Making|BDExtra)\s*0*(\d{1,3})\b",
    ]
    for p in patterns:
        m = re.search(p, s, re.I)
        if not m:
            continue
        val = int(m.group(1))
        if 1 <= val <= 999:
            return val
    return None


def _extract_parallel_variant_suffix(text: str) -> str | None:
    s = str(text or "")
    if not s:
        return None
    patterns = [
        (r"\bNC\s*Ver\.?\b", "NC Ver"),
        (r"\bOn\s*Air\s*Ver\.?\b", "On Air Ver"),
        (r"\bOriginal\s*Staff\s*Credit\s*Ver\.?\b", "Original Staff Credit Ver"),
        (r"\b([A-Za-z][A-Za-z0-9]{1,24})\s*[-_ ]hen\b", r"\1-hen"),
        (r"\bMovie\s*([1-9])(?:\s*/\s*([1-9]))?\b", None),
        (r"\bPart\s*([1-9])(?:\s*/\s*([1-9]))?\b", None),
    ]
    for pattern, template in patterns:
        m = re.search(pattern, s, re.I)
        if not m:
            continue
        if template:
            if r"\1" in template and m.lastindex and m.lastindex >= 1:
                return re.sub(r"\s+", " ", template.replace(r"\1", m.group(1))).strip()
            return template
        left = m.group(1) if m.lastindex and m.lastindex >= 1 else ""
        right = m.group(2) if m.lastindex and m.lastindex >= 2 else ""
        if right:
            return f"Movie {left}/{right}"
        return f"Movie {left}"
    return None


def _extract_distinguish_source_tags(text: str) -> list[str]:
    s = str(text or "")
    if not s:
        return []
    patterns = [
        (r"\bNC(?:OP|ED)?(?=\d|\b)", "NC Ver"),
        (r"\bOn\s*Air\s*Ver\.?\b", "On Air Ver"),
        (r"\bTrue\s*Birth\s*Edition\b", "True Birth Edition"),
        (r"\b([A-Za-z][A-Za-z0-9]{1,24})\s*[-_ ]hen\b", r"\1-hen"),
        (r"\bEX\s*Season\s*0*(\d{1,2})\b", r"EX Season \1"),
        (r"\bOriginal\s*Staff\s*Credit\s*Ver\.?\b", "Original Staff Credit Ver"),
        (r"\b(?:ver\.?\s*\d+|v\d+)\b", None),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for pattern, template in patterns:
        for m in re.finditer(pattern, s, re.I):
            if template:
                tag = template
                if r"\1" in template and m.lastindex and m.lastindex >= 1:
                    tag = template.replace(r"\1", m.group(1))
                tag = re.sub(r"\s+", " ", tag).strip()
            else:
                tag = re.sub(r"\s+", " ", str(m.group(0) or "")).strip().rstrip(".")
            if not tag:
                continue
            key = tag.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(tag)
    return out[:6]


def _merge_distinguish_tags_into_label(label: str | None, tags: list[str], max_len: int = 120) -> tuple[str | None, list[str], list[str]]:
    base = re.sub(r"\s+", " ", str(label or "")).strip()
    if not tags:
        return (base or None), [], []
    lower_base = base.lower()
    merged: list[str] = []
    dropped: list[str] = []
    for tag in tags:
        token = re.sub(r"\s+", " ", str(tag or "")).strip()
        if not token:
            continue
        if lower_base and token.lower() in lower_base:
            continue
        candidate = f"{base} {token}".strip() if base else token
        if len(candidate) > max_len:
            dropped.append(token)
            continue
        base = candidate
        lower_base = base.lower()
        merged.append(token)
    return (base or None), merged, dropped


def _format_conflict_diff_suffix(merged_tags: list[str] | None = None, dropped_diffs: list[str] | None = None) -> str:
    merged = [x for x in (merged_tags or []) if x]
    dropped = [x for x in (dropped_diffs or []) if x]
    if not merged and not dropped:
        return ""
    parts: list[str] = []
    if merged:
        parts.append(f"merged_tags={','.join(merged)}")
    if dropped:
        parts.append(f"dropped_diffs={','.join(dropped)}")
    return " | " + " | ".join(parts)


def _detect_bracket_variant_as_special(filename: str) -> tuple[str | None, str | None, bool]:
    """检测文件名括号中的 variant 关键词，若存在则返回 (category, label, True)。

    用于将正片文件中含 variant 括号（如 [Preview01_1] / [Mystery Camp] / [On Air Ver.]）
    的文件提前降级为特典，避免其先于正片占据主视频目标槽位。
    仅在文件有集号（正片解析成功）且括号内含明确 variant 关键词时才触发。
    """
    bracket_tokens = re.findall(r"\[([^\]]+)\]|\(([^)]+)\)", filename or "")
    for groups in bracket_tokens:
        raw = next((g for g in groups if g), "")
        if not raw:
            continue
        # 排除纯技术标签（编码、分辨率、音频格式等）
        _noise_re = re.compile(
            r"^(?:\d{3,4}p|x26[45]|h26[45]|hevc|av1|ma10p|hi10p|yuv\d+p?\d*|"
            r"flac(?:x\d+)?|aac|ac3|dts|ddp\d?\.?\d?|raw|vcb(?:-?studio)?|mawen\d*|mysilu|"
            r"jpsc|jptc|chs|cht|sc|tc|gb|big5|bd|dvd|webrip|web[-\s]?dl|bdrip|bluray|remux)+$",
            re.I,
        )
        if _noise_re.match(raw.strip()):
            continue

        # Preview 类：Preview01_1 / Preview01 / Preview
        m = re.search(r"\bPreview\s*0*(\d*)", raw, re.I)
        if m:
            idx = m.group(1)
            label = _format_prefix_number("Preview", int(idx)) if idx else "Preview"
            return "preview", label, True

        # On Air Ver
        if re.search(r"\bOn\s*Air\s*Ver\.?\b", raw, re.I):
            return "special", "On Air Ver", True

        # Staff Credit Ver / Musani Staff Credit Ver / Credit Ver
        if re.search(r"\b(?:\w+\s+)*Staff\s+Credit\s+Ver\.?\b|\bCredit\s+Ver\.?\b", raw, re.I):
            label = re.sub(r"\s+", " ", re.sub(r"\[[^\]]*\]|\([^)]*\)", "", raw)).strip()[:60] or "Credit Ver"
            return "special", label, True

        # Mystery Camp / 带「Camp」「Special」等词的括号内容（且不是纯数字/技术标签）
        if re.search(r"\b(?:Mystery\s+Camp|Camp\b)", raw, re.I):
            label = re.sub(r"\s+", " ", raw).strip()[:60]
            return "special", label, True

    return None, None, False


def _apply_parallel_variant_suffix(parse_result: ParseResult, src_path: Path) -> ParseResult:
    if parse_result.extra_category not in ALL_EXTRA_LIKE_CATEGORIES:
        return parse_result
    variant = _extract_parallel_variant_suffix(src_path.stem)
    if not variant:
        return parse_result
    label = str(parse_result.extra_label or "").strip()
    if not label:
        return parse_result._replace(extra_label=variant)
    if variant.lower() in label.lower():
        return parse_result
    return parse_result._replace(extra_label=f"{label} {variant}")


def _needs_readable_suffix(parse_result: ParseResult) -> bool:
    if parse_result.extra_category not in ALL_EXTRA_LIKE_CATEGORIES:
        return False
    stable_idx = _extract_stable_label_index(parse_result.extra_label)
    if stable_idx is not None:
        return False
    if parse_result.episode is None:
        return True
    return int(parse_result.episode) == 1


def _normalize_suffix_text(text: str) -> str:
    s = str(text or "")
    s = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", s)
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(
        r"\b(?:2160p|1080p|720p|480p|4k|x264|x265|h264|h265|hevc|av1|10bit|8bit|ma10p|hi10p|"
        r"flac|aac|ac3|dts|ddp\d?\.?\d?|dvd|pgs|chap|ch(?:s|t)|webrip|web[-\s]?dl|bdrip|bluray|remux|bdmv|"
        r"jpsc|jptc|chs|cht|sc|tc|gb|big5|raw|vcb(?:-?studio)?|mawen\d*|mysilu|yuv\d+p?\d*)\b",
        " ",
        s,
        flags=re.I,
    )
    s = re.sub(r"\b(?:s\d{1,2}e\d{1,3}|\d{1,2}x\d{1,3}|ep?\s*\d{1,3}|e\d{1,3}|sp\d{0,3}|ova\d*)\b", " ", s, flags=re.I)
    s = re.sub(r"[^\w\u4e00-\u9fff\- ]+", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip(" -_")
    return s


def _build_readable_suffix(src_path: Path, parse_result: ParseResult) -> str:
    raw = re.sub(r"\s*-\s*(?:mawen\d*|vcb(?:-?studio)?|mysilu)\s*$", "", src_path.stem, flags=re.I)
    raw = re.sub(r"\[[^\]]*\]", " ", raw)
    candidates = re.split(r"\s*-\s*", raw)
    title_norm = _normalize_suffix_text(parse_result.title).lower()

    for segment in candidates:
        cleaned = _normalize_suffix_text(segment)
        if not cleaned:
            continue
        if title_norm and cleaned.lower() == title_norm:
            continue
        if re.fullmatch(r"\d+", cleaned):
            continue
        has_alpha_or_cjk = re.search(r"[A-Za-z\u4e00-\u9fff]", cleaned) is not None
        if not has_alpha_or_cjk:
            continue
        if len(cleaned) < 3 and not re.search(r"[\u4e00-\u9fff]", cleaned):
            continue
        return cleaned[:28]

    cleaned_all = _normalize_suffix_text(raw)
    if cleaned_all and not re.fullmatch(r"\d+", cleaned_all):
        return cleaned_all[:28]
    return f"h{hashlib.md5(str(src_path).encode('utf-8')).hexdigest()[:6]}"


def _ensure_unique_suffix_label(
    base_label: str,
    src_path: Path,
    parse_result: ParseResult,
    dir_runtime: dict | None,
) -> str:
    if dir_runtime is None:
        return base_label
    registry = dir_runtime.setdefault("suffix_label_registry", {})
    parent_key = str(src_path.parent)
    category_key = str(parse_result.extra_category or "extra")
    key = (parent_key, category_key, base_label.lower())
    idx = int(registry.get(key, 0)) + 1
    registry[key] = idx
    if idx == 1:
        return base_label
    candidate = f"{base_label} #{idx:02d}"
    if len(candidate) > 80:
        candidate = candidate[:80].rstrip()
    return candidate


def _apply_readable_suffix_for_unnumbered_extra(
    parse_result: ParseResult,
    src_path: Path,
    dir_runtime: dict | None,
) -> ParseResult:
    if not _needs_readable_suffix(parse_result):
        return parse_result
    if parse_result.extra_category in SPECIAL_CATEGORIES:
        parse_result = _assign_sequential_episode_for_unnumbered(parse_result, src_path, dir_runtime)
    suffix = _build_readable_suffix(src_path, parse_result)
    if not suffix:
        suffix = f"h{hashlib.md5(src_path.name.encode('utf-8')).hexdigest()[:6]}"
    candidate = suffix
    candidate = _ensure_unique_suffix_label(candidate, src_path, parse_result, dir_runtime)
    return parse_result._replace(extra_label=candidate)


def _assign_sequential_episode_for_unnumbered(
    parse_result: ParseResult,
    src_path: Path,
    dir_runtime: dict | None,
) -> ParseResult:
    if dir_runtime is None:
        return parse_result
    stable_idx = _extract_stable_label_index(parse_result.extra_label)
    if stable_idx is not None:
        return parse_result._replace(episode=stable_idx)
    parent_key = str(src_path.parent)
    key = (parent_key, str(parse_result.extra_category or "special"))
    seq_map = dir_runtime.setdefault("unnumbered_special_seq", {})
    current = int(seq_map.get(key, 0)) + 1
    seq_map[key] = current
    return parse_result._replace(episode=current)


def _apply_special_indexing(
    parse_result: ParseResult,
    src_path: Path,
    dir_runtime: dict | None,
    force_next: bool = False,
) -> ParseResult:
    if dir_runtime is None:
        return parse_result
    category = parse_result.extra_category
    if category not in ALL_EXTRA_LIKE_CATEGORIES:
        return parse_result

    label = parse_result.extra_label or ""
    prefix = _special_label_prefix(category, label)
    used = dir_runtime.setdefault("special_used", {}).setdefault((category, prefix), set())
    idx = None if force_next else _extract_stable_label_index(label)

    version_suffix = ""
    m_ver = re.search(r"\b(?:ver\.?\s*\d+|v\d+)\b", label, re.I)
    if m_ver:
        version_suffix = m_ver.group(0).strip()

    if idx is None or force_next:
        next_idx = 1
        while next_idx in used:
            next_idx += 1
        if force_next:
            append_log(f"INFO: 特典重映射: {prefix}{next_idx:02d} ← {src_path.name}")
        idx = next_idx

    used.add(idx)
    new_label = _compose_indexed_extra_label(prefix, idx, label)
    if version_suffix and version_suffix.lower() not in new_label.lower():
        new_label = f"{new_label} {version_suffix}"
    updated = parse_result._replace(extra_label=new_label)
    if category in SPECIAL_CATEGORIES:
        updated = updated._replace(episode=idx)
    return updated


def _compose_indexed_extra_label(prefix: str, idx: int, seed_label: str | None) -> str:
    base = _format_prefix_number(prefix, idx)
    suffix = _extract_preserved_extra_suffix(seed_label, prefix)
    if not suffix:
        return base
    return f"{base} {suffix}"


def _extract_preserved_extra_suffix(label: str | None, prefix: str) -> str:
    s = re.sub(r"\s+", " ", str(label or "")).strip()
    if not s:
        return ""
    s = re.sub(rf"^\s*{re.escape(prefix)}\s*0*\d{{1,3}}\b", "", s, flags=re.I)
    s = re.sub(rf"^\s*{re.escape(prefix)}\b", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -_")
    return s


def _sanitize_special_log_label(label: str | None) -> str:
    s = re.sub(r"\s+", " ", str(label or "")).strip(" -_")
    if not s:
        return ""
    patterns = [
        r"\bnekomoe(?:\s+kissaten)?\b",
        r"\bvcb(?:\s*-\s*studio|\s+studio)?\b",
        r"\bmawen\d*\b",
        r"\bmysilu\b",
        r"\bairota\b",
        r"\bdmhy\b",
        r"\b(?:fansub|raws?|sub)\b",
        r"(?:字幕组|字幕社|压制组|搬运组)",
        r"\b(?:nc|on\s*air)\s*ver\.?\b",
    ]
    for pattern in patterns:
        s = re.sub(pattern, " ", s, flags=re.I)
    s = re.sub(r"\s*[&/|+]+\s*", " ", s)
    s = re.sub(r"\s+", " ", s).strip(" -_")
    return s


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
    clean_label = _sanitize_special_log_label(label)
    if clean_label:
        append_log(f"Special 识别: {clean_label} -> {display}")
    elif label:
        append_log(f"Special 识别: {display} -> {display}")
    else:
        append_log(f"Special 分类: {display}")


def _log_special_resolution(
    src_path: Path,
    parse_result: ParseResult,
    raw_label: str | None,
    remapped: bool,
) -> None:
    category = parse_result.extra_category
    if category is None:
        return
    final_label = _sanitize_special_log_label(parse_result.extra_label) or "-"
    raw = _sanitize_special_log_label(raw_label) or "-"
    index_text = str(parse_result.episode) if parse_result.episode is not None else "-"
    append_log(
        "INFO: special resolved: file=%s, raw_label=%s, category=%s, final_label=%s, index=%s, remap=%s"
        % (src_path.name, raw, category, final_label, index_text, str(bool(remapped)).lower())
    )


def _normalized_episode_key(parse_result: ParseResult, src_path: Path, filename: str) -> int | None:
    if parse_result.extra_category in SPECIAL_CATEGORIES:
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

    try:
        with httpx.Client(timeout=10) as client:
            cn = _fetch_tmdb_localized_title(client, url, media_type, "zh-CN")
            tw = _fetch_tmdb_localized_title(client, url, media_type, "zh-TW")
        chosen, lang = _pick_display_title(cn, tw, fallback_title)
        append_log(f"INFO: TMDB 标题已解析: 语言={lang}, 标题={chosen or ''}")
        if chosen:
            return chosen
        return None if strict else fallback_title
    except Exception:
        return None if strict else fallback_title


def _fetch_tmdb_localized_title(client: httpx.Client, url: str, media_type: str, language: str) -> str | None:
    params = {"api_key": settings.tmdb_api_key, "language": language}
    resp = client.get(url, params=params)
    if resp.status_code != 200:
        return None
    data = resp.json() if resp.content else {}
    if not isinstance(data, dict):
        return None
    if media_type == "tv":
        value = data.get("name")
    else:
        value = data.get("title")
    title = str(value or "").strip()
    return title or None


def _has_cjk(text: str) -> bool:
    return re.search(r"[\u4e00-\u9fff]", str(text or "")) is not None


def _looks_like_bad_localized_title(text: str) -> bool:
    s = re.sub(r"\s+", " ", str(text or "")).strip()
    if not s:
        return True
    if not _has_cjk(s):
        return True
    normalized = re.sub(r"[\W_]+", "", s, flags=re.U)
    if normalized and re.fullmatch(r"[A-Za-z0-9]+", normalized):
        return True
    return False


def _pick_display_title(cn: str | None, tw: str | None, fallback: str) -> tuple[str | None, str]:
    cn_val = str(cn or "").strip()
    tw_val = str(tw or "").strip()
    fallback_val = str(fallback or "").strip()
    if cn_val and not _looks_like_bad_localized_title(cn_val):
        return cn_val, "zh-CN"
    if tw_val and not _looks_like_bad_localized_title(tw_val):
        return tw_val, "zh-TW"
    if cn_val:
        return cn_val, "zh-CN(raw)"
    if tw_val:
        return tw_val, "zh-TW(raw)"
    return (fallback_val or None), "fallback"


def _is_final_season_title(name: str) -> bool:
    text = str(name or "")
    return re.search(r"\b(?:the\s+)?final\s+season(?:\s+part\s*\d+)?\b", text, re.I) is not None


def _rederive_season_from_context(
    media_dir: Path,
    context: dict,
    tmdb_seasons: list[dict] | None = None,
) -> int | None:
    """
    在 season_aware 失败后，从目录名/上下文中二次推导季号。
    优先级：
    1. 路径中的 Season 目录（最强，已由 _extract_season_from_path 处理但再确认一次）
    2. 目录名中的标准季号模式（SxxEyy / Season xx / 第N季 / 序数词 / 罗马数字）
    3. 序数词 season（second season / third season 等）
    4. 罗马数字 season（Season II / III 等）
    5. context 中的 season_hint（来自目录结构而非 bang 轻提示时）
    6. TMDB 季名称相似度匹配（调用 infer_season_from_tmdb_seasons）
    """
    dir_name = media_dir.name

    # 1. 路径中的 Season 目录
    from_path = _extract_season_from_path(media_dir)
    if from_path is not None:
        return from_path

    # 2. 目录名中的标准季号模式
    patterns = [
        r"\bS(\d{1,2})\b",
        r"\bSeason\s*(\d{1,2})\b",
        r"\b(\d{1,2})(?:st|nd|rd|th)\s*Season\b",
        r"第\s*(\d{1,2})\s*季",
    ]
    for p in patterns:
        m = re.search(p, dir_name, re.I)
        if m:
            val = int(m.group(1))
            if 1 <= val <= 20:
                return val

    # 3. 序数词 season（second season / third season 等）
    ordinal_map = {
        "first": 1, "second": 2, "third": 3, "fourth": 4,
        "fifth": 5, "sixth": 6, "seventh": 7, "eighth": 8,
        "ninth": 9, "tenth": 10,
    }
    m = re.search(
        r"\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\s+season\b",
        dir_name, re.I,
    )
    if m:
        return ordinal_map.get(m.group(1).lower())

    # 4. 罗马数字 season（Season II / Season III 等）
    roman_map = {
        "I": 1, "II": 2, "III": 3, "IV": 4, "V": 5,
        "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10,
    }
    m = re.search(r"\bSeason\s+(I|II|III|IV|V|VI|VII|VIII|IX|X)\b", dir_name, re.I)
    if m:
        token = m.group(1).upper()
        val = roman_map.get(token)
        if val:
            return val

    # 5. context 中的 season_hint
    # bang 候选需经 TMDB 有效季验证；其他来源直接采用
    hint = context.get("season_hint")
    if isinstance(hint, int) and hint > 0:
        if context.get("bang_season_hint") or context.get("season_hint_source") == "bang":
            # bang 候选：只有在 TMDB 有效季中才接受，否则继续到 TMDB 名称推导
            _bang_in_tmdb = (
                tmdb_seasons
                and any(
                    int(s.get("season_number", 0)) == hint
                    for s in tmdb_seasons
                    if s.get("season_number") is not None
                )
            )
            if _bang_in_tmdb:
                append_log(f"INFO: bang 候选 season={hint} 在 TMDB 有效季中，采用（来源=bang）")
                return hint
            append_log(f"INFO: bang 候选 season={hint} 不在 TMDB 有效季中，跳过（继续 TMDB 名称推导）")
        else:
            return hint

    # 6. TMDB 季名称相似度匹配
    if tmdb_seasons:
        result = infer_season_from_tmdb_seasons(dir_name, tmdb_seasons)
        if result is not None:
            inferred_season, ratio = result
            append_log(
                f"INFO: TMDB季名匹配推导: season={inferred_season}, ratio={ratio:.2f}, dir={dir_name}"
            )
            return inferred_season

    return None


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
    season_hint_confidence = str(context.get("season_hint_confidence") or "").strip().lower() or None
    season_hint_source = str(context.get("season_hint_source") or "").strip() or None
    season_aware_done = bool(context.get("season_aware_done"))
    season_aware_had_candidates = bool(context.get("season_aware_had_candidates"))
    if isinstance(season_hint, int) and season_hint <= 0:
        season_hint = None

    try:
        details = get_tmdb_tv_details_sync(int(tmdb_id))
    except Exception:
        details = None
    if not details:
        # TMDB API 失败时优雅降级：利用已有 season_hint，而屏弃硬失败
        fallback_chosen = season_from_path or season_hint or 1
        append_log(
            f"WARNING: 无法获取 TMDB 季信息 (tmdbid={tmdb_id})，降级使用 season={fallback_chosen}"
        )
        context["season_hint"] = fallback_chosen
        context["resolved_season"] = fallback_chosen
        context["final_resolved_season"] = None
        return True, None

    valid_seasons = sorted(
        {
            int(x.get("season_number"))
            for x in details.get("seasons", [])
            if x.get("season_number") is not None and int(x.get("season_number")) > 0
        }
    )

    # 存入 context，供后续 AI 映射调用使用
    context["_tmdb_series_details"] = details

    chosen = season_from_path or season_hint
    is_final = _is_final_season_title(media_dir.name) or bool(context.get("final_hint"))

    tmdb_season_list = details.get("seasons", []) if isinstance(details, dict) else []

    # OVA/-hen 目录早期检测：带 OVA/OAD/-hen 后缀的目录通常对应 Season 0
    # 仅在无明确季号来源且 TMDB 存在 Season 0 时生效
    _has_tmdb_season0 = any(
        int(s.get("season_number", -1)) == 0
        for s in tmdb_season_list
        if s.get("season_number") is not None
    )
    _ova_dir_pattern = re.compile(
        r"(?:\b(?:OVA|OAD)\b|[-_\s]hen\b)",
        re.I,
    )
    if (
        _has_tmdb_season0
        and season_from_path is None
        and season_hint_source not in ("explicit", "final")
        and season_hint_confidence != "high"
        and _ova_dir_pattern.search(media_dir.name)
    ):
        chosen = 0
        append_log(
            f"INFO: 目录名含 OVA/OAD/-hen 关键词且 TMDB 存在 Season 0，映射 season=0: dir={media_dir.name!r}"
        )

    append_log(
        f"INFO: [season稳定] 开始: dir={media_dir.name!r}, "
        f"candidate={chosen}, source={season_hint_source!r}, "
        f"confidence={season_hint_confidence!r}, valid_seasons={valid_seasons}"
    )

    # 无稳定季号时：先尝试 TMDB 季名称匹配推导，否则默认第一季
    if chosen is None and len(valid_seasons) > 1 and not is_final:
        inferred = infer_season_from_tmdb_seasons(media_dir.name, tmdb_season_list)
        if inferred is not None:
            chosen = inferred[0]
            append_log(
                f"INFO: TMDB季名匹配推导季号: season={chosen}, ratio={inferred[1]:.2f}"
            )
        else:
            append_log("INFO: 多季作品且无稳定季号信息，默认使用第一季")
            chosen = 1

    # 「季名先行匹配」：无论是否有 chosen，若季号来源不可靠（非路径/显式指定/高置信度），
    # 都先用 TMDB 季名相似度匹配一次，有更优答案时覆盖 chosen。
    # 这解决了 Kakegurui×× / Tokyo Ghoul √A 等全部默认 Season 1 的问题。
    # chosen=0（已被 OVA/-hen 检测设置）不参与季名匹配覆盖。
    if chosen != 0 and not is_final and season_from_path is None and season_hint_source not in ("explicit", "final") and season_hint_confidence != "high":
        inferred_early = infer_season_from_tmdb_seasons(media_dir.name, tmdb_season_list)
        if inferred_early is not None:
            inferred_num, inferred_ratio = inferred_early
            # 只有当匹配到的季号与当前 chosen 不同，且置信度足够高时才覆盖
            if inferred_num != chosen and inferred_ratio >= 0.62:
                append_log(
                    f"INFO: 季名先行匹配: season={inferred_num} ratio={inferred_ratio:.2f} "
                    f"覆盖原 candidate={chosen}（来源={season_hint_source}）"
                )
                chosen = inferred_num

    if chosen is not None and valid_seasons and chosen not in valid_seasons:
        # Season 0（OVA/Specials）特判：valid_seasons 排除了 season_number<=0，
        # 但若 chosen=0 且 TMDB 确实存在 season 0，直接采用而不进入冲突流程。
        if chosen == 0 and _has_tmdb_season0:
            append_log(
                f"INFO: OVA 目录 season=0 在 TMDB 中存在，直接采用（跳过冲突流程）"
            )
            resolved = resolve_season_by_tmdb(None, int(tmdb_id), 0, final_hint=False)
            if resolved is None:
                resolved = 0
            context["season_hint"] = 0
            context["resolved_season"] = resolved
            context["final_resolved_season"] = None
            append_log(
                f"INFO: [season稳定] 完成: dir={media_dir.name!r}, candidate={season_hint} → resolved_season=0, source=ova_dir_detect"
            )
            return True, None
        append_log(
            f"INFO: 季号 {chosen} 在 TMDB 候选 tmdbid={tmdb_id} 中不存在，尝试 TMDB 季名匹配和重新搜索"
        )
        # 先尝试通过 TMDB 季名匹配推导新季号
        inferred_for_mismatch = infer_season_from_tmdb_seasons(media_dir.name, tmdb_season_list)
        if inferred_for_mismatch is not None and inferred_for_mismatch[0] != chosen:
            new_chosen = inferred_for_mismatch[0]
            append_log(
                f"INFO: TMDB季名匹配得到新季号 {new_chosen}（原 chosen={chosen}）, ratio={inferred_for_mismatch[1]:.2f}"
            )
            if new_chosen in valid_seasons:
                chosen = new_chosen
                append_log(f"INFO: 季名匹配有效，直接使用 season={chosen}")
                resolved = resolve_season_by_tmdb(None, int(tmdb_id), chosen, final_hint=is_final)
                if resolved is None:
                    return False, "Final Season 解析失败或 TMDB 不可用"
                context["season_hint"] = chosen
                context["resolved_season"] = resolved
                context["final_resolved_season"] = resolved if is_final else None
                return True, None
        reliable_hint = (
            season_from_path is not None
            or season_hint_source in ("explicit", "final")
            or season_hint_confidence == "high"
        )
        # K-ON!! 类情形：季号仅来自标题 !! 符号，属于弱提示，不触发 re-search
        if reliable_hint and season_from_path is None:
            _bang_val = extract_bang_season(context.get("title") or "")
            if _bang_val is not None and season_hint is not None and season_hint == _bang_val:
                reliable_hint = False
                context["bang_season_hint"] = True
                context.setdefault("season_hint_source", "title_punctuation")
                append_log(
                    f"INFO: season_hint={season_hint} 来自标题 !! 轻提示，降为不可信，不触发 re-search"
                )
        if not reliable_hint:
            # bang 候选：若目标 TMDB 确实存在该季，可直接采用，不能无条件丢弃
            if (
                context.get("bang_season_hint") or season_hint_source == "bang"
            ) and chosen is not None and valid_seasons and chosen in valid_seasons:
                append_log(
                    f"INFO: bang 候选 season={chosen} 在 TMDB 有效季 {valid_seasons} 中，直接采用"
                )
                # chosen 保持不变，跳过 re-search 直接进入 resolve
            else:
                append_log(
                    f"INFO: 季号提示较弱或疑似标题数字，season={chosen} 不在 TMDB 有效季 {valid_seasons} 中，"
                    f"跳过重新搜索并回退到默认季号"
                )
                chosen = None
        elif season_aware_done and season_aware_had_candidates:
            # 软二次校验：不立即硬失败，先尝试从目录名/上下文再次推导季号
            rederived_season = _rederive_season_from_context(media_dir, context, tmdb_seasons=details.get("seasons", []))
            rederived_is_valid = False
            if rederived_season is not None and rederived_season != chosen:
                append_log(
                    f"INFO: season_aware_had_candidates=True 但二次推导得到不同季号 {rederived_season}，"
                    f"尝试以新季号继续（原 chosen={chosen}）"
                )
                chosen = rederived_season
                if chosen in valid_seasons:
                    # 新季号有效，跳过 re-search 直接进入后续解析
                    rederived_is_valid = True
                else:
                    # 新季号仍无效，再尝试 season-aware 重搜
                    append_log(f"INFO: 二次推导季号 {chosen} 仍不在 TMDB valid_seasons 中，尝试重新搜索")
            elif rederived_season is not None and rederived_season == chosen:
                # 二次推导和原来一致，说明来源可信但 TMDB 没有这一季
                # 仍然尝试 season-aware re-search，不直接失败
                append_log(
                    f"INFO: season_aware_had_candidates=True, 二次推导季号一致 chosen={chosen}，尝试重新搜索"
                )
            else:
                # 无法从上下文推导出有效季号，保守处理：回退到默认季号，不进入 re-search
                append_log(
                    f"INFO: season_aware_had_candidates=True 但无法二次推导季号，回退到默认季号（原 chosen={chosen}）"
                )
                chosen = None
            if rederived_is_valid:
                resolved = resolve_season_by_tmdb(None, int(tmdb_id), chosen, final_hint=is_final)
                if resolved is None:
                    return False, "Final Season 解析失败或 TMDB 不可用"
                context["season_hint"] = chosen
                context["resolved_season"] = resolved
                context["final_resolved_season"] = resolved if is_final else None
                return True, None
        elif season_aware_done and not season_aware_had_candidates:
            append_log("INFO: 识别阶段季号感知无候选结果，允许在稳定阶段再次搜索")
        if chosen is None:
            resolved = resolve_season_by_tmdb(None, int(tmdb_id), chosen, final_hint=is_final)
            if resolved is None:
                return False, "Final Season 解析失败或 TMDB 不可用"
            context["season_hint"] = chosen
            context["resolved_season"] = resolved
            context["final_resolved_season"] = resolved if is_final else None
            return True, None
        if not bool(getattr(settings, "season_aware_research_enabled", True)):
            return False, f"季号不匹配 TMDB: season_hint={chosen}"

        best, tried_queries, had_candidates = recognize_directory_with_season_hint_trace(
            media_dir,
            "tv",
            chosen,
            structure_hint="tv",
        )
        context["season_aware_done"] = True
        context["season_aware_had_candidates"] = had_candidates
        context["season_aware_tried_queries"] = tried_queries
        if tried_queries:
            append_log(f"INFO: 已尝试重新搜索的查询: {tried_queries}")
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
    append_log(
        f"INFO: [season稳定] 完成: dir={media_dir.name!r}, "
        f"candidate={chosen} → resolved_season={resolved}, source={context.get('season_hint_source')!r}"
    )
    return True, None
