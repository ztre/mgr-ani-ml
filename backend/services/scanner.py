"""扫描任务编排（v2）：目录驱动、统一识别、事务执行与回滚。"""
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock

import httpx
from sqlalchemy.orm import Session

from ..api.logs import append_log, current_task_id
from ..config import settings
from ..models import DirectoryState, InodeRecord, MediaRecord, ScanTask, SyncGroup
from .emby import refresh_emby_library
from .group_routing import resolve_movie_target_root
from .linker import get_inode, is_same_inode, path_excluded
from .metadata import scrape_movie_metadata, scrape_tv_metadata
from .parser import ParseResult, get_tmdb_tv_details_sync, parse_movie_filename, parse_tv_filename
from .recognition_flow import recognize_directory_with_fallback
from .renamer import compute_movie_target_path, compute_tv_target_path

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".webm", ".flv"}
ATTACHMENT_EXTS = {".ass", ".srt", ".ssa", ".vtt", ".mka"}
MEDIA_EXTS = VIDEO_EXTS | ATTACHMENT_EXTS
INTERVIEW_PATTERN = re.compile(r"\b(IV\d*|Interview|Interviews)\b", re.I)
TRAILER_PATTERN = re.compile(
    r"\b(Preview\d*|Web\s*Preview\d*|CM\d*|SPOT\d*|PV\d*|Trailer\d*|Teaser\d*)\b",
    re.I,
)
SCAN_LOCK = Lock()


@dataclass
class OperationLog:
    created_dirs: set[Path] = field(default_factory=set)
    created_links: set[Path] = field(default_factory=set)
    created_files: set[Path] = field(default_factory=set)


class DirectoryProcessError(Exception):
    pass


def tag_task_type_with_issue(task_type: str) -> str:
    base = str(task_type or "").strip() or "scan"
    return base if base.endswith(":issue") else f"{base}:issue"


def run_scan(
    db: Session,
    group_id: int | None = None,
    task_type_override: str | None = None,
    target_name_override: str | None = None,
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
            for group in groups:
                append_log(f"处理同步组: {group.name}")
                group_first_dir, group_has_issues = _process_sync_group(db, group)
                if first_scanned_dir_name is None and group_first_dir:
                    first_scanned_dir_name = group_first_dir
                if group_has_issues:
                    has_issues = True

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


def _process_sync_group(db: Session, group: SyncGroup) -> tuple[str | None, bool]:
    """处理单个同步组。"""
    source = Path(group.source)
    tv_target_root = Path(group.target)
    movie_target_root, movie_route_reason = resolve_movie_target_root(db, group)
    if movie_route_reason:
        append_log(f"TV转移电影目标路径路由: {movie_route_reason}: {movie_target_root}")

    include = group.include or ""
    exclude = group.exclude or ""

    if not source.exists():
        append_log(f"源目录不存在: {source}")
        return None, False

    media_dirs = _collect_video_leaf_dirs(source, include, exclude)
    append_log(f"找到 {len(media_dirs)} 个待处理目录")
    first_dir_name = _extract_first_dir_name(media_dirs, source)
    has_issues = False

    blocked_dirs: set[Path] = _load_existing_recorded_dirs(db, group.id, source)

    for media_dir in media_dirs:
        if _is_under_pending_dir(media_dir, blocked_dirs):
            append_log(f"跳过已在媒体记录/待办中的目录: {media_dir}")
            continue

        signature = _build_dir_signature(media_dir, source, include, exclude)
        if _can_skip_dir_by_signature(db, group.id, media_dir, signature):
            append_log(f"跳过未变化成功目录: {media_dir}")
            continue

        _upsert_dir_state(db, group.id, media_dir, signature, "SCANNED", None)

        best, snapshot, fallback_round = recognize_directory_with_fallback(media_dir, group.source_type)
        if best is None:
            reason = f"目录级识别失败或低置信度: {media_dir.name}"
            pending_dir = _mark_dir_pending(db, media_dir, source, group.id, group.source_type, reason)
            blocked_dirs.add(pending_dir)
            _upsert_dir_state(db, group.id, media_dir, signature, "LOW_CONFIDENCE", reason)
            db.commit()
            continue

        recognized_type = best.media_type
        target_type = recognized_type
        if target_type == "movie" and movie_target_root is None:
            reason = "识别为电影但未找到可用电影目标路径"
            pending_dir = _mark_dir_pending(db, media_dir, source, group.id, group.source_type, reason)
            blocked_dirs.add(pending_dir)
            _upsert_dir_state(db, group.id, media_dir, signature, "FAILED", reason)
            continue

        target_root = tv_target_root if target_type == "tv" else movie_target_root
        resolved_title = _resolve_chinese_title_by_tmdb(
            media_type=target_type,
            tmdb_id=best.tmdb_id,
            fallback_title=best.title or snapshot.main_title,
        )
        context = {
            "media_type": target_type,
            "tmdb_id": best.tmdb_id,
            "tmdb_data": best.tmdb_data,
            "title": resolved_title,
            "year": best.year or snapshot.year_hint,
            "target_root": str(target_root),
            "score": best.score,
            "fallback_round": fallback_round,
            "season_hint": snapshot.season_hint,
            "special_hint": snapshot.special_hint,
            "_has_issues": False,
        }

        stable_ok, stable_reason = _stabilize_directory_context(media_dir, context)
        if not stable_ok:
            reason = f"目录稳定决策失败: {stable_reason}"
            pending_dir = _mark_dir_pending(db, media_dir, source, group.id, group.source_type, reason)
            blocked_dirs.add(pending_dir)
            _upsert_dir_state(db, group.id, media_dir, signature, "LOW_CONFIDENCE", reason)
            db.commit()
            continue

        append_log(
            f"目录识别成功: {media_dir.name} -> {target_type}:{context['title']} "
            f"(score={best.score:.3f}, fallback_round={fallback_round})"
        )

        _upsert_dir_state(db, group.id, media_dir, signature, "IDENTIFIED", None)

        media_files = _collect_media_files_under_dir(media_dir, include, exclude, source)
        video_files = sorted([p for p in media_files if p.suffix.lower() in VIDEO_EXTS], key=lambda p: p.as_posix())
        attachment_files = sorted([p for p in media_files if p.suffix.lower() in ATTACHMENT_EXTS], key=lambda p: p.as_posix())

        op_log = OperationLog()
        seen_targets: dict[Path, Path] = {}
        dir_runtime: dict = {
            "video_anchor_by_parent": {},
            "video_anchor_by_parent_episode": {},
        }

        try:
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

            _upsert_dir_state(db, group.id, media_dir, signature, "SUCCESS", None)
            db.commit()
            if context.get("_has_issues"):
                has_issues = True
        except DirectoryProcessError as e:
            db.rollback()
            _rollback_operations(op_log)
            reason = str(e)
            pending_dir = _mark_dir_pending(db, media_dir, source, group.id, group.source_type, reason)
            blocked_dirs.add(pending_dir)
            _upsert_dir_state(db, group.id, media_dir, signature, "FAILED", reason)
            db.commit()

    return first_dir_name, has_issues


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

    # 附件跟随正片：优先复用同目录(同集)视频的目标路径。
    if is_attachment:
        anchor_dst = _resolve_attachment_follow_target(src_path, parse_result, dir_runtime)
        if anchor_dst is not None:
            dst_path = _build_attachment_target_from_anchor(anchor_dst, parse_result, ext)
            _execute_transactional_outputs(
                src_path=src_path,
                dst_path=dst_path,
                media_type=media_type,
                parse_result=parse_result,
                tmdb_data=tmdb_data,
                should_scrape=False,
                op_log=op_log,
            )
            _upsert_media_record(db, sync_group_id, src_path, dst_path, media_type, tmdb_id, status=record_status)
            _upsert_inode_record(db, sync_group_id, src_path, dst_path)
            append_log(f"附件跟随正片: {src_path.name} -> {dst_path}")
            return

    # Special folder guard:
    # - never enter movie normal flow
    # - never construct movie target path
    # - special classification first
    if is_under_special_folder(src_path):
        special_type = classify_special_file(src_path.name)
        if not special_type:
            append_log(f"Special 目录非白名单跳过: {src_path.name}")
            return

        if media_type == "movie":
            dst_path = _compute_movie_special_target_path(
                target_root=target_root,
                parse_result=parse_result,
                tmdb_id=tmdb_id,
                special_type=special_type,
                src_filename=src_path.name,
            )
            should_scrape = False
        else:
            season = _extract_season_from_path(src_path) or context.get("resolved_season") or context.get("season_hint") or 1
            if season <= 0:
                season = 1
            parse_result = parse_result._replace(season=season)
            dst_path = _compute_tv_special_target_path(
                target_root=target_root,
                parse_result=parse_result,
                tmdb_id=tmdb_id,
                season=season,
                special_type=special_type,
                src_filename=src_path.name,
            )
            should_scrape = (not is_attachment) and _should_scrape_for_target(media_type, dst_path, parse_result)

        # Special flow does not participate in dedup conflict decision.
        try:
            _execute_transactional_outputs(
                src_path=src_path,
                dst_path=dst_path,
                media_type=media_type,
                parse_result=parse_result,
                tmdb_data=tmdb_data,
                should_scrape=should_scrape,
                op_log=op_log,
            )
        except DirectoryProcessError as e:
            if "目标路径已被其他文件占用" in str(e):
                context["_has_issues"] = True
                append_log(f"Special 目录目标冲突已跳过: {src_path.name} -> {dst_path}")
                return
            raise
        _upsert_media_record(db, sync_group_id, src_path, dst_path, media_type, tmdb_id, status=record_status)
        _upsert_inode_record(db, sync_group_id, src_path, dst_path)
        if ext in VIDEO_EXTS:
            _register_video_anchor(src_path, dst_path, parse_result, dir_runtime)
        append_log(f"Special 目录处理成功: {src_path.name} -> {dst_path}")
        return

    if media_type == "tv" and _should_ignore_zero_episode(src_path.name):
        append_log(f"跳过第0集/00集文件: {src_path.name}")
        return

    if media_type == "tv":
        if parse_result.episode is None:
            ep = _extract_episode_from_filename_loose(src_path.name)
            if ep is not None:
                parse_result = parse_result._replace(episode=ep)

        resolved_season = context.get("resolved_season")
        season = _extract_season_from_path(src_path) or resolved_season or 1
        parse_result = parse_result._replace(season=season)

        dst_path = compute_tv_target_path(target_root, parse_result, tmdb_id, ext)
    else:
        dst_path = compute_movie_target_path(target_root, parse_result, tmdb_id, ext)

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

    append_log(f"处理成功: {src_path.name} -> {dst_path}")


def resolve_season(tmdb_id: int, season_hint: int | None, final_season: bool = False) -> int:
    details = get_tmdb_tv_details_sync(tmdb_id)
    if not details:
        raise ValueError("无法获取 TMDB 剧集详情")

    available = {
        int(s.get("season_number"))
        for s in details.get("seasons", [])
        if s.get("season_number") is not None and int(s.get("season_number")) > 0
    }

    if final_season and not season_hint and available:
        return max(available)

    if season_hint and season_hint in available:
        return season_hint

    if season_hint and season_hint not in available:
        append_log(f"WARNING: season_hint={season_hint} not in TMDB, fallback to 1")

    return 1


def classify_special_file(filename: str) -> str | None:
    name = Path(filename or "").stem
    if INTERVIEW_PATTERN.search(name):
        return "Interviews"
    if TRAILER_PATTERN.search(name):
        return "Trailers"
    return None


def _compute_tv_special_target_path(
    target_root: Path,
    parse_result: ParseResult,
    tmdb_id: int | None,
    season: int,
    special_type: str,
    src_filename: str,
) -> Path:
    title = _safe_name(parse_result.title)
    year_part = f" ({parse_result.year})" if parse_result.year else ""
    tmdb_part = f" [tmdbid={tmdb_id}]" if tmdb_id else ""
    show_dir = target_root / f"{title}{year_part}{tmdb_part}"
    filename = _build_special_renamed_filename(parse_result, special_type, src_filename)
    return show_dir / f"Season {season:02d}" / special_type / filename


def _compute_movie_special_target_path(
    target_root: Path,
    parse_result: ParseResult,
    tmdb_id: int | None,
    special_type: str,
    src_filename: str,
) -> Path:
    title = _safe_name(parse_result.title)
    year_part = f" ({parse_result.year})" if parse_result.year else ""
    tmdb_part = f" [tmdbid={tmdb_id}]" if tmdb_id else ""
    movie_dir = target_root / f"{title}{year_part}{tmdb_part}"
    filename = _build_special_renamed_filename(parse_result, special_type, src_filename)
    return movie_dir / special_type / filename


def _build_special_renamed_filename(parse_result: ParseResult, special_type: str, src_filename: str) -> str:
    src = Path(src_filename)
    ext = src.suffix
    title = _safe_name(parse_result.title)
    year_part = f" ({parse_result.year})" if parse_result.year else ""
    lang_part = parse_result.subtitle_lang or ""

    token = _extract_special_token_from_filename(src.stem)
    if token:
        base = f"{title}{year_part} - {token}"
    else:
        base = f"{title}{year_part}"
    return f"{base}{lang_part}{ext}"


def _extract_special_token_from_filename(stem: str) -> str | None:
    patterns = [
        r"\b(PV\s*EP?\s*\d+)\b",
        r"\b(PV[\s\-_]*\d+)\b",
        r"\b(CM[\s\-_]*\d+)\b",
        r"\b(SPOT[\s\-_]*\d+)\b",
        r"\b(Preview[\s\-_]*\d+)\b",
        r"\b(Web\s*Preview[\s\-_]*\d+)\b",
        r"\b(IV[\s\-_]*\d+)\b",
        r"\b(Trailer\d*)\b",
        r"\b(Teaser(?:\s*PV)?\d*)\b",
    ]
    for p in patterns:
        m = re.search(p, stem, re.I)
        if m:
            token = re.sub(r"[\-_]+", " ", m.group(1))
            token = re.sub(r"\s+", " ", token).strip()
            pv_ep = re.match(r"^PV\s*EP?\s*(\d+)$", token, re.I)
            if pv_ep:
                return f"PVEP{pv_ep.group(1)}"
            return token
    return None


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


def _is_specials_path(src_path: Path) -> bool:
    return any(part.lower() in {"sps", "specials"} for part in src_path.parts)


def is_under_special_folder(file_path: Path) -> bool:
    for parent in file_path.parents:
        name = parent.name.lower()
        if name in {"sps", "special", "specials", "extras", "sp"}:
            return True
    return False


def _extract_episode_from_filename_loose(filename: str) -> int | None:
    stem = Path(filename).stem
    patterns = [
        r"\[(\d{1,3})\]",
        r"\((\d{1,3})\)",
        r"\bEP?\s*(\d{1,3})\b",
        r"第\s*(\d{1,3})\s*[集话話]",
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
    parent_key = str(src_path.parent)
    by_parent[parent_key] = dst_path
    ep = parse_result.episode
    if ep is None:
        ep = _extract_episode_from_filename_loose(src_path.name)
    if ep is not None:
        by_parent_ep[(parent_key, int(ep))] = dst_path


def _resolve_attachment_follow_target(src_path: Path, parse_result: ParseResult, dir_runtime: dict | None) -> Path | None:
    if dir_runtime is None:
        return None
    by_parent = dir_runtime.get("video_anchor_by_parent", {})
    by_parent_ep = dir_runtime.get("video_anchor_by_parent_episode", {})
    parent_key = str(src_path.parent)

    ep = parse_result.episode
    if ep is None:
        ep = _extract_episode_from_filename_loose(src_path.name)
    if ep is not None:
        candidate = by_parent_ep.get((parent_key, int(ep)))
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


def _deduplicate_target_or_raise(seen_targets: dict[Path, Path], src_path: Path, dst_path: Path) -> None:
    prev = seen_targets.get(dst_path)
    if prev is None:
        seen_targets[dst_path] = src_path
        return
    if prev != src_path:
        raise DirectoryProcessError(f"多个源文件映射到同一目标: {prev.name} / {src_path.name}")


def _should_scrape_for_target(media_type: str, dst_path: Path, parse_result: ParseResult) -> bool:
    if media_type == "movie" and parse_result.extra_type and dst_path.parent.name.lower() == "extras":
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
                subdirs.append(entry)
                continue
            if not entry.is_file() or entry.suffix.lower() not in VIDEO_EXTS:
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
    append_log(f"转待办目录: {pending_dir} | 原因: {reason}")

    existing = db.query(MediaRecord).filter(
        MediaRecord.original_path == str(pending_dir),
        MediaRecord.sync_group_id == sync_group_id,
        MediaRecord.status == "pending_manual",
    ).first()
    if existing is None:
        db.add(
            MediaRecord(
                sync_group_id=sync_group_id,
                original_path=str(pending_dir),
                target_path=None,
                type=source_type,
                tmdb_id=None,
                bangumi_id=None,
                status="pending_manual",
                size=0,
            )
        )
    db.commit()
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
    state = db.query(DirectoryState).filter(
        DirectoryState.sync_group_id == sync_group_id,
        DirectoryState.dir_path == str(media_dir),
    ).first()
    if state is None:
        db.add(
            DirectoryState(
                sync_group_id=sync_group_id,
                dir_path=str(media_dir),
                signature=signature,
                status=status,
                last_error=last_error,
            )
        )
    else:
        state.signature = signature
        state.status = status
        state.last_error = last_error


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


def _safe_name(text: str) -> str:
    bad = '<>:"/\\|?*'
    s = "".join("_" if c in bad else c for c in (text or "Unknown"))
    return s.strip() or "Unknown"


def _resolve_chinese_title_by_tmdb(media_type: str, tmdb_id: int | None, fallback_title: str) -> str:
    if not tmdb_id or not settings.tmdb_api_key:
        return fallback_title

    endpoint = "tv" if media_type == "tv" else "movie"
    url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
    params = {"api_key": settings.tmdb_api_key, "language": "zh-CN"}

    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url, params=params)
        if resp.status_code != 200:
            return fallback_title
        data = resp.json() if resp.content else {}
        if not isinstance(data, dict):
            return fallback_title
        if media_type == "tv":
            title = str(data.get("name") or "").strip()
        else:
            title = str(data.get("title") or "").strip()
        return title or fallback_title
    except Exception:
        return fallback_title


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
    is_final = _is_final_season_title(media_dir.name)

    # 无目录/识别季号，且 TMDB 多季且不是 Final Season -> 不稳定，转待办
    if not chosen and len(valid_seasons) > 1 and not is_final:
        return False, "多季作品且目录缺少稳定季号信息"

    try:
        resolved = resolve_season(int(tmdb_id), chosen, final_season=is_final)
    except Exception as e:
        return False, f"SeasonResolver 失败: {e}"

    context["season_hint"] = chosen
    context["resolved_season"] = resolved
    return True, None
