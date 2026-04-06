from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
import queue
from threading import Lock, Thread
from typing import Callable

from ..api.logs import append_log, append_task_log, current_task_id
from ..database import SessionLocal, TaskSessionLocal
from ..models import ScanTask

TaskRunner = Callable[..., None]
logger = logging.getLogger(__name__)


@dataclass
class QueuedTaskJob:
    task_id: int
    runner: TaskRunner


TASK_QUEUE: queue.Queue[QueuedTaskJob] = queue.Queue()
_TASK_QUEUE_WORKER: Thread | None = None
_TASK_QUEUE_LOCK = Lock()
_TASK_QUEUE_RECOVERY_DONE = False


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _is_task_error_already_logged(exc: Exception) -> bool:
    return bool(getattr(exc, "_task_log_handled", False))


def _extract_task_error_detail(exc: Exception) -> str:
    detail = getattr(exc, "detail", None)
    if detail is not None:
        text = str(detail).strip()
        if text:
            return text
    text = str(exc).strip()
    if text:
        return text
    return exc.__class__.__name__


def _mark_stale_tasks() -> None:
    global _TASK_QUEUE_RECOVERY_DONE
    if _TASK_QUEUE_RECOVERY_DONE:
        return

    with _TASK_QUEUE_LOCK:
        if _TASK_QUEUE_RECOVERY_DONE:
            return

        db = TaskSessionLocal()
        try:
            stale_tasks = db.query(ScanTask).filter(ScanTask.status.in_(("queued", "running", "cancelling"))).all()
            if stale_tasks:
                finished_at = _now_utc()
                for task in stale_tasks:
                    previous_status = str(task.status or "")
                    task.status = "cancelled" if previous_status == "queued" else "failed"
                    task.finished_at = finished_at
                    task.log_file = f"task_{task.id}.log"
                    append_task_log(task.id, f"服务重启，未完成任务已终止（原状态: {previous_status or '-'}）")
                db.commit()
        finally:
            db.close()
            _TASK_QUEUE_RECOVERY_DONE = True


def _execute_job(job: QueuedTaskJob) -> None:
    task_db = TaskSessionLocal()
    worker_db = SessionLocal()
    token = None
    try:
        task = task_db.query(ScanTask).filter(ScanTask.id == job.task_id).first()
        if not task:
            return

        if task.status == "cancelled":
            if task.finished_at is None:
                task.finished_at = _now_utc()
                task.log_file = f"task_{task.id}.log"
                task_db.commit()
            return

        task.status = "running"
        task.log_file = f"task_{task.id}.log"
        task_db.commit()

        token = current_task_id.set(task.id)
        append_log("任务开始执行，前序任务已完成")
        job.runner(worker_db, task)

        if task.status in {"queued", "running", "cancelling"}:
            task.status = "completed"
        if task.finished_at is None:
            task.finished_at = _now_utc()
        task.log_file = f"task_{task.id}.log"
        task_db.commit()
    except Exception as exc:
        worker_db.rollback()
        task_db.rollback()
        logger.exception("Task %s failed", job.task_id)
        task = task_db.query(ScanTask).filter(ScanTask.id == job.task_id).first()
        if task is not None:
            if token is None:
                token = current_task_id.set(task.id)
            if not _is_task_error_already_logged(exc):
                append_log(f"任务异常: {_extract_task_error_detail(exc)}")
            task.status = "failed"
            task.finished_at = _now_utc()
            task.log_file = f"task_{task.id}.log"
            try:
                task_db.commit()
            except Exception:
                task_db.rollback()
    finally:
        if token is not None:
            current_task_id.reset(token)
        worker_db.close()
        task_db.close()


def _task_queue_worker() -> None:
    while True:
        job = TASK_QUEUE.get()
        try:
            _execute_job(job)
        finally:
            TASK_QUEUE.task_done()


def ensure_task_queue_worker() -> None:
    global _TASK_QUEUE_WORKER
    _mark_stale_tasks()

    with _TASK_QUEUE_LOCK:
        if _TASK_QUEUE_WORKER and _TASK_QUEUE_WORKER.is_alive():
            return
        _TASK_QUEUE_WORKER = Thread(target=_task_queue_worker, name="task-queue-worker", daemon=True)
        _TASK_QUEUE_WORKER.start()


def enqueue_task(
    db,
    *,
    task_type: str,
    target_id: int | None,
    target_name: str | None,
    runner: TaskRunner,
    queued_message: str | None = None,
) -> ScanTask:
    ensure_task_queue_worker()

    task_db = TaskSessionLocal()
    task = ScanTask(
        type=task_type,
        target_id=target_id,
        target_name=target_name,
        status="queued",
    )
    try:
        task_db.add(task)
        task_db.commit()
        task_db.refresh(task)
        task.log_file = f"task_{task.id}.log"
        task_db.commit()
        task_db.refresh(task)

        append_task_log(task.id, queued_message or "任务已进入队列，等待执行")
        TASK_QUEUE.put(QueuedTaskJob(task_id=task.id, runner=runner))
        task_db.expunge(task)
        return task
    finally:
        task_db.close()