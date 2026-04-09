"""Resolve movie target path for TV->Movie fallback routing."""
from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy.orm import Session

from ..config import settings
from ..models import SyncGroup


def resolve_tv_target_root(db: Session, current_group: SyncGroup, source_path: Path | None = None) -> tuple[Path | None, str]:
    """Resolve the TV target root for a given group, mirroring resolve_movie_target_root.

    For tv-type groups returns group.target directly.
    For movie-type groups looks for the best TV group using the same matching strategy.
    """
    if current_group.source_type == "tv":
        return Path(current_group.target), "当前同步组是 tv，直接使用本组 target"

    tv_groups = db.query(SyncGroup).filter(SyncGroup.enabled == True, SyncGroup.source_type == "tv").all()
    if not tv_groups:
        return None, "未找到启用的 tv 同步组"

    targets = sorted({g.target for g in tv_groups if g.target})
    if len(targets) == 1:
        return Path(targets[0]), "唯一 tv target"

    strategy = (settings.movie_fallback_strategy or "auto").strip().lower()

    if strategy in {"prefer_name_match", "auto"}:
        by_name = _pick_by_name(current_group, tv_groups)
        if by_name:
            return Path(by_name.target), f"名称匹配命中: {by_name.name}"

    if strategy in {"prefer_source_prefix", "auto"}:
        by_prefix = _pick_by_prefix(current_group, tv_groups, source_path)
        if by_prefix:
            return Path(by_prefix.target), f"source 前缀命中: {by_prefix.name}"

    if strategy == "unique_only":
        return None, "unique_only 策略下存在多个 tv target"

    return None, f"无法在多个 tv target({len(targets)}) 中唯一决策"


def resolve_movie_target_root(db: Session, current_group: SyncGroup, source_path: Path | None = None) -> tuple[Path | None, str]:
    if current_group.source_type == "movie":
        return Path(current_group.target), "当前同步组是 movie，直接使用本组 target"

    movie_groups = db.query(SyncGroup).filter(SyncGroup.enabled == True, SyncGroup.source_type == "movie").all()
    if not movie_groups:
        return None, "未找到启用的 movie 同步组"

    targets = sorted({g.target for g in movie_groups if g.target})
    if len(targets) == 1:
        return Path(targets[0]), "唯一 movie target"

    strategy = (settings.movie_fallback_strategy or "auto").strip().lower()

    if strategy in {"prefer_name_match", "auto"}:
        by_name = _pick_by_name(current_group, movie_groups)
        if by_name:
            return Path(by_name.target), f"名称匹配命中: {by_name.name}"

    if strategy in {"prefer_source_prefix", "auto"}:
        by_prefix = _pick_by_prefix(current_group, movie_groups, source_path)
        if by_prefix:
            return Path(by_prefix.target), f"source 前缀命中: {by_prefix.name}"

    if strategy == "unique_only":
        return None, "unique_only 策略下存在多个 movie target"

    return None, f"无法在多个 movie target({len(targets)}) 中唯一决策"


def _pick_by_name(current: SyncGroup, candidates: list[SyncGroup]) -> SyncGroup | None:
    base = _tokens(current.name)
    if not base:
        return None

    best = None
    best_score = 0
    tie = False
    for c in candidates:
        score = len(base & _tokens(c.name))
        if score > best_score:
            best = c
            best_score = score
            tie = False
        elif score > 0 and score == best_score:
            tie = True

    if best_score <= 0 or tie:
        return None
    return best


def _pick_by_prefix(current: SyncGroup, candidates: list[SyncGroup], source_path: Path | None) -> SyncGroup | None:
    base = _norm(str(source_path or current.source))
    best = None
    best_score = 0
    tie = False

    for c in candidates:
        score = _common_prefix(base, _norm(c.source))
        if score > best_score:
            best = c
            best_score = score
            tie = False
        elif score > 0 and score == best_score:
            tie = True

    if best_score <= 0 or tie:
        return None
    return best


def _tokens(text: str) -> set[str]:
    return {x for x in re.split(r"[^0-9A-Za-z\u4e00-\u9fff]+", (text or "").lower()) if x}


def _norm(path: str) -> str:
    return str(path or "").replace("\\", "/").rstrip("/").lower()


def _common_prefix(a: str, b: str) -> int:
    aa = [x for x in a.split("/") if x]
    bb = [x for x in b.split("/") if x]
    n = 0
    for l, r in zip(aa, bb):
        if l != r:
            break
        n += 1
    return n
