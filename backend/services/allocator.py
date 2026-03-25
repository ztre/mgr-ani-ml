"""Batch allocation helpers for specials/extras.

Note: The allocation idea is inspired by community rename tools (e.g. Bangumi_Auto_Rename),
but this implementation is an original rewrite for this codebase.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

INDEX_PATTERN = re.compile(r"S(?P<season>\d{2})[_-](?P<prefix>[A-Za-z]+)(?P<index>\d{1,3})", re.I)
SEASON00_PATTERN = re.compile(r"S00E(?P<episode>\d{2,3})", re.I)
LABEL_PATTERN = re.compile(r"\b(OP|ED|SP)(\d{1,3})\b", re.I)


def scan_existing_special_indices(target_root: Path, tmdbid: int | None) -> dict[tuple[int | None, int | None, str], int]:
    """Scan show target dir and return max index for each (tmdbid, season_key, prefix)."""
    if not tmdbid:
        return {}
    show_dirs = [p for p in target_root.iterdir() if p.is_dir() and f"[tmdbid={tmdbid}]" in p.name]
    if not show_dirs:
        return {}
    show_dir = show_dirs[0]

    # 仅扫描已落地的目标目录，构建 (tmdbid, season_key, prefix) -> max_index
    cache: dict[tuple[int | None, int | None, str], int] = {}
    for path in show_dir.rglob("*"):
        if not path.is_file():
            continue
        name = path.name
        m = INDEX_PATTERN.search(name)
        if m:
            season_key = int(m.group("season"))
            prefix = str(m.group("prefix")).upper()
            index = int(m.group("index"))
            key = (tmdbid, season_key, prefix)
            cache[key] = max(cache.get(key, 0), index)
            continue

        m_se = SEASON00_PATTERN.search(name)
        m_label = LABEL_PATTERN.search(name)
        if m_se and m_label:
            episode = int(m_se.group("episode"))
            season_key = episode // 100 if episode >= 100 else None
            prefix = m_label.group(1).upper()
            index = int(m_label.group(2))
            key = (tmdbid, season_key, prefix)
            cache[key] = max(cache.get(key, 0), index)

    return cache


def allocate_indices_for_batch(
    items: list[dict],
    existing_cache: dict,
    preserve_original_index: bool = True,
) -> dict[str, int]:
    """Allocate indices for a batch, grouped by (tmdbid, season_key, prefix)."""
    grouped: dict[tuple[int | None, int | None, str], list[dict]] = {}
    for item in items:
        key = (item.get("tmdbid"), item.get("season_key"), str(item.get("prefix") or "").upper())
        grouped.setdefault(key, []).append(item)

    assignments: dict[str, int] = {}
    for key, group in grouped.items():
        # 先保留 preferred，再为剩余项分配递增序号（确保确定性排序）
        reserved: set[int] = set()
        max_existing = int(existing_cache.get(key, 0) or 0)
        group_sorted = sorted(
            group,
            key=lambda x: (
                x.get("preferred") is None,
                x.get("preferred") if x.get("preferred") is not None else 10**9,
                str(x.get("file_path") or ""),
            ),
        )

        if preserve_original_index:
            for item in group_sorted:
                preferred = item.get("preferred")
                if preferred is None:
                    continue
                if preferred <= max_existing or preferred in reserved:
                    continue
                assignments[str(item["file_path"])] = int(preferred)
                reserved.add(int(preferred))

        # 线性分配 next_free，避免 N×M 重排
        next_free = max(max_existing, max(reserved) if reserved else 0) + 1
        for item in group_sorted:
            path_key = str(item["file_path"])
            if path_key in assignments:
                continue
            assignments[path_key] = int(next_free)
            reserved.add(int(next_free))
            next_free += 1

        existing_cache[key] = max(existing_cache.get(key, 0), max(reserved) if reserved else 0)

    return assignments


def should_fallback_to_pending(item: dict, assigned_target: Path, existing_cache: dict, config: dict) -> tuple[bool, str]:
    """Determine whether the item should be marked pending."""
    if item.get("final_hint") and item.get("final_resolved_season") is None:
        return True, "final season unresolved"

    remap_attempts = int(item.get("remap_attempts") or 0)
    if remap_attempts > int(config.get("max_auto_remap_attempts", 3)):
        return True, "exceeded max auto remap attempts"

    if assigned_target.exists():
        owner_dir = item.get("owner_dir")
        source_dir = item.get("source_dir")
        if owner_dir and source_dir:
            try:
                if Path(str(owner_dir)).resolve() == Path(str(source_dir)).resolve():
                    return False, ""
            except Exception:
                if str(owner_dir) == str(source_dir):
                    return False, ""
        src_path = Path(str(item.get("file_path") or ""))
        try:
            if src_path.exists() and assigned_target.samefile(src_path):
                return False, ""
        except OSError:
            pass
        if item.get("is_attachment"):
            return True, "attachment target occupied by other source"
        return True, "target occupied by other source"

    return False, ""


def mark_pending(item: dict, reason: str, pending_jsonl_path: Path) -> None:
    """Append a pending entry to jsonl file."""
    pending_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    file_path = Path(str(item.get("file_path") or ""))
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "original_path": str(file_path) if str(file_path) else None,
        "source_dir": item.get("source_dir"),
        "sync_group_id": item.get("sync_group_id"),
        "file_type": item.get("file_type") or ("attachment" if item.get("is_attachment") else "extra"),
        "tmdb_id": item.get("tmdbid"),
        "tmdbid": item.get("tmdbid"),
        "season": item.get("season_key"),
        "episode": item.get("episode"),
        "extra_category": item.get("extra_category"),
        "original_name": file_path.name,
        "detected_prefix": item.get("prefix"),
        "preferred_index": item.get("preferred"),
        "suggested_target": str(item.get("suggested_target") or ""),
        "reason": reason,
    }
    with pending_jsonl_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
