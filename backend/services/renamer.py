"""Target path construction based on recognition result."""
from __future__ import annotations

import re
from pathlib import Path

from .media_content_types import EXTRA_CATEGORIES, SPECIAL_CATEGORIES
from .parser import ParseResult


def compute_tv_target_path(
    target_root: Path,
    parse_result: ParseResult,
    tmdb_id: int | None,
    ext: str,
    src_filename: str | None = None,
) -> Path:
    title = _safe_name(parse_result.title)
    year_part = f" ({parse_result.year})" if parse_result.year else ""
    tmdb_part = f" [tmdbid={tmdb_id}]" if tmdb_id else ""

    show_dir = target_root / f"{title}{year_part}{tmdb_part}"

    season = parse_result.season if parse_result.season is not None else 1

    if parse_result.extra_category in SPECIAL_CATEGORIES:
        season_dir = show_dir / "Season 00"
        stable_idx = _extract_stable_label_index(parse_result.extra_label, parse_result.extra_category)
        has_stable_idx = stable_idx is not None
        if parse_result.extra_category == "special":
            if stable_idx is not None:
                special_index = stable_idx
            elif parse_result.episode and int(parse_result.episode) > 1:
                special_index = int(parse_result.episode)
            else:
                special_index = 1
            episode = (season * 100 + special_index) if has_stable_idx else special_index
        else:
            if stable_idx is not None:
                oped_index = stable_idx
            elif parse_result.episode and int(parse_result.episode) > 1:
                oped_index = int(parse_result.episode)
            else:
                oped_index = 1
            label = str(parse_result.extra_label or "").upper()
            if label.startswith("ED") and has_stable_idx:
                episode = season * 100 + (oped_index * 2)
            elif has_stable_idx:
                episode = season * 100 + (oped_index * 2 - 1)
            else:
                episode = oped_index
        base_name = f"{title} - S00E{episode:02d}"
        normalized_label = _normalize_extra_label_for_name(parse_result.extra_label, parse_result.extra_category)
        if has_stable_idx and normalized_label:
            base_name += f" - {normalized_label}"
        elif not has_stable_idx:
            suffix = _clean_special_suffix(normalized_label)
            if not suffix:
                hash_input = f"{title}|{parse_result.extra_label or ''}|{src_filename or ''}"
                suffix = f"h{_short_hash(hash_input)}"
            base_name += f" - {suffix}"
        if parse_result.subtitle_lang:
            base_name += parse_result.subtitle_lang
        return season_dir / f"{base_name}{ext}"

    if parse_result.extra_category and parse_result.extra_category in EXTRA_CATEGORIES:
        extras_dir = show_dir / "extras"
        normalized = _normalize_extra_label_for_name(parse_result.extra_label, parse_result.extra_category)
        cleaned_suffix = _clean_extra_suffix(normalized)
        if cleaned_suffix:
            label = _safe_name(cleaned_suffix)
        else:
            hash_input = f"{title}|{parse_result.extra_label or ''}|{src_filename or ''}|extras"
            label = f"h{_short_hash(hash_input)}"
        name = f"{title}{year_part} S{season:02d}_{label}"
        return extras_dir / f"{name}{ext}"

    season_dir = show_dir / f"Season {season:02d}"
    episode = parse_result.episode or 1
    base_name = f"{title} - S{season:02d}E{episode:02d}"
    # 主视频也支持 extra_label（用于补强后的源标签编码）
    if parse_result.extra_label:
        normalized_label = _normalize_extra_label_for_name(parse_result.extra_label, None)
        if normalized_label:
            base_name += f" - {normalized_label}"
    if parse_result.subtitle_lang:
        base_name += parse_result.subtitle_lang
    return season_dir / f"{base_name}{ext}"


def compute_movie_target_path(target_root: Path, parse_result: ParseResult, tmdb_id: int | None, ext: str) -> Path:
    title = _safe_name(parse_result.title)
    year_part = f" ({parse_result.year})" if parse_result.year else ""
    tmdb_part = f" [tmdbid={tmdb_id}]" if tmdb_id else ""

    movie_dir = target_root / f"{title}{year_part}{tmdb_part}"
    quality = parse_result.quality or "1080p"

    if parse_result.extra_category:
        label = parse_result.extra_label or parse_result.extra_category
        name = f"{title}{year_part} - {label} - {quality}"
        if parse_result.subtitle_lang:
            name += parse_result.subtitle_lang
        return movie_dir / "extras" / f"{name}{ext}"

    name = f"{title}{year_part} - {quality}"
    if parse_result.episode is not None:
        name = f"{title}{year_part} - Part {parse_result.episode:02d} - {quality}"
    if parse_result.subtitle_lang:
        name += parse_result.subtitle_lang
    return movie_dir / f"{name}{ext}"


def _safe_name(text: str) -> str:
    bad = '<>:"/\\|?*'
    s = "".join("_" if c in bad else c for c in (text or "Unknown"))
    return s.strip() or "Unknown"


def _normalize_extra_label_for_name(label: str | None, category: str | None) -> str | None:
    if not label:
        return None
    s = re.sub(r"\s+", " ", str(label)).strip()
    if not s:
        return None
    if category == "oped":
        s = re.sub(r"\bNCOP\b", "OP", s, flags=re.I)
        s = re.sub(r"\bNCED\b", "ED", s, flags=re.I)
        s = re.sub(r"\bOP\s*(\d{1,3})\b", lambda m: f"OP{int(m.group(1)):02d}", s, flags=re.I)
        s = re.sub(r"\bED\s*(\d{1,3})\b", lambda m: f"ED{int(m.group(1)):02d}", s, flags=re.I)
    if category == "special":
        s = re.sub(r"\bSP\s*(\d{1,3})\b", lambda m: f"SP{int(m.group(1)):02d}", s, flags=re.I)
    if category and category not in {"special", "oped"}:
        s = re.sub(r"\bextra\b", "Extra", s, flags=re.I)
    return s.strip()


def _extract_label_index(label: str | None) -> int | None:
    if not label:
        return None
    m = re.search(r"(\d{1,3})", str(label))
    if not m:
        return None
    val = int(m.group(1))
    return val if 1 <= val <= 999 else None


def _extract_stable_label_index(label: str | None, category: str | None) -> int | None:
    s = re.sub(r"\s+", " ", str(label or "")).strip()
    if not s:
        return None
    if category == "special":
        m = re.search(r"\b(?:SP|OVA|OAD|OAV|SPECIAL)\s*0*(\d{1,3})\b", s, re.I)
        if m:
            val = int(m.group(1))
            return val if 1 <= val <= 999 else None
        return None
    if category == "oped":
        m = re.search(r"\b(?:OP|ED|NCOP|NCED)\s*0*(\d{1,3})\b", s, re.I)
        if m:
            val = int(m.group(1))
            return val if 1 <= val <= 999 else None
        return None
    return _extract_label_index(s)


def _clean_special_suffix(label: str | None) -> str:
    s = re.sub(r"\s+", " ", str(label or "")).strip()
    if not s:
        return ""
    # Keep original special fragments reversible (e.g. OVA01/OAD03/Scene 01ex).
    s = re.sub(r"\s*#\d{2,3}\s*$", "", s, flags=re.I)
    s = re.sub(r"[^\w\u4e00-\u9fff\- ]+", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip(" -_")
    if len(s) > 30:
        s = s[:30].rstrip()
    return s


def _clean_extra_suffix(label: str | None) -> str:
    s = re.sub(r"\s+", " ", str(label or "")).strip()
    if not s:
        return ""
    s = re.sub(r"[^\w\u4e00-\u9fff\- #]+", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip(" -_")
    if len(s) > 30:
        s = s[:30].rstrip()
    return s


def _short_hash(text: str) -> str:
    import hashlib

    return hashlib.md5(str(text).encode("utf-8")).hexdigest()[:6]
