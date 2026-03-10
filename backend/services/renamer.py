"""Target path construction based on recognition result."""
from __future__ import annotations

from pathlib import Path

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

    if parse_result.extra_category in {"special", "oped"}:
        season_dir = show_dir / "Season 00"
        if parse_result.extra_category == "special":
            special_index = parse_result.episode or 1
            episode = season * 100 + special_index
        else:
            oped_index = parse_result.episode or 1
            episode = season * 100 + oped_index
        base_name = f"{title} - S00E{episode:02d}"
        if parse_result.extra_label:
            base_name += f" - {parse_result.extra_label}"
        if parse_result.subtitle_lang:
            base_name += parse_result.subtitle_lang
        return season_dir / f"{base_name}{ext}"

    if parse_result.extra_category in {"trailer", "making"}:
        extras_dir = show_dir / "extras"
        label = _safe_name(parse_result.extra_label or "Extra")
        name = f"{title}{year_part} S{season:02d}_{label}"
        return extras_dir / f"{name}{ext}"

    season_dir = show_dir / f"Season {season:02d}"
    episode = parse_result.episode or 1
    base_name = f"{title} - S{season:02d}E{episode:02d}"
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
    if parse_result.subtitle_lang:
        name += parse_result.subtitle_lang
    return movie_dir / f"{name}{ext}"


def _safe_name(text: str) -> str:
    bad = '<>:"/\\|?*'
    s = "".join("_" if c in bad else c for c in (text or "Unknown"))
    return s.strip() or "Unknown"
