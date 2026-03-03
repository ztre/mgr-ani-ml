"""Metadata generation and artwork download."""
from __future__ import annotations

from pathlib import Path

import httpx

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/original"


def scrape_tv_metadata(target_dir: Path, tmdb_data: dict) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)

    title = tmdb_data.get("name", "Unknown")
    year = None
    first_air = str(tmdb_data.get("first_air_date") or "")
    if first_air[:4].isdigit():
        year = int(first_air[:4])

    _write_tvshow_nfo(
        title=title,
        year=year,
        tmdb_id=tmdb_data.get("id"),
        overview=str(tmdb_data.get("overview") or ""),
        path=target_dir / "tvshow.nfo",
    )

    _download_image(tmdb_data.get("poster_path"), target_dir / "poster.jpg")
    _download_image(tmdb_data.get("backdrop_path"), target_dir / "fanart.jpg")


def scrape_movie_metadata(target_dir: Path, tmdb_data: dict) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)

    title = tmdb_data.get("title", "Unknown")
    year = None
    release = str(tmdb_data.get("release_date") or "")
    if release[:4].isdigit():
        year = int(release[:4])

    _write_movie_nfo(
        title=title,
        year=year,
        tmdb_id=tmdb_data.get("id"),
        overview=str(tmdb_data.get("overview") or ""),
        path=target_dir / "movie.nfo",
    )

    _download_image(tmdb_data.get("poster_path"), target_dir / "poster.jpg")
    _download_image(tmdb_data.get("backdrop_path"), target_dir / "fanart.jpg")


def _download_image(image_path: str | None, save_to: Path) -> bool:
    if not image_path:
        return False
    try:
        resp = httpx.get(f"{TMDB_IMAGE_BASE}{image_path}", timeout=20)
    except Exception:
        return False
    if resp.status_code != 200:
        return False
    save_to.write_bytes(resp.content)
    return True


def _write_tvshow_nfo(title: str, year: int | None, tmdb_id: int | None, overview: str, path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>",
                "<tvshow>",
                f"  <title>{_xml(title)}</title>",
                f"  <year>{year or ''}</year>",
                f"  <uniqueid type=\"tmdb\" default=\"true\">{tmdb_id or ''}</uniqueid>",
                f"  <plot>{_xml(overview)}</plot>",
                "</tvshow>",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_movie_nfo(title: str, year: int | None, tmdb_id: int | None, overview: str, path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>",
                "<movie>",
                f"  <title>{_xml(title)}</title>",
                f"  <year>{year or ''}</year>",
                f"  <uniqueid type=\"tmdb\" default=\"true\">{tmdb_id or ''}</uniqueid>",
                f"  <plot>{_xml(overview)}</plot>",
                "</movie>",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _xml(text: str) -> str:
    s = str(text or "")
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
