from __future__ import annotations

from pathlib import Path

from backend.services.metadata import scrape_movie_metadata, scrape_tv_metadata
from backend.services.parser import ParseResult
from backend.services.scanner import OperationLog, _execute_transactional_outputs


def test_scrape_tv_metadata_prefers_display_title(tmp_path: Path):
    target = tmp_path / "tv"
    scrape_tv_metadata(
        target,
        {
            "id": 1,
            "name": "TMDB Name",
            "overview": "plot",
            "first_air_date": "2020-01-01",
            "poster_path": None,
            "backdrop_path": None,
        },
        display_title="繁中標題",
    )
    nfo = (target / "tvshow.nfo").read_text(encoding="utf-8")
    assert "<title>繁中標題</title>" in nfo
    assert "<uniqueid type=\"tmdb\" default=\"true\">1</uniqueid>" in nfo


def test_scrape_movie_metadata_prefers_display_title(tmp_path: Path):
    target = tmp_path / "movie"
    scrape_movie_metadata(
        target,
        {
            "id": 2,
            "title": "TMDB Movie",
            "overview": "plot",
            "release_date": "2021-01-01",
            "poster_path": None,
            "backdrop_path": None,
        },
        display_title="繁中電影",
    )
    nfo = (target / "movie.nfo").read_text(encoding="utf-8")
    assert "<title>繁中電影</title>" in nfo
    assert "<uniqueid type=\"tmdb\" default=\"true\">2</uniqueid>" in nfo


def test_scanner_transactional_outputs_passes_display_title_to_metadata(tmp_path: Path, monkeypatch):
    src = tmp_path / "src" / "video.mkv"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("video")
    dst = tmp_path / "dst" / "Show [tmdbid=1]" / "Season 01" / "Show - S01E01.mkv"

    captured: dict[str, str | None] = {"title": None}

    def fake_scrape_tv(target_dir: Path, tmdb_data: dict, display_title: str | None = None):
        captured["title"] = display_title
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / "tvshow.nfo").write_text("ok", encoding="utf-8")

    import backend.services.scanner as scanner

    monkeypatch.setattr(scanner, "scrape_tv_metadata", fake_scrape_tv)

    _execute_transactional_outputs(
        src_path=src,
        dst_path=dst,
        media_type="tv",
        parse_result=ParseResult(
            title="Show",
            year=2020,
            season=1,
            episode=1,
            is_special=False,
            quality=None,
        ),
        tmdb_data={"id": 1, "name": "TMDB Name"},
        should_scrape=True,
        display_title="最終顯示標題",
        op_log=OperationLog(),
    )

    assert captured["title"] == "最終顯示標題"
