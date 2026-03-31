from __future__ import annotations

from pathlib import Path

import pytest

import backend.services.recognition_flow as rf
import backend.services.scanner as scanner
from backend.services.parser import is_title_number_safe, make_search_name, parse_tv_filename


def test_hibike_season_hint_resolved(monkeypatch):
    def fake_details(tmdbid: int):
        return {"seasons": [{"season_number": 1}, {"season_number": 2}]}

    monkeypatch.setattr(scanner, "get_tmdb_tv_details_sync", fake_details)
    monkeypatch.setattr(rf, "get_tmdb_tv_details_sync", fake_details)
    media_dir = Path("Hibike! Euphonium 2")
    snap = rf.parse_structure_locally(media_dir, structure_hint="tv")
    context = {
        "media_type": "tv",
        "tmdb_id": 100,
        "title": "响け！ユーフォニアム",
        "season_hint": snap.season_hint,
    }
    ok, reason = scanner._stabilize_directory_context(media_dir, context)
    assert ok
    assert reason is None
    assert context.get("resolved_season") == 2


def test_steins_gate_zero_not_season_zero():
    snap = rf.parse_structure_locally(Path("Steins;Gate 0"))
    assert snap.season_hint in {None, 1}
    assert snap.season_hint != 0


def test_tokyo_magnitude_not_trailing_season_hint():
    snap = rf.parse_structure_locally(Path("東京マグニチュード8.0"))
    assert snap.season_hint in {None, 1}


def test_season_aware_candidate_boost(monkeypatch, tmp_path: Path):
    media_dir = tmp_path / "Test Show Season 2"
    media_dir.mkdir()

    def fake_tv_candidates(title: str, year: int | None = None):
        return [
            {"id": 1, "name": "Test Show"},
            {"id": 2, "name": "Test Show Season 2"},
        ]

    def fake_movie_candidates(title: str, year: int | None = None):
        return []

    def fake_to_ranked(query_title: str, media_type: str, item: dict, year_hint: int | None):
        if item["id"] == 1:
            score = 0.80
        else:
            score = 0.75
        return rf.RankedCandidate(
            media_type=media_type,
            tmdb_id=int(item["id"]),
            title=item.get("name") or "",
            score=score,
            popularity=0.0,
            vote_count=0,
            tmdb_data=item,
            year=None,
        )

    def fake_details(tmdbid: int):
        if tmdbid == 2:
            return {"seasons": [{"season_number": 2, "name": "Season 2"}]}
        return {"seasons": [{"season_number": 1, "name": "Season 1"}]}

    monkeypatch.setattr(rf, "search_tmdb_tv_candidates_sync", fake_tv_candidates)
    monkeypatch.setattr(rf, "search_tmdb_movie_candidates_sync", fake_movie_candidates)
    monkeypatch.setattr(rf, "_to_ranked_candidate", fake_to_ranked)
    monkeypatch.setattr(rf, "get_tmdb_tv_details_sync", fake_details)

    best, snapshot, _round = rf.recognize_directory_with_fallback(
        media_dir,
        "tv",
        structure_hint="tv",
    )
    assert snapshot.season_hint == 2
    assert snapshot.season_hint_confidence == "high"
    assert best is not None
    assert best.tmdb_id == 2


def test_make_search_name_strip_date_and_group():
    assert make_search_name("2012.05.12.Steins;Gate.2011.BD.1080p") == "Steins Gate"
    assert make_search_name("AIR - mawen1250 [BD 1920x1080 AVC FLAC AC3]") == "AIR"
    assert make_search_name("[VCB-Studio] Yuru Camp Season 3 [Ma10p_1080p]") == "Yuru Camp Season 3"
    assert make_search_name("Kanon 1920x1080p24 - vcb-studio mawen1250") == "Kanon"
    assert make_search_name("Code Geass Lelouch of the Rebellion (2006) [BD 1080p]") == "Code Geass Lelouch of the Rebellion"


def test_title_number_safe_protection():
    assert not is_title_number_safe("EIGHTY SIX")
    assert not is_title_number_safe("86")
    assert not is_title_number_safe("Steins;Gate 0")
    assert not is_title_number_safe("91 Days")
    assert not is_title_number_safe("Chihayafuru I+II")
    assert not is_title_number_safe("Tokyo Magnitude 8.0")
    assert not is_title_number_safe("東京マグニチュード8.0")


def test_promotion_clip_is_classified_as_extra():
    p = parse_tv_filename("Clannad - Promotion Clip 1.mkv")
    assert p is not None
    assert p.extra_category in {"trailer", "preview", "pv"}
    assert p.extra_label is not None
    assert p.episode is None
    assert "Promotion Clip" in p.extra_label or "PROMOTION CLIP" in p.extra_label.upper()


def test_promotion_clip_without_number_keeps_fragment():
    p = parse_tv_filename("Clannad - Promotion Clip Final Ver.mkv")
    assert p is not None
    assert p.extra_category in {"trailer", "preview", "pv"}
    assert p.extra_label is not None
    assert p.episode is None
    assert "Clip" in p.extra_label or "CLIP" in p.extra_label.upper()


@pytest.mark.parametrize(
    "name,_expected_label",
    [
        ("AIR 2005 - 特集 EP10 59.940fps.mkv", "特集"),
        ("AIR 2005 - 総集編 EP10.mkv", "総集編"),
        ("AIR 2005 - 總集編 EP10.mkv", "總集編"),
        ("AIR 2005 - 特番 EP10.mkv", "特番"),
        ("AIR 2005 - 番外編 EP10.mkv", "番外編"),
        ("AIR 2005 - 番外篇 EP10.mkv", "番外篇"),
        ("AIR 2005 - TV番組特集 EP10.mkv", "TV番組"),
        ("AIR 2005 - テレビ番組 EP10.mkv", "テレビ番組"),
        ("AIR 2005 - アニクリ EP10.mkv", "アニクリ"),
    ],
)
def test_jp_extra_keywords_do_not_override_strong_ep(name: str, _expected_label: str):
    p = parse_tv_filename(name)
    assert p is not None
    assert p.extra_category is None
    assert p.episode == 10


def test_fps_not_misdetected_as_episode_air():
    p = parse_tv_filename("AIR 2005 特典 59.940fps AVC-yuv420p10 FLAC.mkv")
    assert p is not None
    assert p.episode != 59


def test_fps_not_misdetected_as_episode_clannad():
    p = parse_tv_filename("Clannad 23.976fps x264 FLAC.mkv")
    assert p is not None
    assert p.episode != 23


def test_bracket_00_does_not_return_episode_zero():
    p = parse_tv_filename("[VCB-Studio] Show [00][Ma10p_1080p][x265_flac].mkv")
    assert p is not None
    assert p.episode is None


def test_scene_suffix_is_preserved_for_making():
    p = parse_tv_filename("Show - Scene 01ex [BD 1080p].mkv")
    assert p is not None
    assert p.extra_category == "making"
    assert p.extra_label is not None
    assert "Scene 01ex" in p.extra_label


def test_ova_oad_raw_label_is_preserved():
    p1 = parse_tv_filename("Show - OVA01.mkv")
    p2 = parse_tv_filename("Show - OAD03.mkv")
    assert p1 is not None and p2 is not None
    assert p1.extra_category == "special"
    assert p2.extra_category == "special"
    assert p1.extra_label is not None and "OVA01" in p1.extra_label.upper()
    assert p2.extra_label is not None and "OAD03" in p2.extra_label.upper()
