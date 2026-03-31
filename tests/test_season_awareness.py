from __future__ import annotations

from pathlib import Path

import backend.services.allocator as allocator
import backend.services.recognition_flow as rf
import backend.services.scanner as scanner
from backend.services.parser import make_search_name


def test_hibike_season_aware_search_and_resolve(monkeypatch):
    logs: list[str] = []

    def fake_log(message: str):
        logs.append(str(message))

    def fake_tv_candidates(query: str, year: int | None = None):
        q = query.lower()
        if "hibike euphonium" in q and ("season 2" in q or "s2" in q or q.endswith(" 2")):
            return [{"id": 62564, "name": "Sound! Euphonium"}]
        return []

    def fake_movie_candidates(query: str, year: int | None = None):
        return []

    def fake_details(tmdbid: int):
        if tmdbid == 62564:
            return {"seasons": [{"season_number": 1}, {"season_number": 2}]}
        return {"seasons": [{"season_number": 1}]}

    monkeypatch.setattr(rf, "append_log", fake_log)
    monkeypatch.setattr(rf, "search_tmdb_tv_candidates_sync", fake_tv_candidates)
    monkeypatch.setattr(rf, "search_tmdb_movie_candidates_sync", fake_movie_candidates)
    monkeypatch.setattr(rf, "get_tmdb_tv_details_sync", fake_details)
    monkeypatch.setattr(scanner, "get_tmdb_tv_details_sync", fake_details)

    media_dir = Path("[VCB-Studio] Hibike! Euphonium 2 [Ma10p_1080p]")
    snap = rf.parse_structure_locally(media_dir, structure_hint="tv")
    assert snap.season_hint == 2
    assert make_search_name(snap.main_title) == "Hibike Euphonium"

    best, _snapshot, _round = rf.recognize_directory_with_fallback(media_dir, "tv", structure_hint="tv")
    assert best is not None
    assert best.tmdb_id == 62564

    context = {"media_type": "tv", "tmdb_id": 62564, "title": "吹响吧！上低音号", "season_hint": 2}
    ok, reason = scanner._stabilize_directory_context(media_dir, context)
    assert ok and reason is None
    assert context.get("resolved_season") == 2


def test_white_album2_research_with_alternative_titles(monkeypatch):
    logs: list[str] = []

    def fake_log(message: str):
        logs.append(str(message))

    def fake_tv_candidates(query: str, year: int | None = None):
        q = query.lower()
        if q in {"white album season 2", "white album s2", "white album 2"}:
            return [{"id": 100, "name": "White Album"}]
        if q in {"wa2 season 2", "wa2 s2", "wa2 2"}:
            return [{"id": 200, "name": "White Album 2"}]
        return []

    def fake_details(tmdbid: int):
        if tmdbid == 100:
            return {
                "seasons": [{"season_number": 1}],
                "alternative_titles": {"results": [{"title": "WA2"}]},
                "also_known_as": [],
            }
        if tmdbid == 200:
            return {"seasons": [{"season_number": 1}, {"season_number": 2}]}
        return {"seasons": []}

    monkeypatch.setattr(rf, "append_log", fake_log)
    monkeypatch.setattr(rf, "search_tmdb_tv_candidates_sync", fake_tv_candidates)
    monkeypatch.setattr(rf, "get_tmdb_tv_details_sync", fake_details)

    best, tried, had_candidates = rf.recognize_directory_with_season_hint_trace(
        Path("White Album 2"),
        "tv",
        2,
        structure_hint="tv",
    )
    assert best is not None
    assert best.tmdb_id == 200
    assert had_candidates
    assert any("wa2 season 2" in x.lower() for x in tried)
    assert any("re-search tried alternatives" in x for x in logs)


def test_shingeki_no_kyojin_season2_research(monkeypatch):
    logs: list[str] = []

    def fake_log(message: str):
        logs.append(str(message))

    def fake_tv_candidates(query: str, year: int | None = None):
        q = query.lower()
        if q == "shingeki no kyojin season 2":
            return [{"id": 10, "name": "Attack on Titan"}]
        if q == "shingeki no kyojin s2":
            return [{"id": 10, "name": "Attack on Titan"}]
        if q == "shingeki no kyojin 2":
            return [{"id": 10, "name": "Attack on Titan"}]
        if q == "shingeki no kyojin":
            return [
                {"id": 10, "name": "Attack on Titan"},
                {"id": 20, "name": "Attack on Titan"},
            ]
        if q == "attack on titan season 2":
            return [{"id": 20, "name": "Attack on Titan"}]
        return []

    def fake_details(tmdbid: int):
        if tmdbid == 10:
            return {
                "seasons": [{"season_number": 1}],
                "alternative_titles": {"results": [{"title": "Attack on Titan"}]},
                "also_known_as": [],
            }
        if tmdbid == 20:
            return {"seasons": [{"season_number": 1}, {"season_number": 2}]}
        return {"seasons": []}

    monkeypatch.setattr(rf, "append_log", fake_log)
    monkeypatch.setattr(scanner, "append_log", fake_log)
    monkeypatch.setattr(rf, "search_tmdb_tv_candidates_sync", fake_tv_candidates)
    monkeypatch.setattr(rf, "search_tmdb_movie_candidates_sync", lambda *args, **kwargs: [])
    monkeypatch.setattr(rf, "get_tmdb_tv_details_sync", fake_details)
    monkeypatch.setattr(scanner, "get_tmdb_tv_details_sync", fake_details)
    monkeypatch.setattr(scanner, "_get_tmdb_item_by_id", lambda *args, **kwargs: {"id": 20, "name": "Attack on Titan"})
    monkeypatch.setattr(scanner, "_resolve_chinese_title_by_tmdb", lambda *args, **kwargs: "进击的巨人")

    media_dir = Path("Shingeki no Kyojin Season 2")
    context = {
        "media_type": "tv",
        "tmdb_id": 10,
        "title": "进击的巨人",
        "season_hint": 2,
        "season_hint_confidence": "high",
        "season_aware_done": False,
    }
    ok, reason = scanner._stabilize_directory_context(media_dir, context)
    assert ok and reason is None
    assert context["tmdb_id"] == 20
    assert context["resolved_season"] == 2
    assert any("resolved by season-aware re-search -> tmdbid=20, season=2" in x for x in logs)


def test_steins_gate_zero_no_season_zero():
    snap = rf.parse_structure_locally(Path("Steins;Gate 0 [01]"), structure_hint="tv")
    assert snap.season_hint in {None, 1}
    assert snap.season_hint != 0
    assert snap.episode_hint == 1


def test_eighty_six_no_weak_season_hint():
    snap = rf.parse_structure_locally(Path("EIGHTY SIX"), structure_hint="tv")
    assert snap.season_hint is None
    assert snap.season_hint_confidence in {None, "low", "high"}


def test_allocator_batch_linear_assignment(tmp_path: Path):
    target_root = tmp_path / "target"
    show_dir = target_root / "Show [tmdbid=9]" / "extras"
    show_dir.mkdir(parents=True)
    (show_dir / "Show S01_CM01.mkv").write_text("1")
    (show_dir / "Show S01_CM02.mkv").write_text("2")
    (show_dir / "Show S01_CM03.mkv").write_text("3")

    cache = allocator.scan_existing_special_indices(target_root, 9)
    items = [
        {
            "file_path": str(tmp_path / "A.mkv"),
            "source_dir": str(tmp_path / "srcA"),
            "tmdbid": 9,
            "season_key": 1,
            "prefix": "CM",
            "preferred": 1,
            "is_attachment": False,
            "lang": None,
        },
        {
            "file_path": str(tmp_path / "B.mkv"),
            "source_dir": str(tmp_path / "srcB"),
            "tmdbid": 9,
            "season_key": 1,
            "prefix": "CM",
            "preferred": 2,
            "is_attachment": False,
            "lang": None,
        },
    ]
    assigned = allocator.allocate_indices_for_batch(items, cache, preserve_original_index=True)
    assert assigned[str(tmp_path / "A.mkv")] == 4
    assert assigned[str(tmp_path / "B.mkv")] == 5


def test_carnival_phantasm_batch_special_allocation(tmp_path: Path):
    target_root = tmp_path / "target"
    extras_dir = target_root / "Carnival Phantasm [tmdbid=77]" / "extras"
    extras_dir.mkdir(parents=True)
    (extras_dir / "Carnival Phantasm S01_CM01.mkv").write_text("1")
    (extras_dir / "Carnival Phantasm S01_CM02.mkv").write_text("2")
    (extras_dir / "Carnival Phantasm S01_CM03.mkv").write_text("3")

    cache = allocator.scan_existing_special_indices(target_root, 77)
    items = [
        {
            "file_path": str(tmp_path / "CMA.mkv"),
            "source_dir": str(tmp_path / "pack"),
            "tmdbid": 77,
            "season_key": 1,
            "prefix": "CM",
            "preferred": 1,
            "is_attachment": False,
            "lang": None,
        },
        {
            "file_path": str(tmp_path / "CMB.mkv"),
            "source_dir": str(tmp_path / "pack"),
            "tmdbid": 77,
            "season_key": 1,
            "prefix": "CM",
            "preferred": 2,
            "is_attachment": False,
            "lang": None,
        },
    ]
    assigned = allocator.allocate_indices_for_batch(items, cache, preserve_original_index=True)
    assert assigned[str(tmp_path / "CMA.mkv")] == 4
    assert assigned[str(tmp_path / "CMB.mkv")] == 5
