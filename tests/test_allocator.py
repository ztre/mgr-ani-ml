from __future__ import annotations

import queue
from pathlib import Path

from backend.services.allocator import (
    allocate_indices_for_batch,
    mark_pending,
    scan_existing_special_indices,
)
from backend.services.parser import ParseResult, parse_tv_filename
from backend.services.renamer import compute_tv_target_path
from backend.services.recognition_flow import parse_structure_locally, resolve_season
import backend.services.scanner as scanner
from backend.services.scanner import (
    _apply_readable_suffix_for_unnumbered_extra,
    _build_attachment_target_from_anchor,
    _register_video_anchor,
    _resolve_attachment_follow_target,
)


def test_parse_trailing_season_strip():
    snap = parse_structure_locally(Path("[VCB-Studio] Hibike! Euphonium 2 [Ma10p]"), structure_hint="tv")
    assert snap.main_title == "Hibike! Euphonium"
    assert snap.season_hint == 2
    assert snap.season_hint_confidence == "low"


def test_bracket_episode_title_number():
    result = parse_tv_filename("Steins;Gate 0 [01]")
    assert result is not None
    assert result.episode == 1
    assert "Steins;Gate 0" in result.title


def test_batch_cm_allocation(tmp_path: Path):
    target_root = tmp_path / "target"
    show_dir = target_root / "Test Show [tmdbid=1]"
    extras_dir = show_dir / "extras"
    extras_dir.mkdir(parents=True)
    (extras_dir / "Test Show S01_CM01.mkv").write_text("a")
    (extras_dir / "Test Show S01_CM02.mkv").write_text("b")
    (extras_dir / "Test Show S01_CM03.mkv").write_text("c")

    cache = scan_existing_special_indices(target_root, 1)
    items = [
        {
            "file_path": str(tmp_path / "src" / "CM01.mkv"),
            "source_dir": str(tmp_path / "src"),
            "tmdbid": 1,
            "season_key": 1,
            "prefix": "CM",
            "preferred": 1,
            "is_attachment": False,
            "lang": None,
        }
    ]
    assignments = allocate_indices_for_batch(items, cache, preserve_original_index=True)
    assert assignments[str(tmp_path / "src" / "CM01.mkv")] == 4


def test_attachment_bind_same_episode(tmp_path: Path):
    dir_runtime = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "video_anchor_by_parent_special": {},
        "special_target_by_raw_label": {},
    }
    video_src = tmp_path / "src" / "S01E02.mkv"
    video_src.parent.mkdir(parents=True, exist_ok=True)
    video_src.write_text("video")
    video_dst = tmp_path / "dst" / "Season 01" / "Show - S01E02.mkv"
    video_dst.parent.mkdir(parents=True, exist_ok=True)
    video_dst.write_text("video")

    parse_result = parse_tv_filename(str(video_src))
    _register_video_anchor(video_src, video_dst, parse_result, dir_runtime)

    sub_src = tmp_path / "src" / "S01E02.chs.ass"
    sub_src.write_text("sub")
    sub_parse = parse_tv_filename(str(sub_src))
    anchor = _resolve_attachment_follow_target(sub_src, sub_parse, dir_runtime)
    assert anchor == video_dst
    target = _build_attachment_target_from_anchor(anchor, sub_parse, sub_src.suffix)
    assert target.name.endswith(".ass")


def test_attachment_bind_special_by_scene_anchor(tmp_path: Path):
    dir_runtime = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "video_anchor_by_parent_special": {},
        "special_target_by_raw_label": {},
    }
    video1 = tmp_path / "src" / "Show - Scene 01ex.mkv"
    video2 = tmp_path / "src" / "Show - Scene 02ex.mkv"
    video1.parent.mkdir(parents=True, exist_ok=True)
    video1.write_text("v1")
    video2.write_text("v2")
    dst1 = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "Show S01_Making01 Scene 01ex.mkv"
    dst2 = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "Show S01_Making02 Scene 02ex.mkv"
    dst1.parent.mkdir(parents=True, exist_ok=True)
    dst1.write_text("v1")
    dst2.write_text("v2")

    p1 = parse_tv_filename(str(video1))
    p2 = parse_tv_filename(str(video2))
    assert p1 is not None and p2 is not None
    _register_video_anchor(video1, dst1, p1, dir_runtime)
    _register_video_anchor(video2, dst2, p2, dir_runtime)

    sub1 = tmp_path / "src" / "Show - Scene 01ex.zh-CN.ass"
    sub2 = tmp_path / "src" / "Show - Scene 02ex.zh-CN.ass"
    sub3 = tmp_path / "src" / "Show - Scene 03ex.zh-CN.ass"
    sub1.write_text("s1")
    sub2.write_text("s2")
    sub3.write_text("s3")
    subp1 = parse_tv_filename(str(sub1))
    subp2 = parse_tv_filename(str(sub2))
    subp3 = parse_tv_filename(str(sub3))
    assert subp1 is not None and subp2 is not None and subp3 is not None

    assert _resolve_attachment_follow_target(sub1, subp1, dir_runtime) == dst1
    assert _resolve_attachment_follow_target(sub2, subp2, dir_runtime) == dst2
    assert _resolve_attachment_follow_target(sub3, subp3, dir_runtime) is None


def test_attachment_follow_uses_fine_key_when_raw_key_conflicts(tmp_path: Path):
    src_parent = tmp_path / "src"
    src_parent.mkdir(parents=True, exist_ok=True)
    src = src_parent / "Show - OVA01 On Air Ver.zh-CN.ass"
    src.write_text("sub")
    parse_result = parse_tv_filename(str(src))
    assert parse_result is not None
    parse_result = parse_result._replace(extra_category="special", extra_label="OVA01")
    dst = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "Show S00E01 - SP01 OVA01 On Air Ver.mkv"
    raw_key = scanner._build_special_raw_label_key(str(src_parent), "OVA01")
    fine_key = scanner._build_special_fine_label_key(
        str(src_parent),
        raw_label="OVA01",
        source_text=src.stem,
        category="special",
    )
    assert raw_key is not None and fine_key is not None
    resolved = _resolve_attachment_follow_target(
        src,
        parse_result,
        dir_runtime={
            "video_anchor_by_parent": {},
            "video_anchor_by_parent_episode": {},
            "video_anchor_by_parent_stem": {},
            "video_anchor_by_parent_special": {},
            "video_anchor_recent_by_parent": {},
            "special_target_by_raw_label": {raw_key: None},
            "special_target_by_fine_label": {fine_key: dst},
        },
    )
    assert resolved == dst


def test_special_anchor_conflict_returns_none(tmp_path: Path):
    dir_runtime = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "video_anchor_by_parent_special": {},
        "special_target_by_raw_label": {},
    }
    video1 = tmp_path / "src" / "Show - OVA01.mkv"
    video2 = tmp_path / "src" / "Show - OVA01.mp4"
    video1.parent.mkdir(parents=True, exist_ok=True)
    video1.write_text("v1")
    video2.write_text("v2")
    dst1 = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "a.mkv"
    dst2 = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "b.mkv"
    dst1.parent.mkdir(parents=True, exist_ok=True)
    dst1.write_text("1")
    dst2.write_text("2")
    p1 = parse_tv_filename(str(video1))
    p2 = parse_tv_filename(str(video2))
    assert p1 is not None and p2 is not None

    # Force same normalized stem/category/original label key -> multi-candidate conflict.
    p1 = p1._replace(extra_label="OVA01")
    p2 = p2._replace(extra_label="OVA01")
    _register_video_anchor(video1, dst1, p1, dir_runtime)
    _register_video_anchor(video2, dst2, p2, dir_runtime)

    sub = tmp_path / "src" / "Show - OVA01.zh-CN.ass"
    sub.write_text("s")
    sub_parse = parse_tv_filename(str(sub))
    assert sub_parse is not None
    sub_parse = sub_parse._replace(extra_label="OVA01")
    assert _resolve_attachment_follow_target(sub, sub_parse, dir_runtime) is None


def test_attachment_bind_special_by_raw_label_bridge(tmp_path: Path):
    dir_runtime = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "video_anchor_by_parent_special": {},
        "special_target_by_raw_label": {},
    }
    video1 = tmp_path / "src" / "Show - OVA01.mkv"
    video2 = tmp_path / "src" / "Show - OVA02.mkv"
    video1.parent.mkdir(parents=True, exist_ok=True)
    video1.write_text("v1")
    video2.write_text("v2")
    dst1 = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "Show S01_SP04 OVA01.mkv"
    dst2 = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "Show S01_SP06 OVA02.mkv"
    dst1.parent.mkdir(parents=True, exist_ok=True)
    dst1.write_text("1")
    dst2.write_text("2")

    p1 = parse_tv_filename(str(video1))
    p2 = parse_tv_filename(str(video2))
    assert p1 is not None and p2 is not None
    p1 = p1._replace(extra_category="special", extra_label="SP04", episode=4)
    p2 = p2._replace(extra_category="special", extra_label="SP06", episode=6)
    _register_video_anchor(video1, dst1, p1, dir_runtime, raw_label="SP01")
    _register_video_anchor(video2, dst2, p2, dir_runtime, raw_label="SP02")

    sub = tmp_path / "src" / "Show - OVA01.zh-CN.ass"
    sub.write_text("s")
    sub_parse = parse_tv_filename(str(sub))
    assert sub_parse is not None
    sub_parse = sub_parse._replace(extra_category="special", extra_label="SP01", episode=1)

    assert _resolve_attachment_follow_target(sub, sub_parse, dir_runtime) == dst1


def test_special_raw_label_conflict_returns_none(tmp_path: Path):
    dir_runtime = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "video_anchor_by_parent_special": {},
        "special_target_by_raw_label": {},
    }
    video1 = tmp_path / "src" / "Show - OVA01.mkv"
    video2 = tmp_path / "src" / "Show - OVA01.mp4"
    video1.parent.mkdir(parents=True, exist_ok=True)
    video1.write_text("1")
    video2.write_text("2")
    dst1 = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "a.mkv"
    dst2 = tmp_path / "dst" / "Show [tmdbid=1]" / "extras" / "b.mkv"
    dst1.parent.mkdir(parents=True, exist_ok=True)
    dst1.write_text("a")
    dst2.write_text("b")
    p1 = parse_tv_filename(str(video1))
    p2 = parse_tv_filename(str(video2))
    assert p1 is not None and p2 is not None
    p1 = p1._replace(extra_category="special", extra_label="SP04", episode=4)
    p2 = p2._replace(extra_category="special", extra_label="SP06", episode=6)
    _register_video_anchor(video1, dst1, p1, dir_runtime, raw_label="SP01")
    _register_video_anchor(video2, dst2, p2, dir_runtime, raw_label="SP01")

    sub = tmp_path / "src" / "Show - OVA01.zh-CN.ass"
    sub.write_text("s")
    sub_parse = parse_tv_filename(str(sub))
    assert sub_parse is not None
    sub_parse = sub_parse._replace(extra_category="special", extra_label="SP01", episode=1)

    assert _resolve_attachment_follow_target(sub, sub_parse, dir_runtime) is None


def test_final_season_resolve():
    class StubTMDB:
        def get_tv_details(self, tmdbid: int):
            return {
                "seasons": [
                    {"season_number": 1, "name": "Season 1"},
                    {"season_number": 2, "name": "Final Season"},
                ]
            }

    season = resolve_season(StubTMDB(), 1, None, final_hint=True)
    assert season == 2

    class BadTMDB:
        def get_tv_details(self, tmdbid: int):
            raise RuntimeError("offline")

    season_none = resolve_season(BadTMDB(), 1, None, final_hint=True)
    assert season_none is None


def test_pending_jsonl(tmp_path: Path):
    pending = tmp_path / "pending.jsonl"
    item = {
        "file_path": str(tmp_path / "src" / "CM01.mkv"),
        "source_dir": str(tmp_path / "src"),
        "tmdbid": 1,
        "prefix": "CM",
        "preferred": 1,
        "suggested_target": "target",
    }
    mark_pending(item, "conflict", pending)
    lines = pending.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1


def test_unnumbered_special_suffix_keeps_unique_names(tmp_path: Path):
    dir_runtime = {"suffix_label_registry": {}}
    src1 = tmp_path / "AIR - TV番组特集 [1080p].mkv"
    src2 = tmp_path / "AIR - MEMORY OF VOICE [1080p].mkv"
    src1.write_text("a")
    src2.write_text("b")

    p1 = parse_tv_filename(str(src1))
    p2 = parse_tv_filename(str(src2))
    assert p1 is not None and p2 is not None
    p1 = p1._replace(extra_category="special", extra_label="SP", episode=1, season=1, title="AIR 2005")
    p2 = p2._replace(extra_category="special", extra_label="SP", episode=1, season=1, title="AIR 2005")

    p1 = _apply_readable_suffix_for_unnumbered_extra(p1, src1, dir_runtime)
    p2 = _apply_readable_suffix_for_unnumbered_extra(p2, src2, dir_runtime)

    assert p1.extra_label != p2.extra_label
    assert p1.episode == 1
    assert p2.episode == 2
    dst1 = compute_tv_target_path(tmp_path / "target", p1, 1, ".mkv")
    dst2 = compute_tv_target_path(tmp_path / "target", p2, 1, ".mkv")
    assert dst1 != dst2
    assert "Season 00" in dst1.as_posix() and "Season 00" in dst2.as_posix()
    assert "S00E01" in dst1.name
    assert "S00E02" in dst2.name


def test_extras_name_fallback_to_hash_when_label_empty(tmp_path: Path):
    p = parse_tv_filename("Show - Trailer.mkv")
    assert p is not None
    p = p._replace(title="Show", year=2020, season=1, extra_category="trailer", extra_label="")
    dst = compute_tv_target_path(tmp_path / "target", p, 9, ".mkv", src_filename="Show - Trailer.mkv")
    assert "/extras/" in dst.as_posix()
    assert "_h" in dst.name


def test_media_task_error_classification():
    e1 = scanner.DirectoryProcessError("多个源文件映射到同一目标: a / b")
    assert scanner._classify_media_task_error(e1) == "deterministic_conflict"
    e1b = scanner.DirectoryProcessError("batch target conflict")
    assert scanner._classify_media_task_error(e1b) == "deterministic_conflict"

    e2 = RuntimeError("database is locked")
    assert scanner._classify_media_task_error(e2) == "transient"

    e3 = RuntimeError("unknown fatal")
    assert scanner._classify_media_task_error(e3) == "other"


def test_episode_parse_keeps_explicit_ep_token():
    result = parse_tv_filename("AIR 2005 - EP10 [BD 1920x1080 AVC-yuv420p10 FLACx2].mkv")
    assert result is not None
    assert result.episode == 10


def test_episode_parse_ignores_tech_numeric_tokens():
    result = parse_tv_filename("AIR 2005 - TV番组特集 [DVD 708x480 AVC-yuv420p10 FLACx2] - mawen1250.mkv")
    assert result is not None
    assert result.episode is None
    assert result.extra_category == "making"
    assert scanner._extract_episode_from_filename_loose("AIR 2005 - TV番组特集 [DVD 708x480 AVC-yuv420p10 FLACx2] - mawen1250.mkv") is None


def test_tv_unnumbered_files_fallback_to_extras_with_unique_labels(tmp_path: Path):
    dir_runtime = {"suffix_label_registry": {}}
    src1 = tmp_path / "AIR 2005 - TV番组特集 [DVD 708x480 AVC-yuv420p10 FLACx2] - mawen1250.mkv"
    src2 = tmp_path / "AIR 2005 - TV番组特集 [BD 1920x1080 AVC-yuv420p10 FLACx2] - mawen1250.mkv"
    src1.write_text("a")
    src2.write_text("b")

    p1 = parse_tv_filename(str(src1))
    p2 = parse_tv_filename(str(src2))
    assert p1 is not None and p2 is not None
    p1 = p1._replace(title="AIR 2005", season=1, episode=None, extra_category="making", extra_label=None, is_special=False)
    p2 = p2._replace(title="AIR 2005", season=1, episode=None, extra_category="making", extra_label=None, is_special=False)

    p1 = _apply_readable_suffix_for_unnumbered_extra(p1, src1, dir_runtime)
    p2 = _apply_readable_suffix_for_unnumbered_extra(p2, src2, dir_runtime)

    assert p1.extra_label == "TV番组特集"
    assert p2.extra_label == "TV番组特集 #02"
    dst1 = compute_tv_target_path(tmp_path / "target", p1, 1722, ".mkv", src_filename=src1.name)
    dst2 = compute_tv_target_path(tmp_path / "target", p2, 1722, ".mkv", src_filename=src2.name)
    assert "/extras/" in dst1.as_posix()
    assert "/extras/" in dst2.as_posix()
    assert dst1 != dst2


def test_apply_special_indexing_preserves_scene_suffix(tmp_path: Path):
    dir_runtime = {"special_used": {}}
    src = tmp_path / "Show - Scene 01ex.mkv"
    src.write_text("a")
    p = parse_tv_filename(str(src))
    assert p is not None
    p = p._replace(title="Show", season=1, extra_category="making", extra_label="Scene 01ex")
    updated = scanner._apply_special_indexing(p, src, dir_runtime)
    assert updated.extra_label is not None
    assert updated.extra_label.startswith("Making01")
    assert "Scene 01ex" in updated.extra_label


def test_build_allocation_items_prefers_scene_index(tmp_path: Path):
    src1 = tmp_path / "Show - Scene 01ex.mkv"
    src2 = tmp_path / "Show - Scene 02ex.mkv"
    src1.write_text("1")
    src2.write_text("2")
    context = {"tmdb_id": 7, "resolved_season": 1, "final_hint": False, "final_resolved_season": None}
    items, item_map = scanner._build_allocation_items([src1, src2], context, "tv")
    assert len(items) == 2
    assert item_map[str(src1)]["preferred"] == 1
    assert item_map[str(src2)]["preferred"] == 2
    assert "Scene 01ex" in str(item_map[str(src1)].get("final_label_seed") or "")
    assert "Scene 02ex" in str(item_map[str(src2)].get("final_label_seed") or "")


def test_renamer_keeps_scene_suffix_in_extras_name(tmp_path: Path):
    p1 = parse_tv_filename("Show - Scene 01ex.mkv")
    p2 = parse_tv_filename("Show - Scene 02ex.mkv")
    assert p1 is not None and p2 is not None
    p1 = p1._replace(title="Show", year=2020, season=1, extra_category="making", extra_label="Making18 Scene 01ex")
    p2 = p2._replace(title="Show", year=2020, season=1, extra_category="making", extra_label="Making18 Scene 02ex")
    dst1 = compute_tv_target_path(tmp_path / "target", p1, 1, ".mkv", src_filename="Show - Scene 01ex.mkv")
    dst2 = compute_tv_target_path(tmp_path / "target", p2, 1, ".mkv", src_filename="Show - Scene 02ex.mkv")
    assert "Scene 01ex" in dst1.name
    assert "Scene 02ex" in dst2.name
    assert dst1 != dst2


def test_renamer_keeps_raw_special_fragment(tmp_path: Path):
    p = parse_tv_filename("Show - OVA01.mkv")
    assert p is not None
    p = p._replace(title="Show", year=2020, season=1, episode=4, extra_category="special", extra_label="SP04 OVA01")
    dst = compute_tv_target_path(tmp_path / "target", p, 1, ".mkv", src_filename="Show - OVA01.mkv")
    assert "SP04 OVA01" in dst.name


def test_media_worker_marks_pending_after_second_deterministic_conflict(monkeypatch):
    calls: list[bool] = []
    pending_calls: list[tuple] = []

    task = scanner.MediaTask(
        path=Path("/tmp/source/Test Show"),
        media_type="tv",
        parsed=None,  # type: ignore[arg-type]
        created_at=None,  # type: ignore[arg-type]
        sync_group_id=7,
        task_id=None,
    )
    q: queue.Queue = queue.Queue()
    q.put(task)
    q.put(None)

    class FakeGroup:
        id = 7
        source = "/tmp/source"
        source_type = "tv"

    class FakeQuery:
        def __init__(self, group):
            self.group = group

        def filter(self, *_args, **_kwargs):
            return self

        def first(self):
            return self.group

    class FakeSession:
        def query(self, _model):
            return FakeQuery(FakeGroup())

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    def fake_handle(_db, _task, force_recompute_names: bool = False):
        calls.append(force_recompute_names)
        raise scanner.DirectoryProcessError("多个源文件映射到同一目标: x")

    def fake_mark_pending(db, src_path, source_root, sync_group_id, source_type, reason):
        pending_calls.append((db, src_path, source_root, sync_group_id, source_type, reason))
        return src_path

    monkeypatch.setattr(scanner, "MEDIA_TASK_QUEUE", q)
    monkeypatch.setattr(scanner, "_handle_media_task", fake_handle)
    monkeypatch.setattr(scanner, "_mark_dir_pending", fake_mark_pending)
    monkeypatch.setattr(scanner, "SessionLocal", lambda: FakeSession())
    monkeypatch.setattr(scanner, "append_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scanner.time, "sleep", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not sleep")))

    scanner._media_task_worker()

    assert calls == [False, True]
    assert len(pending_calls) == 1
    assert pending_calls[0][3] == 7
    assert pending_calls[0][4] == "tv"
    assert "deterministic conflict after recompute" in pending_calls[0][5]


def test_target_name_conflict_enriches_with_source_tags_only_on_conflict(tmp_path: Path, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir(parents=True, exist_ok=True)
    src1 = src_dir / "Show - OP01.mkv"
    src2 = src_dir / "Show - NCOP01.mkv"
    src1.write_text("a", encoding="utf-8")
    src2.write_text("b", encoding="utf-8")

    base_parse = ParseResult(
        title="Show",
        year=2020,
        season=1,
        episode=1,
        is_special=True,
        quality=None,
        extra_category="oped",
        extra_label="OP01",
    )
    monkeypatch.setattr(scanner, "get_inode", lambda _p: None)
    monkeypatch.setattr(scanner, "parse_tv_filename", lambda _name: base_parse)
    monkeypatch.setattr(scanner, "_upsert_media_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(scanner, "_upsert_inode_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(scanner, "_execute_transactional_outputs", lambda **_kwargs: None)
    monkeypatch.setattr(scanner, "_record_unhandled_item", lambda *args, **kwargs: None)

    captured: list[Path] = []

    def _capture_seen(seen_targets: dict[Path, Path], src_path: Path, dst_path: Path):
        captured.append(dst_path)
        prev = seen_targets.get(dst_path)
        if prev is None:
            seen_targets[dst_path] = src_path
            return
        if prev != src_path:
            raise scanner.DirectoryProcessError(f"多个源文件映射到同一目标: {prev.name} / {src_path.name}")

    monkeypatch.setattr(scanner, "_deduplicate_target_or_raise", _capture_seen)

    context = {
        "media_type": "tv",
        "tmdb_id": 1,
        "tmdb_data": {"id": 1, "name": "Show"},
        "title": "Show",
        "year": 2020,
        "target_root": str(tmp_path / "target"),
        "extra_context": True,
    }
    dir_runtime = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "video_anchor_by_parent_special": {},
        "video_anchor_recent_by_parent": {},
        "pending_count": 0,
        "skipped_count": 0,
    }
    seen_targets: dict[Path, Path] = {}

    scanner._process_file(
        db=None,
        src_path=src1,
        sync_group_id=1,
        context=dict(context),
        seen_targets=seen_targets,
        op_log=scanner.OperationLog(),
        dir_runtime=dict(dir_runtime),
    )
    scanner._process_file(
        db=None,
        src_path=src2,
        sync_group_id=1,
        context=dict(context),
        seen_targets=seen_targets,
        op_log=scanner.OperationLog(),
        dir_runtime=dict(dir_runtime),
    )

    assert len(captured) >= 2
    first_name = captured[0].name
    second_name = captured[1].name
    assert "NC Ver" not in first_name
    assert "NC Ver" in second_name
    assert first_name != second_name
