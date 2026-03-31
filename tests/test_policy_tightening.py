from __future__ import annotations

import json
from pathlib import Path

import backend.services.recognition_flow as rf
import backend.services.scanner as scanner
import pytest
from backend.services.parser import ParseResult, parse_tv_filename
from backend.services.scanner import DirectoryProcessError, OperationLog


class _DummyDb:
    def query(self, *_args, **_kwargs):  # pragma: no cover - should not be used in these tests
        raise AssertionError("unexpected db.query")


def test_parser_extra_with_strong_episode_keeps_mainline():
    p = parse_tv_filename("Show - Trailer EP03.mkv")
    assert p is not None
    assert p.episode == 3
    assert p.extra_category is None


def test_parser_extra_only_still_short_circuits():
    p = parse_tv_filename("Show - Trailer.mkv")
    assert p is not None
    assert p.extra_category == "trailer"
    assert p.episode is None


def test_recognition_prefers_tv_when_structure_hint_tv(monkeypatch):
    def fake_tv(_title: str, _year: int | None = None):
        return [{"id": 1, "name": "TV Candidate"}]

    def fake_movie(_title: str, _year: int | None = None):
        return [{"id": 2, "title": "Movie Candidate"}]

    def fake_to_ranked(_query_title: str, media_type: str, item: dict, _year_hint: int | None):
        score = 0.55 if media_type == "tv" else 0.95
        return rf.RankedCandidate(
            media_type=media_type,
            tmdb_id=int(item["id"]),
            title=str(item.get("name") or item.get("title") or ""),
            score=score,
            popularity=0.0,
            vote_count=0,
            tmdb_data=item,
            year=None,
        )

    monkeypatch.setattr(rf, "search_tmdb_tv_candidates_sync", fake_tv)
    monkeypatch.setattr(rf, "search_tmdb_movie_candidates_sync", fake_movie)
    monkeypatch.setattr(rf, "_to_ranked_candidate", fake_to_ranked)
    best = rf._unified_competitive_search(
        ["Show"],
        year_hint=None,
        special_hint=False,
        structure_hint="tv",
        season_hint=None,
        season_hint_confidence=None,
    )
    assert best is not None
    assert best.media_type == "tv"


def test_scanner_main_video_without_episode_fail_fast(tmp_path: Path, monkeypatch):
    src = tmp_path / "src" / "Show - Unknown.mkv"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("v", encoding="utf-8")
    target_root = tmp_path / "target"

    monkeypatch.setattr(scanner, "get_inode", lambda _p: None)
    monkeypatch.setattr(
        scanner,
        "parse_tv_filename",
        lambda _name: ParseResult(
            title="Show",
            year=2020,
            season=1,
            episode=None,
            is_special=False,
            quality=None,
            extra_category=None,
            extra_label=None,
        ),
    )
    unhandled_path = tmp_path / "unhandled.jsonl"
    monkeypatch.setattr(scanner.settings, "unhandled_jsonl_path", str(unhandled_path))
    monkeypatch.setattr(scanner.settings, "unprocessed_items_jsonl_path", "")
    monkeypatch.setattr(scanner.settings, "review_jsonl_path", "")

    with pytest.raises(DirectoryProcessError):
        scanner._process_file(
            db=_DummyDb(),
            src_path=src,
            sync_group_id=1,
            context={
                "media_type": "tv",
                "tmdb_id": 100,
                "tmdb_data": {"id": 100, "name": "Show"},
                "title": "Show",
                "year": 2020,
                "target_root": str(target_root),
            },
            seen_targets={},
            op_log=OperationLog(),
            dir_runtime={},
        )

    lines = [x for x in unhandled_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert lines
    payload = json.loads(lines[-1])
    assert payload["reason"] == "main video missing episode signal"
    assert payload["file_type"] == "video"


def test_scanner_special_attachment_unmatched_logged(tmp_path: Path, monkeypatch):
    src = tmp_path / "src" / "Show - OVA01.ass"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("sub", encoding="utf-8")
    target_root = tmp_path / "target"

    monkeypatch.setattr(scanner, "get_inode", lambda _p: None)
    monkeypatch.setattr(
        scanner,
        "parse_tv_filename",
        lambda _name: ParseResult(
            title="Show",
            year=2020,
            season=1,
            episode=1,
            is_special=True,
            quality=None,
            extra_category="special",
            extra_label="SP01",
        ),
    )
    monkeypatch.setattr(scanner, "_resolve_attachment_follow_target", lambda *_args, **_kwargs: None)
    unhandled_path = tmp_path / "unhandled.jsonl"
    monkeypatch.setattr(scanner.settings, "unhandled_jsonl_path", str(unhandled_path))
    monkeypatch.setattr(scanner.settings, "unprocessed_items_jsonl_path", "")
    monkeypatch.setattr(scanner.settings, "review_jsonl_path", "")

    scanner._process_file(
        db=_DummyDb(),
        src_path=src,
        sync_group_id=1,
        context={
            "media_type": "tv",
            "tmdb_id": 100,
            "tmdb_data": {"id": 100, "name": "Show"},
            "title": "Show",
            "year": 2020,
            "target_root": str(target_root),
        },
        seen_targets={},
        op_log=OperationLog(),
        dir_runtime={
            "video_anchor_by_parent": {},
            "video_anchor_by_parent_episode": {},
            "video_anchor_by_parent_stem": {},
            "video_anchor_by_parent_special": {},
            "video_anchor_recent_by_parent": {},
            "special_target_by_raw_label": {},
            "pending_count": 0,
            "skipped_count": 0,
        },
    )

    lines = [x for x in unhandled_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert lines
    payload = json.loads(lines[-1])
    assert payload["reason"] == "special attachment raw-label unmatched"
    assert payload["file_type"] == "attachment"


def test_detect_special_dir_context_supports_bonus_variants(tmp_path: Path):
    p1 = tmp_path / "Show" / "Bonus Disc" / "x.mkv"
    p2 = tmp_path / "Show" / "special_disc" / "x.mkv"
    p3 = tmp_path / "Show" / "SP" / "x.mkv"
    assert scanner.detect_special_dir_context(p1)[0]
    assert scanner.detect_special_dir_context(p2)[0]
    assert scanner.detect_special_dir_context(p3)[0]
    assert scanner.detect_strong_special_dir_context(p1)[0]
    assert scanner.detect_strong_special_dir_context(p2)[0]
    assert scanner.detect_strong_special_dir_context(p3)[0]


def test_detect_media_type_from_structure_prefers_tv_for_special_dir(tmp_path: Path):
    media_dir = tmp_path / "Bonus"
    media_dir.mkdir(parents=True, exist_ok=True)
    (media_dir / "clip.mkv").write_text("x", encoding="utf-8")
    assert scanner._detect_media_type_from_structure(media_dir) == "tv"


def test_bonus_ep_creditless_forced_into_extra_context(tmp_path: Path, monkeypatch):
    src = tmp_path / "Show" / "Bonus" / "EP12 Creditless Ending.mkv"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("v", encoding="utf-8")
    target_root = tmp_path / "target"
    captured: dict[str, ParseResult | None] = {"parse": None}

    monkeypatch.setattr(scanner, "get_inode", lambda _p: None)
    monkeypatch.setattr(
        scanner,
        "parse_tv_filename",
        lambda _name: ParseResult(
            title="Show",
            year=2020,
            season=1,
            episode=12,
            is_special=False,
            quality=None,
            extra_category=None,
            extra_label=None,
        ),
    )
    monkeypatch.setattr(scanner, "_upsert_media_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(scanner, "_upsert_inode_record", lambda *args, **kwargs: None)

    def fake_outputs(*, parse_result: ParseResult, **_kwargs):
        captured["parse"] = parse_result

    monkeypatch.setattr(scanner, "_execute_transactional_outputs", fake_outputs)

    scanner._process_file(
        db=_DummyDb(),
        src_path=src,
        sync_group_id=1,
        context={
            "media_type": "tv",
            "tmdb_id": 100,
            "tmdb_data": {"id": 100, "name": "Show"},
            "title": "Show",
            "year": 2020,
            "target_root": str(target_root),
            "extra_context": True,
        },
        seen_targets={},
        op_log=OperationLog(),
        dir_runtime={
            "video_anchor_by_parent": {},
            "video_anchor_by_parent_episode": {},
            "video_anchor_by_parent_stem": {},
            "video_anchor_by_parent_special": {},
            "video_anchor_recent_by_parent": {},
            "pending_count": 0,
            "skipped_count": 0,
        },
    )
    assert captured["parse"] is not None
    assert captured["parse"].extra_category == "oped"


def test_special_dir_unknown_video_skips_and_logs(tmp_path: Path, monkeypatch):
    src = tmp_path / "Show" / "Bonus" / "unknown_clip.mkv"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("v", encoding="utf-8")
    target_root = tmp_path / "target"
    unhandled_path = tmp_path / "unhandled.jsonl"

    monkeypatch.setattr(scanner, "get_inode", lambda _p: None)
    monkeypatch.setattr(
        scanner,
        "parse_tv_filename",
        lambda _name: ParseResult(
            title="Show",
            year=2020,
            season=1,
            episode=12,
            is_special=False,
            quality=None,
            extra_category=None,
            extra_label=None,
        ),
    )
    monkeypatch.setattr(scanner.settings, "unhandled_jsonl_path", str(unhandled_path))
    monkeypatch.setattr(scanner.settings, "unprocessed_items_jsonl_path", "")
    monkeypatch.setattr(scanner.settings, "review_jsonl_path", "")

    scanner._process_file(
        db=_DummyDb(),
        src_path=src,
        sync_group_id=1,
        context={
            "media_type": "tv",
            "tmdb_id": 100,
            "tmdb_data": {"id": 100, "name": "Show"},
            "title": "Show",
            "year": 2020,
            "target_root": str(target_root),
            "extra_context": True,
        },
        seen_targets={},
        op_log=OperationLog(),
        dir_runtime={"pending_count": 0, "skipped_count": 0},
    )

    lines = [x for x in unhandled_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert lines
    payload = json.loads(lines[-1])
    assert payload["reason"] == "extra category unresolved in special dir"
    assert payload["file_type"] == "extra"


def test_strong_special_dir_fallback_keeps_extra_not_mainline(tmp_path: Path, monkeypatch):
    src = tmp_path / "Show" / "Bonus" / "[VCB] Show [Sword Art Offline 05][Ma10p_1080p][x265_flac].mkv"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("v", encoding="utf-8")
    target_root = tmp_path / "target"
    captured: dict[str, object] = {}

    monkeypatch.setattr(scanner, "get_inode", lambda _p: None)
    monkeypatch.setattr(
        scanner,
        "parse_tv_filename",
        lambda _name: ParseResult(
            title="Show",
            year=2020,
            season=1,
            episode=5,
            is_special=False,
            quality=None,
            extra_category=None,
            extra_label=None,
        ),
    )
    monkeypatch.setattr(scanner, "_upsert_media_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(scanner, "_upsert_inode_record", lambda *args, **kwargs: None)

    def fake_outputs(*, parse_result: ParseResult, dst_path: Path, **_kwargs):
        captured["parse"] = parse_result
        captured["dst"] = dst_path

    monkeypatch.setattr(scanner, "_execute_transactional_outputs", fake_outputs)

    scanner._process_file(
        db=_DummyDb(),
        src_path=src,
        sync_group_id=1,
        context={
            "media_type": "tv",
            "tmdb_id": 100,
            "tmdb_data": {"id": 100, "name": "Show"},
            "title": "Show",
            "year": 2020,
            "target_root": str(target_root),
        },
        seen_targets={},
        op_log=OperationLog(),
        dir_runtime={
            "video_anchor_by_parent": {},
            "video_anchor_by_parent_episode": {},
            "video_anchor_by_parent_stem": {},
            "video_anchor_by_parent_special": {},
            "video_anchor_recent_by_parent": {},
            "special_used": {},
            "suffix_label_registry": {},
            "pending_count": 0,
            "skipped_count": 0,
        },
    )

    parse_result = captured.get("parse")
    dst_path = captured.get("dst")
    assert isinstance(parse_result, ParseResult)
    assert isinstance(dst_path, Path)
    assert parse_result.extra_category == "bdextra"
    assert parse_result.episode is None
    assert "Sword Art Offline 05" in str(parse_result.extra_label or "")
    assert "/extras/" in dst_path.as_posix()
    assert "Sword Art Offline 05" in dst_path.name


def test_finalize_dir_to_pending_logs_single_warning(tmp_path: Path, monkeypatch):
    messages: list[str] = []

    monkeypatch.setattr(scanner, "append_log", lambda msg: messages.append(msg))
    monkeypatch.setattr(scanner, "_rollback_operations", lambda _op_log: None)
    monkeypatch.setattr(scanner, "_upsert_dir_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(scanner, "_mark_dir_pending", lambda *args, **kwargs: tmp_path / "Show" / "Bonus")

    scanner._finalize_dir_to_pending(
        db=object(),  # type: ignore[arg-type]
        media_dir=tmp_path / "Show" / "Bonus",
        source_root=tmp_path / "Show",
        sync_group_id=1,
        source_type="tv",
        signature="sig",
        state="FAILED",
        reason="target conflict",
        op_log=OperationLog(),
    )

    warning_count = sum(1 for msg in messages if "转待办" in msg)
    assert warning_count == 1


def test_unhandled_items_write_only_to_unprocessed_log(tmp_path: Path, monkeypatch):
    unprocessed_path = tmp_path / "pending" / "unprocessed_items.jsonl"
    review_path = tmp_path / "pending" / "review.jsonl"
    monkeypatch.setattr(scanner.settings, "unprocessed_items_jsonl_path", str(unprocessed_path))
    monkeypatch.setattr(scanner.settings, "review_jsonl_path", str(review_path))
    monkeypatch.setattr(scanner.settings, "unhandled_jsonl_path", "")

    scanner._record_unhandled_item(
        original_path=tmp_path / "src" / "clip.mkv",
        reason="extra target occupied",
        file_type="extra",
        sync_group_id=7,
        tmdb_id=100,
        season=1,
        episode=None,
        extra_category="trailer",
        suggested_target=tmp_path / "dst" / "extras" / "clip.mkv",
    )

    unprocessed_lines = [x for x in unprocessed_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(unprocessed_lines) == 1
    unprocessed_payload = json.loads(unprocessed_lines[0])
    assert unprocessed_payload["reason"] == "extra target occupied"
    assert not review_path.exists() or not [x for x in review_path.read_text(encoding="utf-8").splitlines() if x.strip()]


def test_special_attachment_raw_label_conflict_logged(tmp_path: Path, monkeypatch):
    src = tmp_path / "src" / "Show - OVA01.ass"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("sub", encoding="utf-8")
    target_root = tmp_path / "target"

    monkeypatch.setattr(scanner, "get_inode", lambda _p: None)
    monkeypatch.setattr(
        scanner,
        "parse_tv_filename",
        lambda _name: ParseResult(
            title="Show",
            year=2020,
            season=1,
            episode=1,
            is_special=True,
            quality=None,
            extra_category="special",
            extra_label="SP01",
        ),
    )
    monkeypatch.setattr(scanner, "_resolve_attachment_follow_target", lambda *_args, **_kwargs: None)
    unhandled_path = tmp_path / "unhandled.jsonl"
    monkeypatch.setattr(scanner.settings, "unhandled_jsonl_path", str(unhandled_path))
    monkeypatch.setattr(scanner.settings, "unprocessed_items_jsonl_path", "")
    monkeypatch.setattr(scanner.settings, "review_jsonl_path", "")

    scanner._process_file(
        db=_DummyDb(),
        src_path=src,
        sync_group_id=1,
        context={
            "media_type": "tv",
            "tmdb_id": 100,
            "tmdb_data": {"id": 100, "name": "Show"},
            "title": "Show",
            "year": 2020,
            "target_root": str(target_root),
        },
        seen_targets={},
        op_log=OperationLog(),
        dir_runtime={
            "video_anchor_by_parent": {},
            "video_anchor_by_parent_episode": {},
            "video_anchor_by_parent_stem": {},
            "video_anchor_by_parent_special": {},
            "video_anchor_recent_by_parent": {},
            "special_target_by_raw_label": {(str(src.parent), "SP01"): None},
            "pending_count": 0,
            "skipped_count": 0,
        },
    )

    lines = [x for x in unhandled_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert lines
    payload = json.loads(lines[-1])
    assert payload["reason"] == "special attachment raw-label conflict"


def test_special_attachment_never_fallbacks_to_episode_anchor_when_raw_unmatched(tmp_path: Path):
    src = tmp_path / "src" / "Show - SP01.ass"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("sub", encoding="utf-8")
    parse_result = ParseResult(
        title="Show",
        year=2020,
        season=1,
        episode=1,
        is_special=True,
        quality=None,
        extra_category="special",
        extra_label="SP01",
    )
    candidate = scanner._resolve_attachment_follow_target(
        src,
        parse_result,
        dir_runtime={
            "video_anchor_by_parent": {},
            "video_anchor_by_parent_episode": {(str(src.parent), 101): tmp_path / "target" / "x.mkv"},
            "video_anchor_by_parent_stem": {},
            "video_anchor_by_parent_special": {},
            "video_anchor_recent_by_parent": {str(src.parent): tmp_path / "target" / "recent.mkv"},
            "special_target_by_raw_label": {},
        },
    )
    assert candidate is None


def test_zero_episode_skip_is_not_triggered_for_title_number_work():
    parse_result = ParseResult(
        title="Steins;Gate 0",
        year=2018,
        season=1,
        episode=None,
        is_special=False,
        quality=None,
        extra_category=None,
        extra_label=None,
    )
    assert not scanner._should_ignore_zero_episode(
        "Steins;Gate 0.mkv",
        parse_result=parse_result,
        context_title="Steins;Gate 0",
    )
    assert not scanner._should_ignore_zero_episode(
        "Steins;Gate 0 [01].mkv",
        parse_result=parse_result._replace(episode=1),
        context_title="Steins;Gate 0",
    )
    assert scanner._should_ignore_zero_episode(
        "Show EP00.mkv",
        parse_result=parse_result._replace(title="Show"),
        context_title="Show",
    )


def test_scanner_zero_bracket_episode_is_skipped_and_ep01_kept(tmp_path: Path, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir(parents=True, exist_ok=True)
    src_00 = src_dir / "[VCB-Studio] Show [00][Ma10p_1080p][x265_flac].mkv"
    src_01 = src_dir / "[VCB-Studio] Show [01][Ma10p_1080p][x265_flac].mkv"
    src_00.write_text("v0", encoding="utf-8")
    src_01.write_text("v1", encoding="utf-8")
    target_root = tmp_path / "target"
    unprocessed_path = tmp_path / "pending" / "unprocessed_items.jsonl"
    captured_targets: list[Path] = []

    monkeypatch.setattr(scanner, "get_inode", lambda _p: None)
    monkeypatch.setattr(scanner, "_upsert_media_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(scanner, "_upsert_inode_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(scanner.settings, "unprocessed_items_jsonl_path", str(unprocessed_path))
    monkeypatch.setattr(scanner.settings, "review_jsonl_path", "")
    monkeypatch.setattr(scanner.settings, "unhandled_jsonl_path", "")

    def fake_outputs(*, dst_path: Path, **_kwargs):
        captured_targets.append(dst_path)

    monkeypatch.setattr(scanner, "_execute_transactional_outputs", fake_outputs)

    context = {
        "media_type": "tv",
        "tmdb_id": 100,
        "tmdb_data": {"id": 100, "name": "Show"},
        "title": "Show",
        "year": 2020,
        "target_root": str(target_root),
    }
    seen_targets: dict[Path, Path] = {}
    op_log = OperationLog()
    dir_runtime = {
        "video_anchor_by_parent": {},
        "video_anchor_by_parent_episode": {},
        "video_anchor_by_parent_stem": {},
        "video_anchor_by_parent_special": {},
        "video_anchor_recent_by_parent": {},
        "pending_count": 0,
        "skipped_count": 0,
    }

    scanner._process_file(
        db=_DummyDb(),
        src_path=src_00,
        sync_group_id=1,
        context=context,
        seen_targets=seen_targets,
        op_log=op_log,
        dir_runtime=dir_runtime,
    )
    scanner._process_file(
        db=_DummyDb(),
        src_path=src_01,
        sync_group_id=1,
        context=context,
        seen_targets=seen_targets,
        op_log=op_log,
        dir_runtime=dir_runtime,
    )

    lines = [x for x in unprocessed_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert lines
    payload = json.loads(lines[-1])
    assert payload["reason"] == "zero episode skipped"
    assert len(captured_targets) == 1
    assert "S01E01" in captured_targets[0].name


def test_allocator_pending_payload_contains_review_fields(tmp_path: Path):
    pending_path = tmp_path / "pending.jsonl"
    item = {
        "file_path": str(tmp_path / "src" / "bonus" / "clip.ass"),
        "source_dir": str(tmp_path / "src" / "bonus"),
        "sync_group_id": 9,
        "tmdbid": 101,
        "season_key": 1,
        "episode": 2,
        "extra_category": "special",
        "file_type": "attachment",
        "prefix": "SP",
        "preferred": 2,
        "is_attachment": True,
        "suggested_target": str(tmp_path / "dst" / "Show [tmdbid=101]" / "Specials" / "clip.ass"),
    }

    scanner.mark_pending(item, "target occupied by other source", pending_path)

    lines = [x for x in pending_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["original_path"].endswith("clip.ass")
    assert payload["file_type"] == "attachment"
    assert payload["tmdb_id"] == 101
    assert payload["season"] == 1
    assert payload["episode"] == 2
    assert payload["extra_category"] == "special"
