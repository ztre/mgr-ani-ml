from __future__ import annotations

import json
from pathlib import Path

from backend.api import media


def test_load_pending_log_items_reverses_order_and_tags_kind(tmp_path: Path):
    log_path = tmp_path / "pending.jsonl"
    entries = [
        {"timestamp": "2026-01-01T00:00:00+00:00", "reason": "older", "original_path": "/a"},
        {"timestamp": "2026-01-02T00:00:00+00:00", "reason": "newer", "original_path": "/b"},
    ]
    log_path.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in entries) + "\n", encoding="utf-8")

    items = media._load_pending_log_items(log_path, "pending")

    assert len(items) == 2
    assert items[0]["reason"] == "newer"
    assert items[0]["kind"] == "pending"
    assert items[0]["line_no"] == 2
    assert items[1]["reason"] == "older"


def test_create_pending_log_review_appends_manual_review_entry(tmp_path: Path, monkeypatch):
    review_path = tmp_path / "review.jsonl"
    monkeypatch.setattr(media.settings, "review_jsonl_path", str(review_path))

    payload = media.PendingLogReviewRequest(
        source_kind="unprocessed",
        source_original_path="/media/source/Bonus/clip.mkv",
        source_reason="extra target occupied",
        source_timestamp="2026-03-25T00:00:00+00:00",
        resolution_status="resolved",
        reviewer="tester",
        note="manually moved to extras",
        tmdb_id=100,
        season=1,
        episode=None,
        extra_category="trailer",
        suggested_target="/media/target/Show [tmdbid=100]/extras/clip.mkv",
    )

    resp = media.create_pending_log_review(payload)

    assert resp["ok"] is True
    lines = [x for x in review_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["entry_type"] == "manual_review"
    assert entry["source_kind"] == "unprocessed"
    assert entry["resolution_status"] == "resolved"
    assert entry["reviewer"] == "tester"
    assert entry["tmdb_id"] == 100
