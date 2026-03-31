from __future__ import annotations

from backend.config import _migrate_legacy_pending_path


def test_migrate_legacy_pending_path_to_app_pending():
    assert _migrate_legacy_pending_path("/media/pending/pending.jsonl", "/app/pending/pending.jsonl") == "/app/pending/pending.jsonl"
    assert _migrate_legacy_pending_path("/media/pending/unprocessed_items.jsonl", "/app/pending/unprocessed_items.jsonl") == "/app/pending/unprocessed_items.jsonl"
    assert _migrate_legacy_pending_path("/media/pending/review.jsonl", "/app/pending/review.jsonl") == "/app/pending/review.jsonl"


def test_migrate_legacy_pending_path_keeps_custom_value():
    assert _migrate_legacy_pending_path("/custom/pending/pending.jsonl", "/app/pending/pending.jsonl") == "/custom/pending/pending.jsonl"
