"""AniList fallback helpers for low-confidence directory recognition."""
from __future__ import annotations

from functools import lru_cache
import re

import httpx

from ..config import settings

ANILIST_GRAPHQL_URL = "https://graphql.anilist.co"
ANILIST_MEDIA_QUERY = """
query ($search: String) {
  Media(search: $search, type: ANIME) {
    title {
      english
      romaji
      native
    }
    synonyms
  }
}
"""


def _normalize_query(query: str) -> str:
    return re.sub(r"\s+", " ", str(query or "")).strip()


def _looks_like_latin_alias(text: str) -> bool:
    value = _normalize_query(text)
    if not value:
        return False
    return re.search(r"[A-Za-z]", value) is not None


@lru_cache(maxsize=256)
def _query_anilist_cached(query: str, timeout_seconds: float) -> tuple[str | None, str | None]:
    normalized = _normalize_query(query)
    if not normalized:
        return None, None

    payload = {
        "query": ANILIST_MEDIA_QUERY,
        "variables": {"search": normalized},
    }

    try:
        with httpx.Client(timeout=max(1.0, float(timeout_seconds))) as client:
            response = client.post(ANILIST_GRAPHQL_URL, json=payload)
    except Exception:
        return None, None

    if response.status_code != 200:
        return None, None

    try:
        body = response.json()
    except ValueError:
        return None, None

    media = ((body or {}).get("data") or {}).get("Media") or {}
    if not isinstance(media, dict):
        return None, None

    title_block = media.get("title") or {}
    if not isinstance(title_block, dict):
        title_block = {}

    candidates: list[tuple[str, str]] = []
    english = _normalize_query(str(title_block.get("english") or ""))
    romaji = _normalize_query(str(title_block.get("romaji") or ""))
    if english:
        candidates.append((english, "english"))
    if romaji:
        candidates.append((romaji, "romaji"))

    synonyms = media.get("synonyms") or []
    if isinstance(synonyms, list):
        for item in synonyms:
            value = _normalize_query(str(item or ""))
            if value:
                candidates.append((value, "synonym"))

    seen: set[str] = set()
    for value, source in candidates:
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        if _looks_like_latin_alias(value):
            return value, source

    return None, None


def search_anilist_english_title_sync(query: str) -> tuple[str | None, str | None]:
    if not getattr(settings, "anilist_fallback_enabled", True):
        return None, None
    normalized = _normalize_query(query)
    if not normalized:
        return None, None
    timeout_seconds = float(getattr(settings, "anilist_fallback_timeout_seconds", 8.0) or 8.0)
    return _query_anilist_cached(normalized, timeout_seconds)