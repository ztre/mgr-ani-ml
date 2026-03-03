"""Recognition parser v3: layered anime regex + TMDB search/caching helpers."""
from __future__ import annotations

import asyncio
import re
import time
from collections import OrderedDict, deque
from pathlib import Path
from threading import Lock
from typing import NamedTuple

import httpx

from ..config import settings


class ParseResult(NamedTuple):
    """结构化解析结果。"""

    title: str
    year: int | None
    season: int
    episode: int | None
    is_special: bool
    quality: str | None
    extra_type: str | None = None
    subtitle_lang: str | None = None
    is_ambiguous: bool = False
    final_season_hint: bool = False


# ----- TMDB API protection/caching -----
_TMDB_RATE_LIMIT_PER_SEC = 3
_TMDB_CACHE_MAX = 512
_TMDB_CACHE_TTL_SECONDS = 3600
_TMDB_CALLS: deque[float] = deque(maxlen=16)
_TMDB_LOCK = Lock()
_TMDB_CACHE: OrderedDict[tuple, tuple[float, object]] = OrderedDict()


def parse_tv_filename(filename: str) -> ParseResult | None:
    """Layered anime regex parser for TV-oriented extraction."""
    raw_stem = _stem(filename)
    clean = _preprocess(raw_stem)
    if not clean:
        return None

    quality = _extract_quality(raw_stem)
    subtitle_lang = _extract_subtitle_lang(filename)
    year = _extract_year(raw_stem)

    extra_type = _extract_extra_type(raw_stem)
    is_special = bool(extra_type)

    # Pattern A: SxxEyy (strong TV)
    m_a = re.search(r"\bS(\d{1,2})\s*E(\d{1,3})\b", clean, re.I)
    if m_a:
        season = int(m_a.group(1))
        episode = int(m_a.group(2))
        title = _cleanup_title(_remove_span(clean, m_a.span()))
        if not title:
            title = _cleanup_title(clean)
        if season == 0:
            is_special = True
        return ParseResult(title, year, season, episode, is_special, quality, extra_type, subtitle_lang, False, False)

    # Pattern B: season keyword (strong TV)
    season = _extract_season_keyword(clean)
    if season is not None:
        title = _cleanup_title(_remove_season_tokens(clean))
        if not title:
            title = _cleanup_title(clean)
        return ParseResult(title, year, season, None, is_special, quality, extra_type, subtitle_lang, False, False)

    # Pattern C: roman numeral season (TV)
    roman_season = _extract_roman_season(clean)
    if roman_season is not None:
        title = _cleanup_title(re.sub(r"\b(?:II|III|IV|V|VI|VII|VIII|IX|X)\b", " ", clean, flags=re.I))
        if not title:
            title = _cleanup_title(clean)
        return ParseResult(title, year, roman_season, None, is_special, quality, extra_type, subtitle_lang, False, False)

    # Pattern D: final season (TV)
    final_match = re.search(r"\b(?:the\s+)?final\s+season(?:\s+part\s*\d+)?\b", clean, re.I)
    if final_match:
        title = _cleanup_title(_remove_span(clean, final_match.span()))
        if not title:
            title = _cleanup_title(clean)
        episode = _extract_episode_only(clean)
        return ParseResult(title, year, 1, episode, is_special, quality, extra_type, subtitle_lang, False, True)

    # Pattern G: episode only (weak TV hint)
    episode = _extract_episode_only(clean)
    if episode is not None:
        title = _cleanup_title(_remove_episode_tokens(clean))
        if not title:
            title = _cleanup_title(clean)
        season_guess = 0 if is_special else 1
        return ParseResult(title, year, season_guess, episode, is_special, quality, extra_type, subtitle_lang, False, False)

    # Fallback
    fallback_title = _cleanup_title(clean)
    if not fallback_title:
        return None
    season_guess = 0 if is_special else 1
    return ParseResult(fallback_title, year, season_guess, None, is_special, quality, extra_type, subtitle_lang, False, False)


def parse_movie_filename(filename: str) -> ParseResult | None:
    """Movie parser with v3 preprocess and subtitle-pattern preference."""
    raw_stem = _stem(filename)
    clean = _preprocess(raw_stem)
    if not clean:
        return None

    quality = _extract_quality(raw_stem)
    subtitle_lang = _extract_subtitle_lang(filename)
    year = _extract_year(raw_stem)
    extra_type = _extract_extra_type(raw_stem)

    # Movie subtitle pattern: "Title - Subtitle" (when no strong TV token)
    if not _has_strong_tv_token(clean):
        m = re.match(r"^(?P<left>.+?)\s*-\s*(?P<right>.+)$", clean)
        if m:
            title = _cleanup_title(m.group("left"))
            if title:
                return ParseResult(title, year, 1, None, False, quality, extra_type, subtitle_lang, False, False)

    title = _cleanup_title(clean)
    if not title:
        return None
    return ParseResult(title, year, 1, None, False, quality, extra_type, subtitle_lang, False, False)


async def search_tmdb_tv(title: str, year: int | None = None) -> dict | None:
    """TMDB TV search (best item)."""
    results = await search_tmdb_tv_candidates(title, year)
    return results[0] if results else None


async def search_tmdb_movie(title: str, year: int | None = None) -> dict | None:
    """TMDB movie search (best item)."""
    results = await search_tmdb_movie_candidates(title, year)
    return results[0] if results else None


async def search_tmdb_tv_candidates(title: str, year: int | None = None) -> list[dict]:
    """TMDB TV candidates; year matches are promoted but not exclusive."""
    results = await _tmdb_search("tv", title)
    if not year:
        return results
    same_year, others = [], []
    for item in results:
        y = str(item.get("first_air_date", ""))[:4]
        if y.isdigit() and int(y) == year:
            same_year.append(item)
        else:
            others.append(item)
    return same_year + others


async def search_tmdb_movie_candidates(title: str, year: int | None = None) -> list[dict]:
    """TMDB movie candidates; year matches are promoted but not exclusive."""
    results = await _tmdb_search("movie", title)
    if not year:
        return results
    same_year, others = [], []
    for item in results:
        y = str(item.get("release_date", ""))[:4]
        if y.isdigit() and int(y) == year:
            same_year.append(item)
        else:
            others.append(item)
    return same_year + others


async def get_tmdb_tv_details(tv_id: int) -> dict | None:
    """TMDB TV detail endpoint."""
    if not settings.tmdb_api_key:
        return None

    cache_key = ("detail", "tv", int(tv_id))
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    await _rate_limit_tmdb()
    url = f"https://api.themoviedb.org/3/tv/{tv_id}"
    params = {"api_key": settings.tmdb_api_key, "language": "zh-CN"}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, params=params)
    except Exception:
        return None

    if r.status_code != 200:
        return None

    data = r.json()
    if isinstance(data, dict) and data:
        _cache_set(cache_key, data)
        return data
    return None


def search_tmdb_tv_sync(title: str, year: int | None = None) -> dict | None:
    """Sync wrapper for TMDB TV search."""
    return asyncio.run(search_tmdb_tv(title, year))


def search_tmdb_movie_sync(title: str, year: int | None = None) -> dict | None:
    """Sync wrapper for TMDB movie search."""
    return asyncio.run(search_tmdb_movie(title, year))


def search_tmdb_tv_candidates_sync(title: str, year: int | None = None) -> list[dict]:
    """Sync wrapper for TMDB TV candidate search."""
    return asyncio.run(search_tmdb_tv_candidates(title, year))


def search_tmdb_movie_candidates_sync(title: str, year: int | None = None) -> list[dict]:
    """Sync wrapper for TMDB movie candidate search."""
    return asyncio.run(search_tmdb_movie_candidates(title, year))


def get_tmdb_tv_details_sync(tv_id: int) -> dict | None:
    """Sync wrapper for TMDB TV detail."""
    return asyncio.run(get_tmdb_tv_details(tv_id))


async def _tmdb_search(media_type: str, title: str) -> list[dict]:
    if not settings.tmdb_api_key:
        return []

    query = _normalize_query(title)
    if not query:
        return []

    cache_key = ("search", media_type, query)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    await _rate_limit_tmdb()
    url = f"https://api.themoviedb.org/3/search/{media_type}"
    params = {
        "api_key": settings.tmdb_api_key,
        "query": query,
        "language": "en-US",
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, params=params)
    except Exception:
        return []

    if r.status_code != 200:
        return []

    payload = r.json()
    results = payload.get("results", []) if isinstance(payload, dict) else []
    if isinstance(results, list) and results:
        _cache_set(cache_key, results)
        return results
    return []


async def _rate_limit_tmdb() -> None:
    with _TMDB_LOCK:
        now = time.monotonic()
        while _TMDB_CALLS and now - _TMDB_CALLS[0] >= 1.0:
            _TMDB_CALLS.popleft()

        if len(_TMDB_CALLS) < _TMDB_RATE_LIMIT_PER_SEC:
            _TMDB_CALLS.append(now)
            return

        sleep_for = 1.0 - (now - _TMDB_CALLS[0])

    if sleep_for > 0:
        await asyncio.sleep(sleep_for)

    with _TMDB_LOCK:
        now = time.monotonic()
        while _TMDB_CALLS and now - _TMDB_CALLS[0] >= 1.0:
            _TMDB_CALLS.popleft()
        _TMDB_CALLS.append(now)


def _cache_get(key: tuple):
    with _TMDB_LOCK:
        item = _TMDB_CACHE.get(key)
        if not item:
            return None
        ts, value = item
        if time.time() - ts > _TMDB_CACHE_TTL_SECONDS:
            _TMDB_CACHE.pop(key, None)
            return None
        _TMDB_CACHE.move_to_end(key)
        return value


def _cache_set(key: tuple, value: object) -> None:
    with _TMDB_LOCK:
        _TMDB_CACHE[key] = (time.time(), value)
        _TMDB_CACHE.move_to_end(key)
        while len(_TMDB_CACHE) > _TMDB_CACHE_MAX:
            _TMDB_CACHE.popitem(last=False)


# ----- Regex helpers -----
def _stem(filename: str) -> str:
    return Path(filename).stem if "/" in filename or "\\" in filename else filename


def _preprocess(text: str) -> str:
    s = text
    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.sub(r"\([^\)]*\)", " ", s)
    s = re.sub(r"\{[^\}]*\}", " ", s)

    tech_patterns = [
        r"\b(?:2160p|1080p|720p|480p|4k)\b",
        r"\b(?:x264|x265|h264|h265|hevc|av1)\b",
        r"\b(?:10bit|8bit|ma10p|hi10p)\b",
        r"\b(?:flac|aac|ac3|dts|ddp\d?\.\d?)\b",
        r"\b(?:webrip|web-dl|bdrip|bluray|remux)\b",
    ]
    for p in tech_patterns:
        s = re.sub(p, " ", s, flags=re.I)

    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _cleanup_title(text: str) -> str:
    s = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", text)
    s = re.sub(r"\b(?:special|extra|final|season|part)\b", " ", s, flags=re.I)
    s = re.sub(r"\bS\d{1,2}E\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\bS\d{1,2}\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:II|III|IV|V|VI|VII|VIII|IX|X)\b", " ", s, flags=re.I)
    s = re.sub(r"\s*-\s*", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:200] if s else ""


def _extract_season_keyword(text: str) -> int | None:
    patterns = [
        r"\bS(\d{1,2})\b",
        r"\bSeason\s*(\d{1,2})\b",
        r"\b(\d{1,2})(?:st|nd|rd|th)\s*Season\b",
        r"第\s*(\d{1,2})\s*季",
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            return int(m.group(1))
    return None


def _remove_season_tokens(text: str) -> str:
    s = text
    s = re.sub(r"\bS\d{1,2}\b", " ", s, flags=re.I)
    s = re.sub(r"\bSeason\s*\d{1,2}\b", " ", s, flags=re.I)
    s = re.sub(r"\b\d{1,2}(?:st|nd|rd|th)\s*Season\b", " ", s, flags=re.I)
    s = re.sub(r"第\s*\d{1,2}\s*季", " ", s)
    return s


def _extract_roman_season(text: str) -> int | None:
    roman_map = {
        "II": 2,
        "III": 3,
        "IV": 4,
        "V": 5,
        "VI": 6,
        "VII": 7,
        "VIII": 8,
        "IX": 9,
        "X": 10,
    }
    m = re.search(r"\b(II|III|IV|V|VI|VII|VIII|IX|X)\b$", text, re.I)
    if not m:
        return None
    return roman_map.get(m.group(1).upper())


def _extract_episode_only(text: str) -> int | None:
    patterns = [
        r"第\s*(\d{1,3})\s*[集话話]",
        r"\bEP?\s*(\d{1,3})\b",
        r"\b(\d{1,3})\b$",
        r"\b(\d{1,3})\b",
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if not m:
            continue
        ep = int(m.group(1))
        if 1 <= ep <= 999 and ep not in {2160, 1080, 720, 480}:
            return ep
    return None


def _remove_episode_tokens(text: str) -> str:
    s = text
    s = re.sub(r"第\s*\d{1,3}\s*[集话話]", " ", s, flags=re.I)
    s = re.sub(r"\bEP?\s*\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\b\d{1,3}\b$", " ", s)
    return s


def _has_strong_tv_token(text: str) -> bool:
    if re.search(r"\bS\d{1,2}E\d{1,3}\b", text, re.I):
        return True
    if _extract_season_keyword(text) is not None:
        return True
    if _extract_roman_season(text) is not None:
        return True
    if re.search(r"\b(?:the\s+)?final\s+season\b", text, re.I):
        return True
    return False


def _remove_span(text: str, span: tuple[int, int]) -> str:
    left, right = span
    return (text[:left] + " " + text[right:]).strip()


def _extract_year(text: str) -> int | None:
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return int(m.group(1)) if m else None


def _extract_quality(text: str) -> str | None:
    for q in ("2160p", "4K", "1080p", "720p", "480p"):
        if q.lower() in text.lower():
            return q
    return None


def _extract_subtitle_lang(filename: str) -> str | None:
    name_lower = filename.lower()

    def has_token(tokens: list[str]) -> bool:
        for token in tokens:
            # token boundary: support separators like ., _, -, &, space, brackets, etc.
            if re.search(rf"(^|[^a-z0-9]){re.escape(token)}([^a-z0-9]|$)", name_lower):
                return True
        return False

    # Priority: zh-CN > zh-TW > ja > en
    if "简中" in name_lower or "简体" in name_lower or has_token(["sc", "chs", "gb"]):
        return ".zh-CN"
    if "繁中" in name_lower or "繁体" in name_lower or has_token(["tc", "cht", "big5"]):
        return ".zh-TW"
    if "日文" in name_lower or "日语" in name_lower or has_token(["jp", "ja", "jpn"]):
        return ".ja"
    if "english" in name_lower or has_token(["en", "eng"]):
        return ".en"
    return None


def _extract_extra_type(text: str) -> str | None:
    patterns = [
        r"\b(NCOP\d*)\b",
        r"\b(NCED\d*)\b",
        r"\b(PV\d*)\b",
        r"\b(CM\d*)\b",
        r"\b(OVA\d*)\b",
        r"\b(OAD\d*)\b",
        r"\b(SP\d*)\b",
        r"\b(Menu\d*)\b",
        r"\b(Trailers?)\b",
        r"\b(Specials?)\b",
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            return m.group(1).strip()
    return None


def _normalize_query(title: str) -> str:
    s = re.sub(r"\s+", " ", str(title or "")).strip()
    return s[:200]
