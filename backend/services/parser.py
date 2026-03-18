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

PATTERNS = {
    "bracket_ep": r"\[(\d{1,3})([a-zA-Zβ]*)?\]",
    "sxe": r"[sS](\d{1,2})[eE](\d{1,3})",
    "season_word": r"(?:season|s)\s*(\d{1,2})",
    "roman_end": r"\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\s*$",
    "bang": r"!{2,4}\s*$",
    "trailing_num": r"(?:^|[\s._-])(\d{1,2})\s*$",
    "final_dir": r"\bfinal\s*season?\b",
}

SEARCH_NOISE_PATTERN = re.compile(
    r"\b(?:1080p|720p|2160p|x264|x265|h264|ma10p|ma444|hi10p|flac|aac|ac3|"
    r"web|webrip|bluray|bdrip|bdmv|dvdrip|remux)\b",
    re.I,
)

SPECIAL_TYPE_MAP = {
    # Season 00 pool
    "OP": ("season00", "OP"),
    "NCOP": ("season00", "OP"),
    "ED": ("season00", "ED"),
    "NCED": ("season00", "ED"),
    # extras pools
    "CM": ("extras", "CM"),
    "Making": ("extras", "Making"),
    "Event": ("extras", "Event"),
    "Interview": ("extras", "Interview"),
    "Trailer": ("extras", "Trailer"),
    "IV": ("extras", "IV"),
    # previews
    "PV": ("extras", "PV"),
    "Preview": ("extras", "Preview"),
}


class ParseResult(NamedTuple):
    """结构化解析结果。"""

    title: str
    year: int | None
    season: int
    episode: int | None
    is_special: bool
    quality: str | None
    extra_category: str | None = None
    extra_label: str | None = None
    subtitle_lang: str | None = None
    is_ambiguous: bool = False
    final_season_hint: bool = False


def make_search_name(raw_name: str) -> str:
    """Build a cleaner TMDB query name from raw directory/file name."""
    text = _preprocess(raw_name or "")
    if not text:
        return ""
    text = SEARCH_NOISE_PATTERN.sub(" ", text)
    text = re.sub(r"\bWEB\s+Preview", "Preview", text, flags=re.I)
    text = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    m = re.search(r"(?:^| )([2-9]|[12]\d|30)$", text)
    if m:
        text = text[: m.start(1)].strip()
    return text


# ----- TMDB API protection/caching -----
_TMDB_RATE_LIMIT_PER_SEC = 3
_TMDB_CACHE_MAX = 512
_TMDB_CACHE_TTL_SECONDS = 86400
_TMDB_CALLS: deque[float] = deque(maxlen=16)
_TMDB_LOCK = Lock()
_TMDB_CACHE: OrderedDict[tuple, tuple[float, object]] = OrderedDict()


def parse_tv_filename(filename: str, structure_hint: str | None = None) -> ParseResult | None:
    """Layered anime regex parser for TV-oriented extraction."""
    raw_stem = _stem(filename)
    clean = _preprocess(raw_stem)
    if not clean:
        return None
    episode_text = _preprocess_for_episode(raw_stem)

    quality = _extract_quality(raw_stem)
    subtitle_lang = _extract_subtitle_lang(filename)
    year = _extract_year(raw_stem)

    extra_category, extra_label, extra_from_bracket = classify_extra_from_text(raw_stem)
    is_special = extra_category in {"special", "oped"}

    # 解析优先级：显式 EP/季号 > 罗马/!!/单词季号 > 尾部季号 > Final > 仅集号
    bracket_episode, bracket_suffix = extract_bracket_episode(raw_stem)
    has_special_episode_suffix = bool(bracket_suffix and not bracket_suffix.isdigit())

    se_preview = _extract_season_episode_priority(episode_text)
    if se_preview is not None:
        _season, _episode, _span, kind = se_preview
        # 标题数字（如 Steins;Gate 0 / 86）不要误当集号
        if kind in {"two_digit", "number"} and not is_title_number_safe(_cleanup_title(clean)):
            se_preview = None
        if se_preview is not None and (not extra_from_bracket) and kind in {"sxxeyy", "xxyy", "episode", "e"}:
            extra_category = None
            extra_label = None
            is_special = False

    # When marked as special/OPED, prefer extra token and avoid episode parsing.
    if extra_category in {"special", "oped"}:
        season = _extract_season_hint(episode_text)
        episode = _extract_extra_index(extra_label)
        title = _cleanup_title(clean)
        if not title:
            return None
        return ParseResult(
            title,
            year,
            season or 1,
            episode,
            True,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            False,
            False,
        )

    # Pattern A: explicit bracket episode (strongest)
    if bracket_episode is not None and bracket_episode > 0:
        season_hint = _extract_season_hint(episode_text)
        title = _cleanup_title(_remove_bracket_episode(raw_stem))
        if not title:
            title = _cleanup_title(clean)
        return ParseResult(
            title,
            year,
            season_hint or 1,
            bracket_episode,
            is_special,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            has_special_episode_suffix,
            False,
        )

    # Pattern B: SxxEyy (strong TV)
    se = se_preview or _extract_season_episode_priority(episode_text)
    if se is not None:
        season, episode, span, kind = se
        if kind in {"two_digit", "number"} and not is_title_number_safe(_cleanup_title(clean)):
            se = None
        if se is not None:
            if kind in {"episode", "e", "number", "two_digit"}:
                season_hint = _extract_season_hint(episode_text)
                if season_hint is not None:
                    season = season_hint
            title = _cleanup_title(_remove_span(clean, span))
            if not title:
                title = _cleanup_title(clean)
            if season == 0:
                is_special = True
            return ParseResult(
                title,
                year,
                season,
                episode,
                is_special,
                quality,
                extra_category,
                extra_label,
                subtitle_lang,
                False,
                False,
            )

    # Pattern C: season keyword (strong TV)
    season = _extract_season_keyword(clean)
    if season is not None:
        title = _cleanup_title(_remove_season_tokens(clean))
        if not title:
            title = _cleanup_title(clean)
        return ParseResult(
            title,
            year,
            season,
            None,
            is_special,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            False,
            False,
        )

    # Pattern D: roman numeral season (TV)
    roman_season = _extract_roman_season(clean)
    if roman_season is not None:
        title = _cleanup_title(re.sub(r"\b(?:II|III|IV|V|VI|VII|VIII|IX|X)\b", " ", clean, flags=re.I))
        if not title:
            title = _cleanup_title(clean)
        return ParseResult(
            title,
            year,
            roman_season,
            None,
            is_special,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            False,
            False,
        )

    # Pattern E: bang season (anime)
    bang_season = _extract_bang_season(clean)
    if bang_season is not None:
        title = _cleanup_title(re.sub(r"!{2,4}\s*$", " ", clean))
        if not title:
            title = _cleanup_title(clean)
        return ParseResult(
            title,
            year,
            bang_season,
            None,
            is_special,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            False,
            False,
        )

    # Pattern F: word season
    word_season = _extract_word_season(clean)
    if word_season is not None:
        title = _cleanup_title(re.sub(r"\b(first|second|third|fourth|fifth|sixth)\s+season\b", " ", clean, flags=re.I))
        if not title:
            title = _cleanup_title(clean)
        return ParseResult(
            title,
            year,
            word_season,
            None,
            is_special,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            False,
            False,
        )

    # Pattern G: trailing season number
    trailing_season = _extract_trailing_season_number(clean, structure_hint=structure_hint or "tv")
    if trailing_season is not None:
        # 尾部季号仅在结构提示为 TV 时启用（避免误判标题数字）
        title = _cleanup_title(re.sub(r"(?:^|[\s._-])[2-9]\s*$", " ", clean))
        if not title:
            title = _cleanup_title(clean)
        return ParseResult(
            title,
            year,
            trailing_season,
            None,
            is_special,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            False,
            False,
        )

    # Pattern H: final season (TV)
    final_match = re.search(r"\b(?:the\s+)?final\s+season(?:\s+part\s*\d+)?\b", clean, re.I)
    if final_match:
        title = _cleanup_title(_remove_span(clean, final_match.span()))
        if not title:
            title = _cleanup_title(clean)
        episode = _extract_episode_only(clean, _cleanup_title(clean))
        return ParseResult(
            title,
            year,
            None,
            episode,
            is_special,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            False,
            True,
        )

    # Pattern I: episode only (weak TV hint)
    episode = _extract_episode_only(clean, _cleanup_title(clean))
    if episode is not None:
        title = _cleanup_title(_remove_episode_tokens(clean))
        if not title:
            title = _cleanup_title(clean)
        season_guess = 0 if is_special else 1
        return ParseResult(
            title,
            year,
            season_guess,
            episode,
            is_special,
            quality,
            extra_category,
            extra_label,
            subtitle_lang,
            False,
            False,
        )

    # Fallback
    fallback_title = _cleanup_title(clean)
    if not fallback_title:
        return None
    season_guess = 0 if is_special else 1
    return ParseResult(
        fallback_title,
        year,
        season_guess,
        None,
        is_special,
        quality,
        extra_category,
        extra_label,
        subtitle_lang,
        False,
        False,
    )


def parse_movie_filename(filename: str) -> ParseResult | None:
    """Movie parser with v3 preprocess and subtitle-pattern preference."""
    raw_stem = _stem(filename)
    clean = _preprocess(raw_stem)
    if not clean:
        return None

    quality = _extract_quality(raw_stem)
    subtitle_lang = _extract_subtitle_lang(filename)
    year = _extract_year(raw_stem)
    extra_category, extra_label, _extra_from_bracket = classify_extra_from_text(raw_stem)
    is_special = extra_category in {"special", "oped"}

    # Movie subtitle pattern: "Title - Subtitle" (when no strong TV token)
    if not _has_strong_tv_token(clean):
        m = re.match(r"^(?P<left>.+?)\s*-\s*(?P<right>.+)$", clean)
        if m:
            title = _cleanup_title(m.group("left"))
            if title:
                return ParseResult(
                    title,
                    year,
                    1,
                    None,
                    is_special,
                    quality,
                    extra_category,
                    extra_label,
                    subtitle_lang,
                    False,
                    False,
                )

    title = _cleanup_title(clean)
    if not title:
        return None
    return ParseResult(
        title,
        year,
        1,
        None,
        is_special,
        quality,
        extra_category,
        extra_label,
        subtitle_lang,
        False,
        False,
    )


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
    return Path(filename).stem


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


def _preprocess_for_episode(text: str) -> str:
    def _bracket_repl(m: re.Match) -> str:
        content = m.group(1)
        return f"[{content}]" if re.search(r"\d", content) else " "

    s = text
    s = re.sub(r"\[([^\]]*)\]", _bracket_repl, s)
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
    s = re.sub(r"\b(?:op|ed|ncop|nced|opening|ending|creditless|non[\s\-_]*telop)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:pv|preview|trailer|teaser|tv\s*spot|cm|commercial|advertisement)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:interview|making|behind|documentary)\b", " ", s, flags=re.I)
    s = re.sub(r"\bS\d{1,2}E\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\b\d{1,2}\s*[xX]\s*\d{1,3}\b", " ", s)
    s = re.sub(r"\bEpisode\s*\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\bEP\s*\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\bE\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\bS\d{1,2}\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:II|III|IV|V|VI|VII|VIII|IX|X)\b", " ", s, flags=re.I)
    s = re.sub(r"\s*-\s*", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:200] if s else ""


def extract_bracket_episode(name: str) -> tuple[int | None, str | None]:
    """Extract explicit bracket episode like [01], [01a], [01β]."""
    text = str(name or "")
    for m in re.finditer(PATTERNS["bracket_ep"], text):
        raw_num = m.group(1)
        suffix = (m.group(2) or "").strip()
        if not raw_num.isdigit():
            continue
        ep = int(raw_num)
        if ep <= 0:
            return None, suffix or None
        if suffix and not suffix.isdigit():
            return ep, suffix
        return ep, None
    return None, None


def _remove_bracket_episode(text: str) -> str:
    return re.sub(PATTERNS["bracket_ep"], " ", str(text or ""))


def is_title_number_safe(cleaned_title: str) -> bool:
    """Heuristic guard for title numbers like 'Steins;Gate 0', '86', '91 Days', '3-gatsu'."""
    title = re.sub(r"\s+", " ", str(cleaned_title or "")).strip().lower()
    if not title:
        return True
    if re.fullmatch(r"\d+", title):
        return False
    if title in {"86"}:
        return False
    if title.endswith(" 0") or title.endswith("0"):
        return False
    if re.search(r"\bsteins;gate\s*0\b", title):
        return False
    if re.search(r"\b91\s*days\b", title):
        return False
    if re.search(r"\b3\s*-?gatsu\b", title):
        return False
    return True


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
    s = re.sub(r"\b(?:the\s+)?final\s+season(?:\s+part\s*\d+)?\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:II|III|IV|V|VI|VII|VIII|IX|X)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(first|second|third|fourth|fifth|sixth)\s+season\b", " ", s, flags=re.I)
    return s


def _extract_roman_season(text: str) -> int | None:
    roman_map = {
        "I": 1,
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
    s = str(text or "").strip()
    # Strip trailing bracket tokens like [Ma10p_1080p] so "II [..]" can be detected.
    while True:
        m = re.search(r"\s*\[[^\]]+\]\s*$", s)
        if not m:
            break
        s = s[:m.start()].strip()
    m = re.search(
        r"(?:\b(?:Season|S)\s*(I|II|III|IV|V|VI|VII|VIII|IX|X)\b|\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\b)\s*$",
        s,
        re.I,
    )
    if not m:
        return None
    token = m.group(1) or m.group(2)
    return roman_map.get(str(token).upper())


WORD_SEASON_MAP = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
}


def _extract_word_season(text: str) -> int | None:
    m = re.search(r"\b(first|second|third|fourth|fifth|sixth)\s+season\b", text, re.I)
    if not m:
        return None
    return WORD_SEASON_MAP[m.group(1).lower()]


def _extract_bang_season(title: str) -> int | None:
    m = re.search(PATTERNS["bang"], title)
    if not m:
        return None
    return len(m.group())


def _extract_trailing_season_number(text: str, structure_hint: str | None = None) -> int | None:
    if structure_hint and str(structure_hint).lower() != "tv":
        return None
    s = str(text or "").strip()
    if re.search(r"(19\d{2}|20\d{2})\s*[-~]\s*(19\d{2}|20\d{2})", s):
        return None
    m = re.search(PATTERNS["trailing_num"], s)
    if not m:
        return None
    val = int(m.group(1))
    if val < 2 or val > 30:
        return None
    if val in {2160, 1080, 720, 480}:
        return None
    if 1900 <= val <= 2099:
        return None
    return val


def _extract_episode_only(text: str, cleaned_title: str | None = None) -> int | None:
    patterns = [
        r"第\s*(\d{1,3})\s*[集话話]",
        r"\bEpisode\s*(\d{1,3})\b",
        r"\bE(\d{1,3})\b",
        r"\bEP(\d{1,3})\b",
        r"\b(\d{1,3})\b$",
        r"\b(\d{1,3})\b",
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if not m:
            continue
        ep = int(m.group(1))
        if cleaned_title and not is_title_number_safe(cleaned_title):
            continue
        if 1 <= ep <= 999 and ep not in {2160, 1080, 720, 480} and not (1900 <= ep <= 2099):
            return ep
    return None


def _remove_episode_tokens(text: str) -> str:
    s = text
    s = re.sub(r"第\s*\d{1,3}\s*[集话話]", " ", s, flags=re.I)
    s = re.sub(r"\bEpisode\s*\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\bE\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\bEP\d{1,3}\b", " ", s, flags=re.I)
    s = re.sub(r"\b\d{1,2}\s*[xX]\s*\d{1,3}\b", " ", s)
    s = re.sub(r"\b\d{1,3}\b$", " ", s)
    return s


def detect_final_hint(dir_name: str) -> bool:
    return re.search(PATTERNS["final_dir"], str(dir_name or ""), re.I) is not None


def _has_strong_tv_token(text: str) -> bool:
    if re.search(r"\bS\d{1,2}E\d{1,3}\b", text, re.I):
        return True
    if re.search(r"\b\d{1,2}\s*[xX]\s*\d{1,3}\b", text):
        return True
    if extract_bracket_episode(text)[0] is not None:
        return True
    if _extract_season_keyword(text) is not None:
        return True
    if _extract_roman_season(text) is not None:
        return True
    if _extract_bang_season(text) is not None:
        return True
    if _extract_word_season(text) is not None:
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

    # Priority: zh+ja > zh > ja > en
    # Common fansub tags like JPSC/JPTC are concatenated tokens and need explicit handling.
    has_jpsc = has_token(["jpsc"])
    has_jptc = has_token(["jptc"])
    has_zh_cn = "简中" in name_lower or "简体" in name_lower or has_token(["sc", "chs", "gb"])
    has_zh_tw = "繁中" in name_lower or "繁体" in name_lower or has_token(["tc", "cht", "big5"])
    has_ja = "日文" in name_lower or "日语" in name_lower or has_token(["jp", "ja", "jpn"])
    has_en = "english" in name_lower or has_token(["en", "eng"])

    if has_jpsc:
        return ".zh-CN.ja"
    if has_jptc:
        return ".zh-TW.ja"

    if (has_zh_cn or has_zh_tw) and has_ja:
        if has_zh_cn:
            return ".zh-CN.ja"
        return ".zh-TW.ja"

    if has_zh_cn:
        return ".zh-CN"
    if has_zh_tw:
        return ".zh-TW"
    if has_ja:
        return ".ja"
    if has_en:
        return ".en"
    return None


def classify_extra_from_text(text: str) -> tuple[str | None, str | None, bool]:
    """Return (category, label, from_bracket) for specials/oped/extras with bracket priority."""
    bracket_tokens = re.findall(r"\[([^\]]+)\]", text)
    if bracket_tokens:
        candidates: list[tuple[int, str, str]] = []
        for raw in bracket_tokens:
            info = _match_extra_info(raw)
            if info:
                priority = _special_priority_from_raw(raw, info[0], info[1])
                candidates.append((priority, info[0], info[1]))
        if candidates:
            candidates.sort(key=lambda x: x[0])
            return candidates[0][1], candidates[0][2], True
    info = _match_extra_info(text)
    if info:
        return info[0], info[1], False
    return None, None, False


def _special_priority_from_raw(raw: str, category: str, label: str) -> int:
    token = str(raw or "")
    upper = token.upper()
    if category == "oped":
        if "NCOP" in upper:
            return 0
        if "NCED" in upper:
            return 1
        if re.search(r"\bOP\b", upper):
            return 2
        if re.search(r"\bED\b", upper):
            return 3
        return 4
    if category == "pv":
        return 5
    if category == "cm":
        return 6
    if category == "preview":
        return 7
    return 20


def _match_extra_info(text: str) -> tuple[str, str] | None:
    s = str(text or "")

    # 0) Bracket subtypes and richer trailer/CM/preview patterns
    subtype_patterns = [
        (r"\b(Character)\s*PV\s*(\d{0,3})\b", "character_pv", "CharacterPV"),
        (r"\b(Web)\s*Preview\s*(\d{0,3})\b", "preview", "WebPreview"),
        (r"\b(Next\s*Episode)\s*Preview\s*(\d{0,3})\b", "preview", "Preview"),
    ]
    for p, category, prefix in subtype_patterns:
        m = re.search(p, s, re.I)
        if m:
            idx = m.group(2) or ""
            label = prefix if not idx else _format_token_number(prefix, idx)
            return category, label

    # 1) Special
    special_patterns = [
        r"\b(SP\d*)\b",
        r"\b(SPECIALS?)\b",
        r"\b(OVA\d*)\b",
        r"\b(OAD\d*)\b",
        r"\b(OAV\d*)\b",
        r"\b(BONUS\d*)\b",
        r"\b(EXTRA\s*EPISODE)\b",
    ]
    for p in special_patterns:
        m = re.search(p, s, re.I)
        if m:
            return "special", _normalize_extra_label(m.group(1))

    # 2) OP / ED
    oped_patterns = [
        r"\b(NCOP)\s*0?(\d{0,3})\b",
        r"\b(NCED)\s*0?(\d{0,3})\b",
        r"\b(OP)\s*0?(\d{0,3})\b",
        r"\b(ED)\s*0?(\d{0,3})\b",
        r"\b(OPENING)\b",
        r"\b(ENDING)\b",
        r"\b(CREDITLESS)\b",
        r"\b(NON[\s\-_]*TELOP)\b",
        r"\b(Non[\s\-_]*Telop\s*Ending)\b",
    ]
    for p in oped_patterns:
        m = re.search(p, s, re.I)
        if m:
            token = m.group(1)
            idx = m.group(2) if m.lastindex and m.lastindex >= 2 else ""
            if token.upper().startswith("NCED"):
                label = "ED" if not idx else _format_token_number("ED", idx)
            elif token.upper().startswith("NCOP"):
                label = "OP" if not idx else _format_token_number("OP", idx)
            else:
                label = token if not idx else _format_token_number(token, idx)
            return "oped", _normalize_extra_label(label)

    # 3) PV / Trailer / CM / Preview / Teaser
    trailer_patterns = [
        r"\b(PV)\s*0?(\d{0,3})\b",
        r"\b(PROMOTION\s*VIDEO)\b",
        r"\b(PROMO)\b",
        r"\b(TRAILER)\s*0?(\d{0,3})\b",
        r"\b(TEASER)\s*0?(\d{0,3})\b",
        r"\b(LOG)\s*0?(\d{0,3})\b",
        r"\b(TV\s*SPOT)\b",
        r"\b(PREVIEW)\s*0?(\d{0,3})\b",
        r"\b(CM)\s*0?(\d{0,3})\b",
        r"\b(CM\s*COLLECTION)\s*0?(\d{0,3})\b",
        r"\b(COMMERCIAL)\b",
        r"\b(ADVERTISEMENT)\b",
        r"\b(AD)\b",
    ]
    for p in trailer_patterns:
        m = re.search(p, s, re.I)
        if m:
            token = m.group(1)
            idx = m.group(2) if m.lastindex and m.lastindex >= 2 else ""
            label = token if not idx else _format_token_number(token, idx)
            normalized = _normalize_extra_label(label)
            upper = normalized.upper()
            if upper.startswith("CM"):
                return "cm", normalized
            if upper.startswith("PREVIEW") or upper.startswith("WEBPREVIEW"):
                return "preview", normalized
            if upper.startswith("TEASER"):
                return "teaser", normalized
            if upper.startswith("TRAILER"):
                return "trailer", normalized
            if upper.startswith("PV"):
                return "pv", normalized
            if upper.startswith("TVSPOT"):
                return "trailer", normalized
            return "trailer", normalized

    # 4) IV / MV / Interview / Making / BDExtra
    iv_patterns = [
        r"\b(IV)\s*0?(\d{0,3})\b",
    ]
    for p in iv_patterns:
        m = re.search(p, s, re.I)
        if m:
            token = m.group(1)
            idx = m.group(2) if m.lastindex and m.lastindex >= 2 else ""
            label = token if not idx else _format_token_number(token, idx)
            return "iv", _normalize_extra_label(label)

    mv_patterns = [
        r"\b(MV)\s*0?(\d{0,3})\b",
    ]
    for p in mv_patterns:
        m = re.search(p, s, re.I)
        if m:
            token = m.group(1)
            idx = m.group(2) if m.lastindex and m.lastindex >= 2 else ""
            label = token if not idx else _format_token_number(token, idx)
            return "mv", _normalize_extra_label(label)

    bdextra_patterns = [
        r"\b(BD\s*EXTRA)\b",
        r"\b(BDExtra)\b",
    ]
    for p in bdextra_patterns:
        m = re.search(p, s, re.I)
        if m:
            return "bdextra", _normalize_extra_label(m.group(1))

    # 5) Making / Interview
    making_patterns = [
        r"\b(INTERVIEW\S*)\b",
        r"\b(MAKING)\b",
        r"\b(BEHIND\s*THE\s*SCENES)\b",
        r"\b(STAFF\s*TALK)\b",
        r"\b(CAST\s*TALK)\b",
        r"\b(DOCUMENTARY)\b",
        r"\b(EVENT)\b",
        r"\b(MAKING\s*OF)\b",
        r"\b(SCENE)\b",
        r"\b(ROUGH\s*SKETCH)\b",
    ]
    for p in making_patterns:
        m = re.search(p, s, re.I)
        if m:
            label = _normalize_extra_label(m.group(1))
            if label.upper().startswith("INTERVIEW"):
                return "interview", label
            return "making", label

    return None


def _normalize_extra_label(text: str) -> str:
    s = re.sub(r"[_\-]+", " ", str(text or ""))
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\bNCOP\s*(\d+)\b", lambda m: _format_token_number("OP", m.group(1)), s, flags=re.I)
    s = re.sub(r"\bNCED\s*(\d+)\b", lambda m: _format_token_number("ED", m.group(1)), s, flags=re.I)
    s = re.sub(r"\bNCOP\b", "OP", s, flags=re.I)
    s = re.sub(r"\bNCED\b", "ED", s, flags=re.I)
    s = re.sub(r"\bOpening\b", "OP", s, flags=re.I)
    s = re.sub(r"\bEnding\b", "ED", s, flags=re.I)
    s = re.sub(r"\bCharacter\s*PV\b", "CharacterPV", s, flags=re.I)
    s = re.sub(r"\bWeb\s*Preview\b", "WebPreview", s, flags=re.I)
    s = re.sub(r"\bTV\s*Spot\b", "TVSpot", s, flags=re.I)
    s = re.sub(r"\bCommercial\b", "CM", s, flags=re.I)
    s = re.sub(r"\bCM\s*Collection\s*(\d{1,3}(?:\.\d)?)\b", lambda m: _format_token_number("CM", m.group(1)), s, flags=re.I)
    s = re.sub(r"\bCM\s*Collection\b", "CM", s, flags=re.I)
    s = re.sub(r"\bCM\s*(\d{1,3}(?:\.\d)?)\b", lambda m: _format_token_number("CM", m.group(1)), s, flags=re.I)
    s = re.sub(r"\bPV\s*EP?\s*(\d{1,3}(?:\.\d)?)\b", lambda m: _format_token_number("PVEP", m.group(1)), s, flags=re.I)
    s = re.sub(r"\bPV\s*(\d{1,3}(?:\.\d)?)\b", lambda m: _format_token_number("PV", m.group(1)), s, flags=re.I)
    s = re.sub(r"\bPreview\s*(\d{1,3}(?:\.\d)?)\b", lambda m: _format_token_number("Preview", m.group(1)), s, flags=re.I)
    s = re.sub(r"\bTrailer\s*(\d{1,3}(?:\.\d)?)\b", lambda m: _format_token_number("Trailer", m.group(1)), s, flags=re.I)
    s = re.sub(r"\bTeaser\s*(\d{1,3}(?:\.\d)?)\b", lambda m: _format_token_number("Teaser", m.group(1)), s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:60] if s else ""


def _format_token_number(prefix: str, raw: str) -> str:
    value = str(raw or "").strip()
    if "." in value:
        left, right = value.split(".", 1)
        if left.isdigit():
            return f"{prefix}{int(left):02d}.{right}"
    if value.isdigit():
        return f"{prefix}{int(value):02d}"
    return f"{prefix}{value}"


def _extract_extra_index(label: str | None) -> int | None:
    if not label:
        return None
    m = re.search(r"(\d{1,3})", label)
    if not m:
        return None
    val = int(m.group(1))
    return val if 1 <= val <= 999 else None


def _extract_season_episode_priority(text: str) -> tuple[int, int, tuple[int, int], str] | None:
    m = re.search(r"\bS(\d{1,2})\s*E(\d{1,3})\b", text, re.I)
    if m:
        return int(m.group(1)), int(m.group(2)), m.span(), "sxxeyy"

    m = re.search(r"\b(\d{1,2})\s*[xX]\s*(\d{1,3})\b", text)
    if m:
        return int(m.group(1)), int(m.group(2)), m.span(), "xxyy"

    m = re.search(r"\bEpisode\s*(\d{1,3})\b", text, re.I)
    if m:
        return 1, int(m.group(1)), m.span(), "episode"

    m = re.search(r"\bEP[_\-\s]*([0-9]{1,3})\b", text, re.I)
    if m:
        return 1, int(m.group(1)), m.span(), "ep"

    m = re.search(r"\bE\s*([0-9]{1,3})\b", text, re.I)
    if m:
        return 1, int(m.group(1)), m.span(), "e"

    m = re.search(r"(?<!\d)(\d{2})(?!\d)", text)
    if m:
        return 1, int(m.group(1)), m.span(), "two_digit"

    last_token = None
    for m in re.finditer(r"\b(\d{1,3})\b", text):
        val = int(m.group(1))
        if 1 <= val <= 999 and val not in {2160, 1080, 720, 480} and not (1900 <= val <= 2099):
            last_token = (1, val, m.span(), "number")
    return last_token


def _extract_season_hint(text: str) -> int | None:
    # Prefer explicit season hints (Sxx, Season xx, 1x01)
    se = _extract_season_episode_priority(text)
    if se is not None:
        season, _episode, _span, kind = se
        if kind in {"sxxeyy", "xxyy"}:
            return season
    return (
        _extract_season_keyword(text)
        or _extract_roman_season(text)
        or _extract_bang_season(text)
        or _extract_word_season(text)
        or _extract_trailing_season_number(text, structure_hint="tv")
    )


def _normalize_query(title: str) -> str:
    s = re.sub(r"\s+", " ", str(title or "")).strip()
    return s[:200]
