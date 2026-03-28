"""识别流程 v2：目录级本地解析、统一竞争搜索、打分与 fallback。"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path

from .parser import (
    get_tmdb_tv_details_sync,
    is_title_number_safe,
    parse_tv_filename,
    search_tmdb_movie_candidates_sync,
    search_tmdb_tv_candidates_sync,
    split_main_subtitle,
)
from .search_name_builder import build_search_name, build_search_name_profile
from ..api.logs import append_log
from ..config import settings

MAX_RESULTS_PER_QUERY = 10
FALLBACK_MAX_ROUNDS = 2
DEFAULT_PASS_SCORE = 0.7
DEFAULT_FAIL_SCORE = 0.6
ROMAN_PATTERN = re.compile(r"\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\b", re.I)
SEASON_PATTERN = re.compile(r"\b(S\d+|Season\s*\d+|\d+(st|nd|rd|th)\s+Season)\b", re.I)
EXPLICIT_SEASON_PATTERN = re.compile(
    r"\b(?:S(?:eason)?\s*\d{1,2}|Season\s*\d{1,2}|\d{1,2}(?:st|nd|rd|th)\s+Season|第\s*\d{1,2}\s*季)\b",
    re.I,
)
EXPLICIT_ROMAN_SEASON_PATTERN = re.compile(
    r"(?:\bSeason\s*(I|II|III|IV|V|VI|VII|VIII|IX|X)\b|\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\s*$)",
    re.I,
)
SEASON_DETAIL_MAX_CANDIDATES = 5
SEASON_DETAIL_SCORE_DELTA = 0.1
SEASON_DETAIL_COUNT_THRESHOLD = 5
SEASON_DETAIL_BOOST = 0.2


@dataclass
class LocalParseSnapshot:
    raw_name: str
    cleaned_name: str
    main_title: str
    subtitle: str | None
    season_hint: int | None
    episode_hint: int | None
    year_hint: int | None
    special_hint: bool
    final_hint: bool
    season_hint_confidence: str | None = None
    season_aware_done: bool = False
    season_aware_had_candidates: bool = False
    season_aware_tried_queries: list[str] = field(default_factory=list)


@dataclass
class RankedCandidate:
    media_type: str
    tmdb_id: int | None
    title: str
    score: float
    popularity: float
    vote_count: int
    tmdb_data: dict
    year: int | None
    title_similarity: float = 0.0
    coverage_score: float = 0.0
    length_match_score: float = 0.0
    candidate_pool_size: int = 0


def parse_structure_locally(media_dir: Path, structure_hint: str | None = None) -> LocalParseSnapshot:
    raw_name = media_dir.name
    cleaned_name = _clean_name(raw_name)
    year_hint = _extract_year(cleaned_name)
    title_without_year = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", cleaned_name)
    title_without_year = re.sub(r"\s+", " ", title_without_year).strip()
    main_title, subtitle = split_main_subtitle(title_without_year)

    tv_parse = parse_tv_filename(raw_name, structure_hint=structure_hint or "tv")
    season_hint: int | None = None
    season_hint_confidence: str | None = None
    episode_hint = tv_parse.episode if tv_parse else None
    special_hint = bool(tv_parse.is_special) if tv_parse else False
    final_hint = bool(tv_parse.final_season_hint) if tv_parse else False

    explicit_hint = _extract_explicit_season_hint(raw_name) or _extract_explicit_season_hint(cleaned_name)
    if explicit_hint:
        season_hint = explicit_hint
        season_hint_confidence = "high"
    elif structure_hint == "tv":
        trailing_hint = _extract_trailing_season_number(cleaned_name)
        if trailing_hint:
            season_hint = trailing_hint
            season_hint_confidence = "low"

    # Explicitly avoid treating episode-only parse as season hint at directory level.
    if tv_parse and tv_parse.episode and (tv_parse.season == 1) and not tv_parse.is_special and season_hint_confidence != "high":
        season_hint = season_hint if season_hint_confidence == "low" else None

    if season_hint:
        # 季号从标题中剥离，提升 TMDB 搜索准确度
        main_title = _strip_trailing_season_suffix(main_title, season_hint)

    return LocalParseSnapshot(
        raw_name=raw_name,
        cleaned_name=cleaned_name,
        main_title=main_title or cleaned_name or raw_name,
        subtitle=subtitle,
        season_hint=season_hint,
        episode_hint=episode_hint,
        year_hint=year_hint,
        special_hint=special_hint,
        final_hint=final_hint,
        season_hint_confidence=season_hint_confidence,
    )


def resolve_season(tmdb_client, tmdbid: int, season_hint: int | None, final_hint: bool = False) -> int | None:
    if tmdbid is None:
        return None
    try:
        if tmdb_client is not None:
            details = tmdb_client.get_tv_details(tmdbid)
        else:
            details = get_tmdb_tv_details_sync(tmdbid)
    except Exception:
        details = None

    if not details or not isinstance(details, dict):
        return None

    seasons = [
        s
        for s in details.get("seasons", [])
        if isinstance(s, dict) and s.get("season_number") is not None and int(s.get("season_number")) > 0
    ]
    if not seasons:
        return None

    available = {int(s.get("season_number")) for s in seasons}

    if final_hint:
        for s in seasons:
            name = str(s.get("name") or "")
            overview = str(s.get("overview") or "")
            if re.search(r"\bfinal\b", name, re.I) or re.search(r"\bfinal\b", overview, re.I):
                return int(s.get("season_number"))
        return max(available)

    if season_hint and season_hint in available:
        return int(season_hint)

    if season_hint and season_hint not in available:
        append_log(f"WARNING: season_hint={season_hint} not in TMDB, fallback to 1")

    return 1


def generate_candidate_titles(snapshot: LocalParseSnapshot, include_season_variants: bool = True) -> list[str]:
    candidates: list[str] = []
    profile = build_search_name_profile(snapshot.main_title or snapshot.cleaned_name, snapshot.season_hint)
    search_name = profile.primary
    # season_hint 存在时，为 TMDB 搜索构造季号变体
    if include_season_variants and snapshot.season_hint:
        for query in profile.season_aware:
            _push_candidate(candidates, query)
    _push_candidate(candidates, search_name)
    _push_candidate(candidates, profile.fallback)
    _push_candidate(candidates, snapshot.cleaned_name)
    if snapshot.subtitle:
        _push_candidate(candidates, f"{snapshot.main_title} {snapshot.subtitle}")
    _push_candidate(candidates, snapshot.main_title)
    if snapshot.subtitle:
        _push_candidate(candidates, snapshot.main_title)
    without_year = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", snapshot.main_title)
    _push_candidate(candidates, without_year)
    without_special = re.sub(r"\b(special|extra|final)\b", " ", snapshot.main_title, flags=re.I)
    _push_candidate(candidates, without_special)
    return candidates


def recognize_directory_with_fallback(
    media_dir: Path,
    scan_context_type: str,
    structure_hint: str | None = None,
) -> tuple[RankedCandidate | None, LocalParseSnapshot, int]:
    """
    返回:
    - 识别结果（movie/tv 统一竞争后的最佳候选）
    - 本地解析快照
    - 使用的 fallback 轮次（0 表示首轮）
    """
    snapshot = parse_structure_locally(media_dir, structure_hint=structure_hint)
    search_name = build_search_name(snapshot.main_title or snapshot.cleaned_name)
    append_log(f'INFO: search_name="{search_name}"')
    fast_best: RankedCandidate | None = None
    fast_tried: list[str] = []
    fast_had_candidates = False
    if (
        bool(getattr(settings, "season_aware_research_enabled", True))
        and snapshot.season_hint
        and snapshot.season_hint_confidence == "high"
        and (structure_hint == "tv" or scan_context_type == "tv")
    ):
        fast_best, fast_tried, fast_had_candidates = recognize_directory_with_season_hint_trace(
            media_dir,
            scan_context_type,
            snapshot.season_hint,
            structure_hint=structure_hint,
        )
        snapshot.season_aware_done = True
        snapshot.season_aware_had_candidates = fast_had_candidates
        snapshot.season_aware_tried_queries = list(fast_tried)
        if fast_best and fast_best.score >= _pass_score():
            append_log(f"INFO: tried season-aware queries: {fast_tried}")
            append_log(
                f"INFO: final candidate: tmdbid={fast_best.tmdb_id}, media={fast_best.media_type}, score={fast_best.score:.3f}"
            )
            return fast_best, snapshot, 0

    append_log(f"INFO: tried season-aware queries: {fast_tried}")
    all_candidates = generate_candidate_titles(snapshot, include_season_variants=not snapshot.season_aware_done)
    if not all_candidates:
        append_log("INFO: final pending reason: no candidate titles")
        return None, snapshot, 0

    rounds = [
        all_candidates[:3],
        all_candidates[3:5],
        all_candidates[5:],
    ]
    rounds = [x for x in rounds if x]

    best_global: RankedCandidate | None = None
    best_round = 0
    if fast_best is not None:
        best_global = fast_best

    for round_idx, round_candidates in enumerate(rounds):
        if round_idx > FALLBACK_MAX_ROUNDS:
            break
        best = _unified_competitive_search(
            round_candidates,
            year_hint=snapshot.year_hint,
            special_hint=snapshot.special_hint,
            structure_hint=structure_hint,
            season_hint=snapshot.season_hint,
            season_hint_confidence=snapshot.season_hint_confidence,
        )
        if best and (best_global is None or best.score > best_global.score):
            best_global = best
            best_round = round_idx
        if best and best.score >= _pass_score():
            append_log(
                f"INFO: final candidate: tmdbid={best.tmdb_id}, media={best.media_type}, score={best.score:.3f}"
            )
            return best, snapshot, round_idx

    if best_global and _allow_soft_pass(best_global, snapshot):
        append_log(
            f"INFO: final candidate: tmdbid={best_global.tmdb_id}, media={best_global.media_type}, "
            f"score={best_global.score:.3f} (soft-pass)"
        )
        return best_global, snapshot, best_round
    if best_global and best_global.score >= _fail_score():
        append_log(
            f"INFO: final candidate: tmdbid={best_global.tmdb_id}, media={best_global.media_type}, score={best_global.score:.3f}"
        )
        return best_global, snapshot, best_round
    append_log("INFO: final pending reason: low confidence after fallback rounds")
    return None, snapshot, min(best_round, FALLBACK_MAX_ROUNDS)


def _unified_competitive_search(
    candidate_titles: list[str],
    year_hint: int | None,
    special_hint: bool,
    structure_hint: str | None = None,
    season_hint: int | None = None,
    season_hint_confidence: str | None = None,
) -> RankedCandidate | None:
    pool: list[RankedCandidate] = []
    seen: set[tuple[str, int]] = set()

    for title in candidate_titles:
        tv_items = search_tmdb_tv_candidates_sync(title, year_hint)[:MAX_RESULTS_PER_QUERY]
        movie_items = search_tmdb_movie_candidates_sync(title, year_hint)[:MAX_RESULTS_PER_QUERY]
        append_log(f"搜索候选: {title}, TV 结果: {len(tv_items)}, Movie 结果: {len(movie_items)}")
        for item in tv_items:
            cand = _to_ranked_candidate(title, "tv", item, year_hint)
            if cand and cand.tmdb_id is not None:
                key = (cand.media_type, cand.tmdb_id)
                if key not in seen:
                    seen.add(key)
                    pool.append(cand)

        for item in movie_items:
            cand = _to_ranked_candidate(title, "movie", item, year_hint)
            if cand and cand.tmdb_id is not None:
                key = (cand.media_type, cand.tmdb_id)
                if key not in seen:
                    seen.add(key)
                    pool.append(cand)

    if not pool:
        return None
    for cand in pool:
        cand.candidate_pool_size = len(pool)

    if season_hint and structure_hint == "tv" and season_hint_confidence == "high":
        _apply_season_hint_boost(pool, season_hint)

    # 特典内容优先留在 TV 语境，减少 TV 组误判电影。
    if special_hint:
        for cand in pool:
            if cand.media_type == "tv":
                cand.score = min(1.0, cand.score + 0.03)
    if structure_hint in {"tv", "movie"}:
        for cand in pool:
            if cand.media_type == structure_hint:
                cand.score = min(1.0, cand.score + 0.05)

    tv_pool = [x for x in pool if x.media_type == "tv"]
    movie_pool = [x for x in pool if x.media_type == "movie"]
    best_tv = max(tv_pool, key=lambda x: x.score) if tv_pool else None
    best_movie = max(movie_pool, key=lambda x: x.score) if movie_pool else None

    if structure_hint == "tv" and best_tv:
        return best_tv
    if structure_hint == "movie" and best_movie and best_movie.score >= _fail_score():
        return best_movie

    if best_movie and best_tv:
        return best_tv if best_tv.score >= best_movie.score else best_movie

    return best_movie or best_tv


def recognize_directory_with_season_hint_trace(
    media_dir: Path,
    scan_context_type: str,
    season_hint: int,
    structure_hint: str | None = None,
) -> tuple[RankedCandidate | None, list[str], bool]:
    """Run season-aware re-search and return (best, tried_queries)."""
    if not bool(getattr(settings, "season_aware_research_enabled", True)):
        return None, [], False
    snapshot = parse_structure_locally(media_dir, structure_hint=structure_hint)
    if not season_hint:
        season_hint = snapshot.season_hint or 0
    if not season_hint:
        return None, [], False

    profile = build_search_name_profile(snapshot.main_title or snapshot.cleaned_name, season_hint)
    search_name = profile.primary
    queries = list(profile.season_aware) or _build_season_aware_queries(search_name, season_hint)
    if not queries:
        return None, [], False

    first_pool = _collect_tv_candidates_from_queries(queries, snapshot.year_hint)
    had_candidates = bool(first_pool)
    best = _select_season_matched_candidate(first_pool, season_hint)
    if best is not None:
        return best, queries, had_candidates

    broad_queries: list[str] = []
    _push_candidate(broad_queries, search_name)
    _push_candidate(broad_queries, profile.fallback)
    broad_pool = _collect_tv_candidates_from_queries(broad_queries, snapshot.year_hint)
    had_candidates = had_candidates or bool(broad_pool)
    best = _select_season_matched_candidate(broad_pool, season_hint)
    if best is not None:
        return best, queries + broad_queries, had_candidates

    alias_queries = _build_alias_queries_from_pool(first_pool + broad_pool, season_hint)
    if alias_queries:
        append_log(f"INFO: re-search tried alternatives: {alias_queries}")
        second_pool = _collect_tv_candidates_from_queries(alias_queries, snapshot.year_hint)
        had_candidates = had_candidates or bool(second_pool)
        best = _select_season_matched_candidate(second_pool, season_hint)
        if best is not None:
            return best, queries + broad_queries + alias_queries, had_candidates

    return None, queries + broad_queries + alias_queries, had_candidates


def _apply_season_hint_boost(pool: list[RankedCandidate], season_hint: int) -> None:
    tv_pool = [x for x in pool if x.media_type == "tv" and x.tmdb_id is not None]
    if not tv_pool or not season_hint:
        return
    tv_pool.sort(key=lambda x: x.score, reverse=True)
    top_score = tv_pool[0].score
    # 仅在分数接近或候选很少时才去拉详情，避免过多 TMDB 请求
    should_check = len(tv_pool) <= SEASON_DETAIL_COUNT_THRESHOLD
    if not should_check:
        for cand in tv_pool[:SEASON_DETAIL_MAX_CANDIDATES]:
            if top_score - cand.score <= SEASON_DETAIL_SCORE_DELTA:
                should_check = True
                break
    if not should_check:
        return

    for cand in tv_pool[:SEASON_DETAIL_MAX_CANDIDATES]:
        if top_score - cand.score > SEASON_DETAIL_SCORE_DELTA and len(tv_pool) > SEASON_DETAIL_COUNT_THRESHOLD:
            continue
        details = get_tmdb_tv_details_sync(int(cand.tmdb_id))
        seasons = [
            int(s.get("season_number"))
            for s in details.get("seasons", [])
            if s.get("season_number") is not None
        ] if isinstance(details, dict) else []
        if season_hint in seasons:
            cand.score = min(1.0, cand.score + SEASON_DETAIL_BOOST)
            append_log(
                f'INFO: season_hint match -> use tmdbid={cand.tmdb_id} season={season_hint} for title="{cand.title}"'
            )


def _build_season_aware_queries(search_name: str, season_hint: int) -> list[str]:
    out: list[str] = []
    if not search_name or not season_hint:
        return out
    _push_candidate(out, f"{search_name} season {season_hint}")
    _push_candidate(out, f"{search_name} S{season_hint}")
    _push_candidate(out, f"{search_name} {season_hint}")
    return out


def _collect_tv_candidates_from_queries(queries: list[str], year_hint: int | None) -> list[RankedCandidate]:
    pool: list[RankedCandidate] = []
    seen: set[int] = set()
    for q in queries:
        tv_items = search_tmdb_tv_candidates_sync(q, year_hint)[:MAX_RESULTS_PER_QUERY]
        append_log(f"搜索候选: {q}, TV 结果: {len(tv_items)}, Movie 结果: 0")
        for item in tv_items:
            cand = _to_ranked_candidate(q, "tv", item, year_hint)
            if not cand or cand.tmdb_id is None:
                continue
            if int(cand.tmdb_id) in seen:
                continue
            seen.add(int(cand.tmdb_id))
            pool.append(cand)
    return pool


def _select_season_matched_candidate(pool: list[RankedCandidate], season_hint: int) -> RankedCandidate | None:
    if not pool:
        return None
    _apply_season_hint_boost(pool, season_hint)
    ranked = sorted(pool, key=lambda x: x.score, reverse=True)
    for cand in ranked[:SEASON_DETAIL_MAX_CANDIDATES]:
        details = get_tmdb_tv_details_sync(int(cand.tmdb_id))
        seasons = {
            int(s.get("season_number"))
            for s in (details.get("seasons", []) if isinstance(details, dict) else [])
            if s.get("season_number") is not None
        }
        if season_hint in seasons:
            append_log(
                f'INFO: season_hint match -> use tmdbid={cand.tmdb_id} season={season_hint} for title="{cand.title}"'
            )
            return cand
    return None


def _build_alias_queries_from_pool(pool: list[RankedCandidate], season_hint: int) -> list[str]:
    out: list[str] = []
    top = sorted(pool, key=lambda x: x.score, reverse=True)[:SEASON_DETAIL_MAX_CANDIDATES]
    for cand in top:
        details = get_tmdb_tv_details_sync(int(cand.tmdb_id))
        if not isinstance(details, dict):
            continue
        for alias in _extract_alias_titles(details):
            base = build_search_name(alias)
            for q in _build_season_aware_queries(base, season_hint):
                _push_candidate(out, q)
    return out


def _extract_alias_titles(details: dict) -> list[str]:
    aliases: list[str] = []
    aka = details.get("also_known_as")
    if isinstance(aka, list):
        for item in aka:
            if isinstance(item, str) and item.strip():
                aliases.append(item.strip())
    alt = details.get("alternative_titles")
    if isinstance(alt, dict):
        for item in alt.get("results", []) or []:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or item.get("name") or "").strip()
            if title:
                aliases.append(title)
    dedup: list[str] = []
    seen: set[str] = set()
    for x in aliases:
        key = x.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(x)
    return dedup


def _to_ranked_candidate(query_title: str, media_type: str, item: dict, year_hint: int | None) -> RankedCandidate | None:
    tmdb_id = item.get("id")
    if tmdb_id is None:
        return None

    if media_type == "tv":
        title = str(item.get("name") or "").strip()
        year = _extract_year(str(item.get("first_air_date") or ""))
    else:
        title = str(item.get("title") or "").strip()
        year = _extract_year(str(item.get("release_date") or ""))

    if not title:
        return None

    popularity = float(item.get("popularity") or 0.0)
    vote_count = int(item.get("vote_count") or 0)
    title_similarity = _best_multilang_similarity(query_title, item, media_type)
    coverage_score = _coverage_score(query_title, item, media_type)
    length_match_score = _length_match_score(query_title, item, media_type)
    heat_weight = _heat_weight(popularity, vote_count)

    score = (
        title_similarity * 0.6
        + coverage_score * 0.2
        + length_match_score * 0.2
        + heat_weight * 0.1
    )

    return RankedCandidate(
        media_type=media_type,
        tmdb_id=int(tmdb_id),
        title=title,
        score=max(0.0, min(1.0, score)),
        popularity=popularity,
        vote_count=vote_count,
        tmdb_data=item,
        year=year,
        title_similarity=title_similarity,
        coverage_score=coverage_score,
        length_match_score=length_match_score,
    )


def _clean_name(name: str) -> str:
    text = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", name)
    text = re.sub(r"^\s*(?:19\d{2}|20\d{2})(?:[.\-_/]\d{1,2}){1,2}[.\-_\s]+", " ", text, flags=re.I)
    text = re.sub(r"\s*-\s*(?:mawen\d*|vcb(?:-?studio)?|mysilu)\s*$", " ", text, flags=re.I)
    text = re.sub(r"[._\-]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _strip_trailing_season_suffix(title: str, season_hint: int) -> str:
    if not title or not season_hint:
        return title
    s = str(title)
    s = re.sub(rf"(?:\bSeason\s*{season_hint}\b|\b{season_hint}(?:st|nd|rd|th)\s+Season\b)\s*$", "", s, flags=re.I)
    s = re.sub(rf"(?:^|[\s._-]){season_hint}\s*$", "", s)
    return s.strip() or title


def _push_candidate(out: list[str], value: str | None) -> None:
    if not value:
        return
    normalized = re.sub(r"\s+", " ", value).strip()
    if not normalized:
        return
    if normalized not in out:
        out.append(normalized)


def _extract_year(text: str) -> int | None:
    m = re.search(r"(19\d{2}|20\d{2})", str(text))
    if not m:
        return None
    return int(m.group(1))


def _extract_trailing_season_number(text: str) -> int | None:
    s = re.sub(r"\s+", " ", str(text or "")).strip()
    m = re.search(r"(?:^|[\s._-])([2-9])\s*$", s)
    if not m:
        return None
    title_part = s[: m.start(1)].strip(" ._-")
    if title_part and not is_title_number_safe(title_part):
        return None
    val = int(m.group(1))
    if val < 2 or val > 30:
        return None
    if val in {2160, 1080, 720, 480}:
        return None
    if 1900 <= val <= 2099:
        return None
    return val


def _extract_explicit_season_hint(text: str) -> int | None:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return None
    m = re.search(r"\bS(?:eason)?\s*(\d{1,2})\b", raw, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"\bSeason\s*(\d{1,2})\b", raw, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)\s+Season\b", raw, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"第\s*(\d{1,2})\s*季", raw, re.I)
    if m:
        return int(m.group(1))
    if re.search(r"\bi\s*\+\s*ii\b", raw, re.I):
        return None
    m = EXPLICIT_ROMAN_SEASON_PATTERN.search(raw)
    if not m:
        return None
    token = (m.group(1) or m.group(2) or "").upper()
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
    return roman_map.get(token)


def _allow_soft_pass(best: RankedCandidate, snapshot: LocalParseSnapshot) -> bool:
    if best.score >= _pass_score():
        return True
    if best.score < _fail_score():
        return False
    if best.candidate_pool_size > 3:
        return False
    if best.title_similarity < 0.93:
        return False
    if best.coverage_score < 0.85:
        return False
    if snapshot.year_hint and best.year and int(snapshot.year_hint) != int(best.year):
        return False
    return True


def _pass_score() -> float:
    return float(getattr(settings, "recognition_pass_score", DEFAULT_PASS_SCORE))


def _fail_score() -> float:
    return float(getattr(settings, "recognition_fail_score", DEFAULT_FAIL_SCORE))


def _normalize_for_similarity(title: str) -> str:
    if not title:
        return ""
    title = ROMAN_PATTERN.sub("", title)
    title = SEASON_PATTERN.sub("", title)
    title = re.sub(r"\s+", " ", title)
    return title.strip().lower()


def _best_multilang_similarity(query_title: str, item: dict, media_type: str) -> float:
    candidates = []
    if media_type == "tv":
        candidates.append(item.get("name"))
        candidates.append(item.get("original_name"))
    else:
        candidates.append(item.get("title"))
        candidates.append(item.get("original_title"))

    candidates = [c.strip() for c in candidates if isinstance(c, str) and c.strip()]
    if not candidates:
        return 0.0

    normalized_query = _normalize_for_similarity(query_title)
    best = 0.0
    for c in candidates:
        sim1 = _title_similarity(query_title, c)
        sim2 = _title_similarity(normalized_query, _normalize_for_similarity(c))
        best = max(best, sim1, sim2)
    return best


def _title_similarity(left: str, right: str) -> float:
    l_norm = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", left).lower()
    r_norm = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", right).lower()
    if not l_norm or not r_norm:
        return 0.0
    return SequenceMatcher(a=l_norm, b=r_norm).ratio()


def _coverage_score(query_title: str, item: dict, media_type: str) -> float:
    candidates = []
    if media_type == "tv":
        candidates.extend([item.get("name"), item.get("original_name")])
    else:
        candidates.extend([item.get("title"), item.get("original_title")])
    texts = [x for x in candidates if isinstance(x, str) and x.strip()]
    if not texts:
        return 0.0

    q_tokens = set(re.findall(r"[0-9A-Za-z\u4e00-\u9fff]+", _normalize_for_similarity(query_title)))
    if not q_tokens:
        return 0.0

    best = 0.0
    for text in texts:
        c_tokens = set(re.findall(r"[0-9A-Za-z\u4e00-\u9fff]+", _normalize_for_similarity(text)))
        if not c_tokens:
            continue
        cov = len(q_tokens & c_tokens) / max(1, len(q_tokens))
        best = max(best, cov)
    return max(0.0, min(1.0, best))


def _length_match_score(query_title: str, item: dict, media_type: str) -> float:
    candidates = []
    if media_type == "tv":
        candidates.extend([item.get("name"), item.get("original_name")])
    else:
        candidates.extend([item.get("title"), item.get("original_title")])
    texts = [x for x in candidates if isinstance(x, str) and x.strip()]
    if not texts:
        return 0.0

    q = re.sub(r"\s+", "", _normalize_for_similarity(query_title))
    if not q:
        return 0.0
    q_len = len(q)
    best = 0.0
    for text in texts:
        c = re.sub(r"\s+", "", _normalize_for_similarity(text))
        if not c:
            continue
        c_len = len(c)
        ratio = 1.0 - abs(q_len - c_len) / max(q_len, c_len)
        best = max(best, ratio)
    return max(0.0, min(1.0, best))


def _heat_weight(popularity: float, vote_count: int) -> float:
    pop_w = min(max(popularity, 0.0) / 100.0, 1.0)
    vote_w = min(max(vote_count, 0) / 1000.0, 1.0)
    return (pop_w * 0.7) + (vote_w * 0.3)
