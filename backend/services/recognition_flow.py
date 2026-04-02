"""识别流程 v2：目录级本地解析、统一竞争搜索、打分与 fallback。"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path

from .anilist import search_anilist_english_title_sync
from .parser import (
    extract_bang_season,
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
ANILIST_FALLBACK_ROUND = FALLBACK_MAX_ROUNDS + 1
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
    season_hint_source: str | None = None   # explicit / bang / trailing / final
    season_hint_raw: str | None = None      # raw token: "S02", "!!", "第2季" etc.
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
    season_hint_source: str | None = None
    season_hint_raw: str | None = None
    episode_hint = tv_parse.episode if tv_parse else None
    special_hint = bool(tv_parse.is_special) if tv_parse else False
    final_hint = bool(tv_parse.final_season_hint) if tv_parse else False

    explicit_hint = _extract_explicit_season_hint(raw_name) or _extract_explicit_season_hint(cleaned_name)
    if explicit_hint:
        season_hint = explicit_hint
        season_hint_confidence = "high"
        season_hint_source = "explicit"
    elif tv_parse and tv_parse.season_hint_strength == "bang" and tv_parse.season:
        # bang 弱提示（!! / !!! 等）：保留候选季号，标记低置信度，以便后续 TMDB 校验
        season_hint = tv_parse.season
        season_hint_confidence = "low"
        season_hint_source = "bang"
        season_hint_raw = "!" * tv_parse.season
    elif tv_parse and tv_parse.season and tv_parse.season > 1 and not tv_parse.season_hint_strength:
        # parser.py 通过序数词（fourth season）、尾标数字等识别出的季号（非 bang、非显式标记）
        # 置信度 medium：比 bang 高但比 explicit 低，参与后续 TMDB 校验
        season_hint = tv_parse.season
        season_hint_confidence = "medium"
        season_hint_source = "parser"
    elif structure_hint == "tv":
        trailing_hint = _extract_trailing_season_number(cleaned_name)
        if trailing_hint:
            season_hint = trailing_hint
            season_hint_confidence = "low"
            season_hint_source = "trailing"

    if final_hint and not season_hint_source:
        season_hint_source = "final"

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
        season_hint_source=season_hint_source,
        season_hint_raw=season_hint_raw,
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
        append_log(
            f"WARNING: season_hint={season_hint} not in TMDB valid_seasons={sorted(available)}, fallback=1"
        )

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
        # 当 structure_hint 明确指定类型时，只有匹配该类型的结果才允许提前退出循环。
        # 这修复了 Chihayafuru I+II 类目录被误判为电影的问题：
        # 若第一轮搜索到高置信度电影，但 structure_hint="tv"，应继续尝试后续查询词
        # 以寻找 TV 结果，而非提前以电影结果退出。
        _early_ok = best and best.score >= _pass_score()
        if _early_ok and structure_hint in {"tv", "movie"} and best.media_type != structure_hint:
            _early_ok = False  # 当前轮次结果与 hint 不符，继续尝试后续查询词
        if _early_ok:
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

    anilist_best = _try_anilist_low_confidence_fallback(
        snapshot,
        scan_context_type,
        structure_hint=structure_hint,
    )
    if anilist_best and (best_global is None or anilist_best.score > best_global.score):
        best_global = anilist_best
        best_round = ANILIST_FALLBACK_ROUND

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


def _try_anilist_low_confidence_fallback(
    snapshot: LocalParseSnapshot,
    scan_context_type: str,
    *,
    structure_hint: str | None = None,
) -> RankedCandidate | None:
    if not getattr(settings, "anilist_fallback_enabled", True):
        return None
    target_media = structure_hint or scan_context_type
    if target_media not in {"tv", "movie"}:
        return None

    seed_title = build_search_name(snapshot.main_title or snapshot.cleaned_name)
    if not seed_title:
        return None

    append_log(f'INFO: AniList fallback triggered: query="{seed_title}"')
    alias_title, alias_source = search_anilist_english_title_sync(seed_title)
    if not alias_title:
        append_log("INFO: AniList fallback exhausted: no usable english title")
        return None

    normalized_alias = build_search_name(alias_title)
    if not normalized_alias:
        append_log("INFO: AniList fallback exhausted: normalized english title empty")
        return None
    if _normalize_for_similarity(normalized_alias) == _normalize_for_similarity(seed_title):
        append_log(
            f'INFO: AniList fallback skipped: alias unchanged -> "{normalized_alias}"'
        )
        return None

    append_log(
        f'INFO: AniList fallback english title: source={alias_source or "unknown"}, title="{normalized_alias}"'
    )
    candidate_titles = _build_anilist_candidate_titles(snapshot, normalized_alias, target_media=target_media)
    best = _unified_competitive_search(
        candidate_titles,
        year_hint=snapshot.year_hint,
        special_hint=snapshot.special_hint,
        structure_hint=target_media,
        season_hint=snapshot.season_hint,
        season_hint_confidence=snapshot.season_hint_confidence,
    )
    if best is None:
        append_log("INFO: AniList fallback exhausted: TMDB retry still empty")
        return None
    append_log(
        f"INFO: AniList fallback TMDB retry success: tmdbid={best.tmdb_id}, media={best.media_type}, score={best.score:.3f}"
    )
    return best


def _build_anilist_candidate_titles(
    snapshot: LocalParseSnapshot,
    alias_title: str,
    *,
    target_media: str,
) -> list[str]:
    candidates: list[str] = []
    season_hint = snapshot.season_hint if target_media == "tv" else None
    profile = build_search_name_profile(alias_title, season_hint)
    if target_media == "tv" and season_hint:
        for query in profile.season_aware:
            _push_candidate(candidates, query)
    _push_candidate(candidates, profile.primary)
    _push_candidate(candidates, profile.fallback)
    _push_candidate(candidates, alias_title)
    return candidates


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

    # 非动画类型惩罚：genre_ids 非空且不含 16（Animation）时降分
    genre_ids = item.get("genre_ids") or []
    genre_penalty = 0.0
    if genre_ids and 16 not in genre_ids:
        genre_penalty = -0.1
        score += genre_penalty

    score = max(0.0, min(1.0, score))

    if genre_penalty != 0.0:
        from ..api.logs import append_log as _alog
        _alog(
            f"INFO: genre_penalty={genre_penalty:+.1f} tmdbid={tmdb_id} "
            f"genres={genre_ids} title=\"{title}\""
        )

    return RankedCandidate(
        media_type=media_type,
        tmdb_id=int(tmdb_id),
        title=title,
        score=score,
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
    # 同时剥离罗马数字形式的季号后缀（如 season_hint=4 时剥除结尾的 " IV"）
    _roman_map_rev = {1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
                      6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X"}
    roman_str = _roman_map_rev.get(season_hint)
    if roman_str:
        s = re.sub(rf"(?:[\s._-]+){re.escape(roman_str)}\s*$", "", s)
    return s.strip() or title


def _push_candidate(out: list[str], value: str | None) -> None:
    if not value:
        return
    normalized = re.sub(r"\s+", " ", value).strip()
    if not normalized:
        return
    if normalized not in out:
        out.append(normalized)


_GENERIC_SEASON_NAME_RE = re.compile(r"^\s*Season\s*\d{1,2}\s*$", re.I)


def _clean_dir_name_for_match(dir_name: str) -> str:
    """清理目录名用于季名相似度匹配：去除 bracket 标签、编码组标签和分隔符。"""
    cleaned = re.sub(r"\[[^\]]*\]|\([^\)]*\)", " ", dir_name)
    cleaned = re.sub(r"[._\-]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _normalize_season_match_text(text: str) -> str:
    normalized = str(text or "")
    normalized = re.sub(r"([A-Za-z0-9])([×xX]{2,4})(?=$|[\s._\-:!/])", r"\1 \2", normalized)
    normalized = re.sub(r"([A-Za-z0-9])[×xX]\s*([2-9]|10)\b", r"\1 x\2", normalized)
    normalized = normalized.replace("×", "x")
    normalized = re.sub(r"[’'`]+", "", normalized)
    normalized = re.sub(r"[._\-:/]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip().lower()


def _extract_season_match_markers(text: str) -> set[str]:
    raw = str(text or "")
    normalized = _normalize_season_match_text(raw)
    markers: set[str] = set()

    explicit = _extract_explicit_season_hint(raw) or _extract_explicit_season_hint(normalized)
    if isinstance(explicit, int) and explicit > 1:
        markers.add(f"season:{explicit}")

    bang = extract_bang_season(raw)
    if isinstance(bang, int) and bang > 1:
        markers.add(f"season:{bang}")

    for match in re.finditer(r"\bx\s*([2-9]|10)\b", normalized, re.I):
        markers.add(f"season:{int(match.group(1))}")

    for match in re.finditer(r"\b(x{2,4})\b", normalized, re.I):
        markers.add(f"season:{len(match.group(1))}")

    for match in re.finditer(r"\b([2-9]|10)(st|nd|rd|th)\b", normalized, re.I):
        markers.add(f"ordinal:{int(match.group(1))}")

    if re.search(r"\bs\s*$", normalized, re.I):
        markers.add("suffix:s")

    return markers


def _extract_significant_match_tokens(text: str) -> set[str]:
    normalized = _normalize_season_match_text(text)
    if not normalized:
        return set()
    stopwords = {"season", "series", "the", "part", "final"}
    tokens: set[str] = set()
    for token in re.findall(r"[0-9a-z\u4e00-\u9fff]+", normalized, re.I):
        if token in stopwords:
            continue
        if len(token) >= 3 or re.search(r"[\u4e00-\u9fff]", token):
            tokens.add(token)
    return tokens


def _season_name_match_ratio(cleaned_lower: str, sname: str, dir_markers: set[str] | None = None) -> float:
    """计算目录名与单个季名的匹配率，含 substring bonus。

    策略：
    1. SequenceMatcher 基础相似度
    2. 若季名（去除通用前缀后）是目录名的子串，或目录名是季名的子串，给予 bonus
    3. 通用名称（'Season N'）上限降至 0.5
    """
    from difflib import SequenceMatcher

    dir_markers = dir_markers or set()
    is_generic = bool(_GENERIC_SEASON_NAME_RE.match(sname))
    sname_lower = _normalize_season_match_text(sname)
    ratio = SequenceMatcher(None, cleaned_lower, sname_lower).ratio()
    name_markers = _extract_season_match_markers(sname)

    # Substring bonus：若季名的核心部分（去除 "Season N" 等前缀）出现在目录名中
    # 例：目录="Kakegurui××"，季名="Kakegurui××" → 直接包含
    # 例：目录="Kobayashi-san Chi no Maid Dragon S"，季名="小林さんちのメイドラゴンS" → 无法匹配（语言不同，由 name_en 处理）
    if not is_generic:
        # 季名去除前导 "Series/Season N " 前缀
        core = re.sub(r"^\s*(?:Season|Series)\s*\d+\s*[:\-–]?\s*", "", sname, flags=re.I).strip()
        core_lower = _normalize_season_match_text(core)
        if core_lower and len(core_lower) >= 3:
            if core_lower in cleaned_lower or cleaned_lower in core_lower:
                # 直接包含：给予较强 bonus，确保超过阈值
                ratio = max(ratio, 0.72)
            else:
                # 部分重叠：计算包含率奖励
                overlap = sum(1 for c in core_lower if c in cleaned_lower)
                overlap_ratio = overlap / max(len(core_lower), 1)
                if overlap_ratio >= 0.85:
                    ratio = max(ratio, 0.60)

    dir_numeric = {marker for marker in dir_markers if marker.startswith("season:")}
    name_numeric = {marker for marker in name_markers if marker.startswith("season:")}
    if dir_numeric:
        if dir_numeric & name_numeric:
            ratio = max(ratio, 0.79)
        elif name_numeric and not (dir_numeric & name_numeric):
            ratio -= 0.18
        elif not is_generic:
            ratio -= 0.08
    elif name_numeric and not is_generic:
        # 目录本身没有季号标记时，不要让带明显季号后缀的续作标题
        # （如 Kakegurui×× / Durarara!!×2）反向吞掉基础作的 Season 1。
        # 这里直接压低到阈值以下，避免后续共享标题 token 奖励再次把它抬回去。
        ratio = min(ratio, 0.36)

    dir_has_suffix_s = "suffix:s" in dir_markers
    name_has_suffix_s = "suffix:s" in name_markers
    if dir_has_suffix_s:
        if name_has_suffix_s:
            ratio = max(ratio, 0.76)
        elif not name_numeric:
            ratio -= 0.12

    dir_ordinal = {marker for marker in dir_markers if marker.startswith("ordinal:")}
    name_ordinal = {marker for marker in name_markers if marker.startswith("ordinal:")}
    if dir_ordinal:
        if dir_ordinal & name_ordinal:
            ratio = max(ratio, 0.77)
        elif name_ordinal and not (dir_ordinal & name_ordinal):
            ratio -= 0.24
    elif name_ordinal:
        ratio -= 0.40

    # 通用名称上限降权
    if is_generic:
        ratio = min(ratio, 0.5)

    return max(0.0, min(ratio, 1.0))


def infer_season_from_tmdb_seasons(
    dir_name: str,
    seasons: list[dict],
    threshold: float = 0.55,
) -> tuple[int, float] | None:
    """通过 TMDB season name 与目录名的相似度，推导最可能的季号。

    返回 (season_number, ratio) 或 None。
    同时使用中文季名（name）和英文季名（name_en）进行匹配，取最优结果。
    通用名称（如 'Season 1'）的匹配上限降至 0.5，避免误匹配。
    """
    if not dir_name or not seasons:
        return None
    cleaned = _clean_dir_name_for_match(dir_name)
    cleaned_lower = _normalize_season_match_text(cleaned)
    dir_markers = _extract_season_match_markers(cleaned)
    dir_tokens = _extract_significant_match_tokens(cleaned)

    candidate_token_freq: dict[str, int] = {}
    season_names: dict[int, list[str]] = {}

    valid_season_count = 0

    for s in seasons:
        snum = s.get("season_number")
        if snum is None or int(snum) <= 0:
            continue
        valid_season_count += 1
        names: list[str] = []
        sname_zh = str(s.get("name") or "").strip()
        if sname_zh:
            names.append(sname_zh)
        sname_en = str(s.get("name_en") or "").strip()
        if sname_en and sname_en != sname_zh:
            names.append(sname_en)
        season_names[int(snum)] = names
        tokens = set()
        for name in names:
            tokens.update(_extract_significant_match_tokens(name))
        for token in tokens:
            candidate_token_freq[token] = candidate_token_freq.get(token, 0) + 1

    best_ratio: float = 0.0
    best_season: int | None = None

    for s in seasons:
        snum = s.get("season_number")
        if snum is None or int(snum) <= 0:
            continue
        snum = int(snum)

        # 同时比较中文季名和英文季名，取最高分
        candidate_names = season_names.get(snum, [])

        if not candidate_names:
            continue

        season_best = max(
            _season_name_match_ratio(cleaned_lower, sname, dir_markers=dir_markers)
            for sname in candidate_names
        )

        season_tokens = set()
        for name in candidate_names:
            season_tokens.update(_extract_significant_match_tokens(name))

        if dir_tokens and valid_season_count > 1:
            discriminative_dir_tokens = {
                token
                for token in dir_tokens
                if 0 < candidate_token_freq.get(token, 0) < valid_season_count
            }
            matched_discriminative = discriminative_dir_tokens & season_tokens
            missing_discriminative = discriminative_dir_tokens - season_tokens
            if matched_discriminative:
                matched_bonus = sum(0.02 * min(max(len(token), 2), 6) for token in matched_discriminative)
                season_best += min(0.18, matched_bonus)
            if missing_discriminative:
                missing_penalty = sum(0.02 * min(max(len(token), 2), 6) for token in missing_discriminative)
                season_best -= min(0.18, missing_penalty)
            season_best = max(0.0, min(season_best, 1.0))

        if season_best > best_ratio:
            best_ratio = season_best
            best_season = snum

    if best_season is not None and best_ratio >= threshold:
        return best_season, best_ratio
    return None


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


def _extract_title_embedded_season_hint(text: str) -> int | None:
    normalized = _normalize_season_match_text(_clean_name(str(text or "")))
    if not normalized:
        return None
    stop_tokens = {
        "season", "episode", "ep", "part", "movie", "ova", "oad",
        "sp", "special", "specials", "final",
    }
    for match in re.finditer(r"\bx\s*([2-9]|10)\b", normalized, re.I):
        prefix_tokens = re.findall(r"[0-9a-z\u4e00-\u9fff]+", normalized[: match.start()], re.I)
        suffix_tokens = re.findall(r"[0-9a-z\u4e00-\u9fff]+", normalized[match.end():], re.I)
        if not prefix_tokens or not suffix_tokens:
            continue
        prefix_tail = prefix_tokens[-1]
        suffix_head = suffix_tokens[0]
        if len(prefix_tail) < 2 and not re.search(r"[\u4e00-\u9fff]", prefix_tail):
            continue
        if suffix_head in stop_tokens:
            continue
        if len(suffix_head) < 2 and not re.search(r"[\u4e00-\u9fff]", suffix_head):
            continue
        return int(match.group(1))
    return None


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
    embedded_hint = _extract_title_embedded_season_hint(raw)
    if embedded_hint:
        return embedded_hint
    # 英文序数词季号（second season / third season 等）
    _ordinal_map = {
        "second": 2, "third": 3, "fourth": 4, "fifth": 5,
        "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10,
    }
    m = re.search(
        r"\b(second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\s+season\b",
        raw, re.I,
    )
    if m:
        return _ordinal_map.get(m.group(1).lower())
    if re.search(r"\bi\s*\+\s*ii\b", raw, re.I):
        return None
    # 剥离末尾括号标签（如 [Hi10p_1080p]）后再匹配罗马数字季号
    raw_stripped = raw
    while True:
        _m = re.search(r"\s*\[[^\]]+\]\s*$", raw_stripped)
        if not _m:
            break
        raw_stripped = raw_stripped[: _m.start()].strip()
    m = EXPLICIT_ROMAN_SEASON_PATTERN.search(raw_stripped)
    if not m:
        # Part 2~10 as a season hint (Part 1 is the default, no hint needed)
        m = re.search(r"\bPart\s+([2-9]|10)\b", raw, re.I)
        if m:
            return int(m.group(1))
        m = re.search(r"\bPart\s+(II|III|IV|V|VI|VII|VIII|IX|X)\b", raw, re.I)
        if m:
            roman_map_part = {"II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10}
            return roman_map_part.get(m.group(1).upper())
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
