"""识别流程 v2：目录级本地解析、统一竞争搜索、打分与 fallback。"""
from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

from .parser import (
    get_tmdb_tv_details_sync,
    is_title_number_safe,
    parse_movie_filename,
    parse_tv_filename,
    search_tmdb_movie_candidates_sync,
    search_tmdb_tv_candidates_sync,
)
from ..api.logs import append_log

MAX_RESULTS_PER_QUERY = 10
FALLBACK_MAX_ROUNDS = 2
PASS_SCORE = 0.7
FAIL_SCORE = 0.6
ROMAN_PATTERN = re.compile(r"\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\b", re.I)
SEASON_PATTERN = re.compile(r"\b(S\d+|Season\s*\d+|\d+(st|nd|rd|th)\s+Season)\b", re.I)
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


def parse_structure_locally(media_dir: Path, structure_hint: str | None = None) -> LocalParseSnapshot:
    raw_name = media_dir.name
    cleaned_name = _clean_name(raw_name)
    year_hint = _extract_year(cleaned_name)
    title_without_year = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", cleaned_name)
    title_without_year = re.sub(r"\s+", " ", title_without_year).strip()
    main_title, subtitle = _split_main_subtitle(title_without_year)

    tv_parse = parse_tv_filename(raw_name)
    season_hint = tv_parse.season if tv_parse else None
    episode_hint = tv_parse.episode if tv_parse else None
    special_hint = bool(tv_parse.is_special) if tv_parse else False
    final_hint = bool(tv_parse.final_season_hint) if tv_parse else False

    # 目录级解析：避免把 episode-only 误当季号
    if season_hint is not None and season_hint <= 0:
        season_hint = None

    # Directory-level season hint should not be inferred from episode-only parsing.
    if tv_parse and tv_parse.episode and (season_hint == 1) and not tv_parse.is_special:
        season_hint = None

    # If the parse yielded an episode-only trailing number, prefer treating it as a season hint for directories.
    if season_hint is None and episode_hint and not special_hint:
        trailing_season = _extract_trailing_season_number(cleaned_name)
        if trailing_season and trailing_season == episode_hint and is_title_number_safe(cleaned_name):
            season_hint = trailing_season
            episode_hint = None

    if season_hint:
        # 季号从标题中剥离，提升 TMDB 搜索准确度
        main_title = _strip_trailing_season_suffix(main_title, season_hint)

    if (season_hint is None or season_hint <= 0) and structure_hint == "tv":
        season_hint = _extract_trailing_season_number(cleaned_name)

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


def generate_candidate_titles(snapshot: LocalParseSnapshot) -> list[str]:
    candidates: list[str] = []
    # season_hint 存在时，为 TMDB 搜索构造季号变体
    if snapshot.season_hint:
        _push_candidate(candidates, f"{snapshot.main_title} season {snapshot.season_hint}")
        _push_candidate(candidates, f"{snapshot.main_title} S{snapshot.season_hint}")
        _push_candidate(candidates, f"{snapshot.cleaned_name} {snapshot.season_hint}")
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
    all_candidates = generate_candidate_titles(snapshot)
    if not all_candidates:
        return None, snapshot, 0

    rounds = [
        all_candidates[:3],
        all_candidates[3:5],
        all_candidates[5:],
    ]
    rounds = [x for x in rounds if x]

    best_global: RankedCandidate | None = None
    best_round = 0

    for round_idx, round_candidates in enumerate(rounds):
        if round_idx > FALLBACK_MAX_ROUNDS:
            break
        best = _unified_competitive_search(
            round_candidates,
            year_hint=snapshot.year_hint,
            special_hint=snapshot.special_hint,
            structure_hint=structure_hint,
            season_hint=snapshot.season_hint,
        )
        if best and (best_global is None or best.score > best_global.score):
            best_global = best
            best_round = round_idx
        if best and best.score >= PASS_SCORE:
            return best, snapshot, round_idx

    if best_global and best_global.score >= FAIL_SCORE:
        return best_global, snapshot, best_round
    return None, snapshot, min(best_round, FALLBACK_MAX_ROUNDS)


def resolve_target_media_type(scan_context_type: str, recognized_type: str) -> str:
    """
    说明书决策矩阵：
    - scan_context 不变
    - recognized_type 决定目标根路径
    """
    if recognized_type in {"tv", "movie"}:
        return recognized_type
    return "tv" if scan_context_type == "tv" else "movie"


def _unified_competitive_search(
    candidate_titles: list[str],
    year_hint: int | None,
    special_hint: bool,
    structure_hint: str | None = None,
    season_hint: int | None = None,
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

    if season_hint and structure_hint == "tv":
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

    if structure_hint == "tv" and best_tv and best_tv.score >= FAIL_SCORE:
        return best_tv
    if structure_hint == "movie" and best_movie and best_movie.score >= FAIL_SCORE:
        return best_movie

    if best_movie and best_tv:
        if best_movie.score >= best_tv.score * 0.9:
            return best_movie
        return best_tv

    return best_movie or best_tv


def recognize_directory_with_season_hint(
    media_dir: Path,
    scan_context_type: str,
    season_hint: int,
    structure_hint: str | None = None,
) -> RankedCandidate | None:
    """Run a season-aware re-search to improve season match."""
    snapshot = parse_structure_locally(media_dir, structure_hint=structure_hint)
    if not season_hint:
        season_hint = snapshot.season_hint or 0
    if not season_hint:
        return None
    candidates = []
    _push_candidate(candidates, f"{snapshot.main_title} season {season_hint}")
    _push_candidate(candidates, f"{snapshot.main_title} S{season_hint}")
    _push_candidate(candidates, f"{snapshot.cleaned_name} {season_hint}")
    if not candidates:
        return None
    return _unified_competitive_search(
        candidates,
        year_hint=snapshot.year_hint,
        special_hint=snapshot.special_hint,
        structure_hint=structure_hint or scan_context_type,
        season_hint=season_hint,
    )


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
                f"INFO: 使用 season_hint 匹配 TMDB 条目: title={cand.title}, tmdbid={cand.tmdb_id}, season={season_hint}"
            )


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
    )


def _clean_name(name: str) -> str:
    text = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", name)
    text = re.sub(r"[._\-]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _split_main_subtitle(title: str) -> tuple[str, str | None]:
    if ":" in title:
        left, right = title.split(":", 1)
        return left.strip(), right.strip() or None
    if "-" in title:
        left, right = title.split("-", 1)
        return left.strip(), right.strip() or None
    return title.strip(), None


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
    val = int(m.group(1))
    if val < 2 or val > 30:
        return None
    if val in {2160, 1080, 720, 480}:
        return None
    if 1900 <= val <= 2099:
        return None
    return val


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
