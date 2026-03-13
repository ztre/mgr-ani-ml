"""识别流程 v2：目录级本地解析、统一竞争搜索、打分与 fallback。"""
from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

from .parser import (
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
    )


def generate_candidate_titles(snapshot: LocalParseSnapshot) -> list[str]:
    candidates: list[str] = []
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
    m = re.search(r"(?:^|[\\s._-])(\\d{1,2})\\s*$", s)
    if not m:
        return None
    val = int(m.group(1))
    if val < 1 or val > 30:
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
