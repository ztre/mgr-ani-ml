"""识别解析器 v3：分层动漫正则匹配 + TMDB 搜索/缓存辅助函数。"""
from __future__ import annotations

import asyncio
import re
import time
from collections import OrderedDict, deque
from pathlib import Path
from threading import Lock
from typing import NamedTuple

import httpx

from ..api.logs import append_log
from ..config import settings
from .search_name_builder import build_search_name

PATTERNS = {
    "bracket_ep": r"\[(\d{1,3})(v\d+|[a-zA-Zβ]*)?\]",
    "sxe": r"[sS](\d{1,2})[eE](\d{1,3})",
    "season_word": r"(?:season|s)\s*(\d{1,2})",
    "roman_end": r"\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\s*$",
    "bang": r"!{2,4}\s*$",
    "trailing_num": r"(?:^|[\s._-])(\d{1,2})\s*$",
    "final_dir": r"\bfinal\s*season?\b",
}

SEARCH_NOISE_PATTERN = re.compile(
    r"\b(?:1080p|720p|2160p|x264|x265|h264|ma10p|ma444|hi10p|flac|aac|ac3|"
    r"web|webrip|bluray|bdrip|bdmv|dvdrip|remux|mawen1250|mysilu)\b",
    re.I,
)
FPS_TOKEN_PATTERN = re.compile(r"\b\d{1,3}(?:\.\d{1,3})?fps\b", re.I)

_LEADING_DATE_PREFIX_PATTERN = re.compile(
    r"^\s*(?:19\d{2}|20\d{2})(?:[.\-_/]\d{1,2}){1,2}[.\-_\s]+",
    re.I,
)
_TRAILING_GROUP_PATTERN = re.compile(r"\s*-\s*([A-Za-z0-9][A-Za-z0-9._\-\s]{1,48})$")
_NOISE_SUBTITLE_TOKEN_PATTERN = re.compile(
    r"\b(?:"
    r"vcb(?:-?studio)?|mawen\d*|mysilu|x26[45]|h26[45]|hevc|av1|"
    r"1080p|720p|2160p|480p|4k|ma10p|hi10p|flac|aac|ac3|dts|ddp\d?\.?\d?|"
    r"bluray|bdrip|bdmv|webrip|web[-\s]?dl|remux|sub(?:title)?s?|chs|cht|jpsc|jptc|raw|avc"
    r")\b",
    re.I,
)
_TITLE_NUMBER_PROTECTED_PATTERNS = [
    r"\beighty\s*six\b",
    r"\b86\b",
    r"\bsteins;?\s*gate\s*0\b",
    r"\b91\s*days\b",
    r"\bi\s*\+\s*ii\b",
    r"\b3\s*-?\s*gatsu\b",
    r"\btokyo\s*magnitude\s*8(?:[\s.]?0)\b",
    r"\b東京マグニチュード\s*8(?:[\s.]?0)\b",
]

SPECIAL_TYPE_MAP = {
    # 第 00 季池（OP / ED）
    "OP": ("season00", "OP"),
    "NCOP": ("season00", "OP"),
    "ED": ("season00", "ED"),
    "NCED": ("season00", "ED"),
    # 附加内容池（CM / Making / 活动等）
    "CM": ("extras", "CM"),
    "Making": ("extras", "Making"),
    "Event": ("extras", "Event"),
    "Interview": ("extras", "Interview"),
    "Trailer": ("extras", "Trailer"),
    "IV": ("extras", "IV"),
    # 预告片池
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
    version_tag: str | None = None
    season_hint_strength: str | None = None
    title_punctuation_hint: bool = False


def make_search_name(raw_name: str) -> str:
    return build_search_name(raw_name)


# ----- TMDB API protection/caching -----
_TMDB_RATE_LIMIT_PER_SEC = 3
_TMDB_CACHE_MAX = 512
_TMDB_CACHE_TTL_SECONDS = 86400
_TMDB_CALLS: deque[float] = deque(maxlen=16)
_TMDB_LOCK = Lock()
_TMDB_CACHE: OrderedDict[tuple, tuple[float, object]] = OrderedDict()
_NAME_EN_REFRESHED: set[int] = set()  # 防止同一 tv_id 在本进程内无限重拉 name_en 缓存

_BRACKET_TECH_TOKEN_RE = re.compile(
    r"^(?:"
    r"\d{3,4}[xX]\d{3,4}p?|"
    r"\d{3,4}p|"
    r"(?:hi|ma)\d{2,3}p|"
    r"x26[45]|h26[45]|hevc|av1|"
    r"\d{1,3}(?:\.\d+)?fps|"
    r"flac|aac|ac3|dts|ddp\d?\.?\d?|"
    r"bdrip|bluray|bdmv|webrip|web-?dl|remux|"
    r"chs|cht|jptsc?|raw"
    r")$",
    re.I,
)


def _extract_title_from_all_bracket_format(raw_stem: str) -> str | None:
    """从全括号格式中提取标题。

    这类文件在旧逻辑里会因为 `_preprocess()` 清掉全部方括号后得到空串，
    进而被 parser 直接判定失败。像 `[ANK&VCB-Studio][Code Geass][01][Hi10P]`
    这样的主视频正是这条路径触发的典型回归。

    这里只做非常保守的提取：
    1. 跳过第一个 token，因为它大概率是发布组。
    2. 跳过纯集号/版本号 token。
    3. 跳过分辨率、编码、音轨、字幕等技术 token。
    4. 在剩余 token 中选择最像标题的一项。
    """
    tokens = re.findall(r"\[([^\]]+)\]", raw_stem)
    if len(tokens) < 2:
        return None
    candidates: list[str] = []
    for i, token in enumerate(tokens):
        token = token.strip()
        if i == 0:
            continue
        if re.fullmatch(r"\d+v?\d*", token):
            continue
        if _BRACKET_TECH_TOKEN_RE.match(token):
            continue
        if re.search(r"[A-Za-z\u4e00-\u9fff\u3040-\u30ff]", token):
            candidates.append(token)
    return max(candidates, key=len) if candidates else None


def parse_tv_filename(filename: str, structure_hint: str | None = None) -> ParseResult | None:
    """分层动漫正则解析器，面向 TV 集号/季号提取。"""
    raw_stem = _stem(filename)
    clean = _preprocess(raw_stem)
    _all_bracket_format = False
    if not clean:
        clean = _extract_title_from_all_bracket_format(raw_stem) or ""
        if not clean:
            return None
        _all_bracket_format = True
    episode_text = _preprocess_for_episode(raw_stem)

    quality = _extract_quality(raw_stem)
    subtitle_lang = _extract_subtitle_lang(filename)
    year = _extract_year(raw_stem)

    extra_category, extra_label, extra_from_bracket = classify_extra_from_text(raw_stem)
    is_special = extra_category in {"special", "oped"}

    # 若 OAD/OVA/SP 等匹配自标题文本（非括号内），但文件同时带有纯数字括号集号
    # （含 [00]），说明 OAD/OVA/SP 是系列名称的一部分，而非特典标记
    if not extra_from_bracket and extra_category in {"special", "oped"}:
        if re.search(r"\[\d{1,3}\]", raw_stem):
            extra_category = None
            extra_label = None
            is_special = False

    # 检测 'SeriesTitle - DescriptiveSubtitle - NN' 内联短片模式（优先于集号解析）
    if extra_category is None:
        inline_result = _detect_inline_subtitle_extra(clean)
        if inline_result is not None:
            sub_label, sub_ep = inline_result
            season = _extract_explicit_season_hint_for_extra(episode_text) or 1
            title_parts = re.split(r"\s+-\s+", clean)
            title = _cleanup_title(title_parts[0]) if len(title_parts) >= 2 else _cleanup_title(clean)
            if not title:
                title = _cleanup_title(clean)
            return ParseResult(
                title,
                year,
                season,
                sub_ep,
                True,
                quality,
                "special",
                sub_label,
                subtitle_lang,
                False,
                False,
            )

    # 解析优先级：显式 EP/季号 > 罗马/!!/单词季号 > 尾部季号 > Final > 仅集号
    bracket_episode, bracket_suffix = extract_bracket_episode(raw_stem)
    has_special_episode_suffix = bool(bracket_suffix and not bracket_suffix.isdigit())

    se_preview = _extract_season_episode_priority(episode_text)
    if se_preview is not None:
        _season, _episode, _span, kind = se_preview
        explicit_season = _extract_season_keyword(episode_text)
        has_named_episode_token = bool(
            re.search(r"\b(?:Episode|EP|E)\s*\d{1,3}\b", episode_text, re.I)
            or re.search(r"\b\d{1,2}\s*[xX]\s*\d{1,3}\b", episode_text)
        )
        if kind in {"two_digit", "number"} and explicit_season is not None and not has_named_episode_token:
            se_preview = None
        # 标题数字（如 Steins;Gate 0 / 86）不要误当集号
        if kind in {"two_digit", "number"} and not is_title_number_safe(_cleanup_title(clean)):
            se_preview = None
        if se_preview is not None and (not extra_from_bracket) and kind in {"sxxeyy", "xxyy", "episode", "ep", "e"}:
            extra_category = None
            extra_label = None
            is_special = False

    # 仅在无强 TV 集号结构时才提前返回特典分支
    if extra_category is not None and extra_category != "iv" and not _has_strong_episode_structure(episode_text):
        season = _extract_explicit_season_hint_for_extra(episode_text) or 1
        episode = _extract_extra_index(extra_label) if extra_category in {"special", "oped"} else None
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

    # 模式 A：方括号显式集号（最高优先）
    if bracket_episode is not None and bracket_episode > 0:
        season_hint = _extract_season_hint(episode_text)
        roman_hint = _extract_roman_season(episode_text)
        pattern_a_hint_strength = "roman" if (roman_hint is not None and roman_hint == season_hint) else None
        if _all_bracket_format:
            title = clean
        else:
            title = _cleanup_title(_remove_bracket_episode(raw_stem))
            if not title:
                title = _cleanup_title(clean)
        version_tag = extract_bracket_version_tag(raw_stem)
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
            version_tag,
            pattern_a_hint_strength,
        )

    # 模式 B：SxxEyy 格式（强 TV 信号）
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

    # 模式 C：Season/S 关键字（强 TV 信号）
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

    # 模式 D：罗马数字季号
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

    # 模式 E：叹号季号（!! 动漫）— 弱提示，最终季号由 scanner 决策
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
            None,
            "bang",
            True,
        )

    # 模式 F：英文单词季号（first/second…）
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

    # 模式 G：尾部数字季号
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

    # 模式 H：Final Season 标记
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

    # 模式 I：仅集号（弱 TV 提示）
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

    # 兜底：返回清理后标题，无集号
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
    """电影文件名解析器，优先识别副标题分隔符。"""
    raw_stem = _stem(filename)
    clean = _preprocess(raw_stem)
    if not clean:
        return None

    quality = _extract_quality(raw_stem)
    subtitle_lang = _extract_subtitle_lang(filename)
    year = _extract_year(raw_stem)
    extra_category, extra_label, _extra_from_bracket = classify_extra_from_text(raw_stem)
    is_special = extra_category in {"special", "oped"}

    # 电影副标题模式：「主题 - 副题」（无强 TV 标记时）
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


async def search_tmdb_tv_candidates(title: str, year: int | None = None) -> list[dict]:
    """搜索 TMDB TV 候选，年份匹配优先排前但不排他。"""
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
    """搜索 TMDB 电影候选，年份匹配优先排前但不排他。"""
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
    """获取 TMDB TV 详情（季信息等）。同时附加英文季名以支持目录名匹配。"""
    if not settings.tmdb_api_key:
        return None

    cache_key = ("detail", "tv", int(tv_id))
    cached = _cache_get(cache_key)
    if cached is not None:
        valid_seasons = [s for s in (cached.get("seasons") or []) if (s.get("season_number") or 0) >= 1]
        if valid_seasons and not any(s.get("name_en") for s in valid_seasons) and tv_id not in _NAME_EN_REFRESHED:
            _NAME_EN_REFRESHED.add(tv_id)
            cached = None
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
    if not isinstance(data, dict) or not data:
        return None

    # 同时获取英文版本，将英文季名附加到各 season 条目中，供季名相似度匹配使用
    try:
        await _rate_limit_tmdb()
        params_en = {"api_key": settings.tmdb_api_key, "language": "en-US"}
        async with httpx.AsyncClient(timeout=20) as client:
            r_en = await client.get(url, params=params_en)
        if r_en.status_code == 200:
            data_en = r_en.json()
            if isinstance(data_en, dict):
                en_season_names: dict[int, str] = {}
                for s in data_en.get("seasons", []) or []:
                    snum = s.get("season_number")
                    sname = str(s.get("name") or "").strip()
                    if snum is not None and sname:
                        en_season_names[int(snum)] = sname
                # 将英文季名写入 season 条目的 name_en 字段
                for s in data.get("seasons", []) or []:
                    snum = s.get("season_number")
                    if snum is not None and int(snum) in en_season_names:
                        s["name_en"] = en_season_names[int(snum)]
    except Exception as exc:
        append_log(f"WARNING: en-US season names fetch failed for tv_id={tv_id}: {type(exc).__name__}: {exc}")

    _cache_set(cache_key, data)
    return data


def search_tmdb_tv_candidates_sync(title: str, year: int | None = None) -> list[dict]:
    """TMDB TV 候选搜索的同步封装。"""
    return asyncio.run(search_tmdb_tv_candidates(title, year))


def search_tmdb_movie_candidates_sync(title: str, year: int | None = None) -> list[dict]:
    """TMDB 电影候选搜索的同步封装。"""
    return asyncio.run(search_tmdb_movie_candidates(title, year))


def get_tmdb_tv_details_sync(tv_id: int, *, force_refresh: bool = False) -> dict | None:
    """TMDB TV 详情获取的同步封装。force_refresh=True 时先清除该条目的缓存。"""
    if force_refresh:
        cache_key = ("detail", "tv", int(tv_id))
        _TMDB_CACHE.pop(cache_key, None)
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
    s = _strip_leading_date_prefix(s)
    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.sub(r"\([^\)]*\)", " ", s)
    s = re.sub(r"\{[^\}]*\}", " ", s)

    tech_patterns = [
        r"\b(?:2160p|1080p|720p|480p|4k)\b",
        r"\b\d{1,3}(?:\.\d{1,3})?fps\b",
        r"\b\d{1,3}\.\d{1,3}fps\b",
        r"\b\d{1,3}fps\b",
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
    s = _strip_leading_date_prefix(s)
    s = re.sub(r"\[([^\]]*)\]", _bracket_repl, s)
    s = re.sub(r"\([^\)]*\)", " ", s)
    s = re.sub(r"\{[^\}]*\}", " ", s)

    tech_patterns = [
        r"\b(?:2160p|1080p|720p|480p|4k)\b",
        r"\b\d{1,3}(?:\.\d{1,3})?fps\b",
        r"\b\d{1,3}\.\d{1,3}fps\b",
        r"\b\d{1,3}fps\b",
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
    """提取方括号显式集号，如 [01]、[01a]、[01β]、[13v2]。

    返回 (集号, 后缀)，后缀为字母标记（如 'a'、'β'）。
    版本标记 'v2' 不作为后缀返回——调用 extract_bracket_version_tag() 单独获取。
    """
    text = str(name or "")
    for m in re.finditer(PATTERNS["bracket_ep"], text):
        raw_num = m.group(1)
        suffix = (m.group(2) or "").strip()
        if not raw_num.isdigit():
            continue
        ep = int(raw_num)
        if ep <= 0:
            return None, suffix or None
        # v\d+ 是版本标记，不作为集号后缀
        if suffix and re.match(r"^v\d+$", suffix, re.I):
            return ep, None
        if suffix and not suffix.isdigit():
            return ep, suffix
        return ep, None
    return None, None


def extract_bracket_version_tag(name: str) -> str | None:
    """从方括号集号标记（如 [13v2]）中提取版本标记 'v2'、'v3'。"""
    text = str(name or "")
    for m in re.finditer(PATTERNS["bracket_ep"], text):
        raw_num = m.group(1)
        suffix = (m.group(2) or "").strip()
        if not raw_num.isdigit():
            continue
        if suffix and re.match(r"^v\d+$", suffix, re.I):
            return suffix
    return None


def extract_bang_season(title: str) -> int | None:
    """公开封装：返回标题叹号对应的季号，如 K-ON!!→2，Gintama!!!→3。"""
    return _extract_bang_season(title)


def _remove_bracket_episode(text: str) -> str:
    return re.sub(PATTERNS["bracket_ep"], " ", str(text or ""))


def is_title_number_safe(cleaned_title: str) -> bool:
    """启发式保护：标题中的数字（如 'Steins;Gate 0'、'86'、'91 Days'）不被误判为季/集号。"""
    title = re.sub(r"\s+", " ", str(cleaned_title or "")).strip().lower()
    if not title:
        return True
    if re.fullmatch(r"\d+", title):
        return False
    for pattern in _TITLE_NUMBER_PROTECTED_PATTERNS:
        if re.search(pattern, title, re.I):
            return False
    if title.endswith(" 0") and "gate 0" in title:
        return False
    return True


def _extract_season_keyword(text: str) -> int | None:
    patterns = [
        r"\bS(\d{1,2})\b",
        r"\bSeason\s*(\d{1,2})\b",
        r"\b(\d{1,2})(?:st|nd|rd|th)\s*Season\b",
        r"第\s*(\d{1,2})\s*季",
        r"第\s*(\d{1,2})\s*期",
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
    s = re.sub(r"第\s*\d{1,2}\s*期", " ", s)
    s = re.sub(r"\b(?:the\s+)?final\s+season(?:\s+part\s*\d+)?\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:II|III|IV|V|VI|VII|VIII|IX|X)\b", " ", s, flags=re.I)
    s = re.sub(r"\b(first|second|third|fourth|fifth|sixth)\s+season\b", " ", s, flags=re.I)
    # TV Reproduction / TV Re-edit / TV Broadcast / Broadcast Version tokens.
    # Keep in sync with the tv_repro_patterns block in _match_extra_info() (section 0.7).
    s = re.sub(r"\bTV[\s\-]?Broadcast[\s\-]?Reproduction\b", " ", s, flags=re.I)
    s = re.sub(r"\bTV[\s\-]?Broadcast[\s\-]?Version\b", " ", s, flags=re.I)
    s = re.sub(r"\bTV[\s\-]?Reproduction\b", " ", s, flags=re.I)
    s = re.sub(r"\bTV[\s\-]?Re[\-\s]?edit\b", " ", s, flags=re.I)
    s = re.sub(r"\bTV[\s\-]?Special\b", " ", s, flags=re.I)
    s = re.sub(r"\bTV[\s\-]?Version\b", " ", s, flags=re.I)
    s = re.sub(r"\bBroadcast[\s\-]?Reproduction\b", " ", s, flags=re.I)
    s = re.sub(r"\bBroadcast[\s\-]?Version\b", " ", s, flags=re.I)
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
        r"(?:\b(?:Season|S)\s+(I|II|III|IV|V|VI|VII|VIII|IX|X)\b|\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\b)\s*$",
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
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
}


def _extract_word_season(text: str) -> int | None:
    # Numeric ordinal + Season (e.g. "2nd Season", "3rd Season") — check first
    m = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)\s+season\b", text, re.I)
    if m:
        val = int(m.group(1))
        if 1 <= val <= 20:
            return val
    m = re.search(r"\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\s+season\b", text, re.I)
    if not m:
        return None
    return WORD_SEASON_MAP[m.group(1).lower()]


def _extract_bang_season(title: str) -> int | None:
    m = re.search(PATTERNS["bang"], title)
    if not m:
        return None
    return len(m.group())


def _detect_skip_mainline_variant_label(name: str) -> str | None:
    """Return a normalized variant label for files that should be skipped.

    These files are usually duplicate/alternate cuts of a main episode and
    should not compete for the canonical mainline target path.
    """
    s = str(name or "")
    if not s:
        return None
    if re.search(r"\bNCOPED\b", s, re.I):
        return "NCOPED"
    if re.search(r"\bNC\s*Ver\b", s, re.I) and not re.search(r"\bNC(?:OP|ED)\b", s, re.I):
        return "NC Ver"

    m = re.search(
        r"\[\s*(?:EP?\s*)?\d{1,3}\s*(?:[-_ ]+|\()\s*"
        r"(director'?s\s*cut|extended\s*(?:cut|edition)|special\s*edition|uncut)"
        r"\.?\s*(?:\))?\s*\]",
        s,
        re.I,
    )
    if not m:
        return None

    raw = re.sub(r"\s+", " ", str(m.group(1) or "")).strip().rstrip(".")
    lowered = raw.lower()
    if lowered.startswith("director"):
        return "Director's Cut"
    if lowered.startswith("extended cut"):
        return "Extended Cut"
    if lowered.startswith("extended"):
        return "Extended Edition"
    if lowered.startswith("special"):
        return "Special Edition"
    if lowered.startswith("uncut"):
        return "Uncut"
    return raw or None


def _is_nc_ver_skip_file(name: str) -> bool:
    """Return True for files that should be entirely skipped (not scanned).

    Rules:
    - ``NCOPED`` — combined NC OP+ED file, unsupported
    - ``NC Ver`` when NOT part of NCOP / NCED prefix
    - explicit mainline duplicate cut tags like ``[25(Director's Cut)]``
    """
    return _detect_skip_mainline_variant_label(name) is not None


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
    title_part = s[: m.start(1)].strip(" ._-")
    if not title_part or not re.search(r"[A-Za-z\u4e00-\u9fff]", title_part):
        return None
    cleaned_title = _cleanup_title(title_part)
    if cleaned_title and not is_title_number_safe(cleaned_title):
        return None
    if val < 2 or val > 30:
        return None
    if val in {2160, 1080, 720, 480}:
        return None
    if 1900 <= val <= 2099:
        return None
    return val


def split_main_subtitle(title: str) -> tuple[str, str | None]:
    text = re.sub(r"\s+", " ", str(title or "")).strip()
    if not text:
        return "", None
    for sep in (":", "-"):
        if sep not in text:
            continue
        left, right = text.split(sep, 1)
        left = left.strip()
        right = right.strip()
        if not right:
            return left, None
        if _looks_like_noise_subtitle(right):
            return left, None
        return left, right
    return text, None


def _has_cjk(text: str) -> bool:
    return re.search(r"[\u4e00-\u9fff]", str(text or "")) is not None


def _looks_like_noise_subtitle(text: str) -> bool:
    s = re.sub(r"[\[\]\(\)\{\}_\.]+", " ", str(text or ""))
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return True
    if _NOISE_SUBTITLE_TOKEN_PATTERN.search(s):
        return True
    if len(s) <= 2 and not _has_cjk(s):
        return True
    tokens = [tok for tok in re.split(r"[\s\-]+", s) if tok]
    if tokens and all(_NOISE_SUBTITLE_TOKEN_PATTERN.search(tok) for tok in tokens):
        return True
    if not _has_cjk(s) and len(tokens) <= 2 and any(any(ch.isdigit() for ch in tok) for tok in tokens):
        return True
    return False


def _strip_trailing_release_group(text: str) -> str:
    s = str(text or "").strip()
    while True:
        m = _TRAILING_GROUP_PATTERN.search(s)
        if not m:
            break
        right = m.group(1).strip()
        if not _looks_like_noise_subtitle(right):
            break
        s = s[: m.start()].strip()
    return s


def _strip_leading_date_prefix(text: str) -> str:
    return _LEADING_DATE_PREFIX_PATTERN.sub("", str(text or "")).strip()


def _extract_episode_only(text: str, cleaned_title: str | None = None) -> int | None:
    text = _strip_fps_tokens(text)
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


def _has_strong_episode_structure(text: str) -> bool:
    bracket_ep, _suffix = extract_bracket_episode(text)
    if bracket_ep is not None and bracket_ep > 0:
        return True
    se = _extract_season_episode_priority(text)
    if se is None:
        return False
    _season, _episode, _span, kind = se
    return kind in {"sxxeyy", "xxyy", "episode", "ep", "e"}


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
    has_zh_tw = "繁中" in name_lower or "繁体" in name_lower or "繁體" in name_lower or has_token(["tc", "cht", "big5"])
    has_ja = "日文" in name_lower or "日语" in name_lower or has_token(["jp", "ja", "jpn", "jap", "japanese"])
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


_INLINE_SUBTITLE_STOP_WORDS = frozenset({
    "the", "a", "an", "in", "of", "on", "at", "to", "for", "and",
    "or", "but", "is", "are", "was", "were", "with", "by", "from",
    "up", "as", "into", "about", "that", "this", "its",
})


def _detect_inline_subtitle_extra(clean: str) -> tuple[str, int | None] | None:
    """检测 'SeriesTitle - DescriptiveSubtitle - NN' 内联字幕模式。

    适用场景: BD 附赠短片以系列名为前缀 + 短片标题 + 集号的命名方式，例如:
      'Violet Evergarden - Understanding Violet Evergarden in 5 Minutes - 01'

    检测条件:
      1. clean 末尾有 ' - NN' 裸集号 (1-99)
      2. 剩余部分有至少一个 dash 分隔 (至少两段)
      3. 末段 (subtitle) 有 ≥ 3 个词
      4. subtitle 中出现 ≥ 2 个来自 title 部分的实词 (len>2, 非停用词)
    """
    m = re.search(r"\s+-\s+(\d{1,3})\s*$", clean)
    if not m:
        return None
    ep_num = int(m.group(1))
    if not (1 <= ep_num <= 99):
        return None

    pre_ep = clean[: m.start()].strip()
    dash_parts = re.split(r"\s+-\s+", pre_ep)
    if len(dash_parts) < 2:
        return None

    subtitle_candidate = dash_parts[-1].strip()
    words = [w for w in subtitle_candidate.split() if w]
    if len(words) < 3:
        return None

    # subtitle 不能全是技术/季号标记
    tech_re = re.compile(
        r"^(?:S\d{1,2}|E\d{1,3}|Season|\d{3,4}p|x26[45]|hevc|flac|aac|bdrip|webrip|bluray|bd|dvd)$",
        re.I,
    )
    if all(tech_re.match(w) for w in words):
        return None

    # subtitle 中至少有 2 个实词与 title 部分重叠
    title_part = " ".join(dash_parts[:-1])

    def _content_words(s: str) -> set[str]:
        return {
            w.lower()
            for w in re.split(r"[\s\-_.,!?]+", s)
            if len(w) > 2 and w.lower() not in _INLINE_SUBTITLE_STOP_WORDS
        }

    overlap = _content_words(title_part) & _content_words(subtitle_candidate)
    if len(overlap) < 2:
        return None

    label = _normalize_extra_label(subtitle_candidate) or subtitle_candidate.strip()
    return label, ep_num


def classify_extra_from_text(text: str) -> tuple[str | None, str | None, bool]:
    """Return (category, label, from_bracket) for specials/oped/extras with bracket priority."""
    bracket_tokens = re.findall(r"\[([^\]]+)\]", text)
    if bracket_tokens:
        candidates: list[tuple[int, str, str]] = []
        for raw in bracket_tokens:
            normalized_raw = re.sub(r"\s+", " ", str(raw or "")).strip()
            if re.fullmatch(r"event", normalized_raw, re.I):
                candidates.append((20, "making", "Event"))
                continue
            if re.fullmatch(r"extra", normalized_raw, re.I):
                body_text = re.sub(r"\[[^\]]+\]", " ", str(text or ""))
                if re.search(r"\b(?:Live\s+)?Event\b", body_text, re.I):
                    candidates.append((20, "making", "Event"))
                    continue
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


def _is_release_group_phrase(text: str) -> bool:
    s = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not s:
        return False
    if re.search(r"[\u4e00-\u9fff]", s):
        return bool(re.search(r"(字幕组|字幕社|压制组|搬运组)$", s))
    normalized = re.sub(r"\b(?:[a-z]\.){2,}[a-z]?\b", lambda m: m.group(0).replace(".", ""), s)
    tokens = [tok for tok in re.split(r"[\s\-_.&+/]+", normalized) if tok]
    if not tokens:
        return False
    noise_patterns = [
        r"vcb(?:studio)?",
        r"mawen\d*",
        r"mysilu",
        r"nekomoe",
        r"kissaten",
        r"airota",
        r"dmhy",
        r"sub",
        r"fansub",
        r"raws?",
        r"studio",
    ]
    matched = 0
    for tok in tokens:
        if any(re.fullmatch(p, tok, flags=re.I) for p in noise_patterns):
            matched += 1
    if matched == len(tokens):
        return True
    raw_text = str(text or "")
    if re.search(r"[&+/]", raw_text):
        indicator_count = 0
        groupish_count = 0
        for tok in tokens:
            if re.fullmatch(r"(?:\d{3,4}p|\d{3,4}x\d{3,4}|\d+(?:\.\d+)?fps)", tok, flags=re.I):
                continue
            if re.search(r"(?:sub|studio|raws?|fansub|rec)$", tok, flags=re.I):
                indicator_count += 1
            if re.fullmatch(r"[a-z][a-z0-9]{1,19}|[a-z]{2,24}(?:sub|studio|rec)|[a-z]{2,10}\d*", tok, flags=re.I):
                groupish_count += 1
        if indicator_count >= 1 and groupish_count == len(tokens):
            return True
    return False


def _normalize_fallback_compare_text(text: str) -> str:
    s = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", str(text or ""))
    s = s.replace("_", " ").replace("-", " ").replace(".", " ")
    s = re.sub(r"[^\w\u4e00-\u9fff ]+", " ", s, flags=re.U)
    return re.sub(r"\s+", " ", s).strip().casefold()


def _looks_title_like_fallback_label(candidate: str, parsed_title: str | None) -> bool:
    cand = _normalize_fallback_compare_text(candidate)
    title = _normalize_fallback_compare_text(parsed_title or "")
    if not cand or not title:
        return False
    if len(cand) < 8:
        return False
    return cand in title or title in cand


def _score_fallback_bracket_candidate(raw: str, parsed_title: str | None = None) -> int:
    """Score a bracket candidate for fallback label selection.

    Lower score = better candidate (content label).
    Higher score = worse candidate (release group / tag).
    """
    s = str(raw or "").strip()
    score = 0
    if _is_release_group_phrase(s):
        score += 120
    # Release-group pattern: multi-name joined by '&' (e.g. CASO&Airota&VCB-Studio)
    if "&" in s:
        score += 100
    # All-uppercase short word with no space — bare group tag (e.g. CASO, BDMV)
    if re.fullmatch(r"[A-Z0-9]{1,8}", s):
        score += 50
    # All-letters no space — generic tag token
    elif re.fullmatch(r"[A-Za-z]+", s):
        score += 30
    if parsed_title and _looks_title_like_fallback_label(s, parsed_title):
        score += 80
    if re.search(
        r"\b(?:TVSP|TVSPOT|CMS?|PV|TRAILER|TEASER|PREVIEW|IV|MV|MAKING|COMMENTARY|"
        r"INSERT\s+SONG|STAGE\s+GREETING|SPOT|FLASH\s+ANIME|PICTURE\s+DRAMA|"
        r"OMAKE(?:\s+FLASH)?|MENU(?:JP|UK)?\s*\d*|MENU\s+VOL\.?\s*\d+(?:-\d+)?|"
        r"DEBATE\s+TOURNAMENT|"
        r"LOCATION\s+SCOUTING|CONCEPT\s+MOVIE|REVIEW\s+AVANT|LIVE\s+DUBBED|"
        r"STORYBOARD|DOCUMENTARY|MUSIC\s+CONCERT|CAST\s+COMMENTARY|PARTY)\b",
        s,
        re.I,
    ):
        score -= 25
    if re.search(r"\b(?:NCOP|NCED|OP|ED|SP|OVA|OAD|OAV)\s*0*\d{0,3}(?:[_\-\s]*EP\s*0*\d{1,3}(?:-\d{1,3})?)?\b", s, re.I):
        score -= 30
    # Content-label signals (rewards)
    if re.search(r"\d+\s*$", s):          # trailing number → episode/index
        score -= 20
    if " " in s:                           # multi-word phrase → descriptive label
        score -= 15
    if "'s" in s or "\u2019s" in s:       # possessive → proper noun
        score -= 10
    if re.search(r"\d", s) and re.search(r"[A-Za-z\u4e00-\u9fff]", s):
        score -= 10
    return score


def _extract_stem_based_extra_fallback_label(text: str) -> str | None:
    raw_stem = _stem(str(text or ""))
    if not raw_stem:
        return None
    stem = _strip_leading_date_prefix(raw_stem)
    stem = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", stem)
    stem = _strip_trailing_release_group(stem)
    stem = stem.replace("_", " ").replace(".", " ")
    stem = re.sub(r"\s+", " ", stem).strip(" -")
    if not stem:
        return None
    stem = re.sub(r"^\d{1,3}(?:\.\d+)?[\s._-]+", "", stem).strip()
    if not stem:
        return None

    menu_volume_match = re.search(r"\bVol\.?\s*(\d+(?:-\d+)?)\s*Menu\b", stem, re.I)
    if menu_volume_match:
        return _normalize_extra_label(f"Menu Vol {menu_volume_match.group(1)}")

    candidate_patterns = [
        r"\b(MagiRepo)\b",
        r"\b(MMR)\b$",
        r"\b(Menu(?:[A-Z]{2})?\s*Vol\.?\s*\d+(?:-\d+)?)\b",
        r"\b(Menu(?:[A-Z]{2})?\d+)\b",
        r"\b(Picture\s+Drama(?:\s*\d+)?)\b",
        r"\b(Omake(?:\s+flash)?\s*\d*)\b",
        r"\b(FD(?:\s+[A-Z0-9]+)+)\b$",
        r"\b([A-Za-z0-9][A-Za-z0-9'&]+(?:\s+[A-Za-z0-9][A-Za-z0-9'&]+){0,4}\s+Party(?:\s+\d{4})?)\b$",
        r"\b(\d{1,3}\.\d)\b",
        r"\b(Vol\.?\s*\d+(?:-\d+)?)\b",
        r"^(CV)$",
    ]
    for pattern in candidate_patterns:
        match = re.search(pattern, stem, re.I)
        if not match:
            continue
        label = _normalize_extra_label(match.group(1))
        if label and not _is_release_group_phrase(label):
            return label

    if re.search(r"\b(?:MENU|FD|PARTY|MMR|REPO)\b", stem, re.I):
        label = _normalize_extra_label(stem)
        if label and not _is_release_group_phrase(label):
            return label
    return None


def _looks_like_technical_extra_label(label: str | None) -> bool:
    probe = re.sub(r"[_\-]+", " ", str(label or ""))
    probe = re.sub(r"\s+", " ", probe).strip()
    if not probe:
        return False
    tokens = [tok for tok in probe.split(" ") if tok]
    if not tokens:
        return False
    allowed_numeric = re.compile(r"\d+(?:\.\d+)?")
    return all(_BRACKET_TECH_TOKEN_RE.fullmatch(tok) or allowed_numeric.fullmatch(tok) for tok in tokens)


def extract_strong_extra_fallback_label(text: str) -> str | None:
    """Best-effort readable label for strong special-dir context only.

    Release-group brackets are filtered out first. Among the remaining bracket
    candidates, the one with the lowest _score_fallback_bracket_candidate()
    score is returned.
    """
    raw_candidates = re.findall(r"\[([^\]]+)\]", str(text or ""))
    if not raw_candidates:
        return _extract_stem_based_extra_fallback_label(text)
    noise_only = re.compile(
        r"^(?:\d{1,4}p|x26[45]|h26[45]|hevc|av1|ma10p|hi10p|yuv\d+p?\d*|"
        r"flac(?:x\d+)?|aac|ac3|dts|ddp\d?\.?\d?|raw|vcb(?:-?studio)?|mawen\d*|mysilu|"
        r"jpsc|jptc|chs|cht|sc|tc|gb|big5|bd|dvd|webrip|web[-\s]?dl|bdrip|bluray|remux)+$",
        re.I,
    )
    parsed_title = None
    parsed_source = parse_tv_filename(str(text or ""))
    if parsed_source is not None:
        parsed_title = parsed_source.title
    stem_label = _extract_stem_based_extra_fallback_label(text)
    scored: list[tuple[int, str]] = []
    for raw in raw_candidates:
        cleaned = re.sub(r"\s+", " ", str(raw or "")).strip()
        if not cleaned:
            continue
        noise_probe = re.sub(r"[_\-]+", " ", cleaned)
        noise_probe = re.sub(r"\s+", " ", noise_probe).strip()
        if noise_only.match(noise_probe):
            continue
        noise_tokens = [tok for tok in noise_probe.split(" ") if tok]
        if noise_tokens and all(_BRACKET_TECH_TOKEN_RE.fullmatch(tok) for tok in noise_tokens):
            continue
        if re.fullmatch(r"[0-9]+\.[0-9]+", cleaned):
            scored.append((-5, cleaned))
            continue
        if re.fullmatch(r"[0-9]+", cleaned):
            continue
        if not re.search(r"[A-Za-z\u00c0-\u024f\u0370-\u03ff\u4e00-\u9fff]", cleaned):
            continue
        if _is_release_group_phrase(cleaned):
            continue
        normalized = _normalize_extra_label(cleaned)
        if not normalized:
            continue
        if _is_release_group_phrase(normalized):
            continue
        scored.append((_score_fallback_bracket_candidate(cleaned, parsed_title=parsed_title), normalized))
    if not scored:
        return stem_label
    best_score, best_label = min(scored, key=lambda x: x[0])
    if stem_label and (_looks_title_like_fallback_label(best_label, parsed_title) or _looks_like_technical_extra_label(best_label)):
        return stem_label
    if best_score >= 120:
        return stem_label
    return best_label


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

    m = re.search(r"\b(Web\s*Preview|Preview)\s*0?(\d{1,3})(?:[_\-\s]+(\d{1,3}))\b", s, re.I)
    if m:
        token = re.sub(r"\s+", "", str(m.group(1) or "")).strip() or "Preview"
        label = _format_token_number(token, m.group(2))
        if m.group(3):
            label = f"{label}_{int(m.group(3))}"
        return "preview", _normalize_extra_label(label)

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

    # 0.5) JP/CN next-episode preview tokens -> preview bucket
    jp_preview_patterns = [
        r"(予告)",
    ]
    for p in jp_preview_patterns:
        m = re.search(p, s)
        if m:
            return "preview", _normalize_extra_label(m.group(1))

    # 0.5b) JP/CN special-program tokens -> extras(making) bucket
    jp_making_patterns = [
        r"(TV\s*番組)",
        r"(テレビ番組)",
        r"(特集)",
        r"(総集編)",
        r"(總集編)",
        r"(特番)",
        r"(番外編)",
        r"(番外篇)",
        r"(アニクリ)",
    ]
    for p in jp_making_patterns:
        m = re.search(p, s, re.I)
        if m:
            return "making", _normalize_extra_label(m.group(1))

    # 0.7) TV Reproduction / TV Re-edit / TV Broadcast / Broadcast Version -> special
    # Order from most specific to least specific; bare "TV Version" is handled after oped.
    tv_repro_patterns = [
        r"\b(TV[\s\-]?Broadcast[\s\-]?Reproduction)\b",
        r"\b(TV[\s\-]?Broadcast[\s\-]?Version)\b",
        r"\b(TV[\s\-]?Reproduction)\b",
        r"\b(TV[\s\-]?Re[\-\s]?edit)\b",
        r"\b(TV[\s\-]?Special)\b",
        r"\b(Broadcast[\s\-]?Reproduction)\b",
        r"\b(Broadcast[\s\-]?Version)\b",
    ]
    for p in tv_repro_patterns:
        m = re.search(p, s, re.I)
        if m:
            return "special", _normalize_extra_label(m.group(1))

    # 0.8) Episode-like special variants often seen inside SP/Bonus dirs.
    m = re.search(r"\bSpecial\s+Preview\s+EP\s*0?(\d{1,3})\b", s, re.I)
    if m:
        return "special", _format_token_number("SP", m.group(1))

    episode_variant_patterns = [
        (r"\bNC(?:OP|ED)\s*0?\d{1,3}[_\-\s]*EP\s*0?\d{1,3}(?!\s*-\s*\d)\b", "oped"),
        (r"\bNC\s*Part\s*EP\s*0?\d{1,3}\b", "special"),
        (r"\bEP\s*0?\d{1,3}\s*\((?:[^)]*(?:Live\s*Dubbed|Dubbed|Commentary|NC\s*Ver\.?|On\s*Air\s*Ver\.?)\s*[^)]*)\)", "special"),
    ]
    for pattern, category in episode_variant_patterns:
        m = re.search(pattern, s, re.I)
        if m:
            return category, _normalize_extra_label(m.group(0))

    bdextra_episode_variant_patterns = [
        r"\bPreview[_\-\s]*EP\s*0?\d{1,3}\b",
        r"\b(?:Binaural\s+Commentary|Binaural\s+Sound)\s+EP\s*0?\d{1,3}\b",
        r"\bEyecatch\s+EP\s*0?\d{1,3}(?:-\d{1,3})?\b",
        r"\bEP\s*0?\d{1,3}\s+(?:Review\s+Avant|Information\s+Avant|Notice\s+Avant|[ABC][\s\-]?Part\s+Non[\s\-]?Credit\s+Ver\.?)\b",
        r"\bCommentary\s+On\s+EP\s*0?\d{1,3}\b",
        r"\b(?:NC[\s\-]?Avant|NC[\s\-]?Epilogue)\s*[_\-\s]*EP\s*0?\d{1,3}\b",
        r"\b(?:OP|ED)[_\-\s]*EP\s*0?\d{1,3}\s*\((?:[^)]*O\.?A\.?\s*Ver\.?[^)]*)\)",
    ]
    for pattern in bdextra_episode_variant_patterns:
        m = re.search(pattern, s, re.I)
        if m:
            return "bdextra", _normalize_extra_label(m.group(0))

    numbered_bdextra_patterns = [
        r"\bEvent\s*0?(\d{1,3})\b",
        r"\bMaking\s*0?(\d{1,3})\b",
        r"\bNC(?:OP|ED)\s*0?\d{1,3}[_\-\s]*EP\s*0?\d{1,3}-\d{1,3}\b",
    ]
    for pattern in numbered_bdextra_patterns:
        m = re.search(pattern, s, re.I)
        if m:
            return "bdextra", _normalize_extra_label(m.group(0))

    m = re.fullmatch(r"\s*(\d{1,3}\.\d)\s*", s)
    if m:
        return "special", m.group(1)

    # 1) Special
    raw_special = _extract_raw_special_label(s)
    if raw_special:
        return "special", _normalize_extra_label(raw_special)

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

    if re.search(r"\bLive\s+Event\b", s, re.I):
        return "making", "Event"

    # 2) OP / ED
    oped_patterns = [
        # NCED_EP04-05 区间格式（必须优先于简单 NCED 模式）
        r"\b(NCED)_EP\s*0?(\d{1,3})-(\d{1,3})\b",
        r"\b(NCOP)_EP\s*0?(\d{1,3})-(\d{1,3})\b",
        # NCED_EP04 单集格式
        r"\b(NCED)_EP\s*0?(\d{1,3})\b",
        r"\b(NCOP)_EP\s*0?(\d{1,3})\b",
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
            upper = token.upper()
            # 区间格式：NCED_EP04-05 → ED04-05
            if upper in ("NCED", "NCOP") and m.lastindex and m.lastindex >= 3:
                start_n = m.group(2)
                end_n = m.group(3)
                prefix = "ED" if upper == "NCED" else "OP"
                label = f"{prefix}{start_n.zfill(2)}-{end_n}"
                return "oped", _normalize_extra_label(label)
            idx = m.group(2) if m.lastindex and m.lastindex >= 2 else ""
            if upper.startswith("NCED"):
                label = "ED" if not idx else _format_token_number("ED", idx)
            elif upper.startswith("NCOP"):
                label = "OP" if not idx else _format_token_number("OP", idx)
            else:
                label = token if not idx else _format_token_number(token, idx)
            return "oped", _normalize_extra_label(label)

    # 2.5) TV Version — checked here, after the oped block, so that files whose name
    # contains both an OP/ED token and "TV Version" (e.g. TV-size OP/ED variants) are
    # already returned as "oped" above and never reach this branch.  All other compound
    # TV-reproduction phrases (TV Reproduction, TV Re-edit, …) are handled earlier in
    # section 0.7 because they are unambiguous regardless of context.
    m = re.search(r"\b(TV[\s\-]?Version)\b", s, re.I)
    if m:
        return "special", _normalize_extra_label(m.group(1))

    # 3) PV / Trailer / CM / Preview / Teaser
    trailer_patterns = [
        r"\b(PV)\s*0?(\d{0,3})\b",
        r"\b(PROMOTION\s*VIDEO)\b",
        r"\b(PROMO)\b",
        r"\b(PROMOTION\s*CLIP)\s*0?(\d{0,3})\b",
        r"\b(PROMO\s*CLIP)\s*0?(\d{0,3})\b",
        r"\b(PROMOTION\s*REEL)\s*0?(\d{0,3})\b",
        r"\b((?:PROMOTION|PROMO|REEL)\s*CLIP)\s*0?(\d{0,3})\b",
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
            if idx:
                label = _format_token_number(token, idx)
            else:
                label = _extract_extra_label_fragment(s, m) or token
            normalized = _normalize_extra_label(label)
            upper = normalized.upper()
            if upper.startswith("CM"):
                return "cm", normalized
            if upper.startswith("PREVIEW") or upper.startswith("WEBPREVIEW"):
                return "preview", normalized
            if upper.startswith("TEASER"):
                return "teaser", normalized
            if "PROMOTION CLIP" in upper or "PROMO CLIP" in upper or "PROMOTION REEL" in upper:
                return "trailer", normalized
            if upper.startswith("TRAILER"):
                return "trailer", normalized
            if upper.startswith("PV"):
                return "pv", normalized
            if upper.startswith("TVSPOT"):
                return "trailer", normalized
            return "trailer", normalized

    # 4) IV / MV / Interview / Making / BDExtra
    m = re.fullmatch(r"\s*(IV)(?:\s*0?(\d{1,3})(?:[_\-\s]+(\d{1,3}))?)?\s*", s, re.I)
    if m:
        token = m.group(1)
        if m.group(2):
            label = _format_token_number(token, m.group(2))
        else:
            label = token
        if m.group(3):
            label = f"{label}_{int(m.group(3))}"
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
    scene_label = _extract_scene_label(s)
    if scene_label:
        return "making", _normalize_extra_label(scene_label)

    making_patterns = [
        r"\b(INTERVIEW\S*)\b",
        r"\b(MAKING)\b",
        r"\b(BEHIND\s*THE\s*SCENES)\b",
        r"\b(STAFF\s*TALK)\b",
        r"\b(CAST\s*TALK)\b",
        r"\b(DOCUMENTARY)\b",
        r"\b(Talk\s*Event)\b",
        r"\b(MAKING\s*OF)\b",
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


def _extract_raw_special_label(text: str) -> str | None:
    s = str(text or "")
    if not s:
        return None
    for pattern in (
        r"\b(OAD)\s*[-_ ]*([0-9]{1,3}(?:[A-Za-z]{1,8})?)\b",
        r"\b(OVA)\s*[-_ ]*([0-9]{1,3}(?:[A-Za-z]{1,8})?)\b",
        r"\b(OAV)\s*[-_ ]*([0-9]{1,3}(?:[A-Za-z]{1,8})?)\b",
        r"\b(SP)\s*[-_ ]*([0-9]{1,3}(?:[A-Za-z]{1,8})?)\b",
    ):
        m = re.search(pattern, s, re.I)
        if m:
            return f"{m.group(1)}{m.group(2)}"
    for pattern in (r"\b(OAD)\b", r"\b(OVA)\b", r"\b(OAV)\b", r"\b(SPECIALS?)\b"):
        m = re.search(pattern, s, re.I)
        if m:
            token = m.group(1)
            return "SP" if token.upper().startswith("SPECIAL") else token
    return None


def _extract_scene_label(text: str) -> str | None:
    s = str(text or "")
    if not s:
        return None
    m = re.search(r"\b(Scene)\s*[-_ ]*([0-9]{1,3}(?:[A-Za-z]{1,8})?)\b", s, re.I)
    if m:
        token = f"{m.group(1)} {m.group(2)}"
        return re.sub(r"\s+", " ", token).strip()
    m = re.search(r"\b(Scene)\b", s, re.I)
    if m:
        return m.group(1)
    return None


def _extract_extra_label_fragment(source: str, match: re.Match) -> str | None:
    text = str(source or "")
    if not text:
        return None
    seg_start = text.rfind("-", 0, match.start())
    seg_end = text.find("-", match.end())
    if seg_start < 0:
        seg_start = 0
    else:
        seg_start += 1
    if seg_end < 0:
        seg_end = len(text)
    fragment = text[seg_start:seg_end]
    fragment = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", fragment)
    fragment = re.sub(r"\s+", " ", fragment).strip(" -_")
    if not fragment:
        return None
    return fragment[:60]


def _normalize_extra_label(text: str) -> str:
    s = re.sub(r"[_\-]+", " ", str(text or ""))
    s = re.sub(r"\s+", " ", s).strip()
    # NCED_EP04-05 → ED04-05 / NCOP_EP04-05 → OP04-05（区间格式，优先于简单 NCOP/NCED）
    s = re.sub(
        r"\bNCED_EP\s*0*(\d{1,3})-(\d{1,3})\b",
        lambda m: f"ED{m.group(1).zfill(2)}-{m.group(2)}",
        s,
        flags=re.I,
    )
    s = re.sub(
        r"\bNCOP_EP\s*0*(\d{1,3})-(\d{1,3})\b",
        lambda m: f"OP{m.group(1).zfill(2)}-{m.group(2)}",
        s,
        flags=re.I,
    )
    s = re.sub(
        r"\bNCED_EP\s*0*(\d{1,3})\b",
        lambda m: _format_token_number("ED", m.group(1)),
        s,
        flags=re.I,
    )
    s = re.sub(
        r"\bNCOP_EP\s*0*(\d{1,3})\b",
        lambda m: _format_token_number("OP", m.group(1)),
        s,
        flags=re.I,
    )
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


def _has_alpha_neighbor(text: str, span: tuple[int, int]) -> bool:
    left, right = span
    if left > 0 and text[left - 1].isalpha():
        return True
    if right < len(text) and text[right].isalpha():
        return True
    return False


def _extract_season_episode_priority(text: str) -> tuple[int, int, tuple[int, int], str] | None:
    text = _strip_fps_tokens(text)
    m = re.search(r"\bS(\d{1,2})\s*E(\d{1,3})\b", text, re.I)
    if m:
        season_val = int(m.group(1))
        episode_val = int(m.group(2))
        if episode_val <= 0:
            return None
        return season_val, episode_val, m.span(), "sxxeyy"

    m = re.search(r"\b(\d{1,2})\s*[xX]\s*(\d{1,3})\b", text)
    if m:
        return int(m.group(1)), int(m.group(2)), m.span(), "xxyy"

    m = re.search(r"\bEpisode\s*(\d{1,3})\b", text, re.I)
    if m:
        val = int(m.group(1))
        if val <= 0:
            return None
        return 1, val, m.span(), "episode"

    m = re.search(r"\bEP[_\-\s]*([0-9]{1,3})\b", text, re.I)
    if m:
        val = int(m.group(1))
        if val <= 0:
            return None
        return 1, val, m.span(), "ep"

    m = re.search(r"\bE\s*([0-9]{1,3})\b", text, re.I)
    if m:
        val = int(m.group(1))
        if val <= 0:
            return None
        return 1, val, m.span(), "e"

    m = re.search(r"(?<!\d)(\d{2})(?!\d)", text)
    if m and not _has_alpha_neighbor(text, m.span()):
        val = int(m.group(1))
        if val <= 0:
            return None
        return 1, val, m.span(), "two_digit"

    last_token = None
    for m in re.finditer(r"\b(\d{1,3})\b", text):
        val = int(m.group(1))
        if (
            1 <= val <= 999
            and val not in {2160, 1080, 720, 480}
            and not (1900 <= val <= 2099)
            and not _has_alpha_neighbor(text, m.span())
        ):
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


def _extract_explicit_season_hint_for_extra(text: str) -> int | None:
    # Extras should not infer season from loose trailing numbers like "... Promo Clip 2".
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
    )


def _normalize_query(title: str) -> str:
    s = re.sub(r"\s+", " ", str(title or "")).strip()
    return s[:200]


def _strip_fps_tokens(text: str) -> str:
    s = FPS_TOKEN_PATTERN.sub(" ", str(text or ""))
    return re.sub(r"\s+", " ", s).strip()
