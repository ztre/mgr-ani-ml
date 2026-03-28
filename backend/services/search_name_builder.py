from __future__ import annotations

import re
from dataclasses import dataclass

SEARCH_NOISE_PATTERN = re.compile(
    r"\b(?:1080p|720p|2160p|x264|x265|h264|ma10p|ma444|hi10p|flac|aac|ac3|"
    r"web|webrip|bluray|bdrip|bdmv|dvdrip|remux|mawen1250|mysilu)\b",
    re.I,
)
_LEADING_DATE_PREFIX_PATTERN = re.compile(
    r"^\s*(?:19\d{2}|20\d{2})(?:[.\-_/]\d{1,2}){1,2}[.\-_\s]+",
    re.I,
)
_TRAILING_GROUP_PATTERN = re.compile(r"\s*-\s*([A-Za-z0-9][A-Za-z0-9._\-\s]{1,48})$")
_SOURCE_TAG_PATTERN = re.compile(r"\[(?P<tag>VCB(?:-?Studio)?|Mawen\d*|Mysilu)\]", re.I)
_VERSION_TAG_PATTERN = re.compile(
    r"\b(?:ver\.?\s*\d+|v\d+|nc\s*ver\.?|on\s*air\s*ver\.?|true\s*birth\s*edition)\b",
    re.I,
)
_VERSION_NOISE_PATTERN = re.compile(
    r"\b(?:nc\s*ver\.?|on\s*air\s*ver\.?|true\s*birth\s*edition|creditless|uncensored|director(?:'s)?\s*cut)\b",
    re.I,
)


@dataclass(frozen=True)
class SearchNameProfile:
    primary: str
    fallback: str
    source_tags: tuple[str, ...]
    version_tags: tuple[str, ...]
    season_aware: tuple[str, ...]


def build_search_name(raw_name: str) -> str:
    profile = build_search_name_profile(raw_name)
    return profile.primary


def build_search_name_profile(raw_name: str, season_hint: int | None = None) -> SearchNameProfile:
    text = _preprocess(raw_name or "")
    source_tags = _extract_source_tags(raw_name)
    version_tags = _extract_version_tags(raw_name)
    fallback = re.sub(r"\s+", " ", text).strip()
    if not text:
        return SearchNameProfile("", fallback, source_tags, version_tags, ())
    text = _strip_leading_date_prefix(text)
    text = _strip_trailing_release_group(text)
    main, subtitle = _split_main_subtitle(text)
    if subtitle:
        text = f"{main} {subtitle}".strip()
    else:
        text = main
    text = SEARCH_NOISE_PATTERN.sub(" ", text)
    text = re.sub(r"\b\d{3,4}\s*x\s*\d{3,4}\s*p\s*\d{1,3}\b", " ", text, flags=re.I)
    text = re.sub(r"\b\d{3,4}\s*x\s*\d{3,4}\b", " ", text, flags=re.I)
    text = re.sub(r"\bx\s*\d{3,4}\b", " ", text, flags=re.I)
    text = re.sub(r"\bbd\b", " ", text, flags=re.I)
    text = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", text)
    text = re.sub(r"\bWEB\s+Preview", "Preview", text, flags=re.I)
    text = _VERSION_NOISE_PATTERN.sub(" ", text)
    text = re.sub(r"\b(?:season\s*\d{1,2}|\d{1,2}(?:st|nd|rd|th)\s+season)\b", " ", text, flags=re.I)
    if season_hint:
        text = re.sub(r"\b(?:I|II|III|IV|V|VI|VII|VIII|IX|X)\b\s*$", " ", text, flags=re.I)
    text = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    m = re.search(r"(?:^| )([2-9]|[12]\d|30)$", text)
    if m:
        prefix = text[: m.start(1)].strip()
        if not re.search(r"\b(?:season|s)\s*$", prefix, re.I):
            text = prefix
    season_queries: list[str] = []
    if text and season_hint:
        season_queries.extend([f"{text} season {season_hint}", f"{text} S{season_hint}", f"{text} {season_hint}"])
    if text and version_tags:
        season_queries.append(f"{text} {version_tags[0]}")
    if text and source_tags:
        season_queries.append(f"{text} {source_tags[0]}")
    uniq: list[str] = []
    seen: set[str] = set()
    for item in season_queries:
        norm = re.sub(r"\s+", " ", item).strip()
        if not norm:
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(norm)
    return SearchNameProfile(text, fallback, source_tags, version_tags, tuple(uniq))


def _preprocess(text: str) -> str:
    s = _strip_leading_date_prefix(text)
    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.sub(r"\([^\)]*\)", " ", s)
    s = re.sub(r"\{[^\}]*\}", " ", s)
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _strip_leading_date_prefix(text: str) -> str:
    return _LEADING_DATE_PREFIX_PATTERN.sub("", str(text or "")).strip()


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


def _split_main_subtitle(title: str) -> tuple[str, str | None]:
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


def _looks_like_noise_subtitle(text: str) -> bool:
    s = re.sub(r"[\[\]\(\)\{\}_\.]+", " ", str(text or ""))
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return True
    if SEARCH_NOISE_PATTERN.search(s):
        return True
    tokens = [tok for tok in re.split(r"[\s\-]+", s) if tok]
    if not re.search(r"[\u4e00-\u9fff]", s) and len(tokens) <= 2 and any(any(ch.isdigit() for ch in tok) for tok in tokens):
        return True
    return False


def _extract_source_tags(text: str) -> tuple[str, ...]:
    out: list[str] = []
    for m in _SOURCE_TAG_PATTERN.finditer(str(text or "")):
        tag = re.sub(r"\s+", " ", str(m.group("tag") or "")).strip()
        if tag:
            out.append(tag)
    return tuple(out)


def _extract_version_tags(text: str) -> tuple[str, ...]:
    out: list[str] = []
    for m in _VERSION_TAG_PATTERN.finditer(str(text or "")):
        tag = re.sub(r"\s+", " ", str(m.group(0) or "")).strip()
        if tag:
            out.append(tag)
    return tuple(out)
