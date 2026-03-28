from __future__ import annotations

from dataclasses import dataclass

from .parser import ParseResult

CONTENT_EPISODE = "EPISODE"
CONTENT_SPECIAL = "SPECIAL"
CONTENT_EXTRA = "EXTRA"
CONTENT_ATTACHMENT = "ATTACHMENT"

SPECIAL_CATEGORIES = {"special", "oped"}
EXTRA_CATEGORIES = {
    "pv",
    "cm",
    "preview",
    "trailer",
    "teaser",
    "character_pv",
    "mv",
    "making",
    "interview",
    "bdextra",
}
ALL_EXTRA_LIKE_CATEGORIES = SPECIAL_CATEGORIES | EXTRA_CATEGORIES


@dataclass(frozen=True)
class ContentTypeInfo:
    content_type: str
    category: str | None


def classify_content_type(parse_result: ParseResult, is_attachment: bool) -> ContentTypeInfo:
    category = parse_result.extra_category
    if is_attachment:
        return ContentTypeInfo(CONTENT_ATTACHMENT, category)
    if category in SPECIAL_CATEGORIES:
        return ContentTypeInfo(CONTENT_SPECIAL, category)
    if category in EXTRA_CATEGORIES:
        return ContentTypeInfo(CONTENT_EXTRA, category)
    return ContentTypeInfo(CONTENT_EPISODE, None)
