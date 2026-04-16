from __future__ import annotations

from pathlib import Path

from backend.services.parser import ParseResult, _extract_subtitle_lang
from backend.services.target_path_resolver import _normalize_attachment_stem, build_attachment_target_from_anchor


def test_extract_subtitle_lang_supports_jap_suffix_variants():
    assert _extract_subtitle_lang("Movie.Jap.ass") == ".ja"
    assert _extract_subtitle_lang("Movie.Chs&Jap.ass") == ".zh-CN.ja"
    assert _extract_subtitle_lang("Movie.Cht&Jap.ass") == ".zh-TW.ja"
    assert _extract_subtitle_lang("Movie.JPSC.ass") == ".zh-CN.ja"
    assert _extract_subtitle_lang("Movie.JPTC.ass") == ".zh-TW.ja"


def test_attachment_follow_target_keeps_language_combinations_distinct():
    anchor = Path("/library/名侦探柯南：黑铁的鱼影 (2023) - 1080p.mkv")
    base_result = ParseResult(
        title="名侦探柯南：黑铁的鱼影",
        year=2023,
        season=1,
        episode=None,
        is_special=False,
        quality="1080p",
    )
    source_names = [
        "[SBSUB&VCB-Studio] Detective Conan M26 [Ma10p_1080p][x265_flac].Jap.ass",
        "[SBSUB&VCB-Studio] Detective Conan M26 [Ma10p_1080p][x265_flac].Cht.ass",
        "[SBSUB&VCB-Studio] Detective Conan M26 [Ma10p_1080p][x265_flac].Cht&Jap.ass",
        "[SBSUB&VCB-Studio] Detective Conan M26 [Ma10p_1080p][x265_flac].Chs.ass",
        "[SBSUB&VCB-Studio] Detective Conan M26 [Ma10p_1080p][x265_flac].Chs&Jap.ass",
    ]

    targets = []
    for source_name in source_names:
        parse_result = base_result._replace(subtitle_lang=_extract_subtitle_lang(source_name))
        target = build_attachment_target_from_anchor(anchor, parse_result, ".ass", src_path=Path(source_name))
        targets.append(target.name)

    assert len(targets) == len(set(targets))
    assert any(name.endswith(".ja.ass") for name in targets)
    assert any(name.endswith(".zh-TW.ass") for name in targets)
    assert any(name.endswith(".zh-TW.ja.ass") for name in targets)
    assert any(name.endswith(".zh-CN.ass") for name in targets)
    assert any(name.endswith(".zh-CN.ja.ass") for name in targets)


def test_normalize_attachment_stem_strips_chinese_language_names():
    # 中文语言名后缀（带点分隔）应被剥除，使 stem 与对应视频 stem 规范化后一致
    video_stem = "[BeanSub&FZSD&LoliHouse] Jigokuraku - 01 [WebRip 1080p HEVC-10bit AAC ASSx2]"
    assert _normalize_attachment_stem(f"{video_stem}.简体中文") == _normalize_attachment_stem(video_stem)
    assert _normalize_attachment_stem(f"{video_stem}.繁體中文") == _normalize_attachment_stem(video_stem)
    assert _normalize_attachment_stem(f"{video_stem}.繁体中文") == _normalize_attachment_stem(video_stem)
    assert _normalize_attachment_stem(f"{video_stem}.简中") == _normalize_attachment_stem(video_stem)
    assert _normalize_attachment_stem(f"{video_stem}.繁中") == _normalize_attachment_stem(video_stem)
    assert _normalize_attachment_stem(f"{video_stem}.中文") == _normalize_attachment_stem(video_stem)
    assert _normalize_attachment_stem(f"{video_stem}.日文") == _normalize_attachment_stem(video_stem)
    assert _normalize_attachment_stem(f"{video_stem}.日语") == _normalize_attachment_stem(video_stem)


def test_extract_subtitle_lang_identifies_chinese_full_names():
    assert _extract_subtitle_lang("Jigokuraku - 01 [WebRip 1080p HEVC-10bit AAC ASSx2].简体中文.ass") == ".zh-CN"
    assert _extract_subtitle_lang("Jigokuraku - 01 [WebRip 1080p HEVC-10bit AAC ASSx2].繁體中文.ass") == ".zh-TW"
    assert _extract_subtitle_lang("Jigokuraku - 01 [WebRip 1080p HEVC-10bit AAC ASSx2].繁体中文.ass") == ".zh-TW"


def test_normalize_media_stem_strips_channel_and_version_tags():
    from backend.services.target_path_resolver import _normalize_media_stem
    # 声道标和版本标应被剥除，使附件 stem 与视频 stem 能匹配
    assert _normalize_media_stem("Kanon 2006 EP01 Bluray 1920x1080p24 x264 Hi10P FLAC v2-mawen1250 6CH") == \
           _normalize_media_stem("Kanon 2006 EP01 Bluray 1920x1080p24 x264 Hi10P FLAC-mawen1250")
    # v3 版本标
    assert _normalize_media_stem("Title EP05 Hi10P FLAC v3") == _normalize_media_stem("Title EP05 Hi10P FLAC")
    # 5.1ch 声道标（点在 stem 规范化前保留，可命中 \b\d+(?:\.\d)?ch\b）
    assert _normalize_media_stem("Show EP05 FLAC 5.1ch") == _normalize_media_stem("Show EP05 FLAC")
    # 2ch
    assert _normalize_media_stem("Show 01 AAC 2ch") == _normalize_media_stem("Show 01 AAC")