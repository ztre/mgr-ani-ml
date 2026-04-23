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


def test_normalize_attachment_stem_strips_jap_and_eng_suffixes():
    from backend.services.target_path_resolver import _normalize_media_stem
    # .jap 应被剥除，使 stem 与视频一致
    video = "[A.I.R.nesSub&TxxZ&VCB-Studio] JoJo's Bizarre Adventure - Stardust Crusaders [25][Ma10p_1080p][x265_flac]"
    assert _normalize_attachment_stem(f"{video}.jap") == _normalize_attachment_stem(video)
    # .eng 应被剥除
    assert _normalize_attachment_stem(f"{video}.eng") == _normalize_attachment_stem(video)
    # .sc_v2 / .tc_v2（版本标跟在语言标后面）应被剥除
    bunny = "[DMG&VCB-Studio] Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai [01][Ma10p_1080p][x265_flac]"
    assert _normalize_attachment_stem(f"{bunny}.sc_v2") == _normalize_attachment_stem(bunny)
    assert _normalize_attachment_stem(f"{bunny}.tc_v2") == _normalize_attachment_stem(bunny)
    # 同时验证 _normalize_media_stem 也能剥除后缀变体中的 jap token（二次版本剥除路径）
    assert _normalize_media_stem(f"{bunny}.sc_v2") == _normalize_media_stem(bunny)


def test_attachment_target_does_not_carry_version_suffix():
    """附件目标名不应保留 v2/v3 版本标；视频目标已剥除版本，附件需与其一致。"""
    anchor = Path("/media/links/anime_tv/雪之少女 (2006) [tmdbid=34124]/Season 01/雪之少女 - S01E04.mkv")
    pr = ParseResult(title="雪之少女", year=2006, season=1, episode=4, is_special=False, quality=None)

    # v2 版本附件
    src_v2_ass = Path("Kanon 2006 EP04 Bluray 1920x1080p24 x264 Hi10P FLAC v2-mawen1250.ass")
    src_v2_mka = Path("Kanon 2006 EP04 Bluray 1920x1080p24 x264 Hi10P FLAC v2-mawen1250.6CH.mka")

    target_ass = build_attachment_target_from_anchor(anchor, pr, ".ass", src_path=src_v2_ass)
    target_mka = build_attachment_target_from_anchor(anchor, pr, ".mka", src_path=src_v2_mka)

    assert "v2" not in target_ass.name, f"附件名不应含 v2: {target_ass.name}"
    assert "v2" not in target_mka.name, f"附件名不应含 v2: {target_mka.name}"
    assert target_ass.stem == anchor.stem, f"附件 stem 应等于视频 stem: {target_ass.stem!r} vs {anchor.stem!r}"

    # v3 版本附件
    anchor_v3 = anchor.with_name("雪之少女 - S01E05.mkv")
    src_v3 = Path("Kanon 2006 EP05 Bluray 1920x1080p24 x264 Hi10P FLAC v3-mawen1250.ass")
    target_v3 = build_attachment_target_from_anchor(anchor_v3, pr._replace(episode=5), ".ass", src_path=src_v3)
    assert "v3" not in target_v3.name, f"附件名不应含 v3: {target_v3.name}"


def test_run_single_adjust_moves_companion_attachments(tmp_path):
    """季内调整时，配套附件应随视频一起重链接，且 DB 记录同步更新。"""
    from unittest.mock import MagicMock, patch

    from backend.api.media import _run_single_adjust, AdjustRequest
    from backend.models import InodeRecord, MediaRecord

    # --- 准备临时文件 ---
    src_dir = tmp_path / "source"
    src_dir.mkdir()
    video_file = src_dir / "[VCB] Show [01][1080p][x265_flac].mkv"
    ass_file   = src_dir / "[VCB] Show [01][1080p][x265_flac].sc.ass"
    mka_file   = src_dir / "[VCB] Show [01][1080p][x265_flac].6CH.mka"
    for f in (video_file, ass_file, mka_file):
        f.write_bytes(b"\x00" * 4)

    dst_dir = tmp_path / "links" / "Show (2024) [tmdbid=99999]" / "Season 01"
    dst_dir.mkdir(parents=True)
    old_video_dst = dst_dir / "Show - S01E01.mkv"
    old_video_dst.write_bytes(b"\x00" * 4)

    # --- 构造 MediaRecord ---
    video_row = MagicMock(spec=MediaRecord)
    video_row.id = 1
    video_row.sync_group_id = 10
    video_row.original_path = str(video_file)
    video_row.target_path = str(old_video_dst)
    video_row.type = "tv"
    video_row.tmdb_id = 99999

    att_ass_row = MagicMock(spec=MediaRecord)
    att_ass_row.id = 2
    att_ass_row.sync_group_id = 10
    att_ass_row.original_path = str(ass_file)
    att_ass_row.target_path = str(dst_dir / "Show - S01E01.zh-CN.ass")
    att_ass_row.file_type = "attachment"

    att_mka_row = MagicMock(spec=MediaRecord)
    att_mka_row.id = 3
    att_mka_row.sync_group_id = 10
    att_mka_row.original_path = str(mka_file)
    att_mka_row.target_path = str(dst_dir / "Show - S01E01.mka")
    att_mka_row.file_type = "attachment"

    # 按 original_path 精确查询
    path_to_row = {str(ass_file): att_ass_row, str(mka_file): att_mka_row}

    def db_query_side_effect(model):
        mock_q = MagicMock()
        if model is MediaRecord:
            def filter_fn(*args, **kwargs):
                inner = MagicMock()
                # first() 用于按 original_path 精确查附件
                def first_fn():
                    for arg in args:
                        # 萃取 BinaryExpression 里的右侧值（original_path == str(att_src)）
                        try:
                            right = str(arg.right.value)
                            return path_to_row.get(right)
                        except Exception:
                            pass
                    return None
                inner.first = first_fn
                inner.all.return_value = []
                return inner
            mock_q.filter.side_effect = filter_fn
        elif model is InodeRecord:
            mock_q.filter.return_value = MagicMock(first=MagicMock(return_value=None))
        else:
            mock_q.filter.return_value = MagicMock(first=MagicMock(return_value=None))
        return mock_q

    group = MagicMock()
    group.id = 10
    group.target = str(tmp_path / "links")

    db = MagicMock()
    db.query.side_effect = db_query_side_effect

    data = AdjustRequest(season=1, episode=2)  # 把第1集调整到第2集

    with patch("backend.api.media.append_log"):
        result = _run_single_adjust(db=db, row=video_row, group=group, data=data)

    assert result["has_issues"] is False, f"附件处理不应有失败: {result}"
    assert att_ass_row.status == "manual_fixed", "字幕附件 status 应被更新"
    assert att_mka_row.status == "manual_fixed", "音轨附件 status 应被更新"
    new_ass_target = att_ass_row.target_path
    assert "E02" in new_ass_target or "02" in Path(new_ass_target).name, (
        f"字幕附件目标路径应反映集号变更: {new_ass_target}"
    )
    db.commit.assert_called_once()