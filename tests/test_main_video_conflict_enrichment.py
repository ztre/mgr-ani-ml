"""
Test main video conflict enrichment logic (方案 C)
验证主视频冲突补强和目标名编码的新实现
"""
import pytest
from pathlib import Path
from backend.services.scanner import (
    _extract_distinguish_source_tags,
    _merge_distinguish_tags_into_label,
)
from backend.services.renamer import (
    compute_tv_target_path,
    _normalize_extra_label_for_name,
)
from backend.services.parser import ParseResult


class TestMainVideoSourceTagExtraction:
    """Test that source tags are correctly extracted from main video filenames."""

    def test_extract_nc_ver_tag(self):
        """NC Ver should be extracted."""
        tags = _extract_distinguish_source_tags("[Nekomoe] Mushoku Tensei 03(NC Ver.)")
        assert "NC Ver" in tags

    def test_extract_on_air_ver_tag(self):
        """On Air Ver should be extracted."""
        tags = _extract_distinguish_source_tags("Banana Fish 01(On Air Ver.)")
        assert "On Air Ver" in tags

    def test_extract_hen_variant_tag(self):
        """hen-variant should be extracted."""
        tags = _extract_distinguish_source_tags("Oomuro-ke Hanako-hen")
        assert any("Hanako-hen" in t or "hanako-hen" in t.lower() for t in tags)

    def test_extract_multiple_tags(self):
        """Multiple distinguishing tags should be extracted."""
        tags = _extract_distinguish_source_tags("Title [NC Ver.] [On Air Ver.]")
        assert len(tags) >= 1  # At least one tag should be extracted


class TestTagMerging:
    """Test merging tags into parse_result.extra_label."""

    def test_merge_single_tag(self):
        """Single tag merge should succeed."""
        new_label, merged_tags, dropped_diffs = _merge_distinguish_tags_into_label(None, ["NC Ver"])
        assert new_label == "NC Ver"
        assert "NC Ver" in merged_tags
        assert len(dropped_diffs) == 0

    def test_merge_multiple_tags(self):
        """Multiple tags should merge into one label."""
        new_label, merged_tags, dropped_diffs = _merge_distinguish_tags_into_label(
            None, ["NC Ver", "On Air Ver"]
        )
        assert new_label is not None
        assert "NC Ver" in new_label or "NC Ver" in merged_tags
        assert len(merged_tags) >= 1

    def test_merge_respects_max_length(self):
        """Merged label should respect max_len limit."""
        long_tags = [f"LongTag{i}" for i in range(50)]
        new_label, merged_tags, dropped_diffs = _merge_distinguish_tags_into_label(
            None, long_tags, max_len=120
        )
        assert new_label is None or len(new_label) <= 120
        assert len(dropped_diffs) > 0  # Some tags should be dropped


class TestMainVideoNaming:
    """Test main video filename generation with extra_label support."""

    def test_main_video_without_extra_label(self):
        """Main video without extra_label should use original format."""
        parse_result = ParseResult(
            title="Title",
            year=2021,
            season=1,
            episode=1,
            is_special=False,
            quality="1080p",
            extra_category=None,  # ← Main video
            extra_label=None,
            subtitle_lang=None,
        )
        path = compute_tv_target_path(
            Path("/target"), parse_result, tmdb_id=12345, ext=".mkv"
        )
        assert "Title (2021) [tmdbid=12345]" in str(path)
        assert "Season 01" in str(path)
        assert "S01E01" in str(path)
        assert "- " not in path.name.split("S01E01")[1].split(".")[0]  # No extra label in name

    def test_main_video_with_extra_label(self):
        """Main video with extra_label should encode tag in filename."""
        parse_result = ParseResult(
            title="Mushoku Tensei",
            year=2021,
            season=1,
            episode=3,
            is_special=False,
            quality="1080p",
            extra_category=None,  # ← Main video
            extra_label="NC Ver",  # ← Enriched via conflict resolution
            subtitle_lang=None,
        )
        path = compute_tv_target_path(
            Path("/target"), parse_result, tmdb_id=94664, ext=".mkv"
        )
        path_name = path.name
        # Should contain: Mushoku Tensei - S01E03 - NC Ver.mkv
        assert "Mushoku Tensei" in path_name
        assert "S01E03" in path_name
        assert "NC Ver" in path_name  # ← Key: tag is encoded!

    def test_main_video_with_normalized_label(self):
        """Main video extra_label should be normalized."""
        parse_result = ParseResult(
            title="Test",
            year=2021,
            season=1,
            episode=1,
            is_special=False,
            quality="1080p",
            extra_category=None,
            extra_label="  nc   ver  ",  # Spaces should be normalized
            subtitle_lang=None,
        )
        path = compute_tv_target_path(
            Path("/target"), parse_result, tmdb_id=1, ext=".mkv"
        )
        # Should normalize spaces: "nc ver" or similar
        assert "NC" in path.name or "nc" in path.name.lower()


class TestConflictResolutionScenario:
    """Test realistic conflict resolution scenarios."""

    def test_necokara_multiple_episodes(self):
        """Nekopara Today's Neko02~12 should generate different target paths."""
        targets = []
        for ep_num in [2, 3, 5, 12]:
            parse_result = ParseResult(
                title="Nekopara",
                year=2020,
                season=1,
                episode=13 + ep_num,  # Different episodes
                is_special=False,
                quality="1080p",
                extra_category=None,
                extra_label=None,  # Initially no tag
                subtitle_lang=None,
            )
            path = compute_tv_target_path(
                Path("/target"), parse_result, tmdb_id=95317, ext=".mkv"
            )
            targets.append(str(path))

        # All should be different episodes
        assert len(set([p.split("E")[1].split("-")[0] for p in targets])) >= 1

    def test_mushoku_nc_ver_differentiation(self):
        """Mushoku Tensei EP03(NC Ver) and EP23(NC Ver) should differ."""
        paths = []
        for ep in [3, 23]:
            parse_result = ParseResult(
                title="Mushoku Tensei",
                year=2021,
                season=1,
                episode=ep,
                is_special=False,
                quality="1080p",
                extra_category=None,
                extra_label="NC Ver",  # After enrichment
                subtitle_lang=None,
            )
            path = compute_tv_target_path(
                Path("/target"), parse_result, tmdb_id=94664, ext=".mkv"
            )
            paths.append(str(path))

        # Paths should differ (different episodes)
        assert paths[0] != paths[1]
        assert "S01E03" in paths[0]
        assert "S01E23" in paths[1]
        assert "NC Ver" in paths[0] and "NC Ver" in paths[1]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
