"""
Season 识别链路回归测试（unittest 格式，无需 pytest）

运行方式：
    python3 -m unittest tests.test_season_pipeline -v
"""
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

# mock 外部依赖
for _mod in ("httpx", "sqlalchemy", "sqlalchemy.orm", "sqlalchemy.exc",
             "sqlalchemy.dialects", "sqlalchemy.dialects.sqlite",
             "fastapi", "pydantic", "apscheduler",
             "apscheduler.schedulers", "apscheduler.schedulers.background", "openai"):
    sys.modules.setdefault(_mod, MagicMock())


def _dir(name: str) -> Path:
    return Path("/fake") / name


def _path_with_parts(*parts: str) -> Path:
    p = Path("/fake")
    for part in parts:
        p = p / part
    return p


# ===========================================================================
# 一、parse_tv_filename() bang 检测
# ===========================================================================

class TestParseTvFilenameBang(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.parser import parse_tv_filename, extract_bang_season
        cls.parse_tv_filename = staticmethod(parse_tv_filename)
        cls.extract_bang_season = staticmethod(extract_bang_season)

    def test_k_on_double_bang_season(self):
        r = self.parse_tv_filename("K-ON!!", structure_hint="tv")
        self.assertIsNotNone(r)
        self.assertEqual(r.season, 2)
        self.assertEqual(r.season_hint_strength, "bang")

    def test_gintama_triple_bang_season(self):
        r = self.parse_tv_filename("Gintama!!!", structure_hint="tv")
        self.assertIsNotNone(r)
        self.assertEqual(r.season, 3)
        self.assertEqual(r.season_hint_strength, "bang")

    def test_extract_bang_helper(self):
        self.assertEqual(self.extract_bang_season("K-ON!!"), 2)
        self.assertEqual(self.extract_bang_season("Gintama!!!"), 3)
        self.assertIsNone(self.extract_bang_season("Normal Title"))


# ===========================================================================
# 二、parse_structure_locally() — bang 候选进入 snapshot
# ===========================================================================

class TestParseStructureLocallyBang(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.recognition_flow import parse_structure_locally
        cls.psl = staticmethod(parse_structure_locally)

    def test_bang_preserved_in_snapshot(self):
        snap = self.psl(_dir("K-ON!!"), structure_hint="tv")
        self.assertEqual(snap.season_hint, 2,
                         f"bang 候选 season=2 丢失，实际={snap.season_hint}")
        self.assertEqual(snap.season_hint_source, "bang")
        self.assertEqual(snap.season_hint_confidence, "low")

    def test_bang_raw_field(self):
        snap = self.psl(_dir("K-ON!!"), structure_hint="tv")
        self.assertIsNotNone(snap.season_hint_raw)
        self.assertIn("!", snap.season_hint_raw)

    def test_explicit_overrides_bang(self):
        snap = self.psl(_dir("Show S2!!"), structure_hint="tv")
        self.assertEqual(snap.season_hint, 2)
        self.assertEqual(snap.season_hint_source, "explicit")
        self.assertEqual(snap.season_hint_confidence, "high")


# ===========================================================================
# 三、parse_structure_locally() — 显式季号
# ===========================================================================

class TestParseStructureLocallyExplicit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.recognition_flow import parse_structure_locally
        cls.psl = staticmethod(parse_structure_locally)

    def test_season_keyword(self):
        snap = self.psl(_dir("Attack on Titan Season 2"), structure_hint="tv")
        self.assertEqual(snap.season_hint, 2)
        self.assertEqual(snap.season_hint_source, "explicit")
        self.assertEqual(snap.season_hint_confidence, "high")

    def test_s2_pattern(self):
        snap = self.psl(_dir("My Hero Academia S2"), structure_hint="tv")
        self.assertEqual(snap.season_hint, 2)
        self.assertEqual(snap.season_hint_source, "explicit")

    def test_cjk_season(self):
        snap = self.psl(_dir("进击的巨人 第2季"), structure_hint="tv")
        self.assertEqual(snap.season_hint, 2)
        self.assertEqual(snap.season_hint_source, "explicit")

    def test_roman_numeral_season(self):
        snap = self.psl(_dir("Sword Art Online III"), structure_hint="tv")
        self.assertEqual(snap.season_hint, 3)
        self.assertEqual(snap.season_hint_source, "explicit")

    def test_ordinal_season(self):
        snap = self.psl(_dir("Overlord Fourth Season"), structure_hint="tv")
        self.assertEqual(snap.season_hint, 4)
        self.assertEqual(snap.season_hint_source, "explicit")


# ===========================================================================
# 四、parse_structure_locally() — Final Season
# ===========================================================================

class TestParseStructureLocallyFinal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.recognition_flow import parse_structure_locally
        cls.psl = staticmethod(parse_structure_locally)

    def test_final_hint_flag(self):
        snap = self.psl(_dir("Attack on Titan The Final Season"), structure_hint="tv")
        self.assertTrue(snap.final_hint)

    def test_final_source_set(self):
        snap = self.psl(_dir("Attack on Titan The Final Season"), structure_hint="tv")
        if snap.season_hint_source is not None:
            self.assertIn("final", snap.season_hint_source)


# ===========================================================================
# 五、_extract_season_from_path() — 路径季目录（含罗马数字）
# ===========================================================================

class TestExtractSeasonFromPath(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.scanner import _extract_season_from_path
        cls.ex = staticmethod(_extract_season_from_path)

    def test_season_numeric(self):
        self.assertEqual(self.ex(_path_with_parts("S", "Season 2", "e.mkv")), 2)

    def test_season_no_space(self):
        self.assertEqual(self.ex(_path_with_parts("S", "Season2", "e.mkv")), 2)

    def test_s_prefix(self):
        self.assertEqual(self.ex(_path_with_parts("S", "S03", "e.mkv")), 3)

    def test_cjk(self):
        self.assertEqual(self.ex(_path_with_parts("S", "第2季", "e.mkv")), 2)

    def test_roman_II(self):
        self.assertEqual(self.ex(_path_with_parts("S", "Season II", "e.mkv")), 2)

    def test_roman_III(self):
        self.assertEqual(self.ex(_path_with_parts("S", "Season III", "e.mkv")), 3)

    def test_roman_IV(self):
        self.assertEqual(self.ex(_path_with_parts("S", "Season IV", "e.mkv")), 4)

    def test_none_when_no_dir(self):
        self.assertIsNone(self.ex(_path_with_parts("S", "e.mkv")))

    def test_bang_title_no_season_dir(self):
        self.assertIsNone(self.ex(Path("/media/K-ON!!/ep01.mkv")))


# ===========================================================================
# 六、snapshot 不变式
# ===========================================================================

class TestSnapshotInvariant(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.recognition_flow import parse_structure_locally
        cls.psl = staticmethod(parse_structure_locally)

    def test_bang_not_lost(self):
        for title in ["K-ON!!", "Gintama!!!"]:
            snap = self.psl(_dir(title), structure_hint="tv")
            self.assertIsNotNone(snap.season_hint,
                                 f"{title!r}: bang season 候选丢失")

    def test_explicit_confidence_high(self):
        for title in ["Show Season 2", "Show S2", "Show 第2季"]:
            snap = self.psl(_dir(title), structure_hint="tv")
            self.assertEqual(snap.season_hint_confidence, "high",
                             f"{title!r}: 应为 high，实际={snap.season_hint_confidence}")

    def test_bang_confidence_low(self):
        snap = self.psl(_dir("K-ON!!"), structure_hint="tv")
        self.assertEqual(snap.season_hint_confidence, "low")

    def test_bang_no_override_explicit(self):
        snap = self.psl(_dir("Show S3!!"), structure_hint="tv")
        self.assertEqual(snap.season_hint, 3)
        self.assertEqual(snap.season_hint_source, "explicit")


# ===========================================================================
# 七、_stabilize_directory_context() bang × TMDB 校验
# ===========================================================================

class TestStabilizeBang(unittest.TestCase):
    def _ctx(self, season_hint, source="bang", confidence="low"):
        return {
            "media_type": "tv", "tmdb_id": 12345, "title": "K-ON",
            "target_root": "/t", "score": 0.9, "fallback_round": 0,
            "season_hint": season_hint, "season_hint_confidence": confidence,
            "season_hint_source": source, "season_hint_raw": "!!" * (season_hint or 1),
            "special_hint": False, "final_hint": False,
            "season_aware_done": False, "season_aware_had_candidates": False,
            "season_aware_tried_queries": [], "recompute_target_names": False,
            "extra_context": False, "extra_context_token": None,
            "strong_extra_context": False, "strong_extra_context_token": None,
            "_has_issues": False,
        }

    def _tmdb(self, *ns):
        return {"id": 12345, "name": "K-ON",
                "seasons": [{"season_number": n, "name": f"Season {n}", "overview": ""} for n in ns]}

    def test_bang_in_tmdb_accepted(self):
        import backend.services.scanner as sc
        ctx = self._ctx(2, "bang")
        with patch.object(sc, "get_tmdb_tv_details_sync", return_value=self._tmdb(1, 2, 3)), \
             patch.object(sc, "resolve_season_by_tmdb", return_value=2), \
             patch.object(sc, "_is_final_season_title", return_value=False), \
             patch.object(sc, "infer_season_from_tmdb_seasons", return_value=None):
            ok, reason = sc._stabilize_directory_context(Path("/fake/K-ON!!"), ctx)
        self.assertTrue(ok, f"稳定化失败: {reason}")
        self.assertEqual(ctx.get("resolved_season"), 2)

    def test_bang_not_in_tmdb_falls_back(self):
        import backend.services.scanner as sc
        ctx = self._ctx(2, "bang")
        with patch.object(sc, "get_tmdb_tv_details_sync", return_value=self._tmdb(1)), \
             patch.object(sc, "resolve_season_by_tmdb", return_value=1), \
             patch.object(sc, "_is_final_season_title", return_value=False), \
             patch.object(sc, "infer_season_from_tmdb_seasons", return_value=None):
            ok, reason = sc._stabilize_directory_context(Path("/fake/K-ON!!"), ctx)
        self.assertTrue(ok, f"稳定化失败: {reason}")
        self.assertEqual(ctx.get("resolved_season"), 1)

    def test_explicit_not_downgraded(self):
        import backend.services.scanner as sc
        ctx = self._ctx(2, "explicit", "high")
        with patch.object(sc, "get_tmdb_tv_details_sync", return_value=self._tmdb(1, 2)), \
             patch.object(sc, "resolve_season_by_tmdb", return_value=2), \
             patch.object(sc, "_is_final_season_title", return_value=False), \
             patch.object(sc, "infer_season_from_tmdb_seasons", return_value=None):
            ok, reason = sc._stabilize_directory_context(Path("/fake/Show Season 2"), ctx)
        self.assertTrue(ok, f"稳定化失败: {reason}")
        self.assertEqual(ctx.get("resolved_season"), 2)


# ===========================================================================
# 八、_rederive_season_from_context() bang × TMDB
# ===========================================================================

class TestRederiveBang(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.scanner import _rederive_season_from_context
        cls.rederive = staticmethod(_rederive_season_from_context)

    def test_bang_in_tmdb_returned(self):
        ctx = {"season_hint": 2, "season_hint_source": "bang", "bang_season_hint": True}
        tmdb = [{"season_number": 1, "name": "S1"}, {"season_number": 2, "name": "S2"}]
        r = self.rederive(Path("/fake/K-ON!!"), ctx, tmdb_seasons=tmdb)
        self.assertEqual(r, 2, f"bang 在 TMDB 中，应返回 2，实际={r}")

    def test_bang_not_in_tmdb_not_returned(self):
        ctx = {"season_hint": 2, "season_hint_source": "bang", "bang_season_hint": True}
        tmdb = [{"season_number": 1, "name": "S1"}]
        r = self.rederive(Path("/fake/K-ON!!"), ctx, tmdb_seasons=tmdb)
        self.assertNotEqual(r, 2, f"bang 不在 TMDB 中，不应返回 2，实际={r}")


# ===========================================================================
# 九、infer_season_from_tmdb_seasons() — substring bonus & 英文季名匹配
# ===========================================================================

class TestInferSeasonFromTmdbSeasons(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.recognition_flow import infer_season_from_tmdb_seasons
        cls.infer = staticmethod(infer_season_from_tmdb_seasons)

    def _s(self, num, name_zh, name_en=None):
        s = {"season_number": num, "name": name_zh}
        if name_en:
            s["name_en"] = name_en
        return s

    def test_exact_match_zh(self):
        """中文季名完全匹配（Kakegurui××）"""
        seasons = [
            self._s(1, "Kakegurui"),
            self._s(2, "Kakegurui××"),
        ]
        result = self.infer("Kakegurui××", seasons)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 2)

    def test_exact_match_en_name(self):
        """英文季名通过 name_en 字段匹配"""
        seasons = [
            self._s(1, "第一季", "Kobayashi-san Chi no Maid Dragon"),
            self._s(2, "第二季", "Kobayashi-san Chi no Maid Dragon S"),
        ]
        result = self.infer("Kobayashi-san Chi no Maid Dragon S", seasons)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 2)

    def test_psycho_pass_ii(self):
        """PSYCHO-PASS II 应匹配 S2"""
        seasons = [
            self._s(1, "PSYCHO-PASS", "PSYCHO-PASS"),
            self._s(2, "PSYCHO-PASS II", "PSYCHO-PASS II"),
        ]
        result = self.infer("PSYCHO-PASS II", seasons)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 2)

    def test_tokyo_ghoul_sqrt_a(self):
        """Tokyo Ghoul √A 应匹配 S2"""
        seasons = [
            self._s(1, "Tokyo Ghoul", "Tokyo Ghoul"),
            self._s(2, "Tokyo Ghoul √A", "Tokyo Ghoul √A"),
            self._s(3, "Tokyo Ghoul:re", "Tokyo Ghoul:re"),
        ]
        result = self.infer("Tokyo Ghoul √A", seasons)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 2)

    def test_generic_name_not_preferred(self):
        """通用名称 'Season 1' 不应抢占精确匹配"""
        seasons = [
            self._s(1, "Season 1", "Season 1"),
            self._s(2, "Kakegurui××", "Kakegurui××"),
        ]
        result = self.infer("Kakegurui××", seasons)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 2, "精确匹配季名应优先于通用名")

    def test_no_match_returns_none(self):
        """无匹配时返回 None"""
        seasons = [self._s(1, "SomeShow Season 1", "SomeShow")]
        result = self.infer("CompletlyDifferentTitle", seasons)
        # 阈值 0.55 下不应误匹配
        if result is not None:
            self.assertLess(result[1], 0.55, "误匹配置信度过高")


# ===========================================================================
# 十、reidentify_by_target_dir() — season_override 传递到 resolved_season
# ===========================================================================

class TestReidentifyByTargetDirSeasonOverride(unittest.TestCase):
    def test_season_override_propagates_to_resolved_season(self):
        import tempfile
        import backend.services.scanner as sc

        with tempfile.TemporaryDirectory() as tmpdir:
            video = Path(tmpdir) / "Episode 01.mkv"
            video.write_bytes(b"x")

            group = SimpleNamespace(id=1, name="g1", target=str(Path(tmpdir) / "target"))
            record = SimpleNamespace(original_path=str(video))
            captured = {}

            def _fake_process_file(**kwargs):
                context = kwargs["context"]
                captured["season_hint"] = context.get("season_hint")
                captured["resolved_season"] = context.get("resolved_season")

            with patch.object(sc.settings, "tmdb_api_key", "token"), \
                 patch.object(sc, "_get_tmdb_item_by_id", return_value={"id": 123, "name": "Show", "first_air_date": "2020-01-01"}), \
                 patch.object(sc, "_resolve_chinese_title_by_tmdb", return_value="测试标题"), \
                 patch.object(sc, "resolve_movie_target_root", return_value=(Path(tmpdir) / "movies", None)), \
                 patch.object(sc, "detect_special_dir_context", return_value=(False, None)), \
                 patch.object(sc, "detect_strong_special_dir_context", return_value=(False, None)), \
                 patch.object(sc, "_process_file", side_effect=_fake_process_file):
                db = MagicMock()
                processed, failed, has_issues = sc.reidentify_by_target_dir(
                    db=db,
                    group=group,
                    media_type="tv",
                    tmdb_id=123,
                    title_override=None,
                    year_override=None,
                    season_override=3,
                    episode_offset=None,
                    records=[record],
                )

            self.assertEqual(captured.get("season_hint"), 3)
            self.assertEqual(captured.get("resolved_season"), 3)
            self.assertEqual(processed, 1)
            self.assertEqual(failed, 0)
            self.assertFalse(has_issues)


# ===========================================================================
# 十一、run_scan() — worker 问题标记回传到任务层
# ===========================================================================

class TestRunScanIssuePropagation(unittest.TestCase):
    def test_worker_issue_flag_marks_scan_task_type(self):
        import backend.services.scanner as sc

        class FakeScanTask:
            def __init__(self, type, target_id, status, target_name=None):
                self.id = None
                self.type = type
                self.target_id = target_id
                self.status = status
                self.target_name = target_name
                self.finished_at = None
                self.log_file = None

        class FakeSyncGroup:
            enabled = True
            id = 1

            def __init__(self, gid, name):
                self.id = gid
                self.name = name

        class FakeQuery:
            def __init__(self, rows):
                self.rows = rows

            def filter(self, *args, **kwargs):
                return self

            def all(self):
                return list(self.rows)

            def first(self):
                return self.rows[0] if self.rows else None

        class FakeDB:
            def __init__(self, groups):
                self.groups = groups
                self.added = []

            def add(self, obj):
                if getattr(obj, "id", None) is None:
                    obj.id = len(self.added) + 1
                self.added.append(obj)

            def commit(self):
                return None

            def refresh(self, obj):
                return None

            def rollback(self):
                return None

            def query(self, model):
                if model is FakeSyncGroup:
                    return FakeQuery(self.groups)
                raise AssertionError(f"unexpected query model: {model}")

        db = FakeDB([FakeSyncGroup(7, "group-7")])

        def _fake_process_sync_group(db, group, target_dir_override=None):
            sc._mark_scan_issue_flag(sc.current_task_id.get())
            return group.name, False

        with patch.object(sc, "ScanTask", FakeScanTask), \
             patch.object(sc, "SyncGroup", FakeSyncGroup), \
             patch.object(sc.settings, "tmdb_api_key", "token"), \
             patch.object(sc, "_start_media_worker", return_value=None), \
             patch.object(sc, "refresh_emby_library", return_value=None), \
             patch.object(sc, "_process_sync_group", side_effect=_fake_process_sync_group):
            sc.run_scan(db, group_id=7)

        self.assertEqual(len(db.added), 1)
        self.assertEqual(db.added[0].status, "completed")
        self.assertTrue(str(db.added[0].type).startswith("issue_sp:"))


# ===========================================================================
# 十、_detect_bracket_variant_as_special() — variant 括号检测
# ===========================================================================

class TestDetectBracketVariantAsSpecial(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.scanner import _detect_bracket_variant_as_special
        cls.detect = staticmethod(_detect_bracket_variant_as_special)

    def test_preview_bracket(self):
        """[Preview01_1] 应被检测为 variant"""
        cat, label, from_b = self.detect("OVERLORD IV [Preview01_1][Ma10p_1080p][x265_flac].mkv")
        self.assertIsNotNone(cat)
        self.assertEqual(cat, "preview")

    def test_on_air_ver(self):
        """[On Air Ver.] 应被检测为 variant"""
        cat, label, from_b = self.detect("kiss×sis [09(On Air Ver.)][Ma10p_1080p][x265_flac].mkv")
        self.assertIsNotNone(cat)
        self.assertEqual(cat, "special")

    def test_staff_credit_ver(self):
        """[Musani Staff Credit Ver.] 应被检测为 variant"""
        cat, label, from_b = self.detect("Exodus! [01(Musani Staff Credit Ver.)][Ma10p_1080p][x265_flac_aac].mkv")
        self.assertIsNotNone(cat)
        self.assertEqual(cat, "special")

    def test_mystery_camp(self):
        """[Mystery Camp] 应被检测为 variant"""
        cat, label, from_b = self.detect("Yuru Camp Season 2 [Mystery Camp][Ma10p_1080p][x265_flac].mkv")
        self.assertIsNotNone(cat)

    def test_clean_episode_not_detected(self):
        """干净集号文件 [09] 不应被检测为 variant"""
        cat, label, from_b = self.detect("kiss×sis [09][Ma10p_1080p][x265_flac].mkv")
        self.assertIsNone(cat)

    def test_tech_tag_not_detected(self):
        """纯技术标签 [Ma10p_1080p] 不应被检测为 variant"""
        cat, label, from_b = self.detect("SomeShow [01][Ma10p_1080p][x265_flac].mkv")
        self.assertIsNone(cat)


# ===========================================================================
# 十一、_video_sort_key() — variant 文件排序在正片之后
# ===========================================================================

class TestVideoSortKey(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.services.scanner import _video_sort_key
        cls.sort_key = staticmethod(_video_sort_key)

    def test_variant_sorted_after_clean(self):
        """含 variant 括号后缀的文件排在干净文件之后"""
        clean = Path("/fake/dir/show [09][1080p].mkv")
        variant = Path("/fake/dir/show [09(On Air Ver.)][1080p].mkv")
        self.assertLess(
            self.sort_key(clean),
            self.sort_key(variant),
            "干净文件应排在 variant 文件前面",
        )

    def test_clean_files_sorted_by_path(self):
        """干净文件之间按路径正序排序"""
        a = Path("/fake/dir/show [01][1080p].mkv")
        b = Path("/fake/dir/show [02][1080p].mkv")
        self.assertLess(self.sort_key(a), self.sort_key(b))


# ===========================================================================
# 十三、recognize_directory_with_fallback() — structure_hint 不提前退出
# ===========================================================================

class TestRecognizeDirectoryWithFallbackStructureHint(unittest.TestCase):
    """
    当 structure_hint="tv" 时，首轮结果为高分电影不应提前退出循环。
    这覆盖了 Chihayafuru I+II 被误判为电影版的根本原因：
    第一轮查询「Chihayafuru I II」找到了电影（花牌情缘 Part I），
    旧代码因高分提前退出，新代码应继续尝试后续查询词以发现 TV 版本。
    """
    def setUp(self):
        import backend.services.recognition_flow as rf
        self.rf = rf

    def _make_cand(self, tmdb_id, media_type, score):
        from backend.services.recognition_flow import RankedCandidate
        c = RankedCandidate.__new__(RankedCandidate)
        c.tmdb_id = tmdb_id
        c.media_type = media_type
        c.score = score
        c.year = None
        c.candidate_pool_size = 1
        c.tmdb_data = {}
        return c

    def test_structure_hint_tv_skips_movie_early_return(self):
        """structure_hint=tv 时，高分电影不触发提前退出，应继续搜索找到 TV 结果"""
        rf = self.rf
        movie_cand = self._make_cand(407936, "movie", 0.92)
        tv_cand = self._make_cand(43001, "tv", 0.78)

        # 第一轮调用（>=3 候选词进入 round 0）返回高分 movie；第二轮返回 TV
        call_count = [0]
        def _fake_unified(candidates, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return movie_cand
            return tv_cand

        from pathlib import Path
        # 至少 4 个候选词才会产生 2 轮：[0:3] 和 [3:5]
        FAKE_CANDIDATES = ["Chihayafuru I II", "Chihayafuru I", "Chihayafuru II", "Chihayafuru"]
        with patch.object(rf, "_unified_competitive_search", side_effect=_fake_unified), \
             patch.object(rf, "parse_structure_locally") as mock_snap, \
             patch.object(rf, "build_search_name", return_value="Chihayafuru I II"), \
             patch.object(rf, "generate_candidate_titles", return_value=FAKE_CANDIDATES):
            mock_snap.return_value = MagicMock(
                season_hint=None, season_hint_confidence=None, year_hint=None,
                special_hint=False, final_hint=False, cleaned_name="Chihayafuru I II",
                main_title="Chihayafuru I II", season_aware_done=False,
                season_aware_had_candidates=False, season_aware_tried_queries=[],
            )
            result, _, _ = rf.recognize_directory_with_fallback(
                Path("/fake/Chihayafuru I+II"), "tv", structure_hint="tv"
            )

        self.assertEqual(call_count[0], 2, "应进入第二轮搜索，不在第一轮提前退出")
        self.assertIsNotNone(result)
        self.assertEqual(result.media_type, "tv", "最终结果应为 TV")

    def test_no_hint_movie_can_early_return(self):
        """无 structure_hint 时，高分电影仍可提前退出（不应影响原有行为）"""
        rf = self.rf
        movie_cand = self._make_cand(999, "movie", 0.92)

        call_count = [0]
        def _fake_unified(candidates, **kwargs):
            call_count[0] += 1
            return movie_cand

        from pathlib import Path
        FAKE_CANDIDATES = ["Some Movie", "Some Movie Alt", "Some Movie JP", "Some M"]
        with patch.object(rf, "_unified_competitive_search", side_effect=_fake_unified), \
             patch.object(rf, "parse_structure_locally") as mock_snap, \
             patch.object(rf, "build_search_name", return_value="Some Movie"), \
             patch.object(rf, "generate_candidate_titles", return_value=FAKE_CANDIDATES):
            mock_snap.return_value = MagicMock(
                season_hint=None, season_hint_confidence=None, year_hint=None,
                special_hint=False, final_hint=False, cleaned_name="Some Movie",
                main_title="Some Movie", season_aware_done=False,
                season_aware_had_candidates=False, season_aware_tried_queries=[],
            )
            result, _, _ = rf.recognize_directory_with_fallback(
                Path("/fake/SomeMovie"), "tv", structure_hint=None
            )

        self.assertEqual(call_count[0], 1, "无 structure_hint 时高分电影可在第一轮提前退出")
        self.assertEqual(result.media_type, "movie")

    def test_structure_hint_movie_skips_tv_early_return(self):
        """structure_hint=movie 时，高分 TV 不触发提前退出（对称性）"""
        rf = self.rf
        tv_cand = self._make_cand(111, "tv", 0.92)
        movie_cand = self._make_cand(222, "movie", 0.80)

        call_count = [0]
        def _fake_unified(candidates, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return tv_cand
            return movie_cand

        from pathlib import Path
        FAKE_CANDIDATES = ["SomeFilm", "SomeFilm Alt", "SomeFilm JP", "SomeFilm Short"]
        with patch.object(rf, "_unified_competitive_search", side_effect=_fake_unified), \
             patch.object(rf, "parse_structure_locally") as mock_snap, \
             patch.object(rf, "build_search_name", return_value="SomeFilm"), \
             patch.object(rf, "generate_candidate_titles", return_value=FAKE_CANDIDATES):
            mock_snap.return_value = MagicMock(
                season_hint=None, season_hint_confidence=None, year_hint=None,
                special_hint=False, final_hint=False, cleaned_name="SomeFilm",
                main_title="SomeFilm", season_aware_done=False,
                season_aware_had_candidates=False, season_aware_tried_queries=[],
            )
            result, _, _ = rf.recognize_directory_with_fallback(
                Path("/fake/SomeFilm"), "tv", structure_hint="movie"
            )

        self.assertEqual(call_count[0], 2, "structure_hint=movie 时高分 TV 不应提前退出")
        self.assertEqual(result.media_type, "movie")


if __name__ == "__main__":
    unittest.main(verbosity=2)
