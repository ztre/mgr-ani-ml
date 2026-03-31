import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

for _mod in (
    "httpx",
    "sqlalchemy",
    "sqlalchemy.orm",
    "sqlalchemy.exc",
    "sqlalchemy.dialects",
    "sqlalchemy.dialects.sqlite",
    "fastapi",
    "pydantic",
    "apscheduler",
    "apscheduler.schedulers",
    "apscheduler.schedulers.background",
    "openai",
):
    sys.modules.setdefault(_mod, MagicMock())


class TestRunScanCancellation(unittest.TestCase):
    def test_run_scan_sets_cancelled_status(self):
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

        db = FakeDB([FakeSyncGroup(8, "group-8")])

        def _fake_process_sync_group(db, group, target_dir_override=None):
            sc.request_scan_cancel(sc.current_task_id.get())
            return group.name, False

        with patch.object(sc, "ScanTask", FakeScanTask), \
             patch.object(sc, "SyncGroup", FakeSyncGroup), \
             patch.object(sc.settings, "tmdb_api_key", "token"), \
             patch.object(sc, "_start_media_worker", return_value=None), \
             patch.object(sc, "refresh_emby_library", return_value=None), \
             patch.object(sc, "_process_sync_group", side_effect=_fake_process_sync_group):
            sc.run_scan(db, group_id=8)

        self.assertEqual(len(db.added), 1)
        self.assertEqual(db.added[0].status, "cancelled")


class TestScanCancelFlagLifecycle(unittest.TestCase):
    def test_cancel_flag_roundtrip(self):
        import backend.services.scanner as sc

        scan_task_id = 23
        sc.init_scan_cancel_flag(scan_task_id)

        self.assertFalse(sc.is_scan_cancel_requested(scan_task_id))
        self.assertTrue(sc.request_scan_cancel(scan_task_id))
        self.assertTrue(sc.is_scan_cancel_requested(scan_task_id))
        self.assertTrue(sc.pop_scan_cancel_flag(scan_task_id))
        self.assertFalse(sc.is_scan_cancel_requested(scan_task_id))


class TestRunManualOrganizeCancellation(unittest.TestCase):
    def test_manual_organize_raises_cancelled_and_logs_phase(self):
        import tempfile
        import backend.services.scanner as sc

        with tempfile.TemporaryDirectory() as tmpdir:
            root_dir = Path(tmpdir) / "pending-dir"
            root_dir.mkdir()
            video = root_dir / "Episode 01.mkv"
            video.write_bytes(b"x")

            pending = MagicMock(original_path=str(root_dir))
            group = MagicMock(id=1, name="g1", target=str(Path(tmpdir) / "target"), source=str(root_dir.parent), include="", exclude="")

            sc.init_scan_cancel_flag(99)
            token = sc.current_task_id.set(99)
            try:
                sc.request_scan_cancel(99)
                with patch.object(sc.settings, "tmdb_api_key", "token"), \
                     patch.object(sc, "_get_tmdb_item_by_id", return_value={"id": 123, "name": "Show", "first_air_date": "2020-01-01"}), \
                     patch.object(sc, "_resolve_chinese_title_by_tmdb", return_value="测试标题"), \
                     patch.object(sc, "resolve_movie_target_root", return_value=(Path(tmpdir) / "movies", None)), \
                     patch.object(sc, "_collect_media_files_under_dir", return_value=[video]), \
                     patch.object(sc, "detect_special_dir_context", return_value=(False, None)), \
                     patch.object(sc, "detect_strong_special_dir_context", return_value=(False, None)), \
                     patch.object(sc, "_stabilize_directory_context", return_value=(True, None)), \
                     patch.object(sc, "append_log") as append_log:
                    with self.assertRaises(sc.TaskCancelledError):
                        sc.run_manual_organize(
                            db=MagicMock(),
                            pending=pending,
                            group=group,
                            media_type="tv",
                            tmdb_id=123,
                        )
                joined_logs = "\n".join(call.args[0] for call in append_log.call_args_list if call.args)
                self.assertIn("phase=manual-preflight", joined_logs)
            finally:
                sc.current_task_id.reset(token)
                sc.pop_scan_cancel_flag(99)


if __name__ == "__main__":
    unittest.main(verbosity=2)