from __future__ import annotations

import importlib

from fastapi.testclient import TestClient

from backend import config as config_module
from backend.api import inodes as inodes_api, media as media_api
from backend.models import InodeRecord, MediaRecord
from backend.security import require_auth


def test_media_and_inodes_list_api_include_episode_and_device(tmp_path, monkeypatch):
    main_db_url = f"sqlite:///{tmp_path / 'api-fields.db'}"
    original_db_url = config_module.settings.database_url

    monkeypatch.setattr(config_module.settings, "database_url", main_db_url)
    import backend.database as database_module

    database_module = importlib.reload(database_module)
    database_module.init_db()

    import backend.main as main_module

    main_module = importlib.reload(main_module)

    def override_get_db():
        db = database_module.RequestSessionLocal()
        try:
            yield db
        finally:
            db.close()

    main_module.app.dependency_overrides[require_auth] = lambda: "tester"
    main_module.app.dependency_overrides[media_api.get_db] = override_get_db
    main_module.app.dependency_overrides[inodes_api.get_db] = override_get_db

    db = database_module.SessionLocal()
    try:
        db.add(
            MediaRecord(
                sync_group_id=11,
                original_path="/media/source/Show/episode-07.mkv",
                target_path="/media/links/Show (2024) [tmdbid=7]/Season 01/Show - S01E07.mkv",
                type="tv",
                tmdb_id=7,
                status="scraped",
                season=1,
                episode=7,
                category="episode",
                file_type="video",
                size=123,
            )
        )
        db.add(
            InodeRecord(
                device=2049,
                inode=7007,
                source_path="/media/source/Show/episode-07.mkv",
                target_path="/media/links/Show (2024) [tmdbid=7]/Season 01/Show - S01E07.mkv",
                sync_group_id=11,
                size=123,
            )
        )
        db.commit()

        with TestClient(main_module.app) as client:
            media_response = client.get("/api/media")
            inodes_response = client.get("/api/inodes")

        assert media_response.status_code == 200
        assert inodes_response.status_code == 200

        media_payload = media_response.json()
        inode_payload = inodes_response.json()

        assert media_payload["total"] == 1
        assert inode_payload["total"] == 1

        media_item = media_payload["items"][0]
        inode_item = inode_payload["items"][0]

        assert media_item["episode"] == 7
        assert inode_item["device"] == 2049
    finally:
        main_module.app.dependency_overrides.clear()
        db.close()
        monkeypatch.setattr(config_module.settings, "database_url", original_db_url)
        importlib.reload(database_module)