from __future__ import annotations

import backend.services.scanner as scanner


def test_pick_display_title_prefers_cn():
    title, lang = scanner._pick_display_title("吹响吧！上低音号", "吹響吧！上低音號", "Sound! Euphonium")
    assert title == "吹响吧！上低音号"
    assert lang == "zh-CN"


def test_pick_display_title_fallback_to_tw_when_cn_bad():
    title, lang = scanner._pick_display_title("Sound! Euphonium", "吹響吧！上低音號", "Sound! Euphonium")
    assert title == "吹響吧！上低音號"
    assert lang == "zh-TW"


def test_pick_display_title_fallback_when_cn_tw_unusable():
    title, lang = scanner._pick_display_title("", "Sound! Euphonium", "Sound! Euphonium")
    assert title == "Sound! Euphonium"
    assert lang in {"zh-TW(raw)", "fallback"}


def test_resolve_chinese_title_prefers_tw_when_cn_non_cjk(monkeypatch):
    monkeypatch.setattr(scanner.settings, "tmdb_api_key", "dummy")

    def fake_fetch(_client, _url, _media_type, language: str):
        if language == "zh-CN":
            return "Sound! Euphonium"
        if language == "zh-TW":
            return "吹響吧！上低音號"
        return None

    monkeypatch.setattr(scanner, "_fetch_tmdb_localized_title", fake_fetch)
    title = scanner._resolve_chinese_title_by_tmdb("tv", 62564, "Sound! Euphonium", strict=True)
    assert title == "吹響吧！上低音號"


def test_resolve_chinese_title_strict_still_fallbacks_to_original(monkeypatch):
    monkeypatch.setattr(scanner.settings, "tmdb_api_key", "dummy")

    def fake_fetch(_client, _url, _media_type, _language: str):
        return None

    monkeypatch.setattr(scanner, "_fetch_tmdb_localized_title", fake_fetch)
    title = scanner._resolve_chinese_title_by_tmdb("tv", 62564, "Sound! Euphonium", strict=True)
    assert title == "Sound! Euphonium"
