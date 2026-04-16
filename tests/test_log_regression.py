"""task log 回归测试：以历史成功日志为基准，验证 parse_tv_filename
在 episode 提取和 season_hint_strength 标注上不引入新的错误。

自动扫描 logs/ 目录下所有 task*.log 文件，合并去重后作为测试数据源。

运行方式：
    python3 -m pytest tests/test_log_regression.py -v
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

LOGS_DIR = Path(__file__).resolve().parents[1] / "logs"


def _load_log_entries() -> list[tuple[str, str]]:
    """扫描 logs/ 下所有 task*.log，返回去重后的 [(源文件名, 目标文件名), ...] 列表。"""
    seen: set[tuple[str, str]] = set()
    entries: list[tuple[str, str]] = []
    for log_path in sorted(LOGS_DIR.glob("task*.log")):
        with open(log_path, encoding="utf-8", errors="ignore") as f:
            lines = list(f)

        # 带 episode_offset 的任务，其目标文件名代表“重排后的最终集号”，
        # 不能再拿来校验 parser 的原始 episode 提取是否正确。
        offset_match = next(
            (
                re.search(r"episode_offset\s*=\s*(-?\d+)", line)
                for line in lines
                if "episode_offset" in line
            ),
            None,
        )
        if offset_match and int(offset_match.group(1)) != 0:
            continue

        for line in lines:
                m = re.search(r"处理成功: (.+?) -> (.+)", line)
                if not m:
                    continue
                src_file = m.group(1).strip().split("/")[-1]
                tgt_file = m.group(2).strip().split("/")[-1]
                key = (src_file, tgt_file)
                if key not in seen:
                    seen.add(key)
                    entries.append(key)
    return entries


def _parse_expected(tgt_file: str) -> tuple[int, int] | None:
    """从目标文件名提取 (season, episode)，None 表示跳过（特典/无集号）。"""
    m = re.search(r" - S(\d{2})E(\d{2,3})\.mkv$", tgt_file)
    if not m:
        return None
    s, e = int(m.group(1)), int(m.group(2))
    if s == 0 or e >= 100:  # Season 00 special / index≥100 special
        return None
    return s, e


# ---------------------------------------------------------------------------
# 辅助：一次性加载所有条目（模块级，仅执行一次）
# ---------------------------------------------------------------------------
_ALL_ENTRIES: list[tuple[str, str]] = _load_log_entries()
_MAIN_VIDEO_ENTRIES: list[tuple[str, str, int, int]] = []

for _src, _tgt in _ALL_ENTRIES:
    _exp = _parse_expected(_tgt)
    if _exp is not None:
        _MAIN_VIDEO_ENTRIES.append((_src, _tgt, _exp[0], _exp[1]))


# ---------------------------------------------------------------------------
# Test 1: 日志文件必须存在且包含足够条目
# ---------------------------------------------------------------------------
def test_log_file_loaded():
    log_files = sorted(LOGS_DIR.glob("task*.log"))
    assert log_files, f"logs/ 目录下无 task*.log 文件: {LOGS_DIR}"
    assert len(_ALL_ENTRIES) >= 80, f"日志条目数量不足（仅 {len(_ALL_ENTRIES)} 条），请确认日志文件有效"
    assert len(_MAIN_VIDEO_ENTRIES) >= 30, (
        f"主视频条目数量不足（仅 {len(_MAIN_VIDEO_ENTRIES)} 条）"
    )


# ---------------------------------------------------------------------------
# Test 2: parse_tv_filename 对所有主视频集号无错误提取
# ---------------------------------------------------------------------------
def test_no_episode_extraction_errors():
    """parse_tv_filename 提取的 episode 值与日志期望完全一致（允许 None 由 loose 提取器补充）。"""
    from backend.services.parser import parse_tv_filename

    wrong: list[tuple[str, int, int, int]] = []  # (filename, exp_ep, got_ep, exp_s)

    for src_file, _tgt_file, exp_s, exp_e in _MAIN_VIDEO_ENTRIES:
        r = parse_tv_filename(src_file)
        if r is None or r.episode is None:
            # episode=None 由 scanner loose 提取器处理，不计入错误
            continue
        if r.episode != exp_e:
            wrong.append((src_file, exp_e, r.episode, exp_s))

    assert not wrong, (
        f"{len(wrong)} 个文件集号提取错误（前10）:\n"
        + "\n".join(
            f"  exp=S{es:02d}E{ee:02d} got=E{ge:03d}  {fn}"
            for fn, ee, ge, es in wrong[:10]
        )
    )


# ---------------------------------------------------------------------------
# Test 3: season_hint_strength="roman" 仅对真正罗马数字季名标注
# ---------------------------------------------------------------------------
def test_roman_season_hint_strength_accuracy():
    """season_hint_strength="roman" 的文件排除已知误报（EIGHTY SIX、School Days N 等）。"""
    from backend.services.parser import parse_tv_filename

    # 已知包含可能被误判为罗马数字的单词（不应有 shs="roman"）
    _FALSE_POSITIVE_PATTERNS = [
        re.compile(r"eighty\s*six", re.I),
        re.compile(r"\bSchool\s+Days\s+\d+\b", re.I),
        re.compile(r"\bIX\b(?!\s*[:\-])"),  # 独立 IX 词（非 Season IX）
    ]

    false_positives: list[str] = []

    for src_file, _tgt_file, exp_s, _exp_e in _MAIN_VIDEO_ENTRIES:
        r = parse_tv_filename(src_file)
        if r is None:
            continue
        if getattr(r, "season_hint_strength", None) != "roman":
            continue
        for pat in _FALSE_POSITIVE_PATTERNS:
            if pat.search(src_file):
                false_positives.append(src_file)
                break

    assert not false_positives, (
        f"{len(false_positives)} 个文件被错误标注 season_hint_strength='roman':\n"
        + "\n".join(f"  {fn}" for fn in false_positives[:10])
    )


# ---------------------------------------------------------------------------
# Test 4: Case A 逻辑 - roman 标注的文件 season 不被 resolved_season=1 错误覆写
#   PSYCHO-PASS II 应被标注 roman，期望 parser 给 season=2
# ---------------------------------------------------------------------------
def test_psycho_pass_ii_roman_season_not_overridden():
    """PSYCHO-PASS II 文件应标注 shs='roman' 且 parser season=2。"""
    from backend.services.parser import parse_tv_filename

    psycho_pass_ii_files = [
        src for src, tgt, es, ee in _MAIN_VIDEO_ENTRIES
        if "PSYCHO-PASS II" in src or "PSYCHO PASS II" in src
    ]

    if not psycho_pass_ii_files:
        pytest.skip("日志中无 PSYCHO-PASS II 文件")

    for fn in psycho_pass_ii_files:
        r = parse_tv_filename(fn)
        assert r is not None, f"parse 返回 None: {fn}"
        assert r.season == 2, f"期望 season=2，实际 season={r.season}: {fn}"
        assert getattr(r, "season_hint_strength", None) == "roman", (
            f"期望 shs='roman'，实际 shs={getattr(r,'season_hint_strength',None)}: {fn}"
        )


# ---------------------------------------------------------------------------
# Test 5: EIGHTY SIX 不再被误判为 season=9
# ---------------------------------------------------------------------------
def test_eighty_six_no_false_roman_season():
    """EIGHTY SIX 不应被 _extract_roman_season 误解析为 season=9。"""
    from backend.services.parser import parse_tv_filename, _extract_roman_season

    eighty_six_files = [
        src for src, tgt, es, ee in _MAIN_VIDEO_ENTRIES
        if re.search(r"eighty\s*six", src, re.I)
    ]

    if not eighty_six_files:
        pytest.skip("日志中无 EIGHTY SIX 文件")

    # _extract_roman_season 直接测试
    assert _extract_roman_season("EIGHTY SIX") is None, (
        "_extract_roman_season('EIGHTY SIX') 应返回 None，不应解析 SIX 为 S+IX"
    )

    for fn in eighty_six_files:
        r = parse_tv_filename(fn)
        assert r is not None
        assert r.season != 9, f"EIGHTY SIX 不应得到 season=9: {fn}"
        assert getattr(r, "season_hint_strength", None) != "roman", (
            f"EIGHTY SIX 不应标注 shs='roman': {fn}"
        )


# ---------------------------------------------------------------------------
# Test 6: 覆盖率摘要（信息性，不断言失败）
# ---------------------------------------------------------------------------
def test_coverage_summary(capsys):
    """输出 parser 覆盖率摘要。"""
    from backend.services.parser import parse_tv_filename

    total = len(_MAIN_VIDEO_ENTRIES)
    parse_none = 0
    ep_none = 0
    roman_tagged = 0

    for src_file, _, _es, _ee in _MAIN_VIDEO_ENTRIES:
        r = parse_tv_filename(src_file)
        if r is None:
            parse_none += 1
        elif r.episode is None:
            ep_none += 1
        if r and getattr(r, "season_hint_strength", None) == "roman":
            roman_tagged += 1

    with capsys.disabled():
        print(f"\n=== Log Regression Coverage ===")
        print(f"主视频条目总数: {total}")
        print(f"parse_tv_filename=None: {parse_none}")
        print(f"episode=None (靠 loose 提取器): {ep_none}")
        print(f"season_hint_strength='roman' 标注数: {roman_tagged}")
        print(f"完全解析成功率: {(total - parse_none - ep_none) / total * 100:.1f}%")
