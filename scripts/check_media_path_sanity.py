#!/usr/bin/env python3
"""
check_media_path_sanity.py
检查媒体库目标路径合理性：
  1. DB 中的 target_path 与文件系统是否一致（文件存在？）
  2. 记录的 type（tv/movie）与其所在目录是否匹配（tv 文件不应在 movie 根目录，反之亦然）
  3. 从日志目录扫描"处理成功"记录，找出在日志中有映射但文件系统中已消失的孤立条目
  4. 从日志目录找出曾出现"旧目标目录未完全删除"的目录，检查是否已被清理

用法:
  python scripts/check_media_path_sanity.py [--log-dir logs] [--verbose]

环境要求: 在项目根目录执行，backend/.env 已配置好 DB 路径
"""
from __future__ import annotations

import re
import sys
import os
import argparse
from pathlib import Path
from collections import defaultdict

# ── 加载 Django-style path ────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("PYTHONPATH", str(PROJECT_ROOT))

from backend.database import SessionLocal
from backend.models import SyncGroup, MediaRecord


# ─────────────────────────────────────────────────────────────────────────────

def _load_sync_group_roots(db) -> tuple[dict[int, Path], dict[str, list[int]]]:
    """返回 {group_id: target_root} 和 {source_type: [group_id,...]}"""
    groups = db.query(SyncGroup).all()
    by_id: dict[int, Path] = {}
    by_type: dict[str, list[int]] = defaultdict(list)
    for g in groups:
        if g.target:
            by_id[g.id] = Path(g.target)
        by_type[str(g.source_type or "")].append(g.id)
    return by_id, dict(by_type)


def _resolve_expected_root(db, group_id: int, media_type: str, roots_by_id: dict[int, Path]) -> Path | None:
    """给定 group 和 media_type，返回期望的目标根目录。"""
    if media_type == "movie":
        # 复用与代码相同的路由逻辑
        from backend.services.group_routing import resolve_movie_target_root
        group = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()
        if group is None:
            return None
        root, _ = resolve_movie_target_root(db, group)
        return root
    return roots_by_id.get(group_id)


# ─── 日志解析 ─────────────────────────────────────────────────────────────────

_RE_SUCCESS = re.compile(
    r"处理成功: .+ -> (?P<target>.+\.(?:mkv|mp4|avi|mov|webm|flv|ass|srt|ssa|vtt|mka|sup|idx|sub|nfo))\s*$",
    re.I,
)
_RE_SINGLE_FIX = re.compile(
    r"单文件修正完成: .+ -> (?P<target>.+)\s*$",
)
_RE_BLOCKED_DIR = re.compile(
    r"修正后旧目标目录未完全删除:.*?-> (?P<dirs>.+)$",
)
_RE_BLOCKED_ITEM = re.compile(r"(?P<path>/[^\s(]+)\s+\(Directory not empty\)")


def _parse_logs(log_dir: Path) -> tuple[set[str], set[str]]:
    """
    返回 (logged_targets, ever_blocked_dirs)
    - logged_targets: 日志中曾出现为成功目标的路径集合
    - ever_blocked_dirs: 日志中曾因"未完全删除"而警报的目录路径集合
    """
    logged_targets: set[str] = set()
    ever_blocked_dirs: set[str] = set()

    for log_file in sorted(log_dir.glob("*.log")):
        try:
            text = log_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            m = _RE_SUCCESS.search(line) or _RE_SINGLE_FIX.search(line)
            if m:
                logged_targets.add(m.group("target").strip())
                continue
            m2 = _RE_BLOCKED_DIR.search(line)
            if m2:
                for dm in _RE_BLOCKED_ITEM.finditer(m2.group("dirs")):
                    ever_blocked_dirs.add(dm.group("path").strip())

    return logged_targets, ever_blocked_dirs


# ─── 检查逻辑 ─────────────────────────────────────────────────────────────────

def run_checks(log_dir: Path, verbose: bool) -> int:
    db = SessionLocal()
    try:
        roots_by_id, _roots_by_type = _load_sync_group_roots(db)
        # 收集所有已知的 tv 根目录和 movie 根目录，用于类型错位检测
        tv_roots: set[Path] = set()
        movie_roots: set[Path] = set()
        for g in db.query(SyncGroup).all():
            if not g.target:
                continue
            t = Path(g.target)
            if g.source_type == "tv":
                tv_roots.add(t)
            elif g.source_type == "movie":
                movie_roots.add(t)

        logged_targets, ever_blocked_dirs = _parse_logs(log_dir)

        issues: list[str] = []
        ok_count = 0

        rows = (
            db.query(MediaRecord)
            .filter(MediaRecord.status.in_(["scraped", "manual_fixed"]))
            .filter(MediaRecord.target_path.isnot(None), MediaRecord.target_path != "")
            .all()
        )

        for row in rows:
            target = Path(str(row.target_path).strip())
            media_type = str(row.type or "")
            group_id = row.sync_group_id

            # ── 检查1：文件实际存在 ───────────────────────────────────────────
            if not target.exists():
                issues.append(
                    f"[MISSING]  id={row.id} type={media_type} "
                    f"target={target}"
                )
                continue

            # ── 检查2：type 与所在目录是否错位 ───────────────────────────────
            if media_type == "tv":
                # tv 类型文件不应落在任何 movie 根目录下
                for movie_root in movie_roots:
                    try:
                        target.relative_to(movie_root)
                        issues.append(
                            f"[TYPE_MISMATCH tv-in-movie]  id={row.id} "
                            f"target={target}  (movie_root={movie_root})"
                        )
                        break
                    except ValueError:
                        pass
            elif media_type == "movie":
                # movie 类型文件不应落在纯 tv 根目录下（除非该组本来是 movie 组）
                expected_root = _resolve_expected_root(db, group_id, "movie", roots_by_id)
                if expected_root is not None:
                    try:
                        target.relative_to(expected_root)
                    except ValueError:
                        issues.append(
                            f"[TYPE_MISMATCH movie-not-in-movie-root]  id={row.id} "
                            f"target={target}  (expected_root={expected_root})"
                        )

            ok_count += 1
            if verbose:
                print(f"  OK  id={row.id} type={media_type} target={target}")

        # ── 检查3：日志中的成功目标是否还存在 ────────────────────────────────
        # 取最近一次操作记录（日志是事实，若目标消失说明清理异常或被外部删除）
        db_current_targets = {
            str(r.target_path).strip()
            for r in rows
            if r.target_path
        }
        orphan_logged = logged_targets - db_current_targets
        # 过滤：已不在 DB 且文件系统也不存在才算孤立
        for t in sorted(orphan_logged):
            if not Path(t).exists():
                # 日志目标既不在 DB 也不在文件系统，通常正常（旧版被替换）
                if verbose:
                    print(f"  LOG-GONE (expected)  target={t}")
            else:
                issues.append(
                    f"[ORPHAN_FILE_LOG]  日志曾写入但DB无记录，文件仍存在: {t}"
                )

        # ── 检查4：曾被"未完全删除"警报的目录是否已清理 ─────────────────────
        for blocked in sorted(ever_blocked_dirs):
            p = Path(blocked)
            if p.exists():
                # 统计目录内容
                try:
                    contents = list(p.iterdir())
                    files = [c for c in contents if c.is_file()]
                    subdirs = [c for c in contents if c.is_dir()]
                    # 判断是否都是元数据文件
                    real_exts = {".mkv", ".mp4", ".avi", ".mov", ".webm", ".flv",
                                 ".ass", ".srt", ".ssa", ".vtt", ".mka", ".sup"}
                    real_files = [f for f in files if f.suffix.lower() in real_exts]
                    if real_files:
                        issues.append(
                            f"[BLOCKED_DIR_HAS_MEDIA]  旧目标目录仍含真实媒体文件({len(real_files)}): {blocked}"
                        )
                    elif files or subdirs:
                        issues.append(
                            f"[BLOCKED_DIR_METADATA_ONLY]  旧目标目录仍存在（仅元数据/子目录，可手动清理）: "
                            f"{blocked}  ({len(files)} files, {len(subdirs)} dirs)"
                        )
                    else:
                        issues.append(
                            f"[BLOCKED_DIR_EMPTY]  旧目标目录仍存在但已为空（可手动 rmdir）: {blocked}"
                        )
                except OSError:
                    pass

        # ── 输出结果 ──────────────────────────────────────────────────────────
        print(f"\n{'='*60}")
        print(f"扫描 DB 记录: {len(rows)} 条 | 日志目标: {len(logged_targets)} 条")
        print(f"正常: {ok_count}  | 问题: {len(issues)}")
        print(f"{'='*60}")

        if issues:
            print("\n问题列表:")
            for issue in issues:
                print(f"  {issue}")
            return 1
        else:
            print("\n✓ 未发现路径合理性问题")
            return 0

    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="检查媒体库目标路径合理性")
    parser.add_argument(
        "--log-dir",
        default="logs",
        help="日志目录路径（默认: logs）",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="输出所有 OK 条目",
    )
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    if not log_dir.is_dir():
        print(f"错误: 日志目录不存在: {log_dir}", file=sys.stderr)
        sys.exit(2)

    sys.exit(run_checks(log_dir, verbose=args.verbose))


if __name__ == "__main__":
    main()
