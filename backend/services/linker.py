"""Path filters and hardlink helpers."""
from __future__ import annotations

import fnmatch
import os
import re
from pathlib import Path


def get_file_identity(path: str | Path) -> tuple[int, int] | None:
    try:
        stat_result = os.stat(path)
    except OSError:
        return None
    return int(stat_result.st_dev), int(stat_result.st_ino)


def get_inode(path: str | Path) -> int | None:
    identity = get_file_identity(path)
    if identity is None:
        return None
    return identity[1]


def is_same_inode(path1: str | Path, path2: str | Path) -> bool:
    i1 = get_file_identity(path1)
    i2 = get_file_identity(path2)
    return i1 is not None and i1 == i2


def path_excluded(path: Path, include: str, exclude: str, root_path: Path | None = None) -> bool:
    if include and not _matches(path, include, root_path):
        return True
    if exclude and _matches(path, exclude, root_path):
        return True
    return False


def _matches(path: Path, patterns: str, root_path: Path | None) -> bool:
    if not patterns.strip():
        return False
    ps = [x.strip() for x in re.split(r"[,;\n\r]+", patterns) if x.strip()]
    path_str = path.as_posix()
    rel_str = None
    if root_path:
        try:
            rel_str = path.relative_to(root_path).as_posix()
        except Exception:
            rel_str = None

    for pat in ps:
        p = pat.replace("\\", "/")
        if p.startswith(".") and "/" not in p:
            p = f"*{p}"

        if "/" in p:
            if p.startswith("/") and p.endswith("/"):
                needle = p
                if needle in f"/{path_str}/":
                    return True
                if rel_str and needle in f"/{rel_str}/":
                    return True
            elif p.startswith("/"):
                anchor = p.lstrip("/")
                if rel_str and fnmatch.fnmatch(rel_str, anchor):
                    return True
            else:
                if fnmatch.fnmatch(path_str, p):
                    return True
                if rel_str and fnmatch.fnmatch(rel_str, p):
                    return True
        else:
            if fnmatch.fnmatch(path.name, p):
                return True
    return False
