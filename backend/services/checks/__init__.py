"""Checks service package.

Exposes run_checks() as the main entry point.
"""
from __future__ import annotations

from .runner import run_checks_for_group, run_checks_full

__all__ = ["run_checks_for_group", "run_checks_full"]
