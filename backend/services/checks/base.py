"""Base class for checkers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from ...models import SyncGroup


@dataclass
class IssueData:
    """A single detected issue, before persistence."""
    checker_code: str
    issue_code: str
    severity: str  # "error" | "warning"
    sync_group_id: int | None = None
    source_path: str | None = None
    target_path: str | None = None
    resource_dir: str | None = None
    tmdb_id: int | None = None
    season: int | None = None
    episode: int | None = None
    payload: dict = field(default_factory=dict)


class CheckerBase:
    """Abstract base for all library checkers."""

    checker_code: str = ""

    def run(self, db: "Session", groups: list["SyncGroup"]) -> list[IssueData]:
        raise NotImplementedError
