"""Loader interface: open() -> write(batch)* -> close(success)."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class LoadContext:
    pipeline: str
    run_id: str
    started_at: datetime
    schema: dict[str, dict[str, Any]] | None = None  # normalized column specs, if declared
    extra: dict[str, Any] = field(default_factory=dict)


class Loader(abc.ABC):
    def __init__(self, target: dict[str, Any], ctx: LoadContext):
        self.target = target
        self.ctx = ctx
        self.rows_written = 0

    @property
    def name(self) -> str:
        return self.target.get("name") or self.target["type"]

    def open(self) -> None:  # noqa: B027 - optional hook
        pass

    @abc.abstractmethod
    def write(self, records: list[dict[str, Any]]) -> int:
        """Write a batch; return the number of rows written."""

    def close(self, success: bool) -> None:  # noqa: B027 - optional hook
        """Commit on success; roll back / clean up on failure."""
