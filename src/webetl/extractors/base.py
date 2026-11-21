"""Extractor base class with incremental (watermark) tracking."""

from __future__ import annotations

import abc
from datetime import timezone
from typing import Any, Iterator

from dateutil import parser as dtparser

from webetl.http import HttpClient
from webetl.utils import get_path


def watermark_key(value: Any) -> tuple[int, Any]:
    """Make watermark values comparable: numbers < datetimes < plain strings."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (0, float(value))
    s = str(value)
    try:
        return (0, float(s))
    except ValueError:
        pass
    try:
        dt = dtparser.parse(s)
    except (ValueError, OverflowError):
        return (2, s)
    if dt.tzinfo is not None:  # compare everything as naive UTC
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return (1, dt)


class Extractor(abc.ABC):
    def __init__(self, source: dict[str, Any], client: HttpClient, watermark: str | None = None):
        self.source = source
        self.client = client
        inc = source.get("incremental") or {}
        self.inc_field: str | None = inc.get("field")
        self.inc_param: str | None = inc.get("param")
        self.client_filter: bool = bool(inc.get("client_filter", True))
        self.watermark: str | None = watermark if watermark is not None else inc.get("initial")
        self.max_watermark: Any = None
        self.stats = {"requests": 0, "pages": 0, "records": 0, "skipped_by_watermark": 0}

    @abc.abstractmethod
    def _records(self) -> Iterator[dict[str, Any]]:
        """Yield raw records from the source."""

    def extract(self) -> Iterator[dict[str, Any]]:
        for rec in self._records():
            if self.inc_field:
                value = get_path(rec, self.inc_field)
                if value is not None:
                    if (self.client_filter and self.watermark is not None
                            and watermark_key(value) < watermark_key(self.watermark)):
                        self.stats["skipped_by_watermark"] += 1
                        continue
                    if self.max_watermark is None or watermark_key(value) > watermark_key(self.max_watermark):
                        self.max_watermark = value
            self.stats["records"] += 1
            yield rec

    def close(self) -> None:
        self.client.close()
