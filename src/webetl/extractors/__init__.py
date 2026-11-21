"""Extractors turn a web source into a stream of raw dict records."""

from __future__ import annotations

from typing import Any

from webetl.extractors.api import ApiExtractor
from webetl.extractors.base import Extractor
from webetl.extractors.html import HtmlExtractor
from webetl.http import HttpClient

EXTRACTORS: dict[str, type[Extractor]] = {
    "api": ApiExtractor,
    "html": HtmlExtractor,
}


def build_client(source: dict[str, Any]) -> HttpClient:
    is_html = source["type"] == "html"
    return HttpClient(
        headers=source.get("headers"),
        timeout=float(source.get("timeout", 30)),
        retries=int(source.get("retries", 5)),
        backoff_factor=float(source.get("backoff_factor", 0.5)),
        # Be polite by default when scraping.
        rate_limit_per_sec=source.get("rate_limit_per_sec", 1.0 if is_html else None),
        user_agent=source.get("user_agent"),
        respect_robots=bool(source.get("respect_robots", is_html)),
        verify=source.get("verify_ssl", True),
    )


def build_extractor(source: dict[str, Any], watermark: str | None = None,
                    client: HttpClient | None = None) -> Extractor:
    kind = source["type"]
    if kind not in EXTRACTORS:
        raise ValueError(f"Unknown source type '{kind}'. Available: {sorted(EXTRACTORS)}")
    return EXTRACTORS[kind](source, client or build_client(source), watermark)


__all__ = ["Extractor", "ApiExtractor", "HtmlExtractor", "build_extractor", "build_client", "EXTRACTORS"]
