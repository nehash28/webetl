"""REST/JSON API extractor with auth, pagination and incremental loading.

Supported pagination types:
  none        single request
  page        ?page=N (stops on empty page / short page / max_pages)
  offset      ?offset=N&limit=M
  cursor      cursor value read from the response body (cursor_path) and sent as cursor_param
  next_url    full next-page URL read from the response body (next_url_path)
  link_header RFC 5988 "Link: <...>; rel=next" header (GitHub style)
"""

from __future__ import annotations

import logging
from typing import Any, Iterator
from urllib.parse import urljoin

from webetl.extractors.base import Extractor
from webetl.utils import get_path

log = logging.getLogger(__name__)


class ApiExtractor(Extractor):
    def _auth_kwargs(self, params: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        auth = self.source.get("auth") or {}
        kind = (auth.get("type") or "none").lower()
        if kind == "bearer":
            headers["Authorization"] = f"Bearer {auth['token']}"
        elif kind == "api_key":
            name, value = auth.get("name", "X-API-Key"), auth["value"]
            if auth.get("in", "header") == "query":
                params[name] = value
            else:
                headers[name] = value
        elif kind == "basic":
            return {"auth": (auth["username"], auth["password"])}
        elif kind != "none":
            raise ValueError(f"Unsupported auth type '{kind}'")
        return {}

    def _fetch(self, url: str, params: dict[str, Any]) -> tuple[Any, Any]:
        method = self.source.get("method", "GET").upper()
        headers: dict[str, str] = {}
        params = dict(params)
        extra = self._auth_kwargs(params, headers)
        kwargs: dict[str, Any] = {"params": params, "headers": headers, **extra}
        if method == "POST" and self.source.get("body") is not None:
            kwargs["json"] = self.source["body"]
        resp = self.client.request(method, url, **kwargs)
        self.stats["requests"] += 1
        self.stats["pages"] += 1
        return resp.json(), resp

    def _page_records(self, body: Any) -> list[dict[str, Any]]:
        records = get_path(body, self.source.get("records_path"))
        if records is None:
            return []
        if isinstance(records, dict):
            records = [records]
        if not isinstance(records, list):
            raise ValueError(f"records_path did not resolve to a list (got {type(records).__name__})")
        return [r if isinstance(r, dict) else {"value": r} for r in records]

    def _records(self) -> Iterator[dict[str, Any]]:
        url: str | None = self.source["url"]
        base_params: dict[str, Any] = dict(self.source.get("params") or {})
        if self.inc_param and self.watermark is not None:
            base_params[self.inc_param] = self.watermark

        pag = self.source.get("pagination") or {"type": "none"}
        ptype = pag.get("type", "none")
        max_pages = int(pag.get("max_pages", 10_000))
        size = pag.get("size") or pag.get("limit")
        if size is not None:
            size = int(size)

        page = int(pag.get("start", 1 if ptype == "page" else 0))
        cursor: Any = None
        fetched = 0

        while url and fetched < max_pages:
            params = dict(base_params)
            if ptype == "page":
                params[pag.get("page_param", "page")] = page
                if size:
                    params[pag.get("size_param", "per_page")] = size
            elif ptype == "offset":
                params[pag.get("offset_param", "offset")] = page
                if size:
                    params[pag.get("limit_param", "limit")] = size
            elif ptype == "cursor" and cursor is not None:
                params[pag.get("cursor_param", "cursor")] = cursor
            elif ptype in ("next_url", "link_header") and fetched > 0:
                params = {}  # the next URL already carries its query string

            body, resp = self._fetch(url, params)
            fetched += 1
            records = self._page_records(body)
            yield from records

            if ptype == "none" or not records:
                break
            if ptype == "page":
                if size and len(records) < size:
                    break
                page += 1
            elif ptype == "offset":
                if size and len(records) < size:
                    break
                page += len(records)
            elif ptype == "cursor":
                cursor = get_path(body, pag.get("cursor_path", "next_cursor"))
                if not cursor:
                    break
            elif ptype == "next_url":
                nxt = get_path(body, pag.get("next_url_path", "next"))
                url = urljoin(url, nxt) if nxt else None
            elif ptype == "link_header":
                nxt = resp.links.get("next", {}).get("url")
                url = urljoin(url, nxt) if nxt else None
            else:
                raise ValueError(f"Unknown pagination type '{ptype}'")
        log.info("API extract finished: %s pages, %s records", fetched, self.stats["records"])
