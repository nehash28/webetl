"""HTML scraper driven by CSS selectors.

Field spec forms:
  title: "h3 a"                                   # shorthand -> text of first match
  url:   {selector: "h3 a", attr: href}           # attribute (href/src made absolute)
  tags:  {selector: ".tag", all: true}            # list of all matches
  price: {selector: ".price", regex: "([0-9.]+)"} # first regex group
  raw:   {selector: ".body", attr: html}          # inner HTML
  name:  {selector: ".", attr: data-name}         # "." = the item element itself
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from webetl.extractors.base import Extractor

log = logging.getLogger(__name__)

_URL_ATTRS = {"href", "src", "action", "data-src"}


def _normalize_spec(spec: Any) -> dict[str, Any]:
    if isinstance(spec, str):
        return {"selector": spec}
    if not isinstance(spec, dict) or "selector" not in spec:
        raise ValueError(f"Invalid field spec: {spec!r}")
    return spec


def _value(el: Tag, spec: dict[str, Any], base_url: str) -> Any:
    attr = spec.get("attr", "text")
    if attr == "text":
        val: Any = el.get_text(" ", strip=True)
    elif attr == "html":
        val = el.decode_contents().strip()
    else:
        val = el.get(attr)
        if isinstance(val, list):
            val = " ".join(val)
        if val is not None and spec.get("absolute", attr in _URL_ATTRS):
            val = urljoin(base_url, val)
    if val is not None and spec.get("regex"):
        m = re.search(spec["regex"], str(val))
        val = (m.group(1) if m.groups() else m.group(0)) if m else None
    return val


def extract_fields(root: Tag, fields: dict[str, Any], base_url: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, raw_spec in fields.items():
        spec = _normalize_spec(raw_spec)
        sel = spec["selector"]
        if spec.get("all"):
            els = [root] if sel in ("", ".") else root.select(sel)
            vals = [_value(e, spec, base_url) for e in els]
            out[name] = [v for v in vals if v not in (None, "")]
        else:
            el = root if sel in ("", ".") else root.select_one(sel)
            out[name] = _value(el, spec, base_url) if el is not None else spec.get("default")
    return out


class HtmlExtractor(Extractor):
    def _soup(self, url: str) -> BeautifulSoup:
        resp = self.client.get(url)
        self.stats["requests"] += 1
        if self.source.get("encoding"):
            resp.encoding = self.source["encoding"]
        return BeautifulSoup(resp.text, self.source.get("parser", "lxml"))

    def _start_urls(self) -> list[str]:
        urls = self.source.get("urls") or []
        if self.source.get("url"):
            urls = [self.source["url"], *urls]
        if not urls:
            raise ValueError("html source needs 'url' or 'urls'")
        return list(urls)

    def _records(self) -> Iterator[dict[str, Any]]:
        item_sel = self.source.get("item_selector")
        fields = self.source.get("fields") or {}
        if not item_sel or not fields:
            raise ValueError("html source needs 'item_selector' and 'fields'")
        pag = self.source.get("pagination") or {}
        next_sel = pag.get("next_selector")
        max_pages = int(pag.get("max_pages", 100))
        follow = self.source.get("follow")

        seen: set[str] = set()
        for start in self._start_urls():
            url: str | None = start
            pages = 0
            while url and url not in seen and pages < max_pages:
                seen.add(url)
                soup = self._soup(url)
                pages += 1
                self.stats["pages"] += 1
                for item in soup.select(item_sel):
                    rec = extract_fields(item, fields, url)
                    rec["_page_url"] = url
                    if follow:
                        rec.update(self._follow(rec, follow))
                    yield rec
                nxt = soup.select_one(next_sel) if next_sel else None
                url = urljoin(url, nxt.get("href")) if nxt is not None and nxt.get("href") else None
        log.info("HTML extract finished: %s pages, %s records", self.stats["pages"], self.stats["records"])

    def _follow(self, rec: dict[str, Any], follow: dict[str, Any]) -> dict[str, Any]:
        """Visit a detail page linked from the record and merge more fields."""
        link = rec.get(follow["url_field"])
        if not link:
            return {}
        try:
            soup = self._soup(link)
        except Exception as exc:  # keep the list record even if the detail page fails
            log.warning("Detail page failed for %s: %s", link, exc)
            return {"_detail_error": str(exc)}
        root = soup.select_one(follow["root_selector"]) if follow.get("root_selector") else soup
        return extract_fields(root or soup, follow.get("fields") or {}, link)
