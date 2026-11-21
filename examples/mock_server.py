"""A small local web server that imitates real sources, for demos and tests.

  JSON API   /api/products   page pagination, bearer auth, ?updated_since= incremental
             /api/events     cursor pagination
             /api/repos      GitHub-style Link header pagination
             /api/flaky      fails with 503 twice, then succeeds (retry demo)
  HTML       /catalogue/page-N.html   books.toscrape.com-style listing + detail pages
             /robots.txt     disallows /private/

Run standalone:  python examples/mock_server.py --port 8765
"""

from __future__ import annotations

import argparse
import json
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

TOKEN = "demo-token"
BASE_TIME = datetime(2026, 11, 1, tzinfo=timezone.utc)
CATEGORIES = ["books", "electronics", "garden", "toys"]

PRODUCTS = [
    {
        "id": i,
        "name": f"  Product {i}  ",
        "price": {"amount": f"{(i * 7.35) % 200 + 1:.2f}", "currency": "USD"},
        "category": CATEGORIES[i % len(CATEGORIES)],
        "in_stock": "yes" if i % 3 else "no",
        "tags": ["new"] if i % 5 == 0 else [],
        "updated_at": (BASE_TIME + timedelta(hours=i)).isoformat().replace("+00:00", "Z"),
    }
    for i in range(1, 58)
]
PRODUCTS[12]["price"]["amount"] = "N/A"  # a dirty record that should be rejected

EVENTS = [{"event_id": f"e{i:03d}", "type": ["click", "view", "buy"][i % 3], "ts": 1762000000 + i * 60}
          for i in range(1, 26)]

RATINGS = ["One", "Two", "Three", "Four", "Five"]
BOOKS = [
    {"id": i, "title": f"Book Title {i}", "price": f"£{10 + (i * 3.17) % 40:.2f}",
     "rating": RATINGS[i % 5], "stock": i % 4 != 0, "upc": f"UPC{i:06d}",
     "description": f"A gripping description of book {i}."}
    for i in range(1, 46)
]
BOOKS_PER_PAGE = 20

# Mutable per-server state (e.g. the flaky endpoint counter)
STATE = {"flaky_calls": 0, "slack": []}


def _book_listing(page: int) -> str:
    start = (page - 1) * BOOKS_PER_PAGE
    items = BOOKS[start:start + BOOKS_PER_PAGE]
    pods = "".join(
        f"""<li><article class="product_pod">
  <p class="star-rating {b['rating']}"></p>
  <h3><a href="book-{b['id']}/index.html" title="{b['title']}">{b['title'][:12]}...</a></h3>
  <div class="product_price"><p class="price_color">{b['price']}</p>
  <p class="instock availability">{'In stock' if b['stock'] else 'Out of stock'}</p></div>
</article></li>"""
        for b in items)
    pages = (len(BOOKS) + BOOKS_PER_PAGE - 1) // BOOKS_PER_PAGE
    nxt = f'<li class="next"><a href="page-{page + 1}.html">next</a></li>' if page < pages else ""
    return f"<html><body><ol class='row'>{pods}</ol><ul class='pager'>{nxt}</ul></body></html>"


def _book_detail(book_id: int) -> str | None:
    b = next((x for x in BOOKS if x["id"] == book_id), None)
    if not b:
        return None
    return f"""<html><body><article class="product_page">
<h1>{b['title']}</h1>
<div id="product_description"><h2>Product Description</h2></div><p>{b['description']}</p>
<table class="table table-striped"><tr><th>UPC</th><td>{b['upc']}</td></tr></table>
</article></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # keep test output quiet
        pass

    def _send(self, status: int, body: str | bytes, ctype: str = "application/json", headers=None):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _json(self, obj, status=200, headers=None):
        self._send(status, json.dumps(obj), headers=headers)

    def do_GET(self):  # noqa: C901 - simple router
        u = urlsplit(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        path = u.path
        host = f"http://{self.headers.get('Host')}"

        if path == "/robots.txt":
            return self._send(200, "User-agent: *\nDisallow: /private/\n", "text/plain")

        if path == "/api/products":
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                return self._json({"error": "unauthorized"}, 401)
            items = PRODUCTS
            if "updated_since" in q:
                since = datetime.fromisoformat(q["updated_since"].replace("Z", "+00:00"))
                items = [p for p in items if datetime.fromisoformat(p["updated_at"].replace("Z", "+00:00")) >= since]
            page, per = int(q.get("page", 1)), int(q.get("per_page", 20))
            chunk = items[(page - 1) * per: page * per]
            return self._json({"data": {"items": chunk}, "meta": {"page": page, "total": len(items)}})

        if path == "/api/events":
            start = int(q.get("cursor", 0))
            chunk = EVENTS[start:start + 10]
            nxt = start + 10 if start + 10 < len(EVENTS) else None
            return self._json({"results": chunk, "next_cursor": nxt})

        if path == "/api/repos":
            page, per = int(q.get("page", 1)), int(q.get("per_page", 10))
            repos = [{"id": i, "full_name": f"org/repo-{i}", "owner": {"login": "org"},
                      "stargazers_count": i * 3} for i in range(1, 24)]
            chunk = repos[(page - 1) * per: page * per]
            headers = {}
            if page * per < len(repos):
                headers["Link"] = f'<{host}/api/repos?page={page + 1}&per_page={per}>; rel="next"'
            return self._json(chunk, headers=headers)

        if path == "/api/flaky":
            STATE["flaky_calls"] += 1
            if STATE["flaky_calls"] % 3 != 0:
                return self._json({"error": "try again"}, 503, headers={"Retry-After": "0"})
            return self._json([{"ok": True, "attempt": STATE["flaky_calls"]}])

        if path.startswith("/catalogue/page-") and path.endswith(".html"):
            page = int(path[len("/catalogue/page-"):-5])
            return self._send(200, _book_listing(page), "text/html; charset=utf-8")

        if path.startswith("/catalogue/book-") and path.endswith("/index.html"):
            html = _book_detail(int(path.split("/")[2][5:]))
            if html:
                return self._send(200, html, "text/html; charset=utf-8")

        if path.startswith("/private/"):
            return self._send(200, "<html><body><div class='secret'>nope</div></body></html>", "text/html")

        return self._json({"error": "not found"}, 404)

    def do_POST(self):
        """Fake Slack incoming webhook: /slack/ok records payloads, /slack/bad rejects them."""
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        if self.path == "/slack/ok":
            STATE["slack"].append(json.loads(body))
            return self._send(200, "ok", "text/plain")
        if self.path == "/slack/bad":
            return self._send(404, "no_service", "text/plain")
        return self._json({"error": "not found"}, 404)


def start(port: int = 0) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1", help="use 0.0.0.0 inside Docker")
    a = ap.parse_args()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f"mock sources on http://{a.host}:{a.port}  (token: {TOKEN})", flush=True)
    srv.serve_forever()
