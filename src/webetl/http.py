"""HTTP client with retries, exponential backoff, Retry-After support and rate limiting."""

from __future__ import annotations

import logging
import threading
import time
import urllib.robotparser
from typing import Any
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

DEFAULT_UA = "webetl/0.1 (+https://github.com/your-org/webetl)"


class RateLimiter:
    """Simple minimum-interval limiter (requests per second)."""

    def __init__(self, per_second: float | None):
        self.interval = 1.0 / per_second if per_second else 0.0
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        if not self.interval:
            return
        with self._lock:
            now = time.monotonic()
            delay = self._last + self.interval - now
            if delay > 0:
                time.sleep(delay)
            self._last = time.monotonic()


class HttpClient:
    def __init__(
        self,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        retries: int = 5,
        backoff_factor: float = 0.5,
        rate_limit_per_sec: float | None = None,
        user_agent: str | None = None,
        respect_robots: bool = False,
        verify: bool | str = True,
    ):
        self.timeout = timeout
        self.limiter = RateLimiter(rate_limit_per_sec)
        self.respect_robots = respect_robots
        self._robots: dict[str, urllib.robotparser.RobotFileParser | None] = {}

        retry = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST", "HEAD"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=10)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.verify = verify
        self.user_agent = user_agent or DEFAULT_UA
        self.session.headers.update({"User-Agent": self.user_agent})
        if headers:
            self.session.headers.update(headers)

    # robots.txt ---------------------------------------------------------
    def allowed(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        if origin not in self._robots:
            parser = urllib.robotparser.RobotFileParser()
            try:
                resp = self.session.get(f"{origin}/robots.txt", timeout=self.timeout)
                if resp.status_code >= 400:
                    self._robots[origin] = None  # no robots.txt -> allow all
                else:
                    parser.parse(resp.text.splitlines())
                    self._robots[origin] = parser
            except requests.RequestException:
                self._robots[origin] = None
        parser = self._robots[origin]
        return True if parser is None else parser.can_fetch(self.user_agent, url)

    # requests -------------------------------------------------------------
    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        if not self.allowed(url):
            raise PermissionError(f"robots.txt disallows fetching {url}")
        self.limiter.wait()
        kwargs.setdefault("timeout", self.timeout)
        log.debug("%s %s params=%s", method, url, kwargs.get("params"))
        resp = self.session.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def close(self) -> None:
        self.session.close()
