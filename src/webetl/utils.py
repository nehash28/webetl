"""Small helpers shared across modules."""

from __future__ import annotations

from typing import Any, Iterable, Iterator


def get_path(obj: Any, path: str | None, default: Any = None) -> Any:
    """Read a dotted path such as 'data.items' or 'meta.next.0.href' from nested JSON."""
    if path in (None, "", "."):
        return obj
    cur = obj
    for part in str(path).split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return default
            cur = cur[part]
        elif isinstance(cur, list) and part.lstrip("-").isdigit():
            idx = int(part)
            if not -len(cur) <= idx < len(cur):
                return default
            cur = cur[idx]
        else:
            return default
    return cur


def batched(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def normalize_db_url(url: str) -> str:
    """Use the psycopg2 driver for plain PostgreSQL URLs.

    Hosted Postgres (Neon, Supabase, Heroku, ...) hands out `postgresql://` or `postgres://`
    URLs. SQLAlchemy 2.1 maps those to psycopg 3, which webetl does not install, so pin
    them to psycopg2 (installed with the `postgres` extra). Explicit drivers are kept.
    """
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix):]
    return url
