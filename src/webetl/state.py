"""Persistent pipeline state (incremental watermarks + run history) in a local SQLite file."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class StateStore:
    def __init__(self, path: str | Path = ".webetl/state.db"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS watermarks ("
                " pipeline TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS runs ("
                " run_id TEXT PRIMARY KEY, pipeline TEXT, started_at TEXT, finished_at TEXT,"
                " status TEXT, stats TEXT, error TEXT)"
            )

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def get_watermark(self, pipeline: str) -> str | None:
        with self._conn() as c:
            row = c.execute("SELECT value FROM watermarks WHERE pipeline = ?", (pipeline,)).fetchone()
        return row[0] if row else None

    def set_watermark(self, pipeline: str, value: Any) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as c:
            c.execute(
                "INSERT INTO watermarks (pipeline, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(pipeline) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (pipeline, str(value), now),
            )

    def reset(self, pipeline: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM watermarks WHERE pipeline = ?", (pipeline,))

    def record_run(self, run_id: str, pipeline: str, started_at: str, finished_at: str,
                   status: str, stats: dict[str, Any], error: str | None) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                (run_id, pipeline, started_at, finished_at, status, json.dumps(stats), error),
            )

    def history(self, pipeline: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        sql = "SELECT run_id, pipeline, started_at, finished_at, status, stats, error FROM runs"
        args: tuple = ()
        if pipeline:
            sql += " WHERE pipeline = ?"
            args = (pipeline,)
        sql += " ORDER BY started_at DESC LIMIT ?"
        with self._conn() as c:
            rows = c.execute(sql, args + (limit,)).fetchall()
        keys = ["run_id", "pipeline", "started_at", "finished_at", "status", "stats", "error"]
        out = [dict(zip(keys, r)) for r in rows]
        for r in out:
            r["stats"] = json.loads(r["stats"] or "{}")
        return out


# --- optional: mirror run history into a SQL database ------------------------------
# Set RUN_HISTORY_URL (any SQLAlchemy URL, e.g. your warehouse) and every run is also
# written to an `etl_runs` table there. A dashboard hosted elsewhere (Streamlit Cloud)
# can then show run history and data quality results without access to state.db.

def _runs_table(md):
    import sqlalchemy as sa

    return sa.Table(
        "etl_runs", md,
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("pipeline", sa.String(200), index=True),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20)),
        sa.Column("rows_loaded", sa.BigInteger),
        sa.Column("rows_rejected", sa.BigInteger),
        sa.Column("checks_passed", sa.Integer),
        sa.Column("checks_failed", sa.Integer),
        sa.Column("error", sa.Text),
        sa.Column("stats", sa.Text),  # full stats as JSON
    )


def mirror_run(url: str, run_id: str, pipeline: str, started_at: str, finished_at: str,
               status: str, stats: dict[str, Any], error: str | None) -> None:
    """Write one run to <url>.etl_runs. Never raises: run history must not fail a run."""
    import logging

    import sqlalchemy as sa

    from webetl.utils import normalize_db_url

    log = logging.getLogger(__name__)
    try:
        engine = sa.create_engine(normalize_db_url(url))
        md = sa.MetaData()
        table = _runs_table(md)
        md.create_all(engine, checkfirst=True)
        t = stats.get("transform") or {}
        q = stats.get("quality") or {}
        row = {
            "run_id": run_id, "pipeline": pipeline,
            "started_at": datetime.fromisoformat(started_at), "finished_at": datetime.fromisoformat(finished_at),
            "status": status, "rows_loaded": t.get("out"), "rows_rejected": t.get("rejected"),
            "checks_passed": q.get("passed"), "checks_failed": q.get("failed"),
            "error": error, "stats": json.dumps(stats, default=str),
        }
        with engine.begin() as conn:
            conn.execute(sa.delete(table).where(table.c.run_id == run_id))
            conn.execute(sa.insert(table), [row])
        engine.dispose()
    except Exception as exc:
        log.warning("could not write run history to %s: %s", sa.make_url(url).render_as_string(hide_password=True)
                    if "://" in url else "RUN_HISTORY_URL", exc)
