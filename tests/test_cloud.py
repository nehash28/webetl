import json
import sqlite3

from webetl.pipeline import Pipeline
from webetl.state import mirror_run
from webetl.utils import normalize_db_url


def test_normalize_db_url():
    assert normalize_db_url("postgresql://u:p@h/db?sslmode=require") == "postgresql+psycopg2://u:p@h/db?sslmode=require"
    assert normalize_db_url("postgres://u:p@h/db") == "postgresql+psycopg2://u:p@h/db"
    assert normalize_db_url("postgresql+psycopg://u@h/db") == "postgresql+psycopg://u@h/db"
    assert normalize_db_url("sqlite:///x.db") == "sqlite:///x.db"


def test_run_history_mirrored_to_sql(server, paths, monkeypatch):
    hist = paths["tmp"] / "hist.db"
    monkeypatch.setenv("RUN_HISTORY_URL", f"sqlite:///{hist}")
    cfg = {"name": "mirror", "rejects": {"path": paths["rejects"]},
           "source": {"type": "api", "url": f"{server}/api/events", "records_path": "results",
                      "pagination": {"type": "cursor", "cursor_param": "cursor", "cursor_path": "next_cursor"}},
           "load": [{"type": "sql", "url": paths["db"], "table": "events", "key": ["event_id"]}],
           "checks": [{"type": "row_count", "min": 1}]}
    ok = Pipeline(cfg, state_path=paths["state"]).run()
    cfg["source"]["url"] = f"{server}/nope"
    bad = Pipeline(cfg, state_path=paths["state"]).run()
    con = sqlite3.connect(hist)
    rows = con.execute("SELECT run_id, status, rows_loaded, checks_passed, error, stats FROM etl_runs "
                       "ORDER BY started_at").fetchall()
    assert [(r[0], r[1]) for r in rows] == [(ok.run_id, "success"), (bad.run_id, "failed")]
    assert rows[0][2] == 25 and rows[0][3] == 1 and "404" in rows[1][4]
    assert json.loads(rows[0][5])["transform"]["out"] == 25


def test_mirror_failure_never_breaks_run(tmp_path, caplog):
    mirror_run("not-a-url", "r1", "p", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00",
               "success", {}, None)
    assert "could not write run history" in caplog.text
