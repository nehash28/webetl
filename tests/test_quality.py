import json
import os
from pathlib import Path

import pytest

from webetl.config import load_config
from webetl.pipeline import Pipeline
from webetl.quality import QualityConfigError, QualityRunner, validate_checks

CONFIGS = Path(__file__).resolve().parents[1] / "configs"


@pytest.fixture
def cfg(server, paths, monkeypatch):
    monkeypatch.setenv("API_BASE_URL", server)
    monkeypatch.setenv("DATABASE_URL", paths["db"])
    monkeypatch.setenv("LAKE_PATH", paths["lake"])
    monkeypatch.setenv("SLACK_WEBHOOK_URL", f"{server}/slack/ok")
    monkeypatch.setenv("SMTP_HOST", "")
    c = load_config(CONFIGS / "products_medallion.yaml")
    c["rejects"] = {"path": paths["rejects"]}
    return c


def _by_name(res):
    return {r["name"]: r for r in res.stats["quality"]["results"]}


def test_all_example_checks_pass(cfg, paths, slack_posts):
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "success", res.error
    q = res.stats["quality"]
    assert (q["passed"], q["failed"], q["errors"], q["warnings"]) == (8, 0, [], [])
    assert _by_name(res)["silver_has_data"]["value"] == 56
    # still one rejected record -> a `rejects` alert, not a quality warning
    assert ":warning:" in json.dumps(slack_posts[0])


def test_error_check_fails_run_and_alerts(cfg, paths, slack_posts):
    cfg["checks"].append({"name": "cheap_only", "type": "range", "column": "price", "max": 50})
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "failed"
    assert res.error == "data quality checks failed: cheap_only"
    r = _by_name(res)["cheap_only"]
    assert r["status"] == "fail" and r["value"] == 41
    post = json.dumps(slack_posts[0])
    assert "FAILED" in post and "cheap_only" in post and "41 row(s)" in post
    # like dbt tests, the data is committed; the failure is loud, not a rollback
    assert res.stats["load"]["warehouse"] == 56


def test_warn_check_keeps_success_but_sends_warning(cfg, paths, slack_posts):
    cfg["checks"][3]["values"] = ["books", "toys"]  # known_categories, severity warn
    cfg["max_reject_ratio"] = None
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "success"
    assert res.stats["quality"]["warnings"] == ["known_categories"]
    assert _by_name(res)["known_categories"]["value"] == 28
    post = json.dumps(slack_posts[0])
    assert "data quality warnings" in post and ":large_yellow_circle:" in post


def test_broken_check_counts_as_failure(cfg, paths):
    cfg["checks"] = [{"name": "typo", "type": "not_null", "columns": ["no_such_column"]}]
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "failed"
    assert _by_name(res)["typo"]["status"] == "error"


def test_run_table_and_where_filter(cfg, paths):
    cfg["checks"] = [
        {"name": "run_rows", "type": "row_count", "table": "run", "min": 56, "max": 56},
        {"name": "toys_cheap", "type": "range", "table": "run", "column": "price", "max": 1,
         "where": "category = 'toys'", "severity": "warn"},
        {"name": "dupes_in_history", "type": "unique", "table": "silver_all", "columns": ["id"], "severity": "warn"},
    ]
    p = Pipeline(cfg, state_path=paths["state"])
    p.run()
    res = p.run(full_refresh=True)  # second load: silver_all now has every id twice, silver does not
    by = _by_name(res)
    assert by["run_rows"]["status"] == "pass"
    assert by["toys_cheap"]["value"] == 14
    assert by["dupes_in_history"]["value"] == 56
    assert res.status == "success" and set(res.stats["quality"]["warnings"]) == {"toys_cheap", "dupes_in_history"}


def test_check_command_skips_run_table(cfg, paths, capsys):
    Pipeline(cfg, state_path=paths["state"]).run()
    summary = Pipeline(cfg, state_path=paths["state"]).run_checks()
    assert summary["skipped"] == 1 and summary["failed"] == 0


def test_checks_without_medallion(server, paths):
    cfg = {"name": "plain", "rejects": {"path": paths["rejects"]},
           "source": {"type": "api", "url": f"{server}/api/events", "records_path": "results",
                      "pagination": {"type": "cursor", "cursor_param": "cursor", "cursor_path": "next_cursor"}},
           "load": [{"type": "sql", "url": paths["db"], "table": "events", "key": ["event_id"]}],
           "checks": [{"type": "unique", "columns": ["event_id"]},
                      {"type": "accepted_values", "column": "type", "values": ["click", "view", "buy"]},
                      {"type": "row_count", "min": 25}]}
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "success" and res.stats["quality"]["passed"] == 3
    assert {r["table"] for r in res.stats["quality"]["results"]} == {"run"}


@pytest.mark.parametrize("bad", [
    [{"type": "nope"}],
    [{"type": "unique"}],
    [{"type": "range", "column": "x"}],
    [{"type": "unique", "columns": ["x; drop table y"]}],
    [{"type": "row_count", "min": 1, "severity": "fatal"}],
    [{"name": "a", "type": "row_count", "min": 1}, {"name": "a", "type": "row_count", "min": 2}],
])
def test_invalid_check_configs(bad):
    with pytest.raises(QualityConfigError):
        validate_checks(bad)


@pytest.mark.skipif(not os.environ.get("WEBETL_TEST_POSTGRES"), reason="set WEBETL_TEST_POSTGRES=<sqlalchemy url>")
def test_medallion_on_postgres(cfg, paths, monkeypatch):
    import sqlalchemy as sa

    url = os.environ["WEBETL_TEST_POSTGRES"]
    cfg["load"][0]["url"] = url
    cfg["medallion"]["gold"]["warehouse"] = url
    cfg["load"][0]["table"] = "pytest_products"
    cfg["medallion"]["gold"]["table_prefix"] = "pytest_gold_"
    for _ in range(2):
        res = Pipeline(cfg, state_path=paths["state"]).run(full_refresh=True)
        assert res.status == "success", res.error
    eng = sa.create_engine(url)
    with eng.connect() as c:
        assert c.execute(sa.text("SELECT COUNT(*), COUNT(DISTINCT id) FROM pytest_products")).one() == (56, 56)
        assert c.execute(sa.text("SELECT SUM(products) FROM pytest_gold_category_summary")).scalar() == 56
    with eng.begin() as c:
        for t in ("pytest_products", "pytest_gold_category_summary", "pytest_gold_price_bands",
                  "pytest_gold_out_of_stock", "pytest_gold_daily_loads"):
            c.execute(sa.text(f"DROP TABLE IF EXISTS {t}"))
    QualityRunner  # imported for API surface check
