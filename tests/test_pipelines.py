import gzip
import json
import sqlite3
from pathlib import Path

import pyarrow.dataset as ds
import pytest
import yaml

from webetl.config import load_config
from webetl.extractors import build_extractor
from webetl.pipeline import Pipeline

CONFIGS = Path(__file__).resolve().parents[1] / "configs"


def _rows(db_url, sql):
    con = sqlite3.connect(db_url.replace("sqlite:///", ""))
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _products_cfg(server, paths, monkeypatch):
    monkeypatch.setenv("API_BASE_URL", server)
    monkeypatch.setenv("DATABASE_URL", paths["db"])
    monkeypatch.setenv("LAKE_PATH", paths["lake"])
    cfg = load_config(CONFIGS / "products_api.yaml")
    cfg["rejects"] = {"path": paths["rejects"]}
    cfg["source"]["rate_limit_per_sec"] = None
    return cfg


def test_api_pipeline_end_to_end(server, paths, monkeypatch):
    cfg = _products_cfg(server, paths, monkeypatch)
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "success", res.error
    assert res.stats["extract"]["pages"] == 3
    assert res.stats["transform"] == {"in": 57, "out": 56, "filtered": 0, "rejected": 1}

    assert _rows(paths["db"], "select count(*), sum(in_stock) from products") == [(56, 37)]
    assert _rows(paths["db"], "select name, price, currency from products where id = 1") == [("Product 1", 8.35, "USD")]

    lake = ds.dataset(Path(paths["lake"]) / "curated" / "products_api", format="parquet", partitioning="hive")
    table = lake.to_table()
    assert table.num_rows == 56
    assert sorted(set(table.column("category").to_pylist())) == ["books", "electronics", "garden", "toys"]
    assert str(table.schema.field("updated_at").type) == "timestamp[us, tz=UTC]"

    raw_files = list((Path(paths["lake"]) / "raw" / "products_api").rglob("*.jsonl.gz"))
    assert len(raw_files) == 1
    raw_lines = gzip.decompress(raw_files[0].read_bytes()).decode().splitlines()
    assert len(raw_lines) == 57 and json.loads(raw_lines[0])["price"]["currency"] == "USD"

    reject_files = list(Path(paths["rejects"]).rglob("*.jsonl"))
    reject = json.loads(reject_files[0].read_text())
    assert reject["_record"]["id"] == 13 and "price" in reject["_reason"]

    # Incremental re-run: only records at/after the watermark are requested.
    res2 = Pipeline(cfg, state_path=paths["state"]).run()
    assert res2.stats["watermark"]["before"] == "2026-11-03T09:00:00Z"
    assert res2.stats["extract"]["records"] == 1
    assert _rows(paths["db"], "select count(*) from products") == [(56,)]  # upsert, no duplicates

    # Full refresh ignores the watermark.
    res3 = Pipeline(cfg, state_path=paths["state"]).run(full_refresh=True)
    assert res3.stats["extract"]["records"] == 57


def test_reject_ratio_gate_rolls_back_lake(server, paths, monkeypatch):
    cfg = _products_cfg(server, paths, monkeypatch)
    cfg["max_reject_ratio"] = 0.0
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "failed" and "DataQualityError" in res.error
    assert not list((Path(paths["lake"]) / "curated").rglob("*.parquet"))
    assert Pipeline(cfg, state_path=paths["state"]).state.get_watermark("products_api") is None
    assert list(Path(paths["rejects"]).rglob("*.jsonl"))  # rejects are still kept for debugging


def test_auth_failure_fails_run(server, paths, monkeypatch):
    cfg = _products_cfg(server, paths, monkeypatch)
    cfg["source"]["auth"]["token"] = "wrong"
    cfg["source"]["retries"] = 0
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "failed" and "401" in res.error


def test_dry_run_writes_nothing(server, paths, monkeypatch):
    cfg = _products_cfg(server, paths, monkeypatch)
    res = Pipeline(cfg, state_path=paths["state"]).run(dry_run=True, limit=5)
    assert res.status == "success" and len(res.sample) == 5
    assert not Path(paths["db"].replace("sqlite:///", "")).exists()


@pytest.mark.parametrize("source,expected", [
    ({"url": "/api/events", "records_path": "results",
      "pagination": {"type": "cursor", "cursor_param": "cursor", "cursor_path": "next_cursor"}}, 25),
    ({"url": "/api/repos", "params": {"per_page": 10}, "pagination": {"type": "link_header"}}, 23),
    ({"url": "/api/flaky", "backoff_factor": 0}, 1),
])
def test_api_pagination_and_retries(server, source, expected):
    source = {"type": "api", **source, "url": server + source["url"]}
    ex = build_extractor(source)
    assert len(list(ex.extract())) == expected


def test_html_scraper_with_detail_pages(server, paths, monkeypatch):
    monkeypatch.setenv("BOOKS_BASE_URL", server)
    monkeypatch.setenv("DATABASE_URL", paths["db"])
    monkeypatch.setenv("LAKE_PATH", paths["lake"])
    cfg = load_config(CONFIGS / "books_scrape.yaml")
    cfg["source"]["rate_limit_per_sec"] = None
    cfg["rejects"] = {"path": paths["rejects"]}
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "success", res.error
    assert res.stats["extract"]["pages"] == 3
    rows = _rows(paths["db"], "select upc, title, price, rating, in_stock, description, url from books order by upc")
    assert len(rows) == 45
    upc, title, price, rating, in_stock, desc, url = rows[0]
    assert (upc, title, price, rating, in_stock) == ("UPC000001", "Book Title 1", 13.17, 2, 1)
    assert desc.startswith("A gripping") and url == f"{server}/catalogue/book-1/index.html"
    table = ds.dataset(Path(paths["lake"]) / "curated" / "books_scrape", format="parquet",
                       partitioning="hive").to_table()
    assert table.num_rows == 45 and "ingest_date" in table.column_names


def test_robots_txt_is_respected(server, paths):
    cfg = {"name": "private", "source": {"type": "html", "url": f"{server}/private/x.html",
                                         "item_selector": ".secret", "fields": {"t": "."}},
           "rejects": {"path": paths["rejects"]},
           "load": [{"type": "sql", "url": paths["db"], "table": "t"}]}
    res = Pipeline(cfg, state_path=paths["state"]).run()
    assert res.status == "failed" and "robots.txt" in res.error


def test_sql_replace_and_schema_evolution(paths, server, monkeypatch):
    base = {"name": "evo", "rejects": {"path": paths["rejects"]},
            "source": {"type": "api", "url": f"{server}/api/repos", "params": {"per_page": 5},
                       "pagination": {"type": "link_header", "max_pages": 1}},
            "transform": [{"flatten": {"sep": "_"}}, {"select": ["id", "full_name"]}]}
    load = {"type": "sql", "url": paths["db"], "table": "repos", "mode": "replace"}
    assert Pipeline({**base, "load": [load]}, state_path=paths["state"]).run().status == "success"
    assert _rows(paths["db"], "select count(*) from repos") == [(5,)]

    # second run adds a column -> ALTER TABLE; replace swaps in fresh data
    base2 = {**base, "transform": [{"flatten": {"sep": "_"}},
                                   {"select": ["id", "full_name", "owner_login", "stargazers_count"]}]}
    base2["source"] = {**base["source"], "params": {"per_page": 3}}
    assert Pipeline({**base2, "load": [load]}, state_path=paths["state"]).run().status == "success"
    assert _rows(paths["db"], "select count(*), max(owner_login) from repos") == [(3, "org")]
    tables = {r[0] for r in _rows(paths["db"], "select name from sqlite_master where type='table'")}
    assert tables == {"repos"}  # staging table cleaned up

    # append mode + evolution on an existing table
    append = {**load, "table": "repos_log", "mode": "append"}
    Pipeline({**base, "load": [append]}, state_path=paths["state"]).run()
    Pipeline({**base2, "load": [append]}, state_path=paths["state"]).run()
    assert _rows(paths["db"], "select count(*), count(stargazers_count) from repos_log") == [(8, 3)]


def test_example_configs_are_valid():
    for path in CONFIGS.glob("*.yaml"):
        raw = yaml.safe_load(path.read_text())
        assert raw["name"] and raw["source"]["type"] in ("api", "html")
