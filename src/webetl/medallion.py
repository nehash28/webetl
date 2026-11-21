"""Medallion data lake: bronze (raw) -> silver (clean) -> gold (business tables).

Config block:

  medallion:
    root: data/lake                 # local path or s3:// / gs:// / abfs://
    storage_options: {}             # passed to fsspec (credentials, endpoint_url, ...)
    bronze: true                    # land untouched source records as JSONL.gz
    silver:
      partition_by: [ingest_date]   # typed Parquet, one folder per load date
      key: [id]                     # optional: `silver` view keeps the latest row per key
    gold:
      warehouse: sqlite:///data/warehouse.db   # optional: also publish gold tables here
      models:                       # built in order, after every successful load
        - name: category_summary
          sql: SELECT category, COUNT(*) AS products FROM silver GROUP BY category
        - name: top_categories      # later models can read earlier ones by name
          sql: SELECT * FROM category_summary ORDER BY products DESC LIMIT 3

Layout:  <root>/bronze/<pipeline>/ingest_date=YYYY-MM-DD/part-*.jsonl.gz
         <root>/silver/<pipeline>/ingest_date=YYYY-MM-DD/part-*.parquet
         <root>/gold/<pipeline>/<model>.parquet

Inside gold SQL (DuckDB dialect):
  silver_all  every silver row ever loaded (full history)
  silver      latest row per silver.key (equals silver_all when no key is set)
  <model>     any gold model built earlier in the list
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from typing import Any

import fsspec

log = logging.getLogger(__name__)

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sql_str(value: str) -> str:
    """Quote a string as a SQL literal."""
    return "'" + value.replace("'", "''") + "'"


def expand_medallion(cfg: dict[str, Any]) -> dict[str, Any]:
    """Turn a `medallion:` block into the raw landing zone + a silver lake load target."""
    m = cfg.get("medallion")
    if not m:
        return cfg
    if "root" not in m:
        raise ValueError("medallion.root is required")
    root = str(m["root"]).rstrip("/")
    opts = m.get("storage_options") or {}
    silver = m.get("silver") or {}
    for model in (m.get("gold") or {}).get("models") or []:
        if not _IDENT.match(str(model.get("name", ""))) or not model.get("sql"):
            raise ValueError(f"gold model needs a SQL-safe 'name' and 'sql': {model!r}")
    for k in silver.get("key") or []:
        if not _IDENT.match(k):
            raise ValueError(f"invalid silver key column {k!r}")
    if m.get("bronze", True) and not cfg.get("raw"):
        cfg["raw"] = {"path": f"{root}/bronze", "storage_options": opts}
    if not any(t.get("name") == "silver" for t in cfg["load"]):
        cfg["load"].append({
            "type": "lake", "name": "silver", "path": f"{root}/silver", "format": "parquet",
            "partition_by": silver.get("partition_by", ["ingest_date"]),
            "compression": silver.get("compression", "snappy"), "storage_options": opts,
        })
    return cfg


class GoldBuilder:
    def __init__(self, pipeline: str, medallion: dict[str, Any], run_id: str = "manual"):
        self.pipeline = pipeline
        self.m = medallion
        self.run_id = run_id
        root = str(medallion["root"]).rstrip("/")
        self.fs, fs_root = fsspec.core.url_to_fs(root, **(medallion.get("storage_options") or {}))
        self.silver_dir = f"{fs_root}/silver/{pipeline}"
        self.gold_dir = f"{fs_root}/gold/{pipeline}"
        self.remote = not isinstance(self.fs, fsspec.implementations.local.LocalFileSystem)
        self.gold_cfg = medallion.get("gold") or {}

    def _connect(self):
        import duckdb

        con = duckdb.connect()
        if self.remote:
            con.register_filesystem(self.fs)
        protocol = self.fs.protocol[0] if isinstance(self.fs.protocol, (tuple, list)) else self.fs.protocol
        prefix = f"{protocol}://" if self.remote else ""
        files = self.fs.glob(f"{self.silver_dir}/**/*.parquet")
        if not files:
            raise FileNotFoundError(f"no silver Parquet files under {self.silver_dir}")
        pattern = _sql_str(f"{prefix}{self.silver_dir}/**/*.parquet")
        con.execute(
            f"CREATE VIEW silver_all AS SELECT * FROM read_parquet({pattern}, "
            "hive_partitioning = true, union_by_name = true)")
        key = (self.m.get("silver") or {}).get("key") or []
        if key:
            cols = {r[0] for r in con.execute("DESCRIBE silver_all").fetchall()}
            order = "_etl_loaded_at" if "_etl_loaded_at" in cols else (
                "ingest_date" if "ingest_date" in cols else "1")
            con.execute(
                f"CREATE VIEW silver AS SELECT * FROM silver_all QUALIFY row_number() OVER "
                f"(PARTITION BY {', '.join(key)} ORDER BY {order} DESC) = 1")
        else:
            con.execute("CREATE VIEW silver AS SELECT * FROM silver_all")
        return con

    connect = _connect

    def attach_gold(self, con) -> list[str]:
        """Expose already-built gold Parquet files as views (used by quality checks)."""
        protocol = self.fs.protocol[0] if isinstance(self.fs.protocol, (tuple, list)) else self.fs.protocol
        prefix = f"{protocol}://" if self.remote else ""
        names = []
        for model in self.gold_cfg.get("models") or []:
            path = f"{self.gold_dir}/{model['name']}.parquet"
            if self.fs.exists(path):
                con.execute(f"CREATE OR REPLACE VIEW {model['name']} AS "
                            f"SELECT * FROM read_parquet({_sql_str(prefix + path)})")
                names.append(model["name"])
        return names

    def build(self) -> dict[str, int]:
        models = self.gold_cfg.get("models") or []
        if not models:
            return {}
        con = self._connect()
        built: dict[str, int] = {}
        self.fs.makedirs(self.gold_dir, exist_ok=True)
        with tempfile.TemporaryDirectory() as tmp:
            for model in models:
                name, sql = model["name"], model["sql"]
                con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
                rows = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                local = os.path.join(tmp, f"{name}.parquet")
                con.execute(f"COPY {name} TO {_sql_str(local)} (FORMAT parquet, COMPRESSION zstd)")
                # Publish atomically: upload under a temp name, then move over the old file.
                final = f"{self.gold_dir}/{name}.parquet"
                staging = f"{self.gold_dir}/.{name}.{self.run_id[:8]}.tmp"
                self.fs.put_file(local, staging)
                if self.fs.exists(final):
                    self.fs.rm(final)
                self.fs.mv(staging, final)
                built[name] = int(rows)
                log.info("gold model %s: %d rows -> %s", name, rows, final)
            if self.gold_cfg.get("warehouse"):
                self._publish(con, models)
        con.close()
        return built

    def _publish(self, con, models: list[dict[str, Any]]) -> None:
        """Copy gold tables into a SQL warehouse (atomic replace per table)."""
        from webetl.loaders.sql import SqlLoader
        from webetl.loaders.base import LoadContext

        ctx = LoadContext(self.pipeline, self.run_id if len(self.run_id) >= 8 else self.run_id + "00000000",
                          datetime.now(timezone.utc))
        prefix = self.gold_cfg.get("table_prefix", "gold_")
        for model in models:
            table = model.get("table") or f"{prefix}{model['name']}"
            cur = con.execute(f"SELECT * FROM {model['name']}")
            cols = [d[0] for d in cur.description]
            records = [dict(zip(cols, row)) for row in cur.fetchall()]
            loader = SqlLoader({"type": "sql", "url": self.gold_cfg["warehouse"], "table": table,
                                "mode": "replace"}, ctx)
            loader.open()
            ok = False
            try:
                loader.write(records)
                ok = True
            finally:
                loader.close(success=ok)
            log.info("published gold table %s (%d rows)", table, len(records))
