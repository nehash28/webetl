"""Load into any SQLAlchemy-supported database (SQLite, PostgreSQL, MySQL, ...).

Target options
  url:     sqlalchemy URL, e.g. sqlite:///data/warehouse.db or postgresql+psycopg2://u:p@host/db
  table:   destination table
  db_schema: optional database schema / namespace
  mode:    append | upsert | replace   (default: upsert if key is set, else append)
  key:     [col, ...] primary key used for upserts (created as the table's PK)

Behaviour
  * The table is created on first write: column types come from the pipeline `schema`
    when declared, otherwise they are inferred from the data.
  * New columns that show up later are added with ALTER TABLE (additive schema evolution).
  * upsert uses native INSERT ... ON CONFLICT (SQLite/PostgreSQL) or ON DUPLICATE KEY
    (MySQL); other dialects fall back to delete-then-insert in one transaction.
  * replace loads into a staging table and swaps it in only if the whole run succeeds.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from webetl.loaders.base import LoadContext, Loader
from webetl.utils import normalize_db_url

log = logging.getLogger(__name__)

_SCHEMA_TYPES: dict[str, Any] = {
    "str": sa.Text, "int": sa.BigInteger, "float": sa.Float, "decimal": sa.Numeric,
    "bool": sa.Boolean, "datetime": lambda: sa.DateTime(timezone=True), "date": sa.Date, "json": sa.JSON,
}


def _infer_type(value: Any) -> Any:
    if isinstance(value, bool):
        return sa.Boolean()
    if isinstance(value, int):
        return sa.BigInteger()
    if isinstance(value, float):
        return sa.Float()
    if isinstance(value, Decimal):
        return sa.Numeric()
    if isinstance(value, datetime):
        return sa.DateTime(timezone=True)
    if isinstance(value, date):
        return sa.Date()
    if isinstance(value, (dict, list)):
        return sa.JSON()
    return sa.Text()


class SqlLoader(Loader):
    def __init__(self, target: dict[str, Any], ctx: LoadContext):
        super().__init__(target, ctx)
        self.url = normalize_db_url(target["url"])
        self.table_name = target["table"]
        self.db_schema = target.get("db_schema")
        self.key: list[str] = list(target.get("key") or [])
        self.mode = target.get("mode") or ("upsert" if self.key else "append")
        if self.mode not in ("append", "upsert", "replace"):
            raise ValueError(f"Unknown sql mode '{self.mode}'")
        if self.mode == "upsert" and not self.key:
            raise ValueError("sql mode 'upsert' requires 'key'")
        self.engine: Engine | None = None
        self.table: sa.Table | None = None
        self.write_table_name = self.table_name

    def open(self) -> None:
        if self.url.startswith("sqlite:///"):
            from pathlib import Path
            db_path = self.url[len("sqlite:///"):]
            if db_path and db_path != ":memory:":
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.engine = sa.create_engine(self.url, future=True)
        if self.mode == "replace":
            self.write_table_name = f"{self.table_name}__staging_{self.ctx.run_id[:8]}"

    # -- table management ------------------------------------------------------
    def _column_types(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        types: dict[str, Any] = {}
        if self.ctx.schema:
            for col, spec in self.ctx.schema.items():
                types[col] = _SCHEMA_TYPES[spec["type"]]()
        for rec in records:
            for col, val in rec.items():
                if col not in types and val is not None:
                    types[col] = _infer_type(val)
        for rec in records:  # columns that are always null
            for col in rec:
                types.setdefault(col, sa.Text())
        return types

    def _ensure_table(self, records: list[dict[str, Any]]) -> None:
        assert self.engine is not None
        types = self._column_types(records)
        insp = sa.inspect(self.engine)
        md = sa.MetaData(schema=self.db_schema)
        if not insp.has_table(self.write_table_name, schema=self.db_schema):
            missing_keys = [k for k in self.key if k not in types]
            if missing_keys:
                raise ValueError(f"key columns not found in data: {missing_keys}")
            cols = [sa.Column(c, t, primary_key=c in self.key, nullable=c not in self.key)
                    for c, t in types.items()]
            self.table = sa.Table(self.write_table_name, md, *cols)
            md.create_all(self.engine)
            log.info("created table %s with %d columns", self.write_table_name, len(cols))
            return
        if self.table is None:
            self.table = sa.Table(self.write_table_name, md, autoload_with=self.engine)
        existing = set(self.table.columns.keys())
        new_cols = [c for c in types if c not in existing]
        if new_cols:
            prep = self.engine.dialect.identifier_preparer
            qualified = prep.format_table(self.table)
            with self.engine.begin() as conn:
                for c in new_cols:
                    ddl = types[c].compile(dialect=self.engine.dialect)
                    conn.execute(sa.text(f"ALTER TABLE {qualified} ADD COLUMN {prep.quote(c)} {ddl}"))
                    log.info("schema evolution: added column %s.%s %s", self.write_table_name, c, ddl)
            md = sa.MetaData(schema=self.db_schema)
            self.table = sa.Table(self.write_table_name, md, autoload_with=self.engine)

    # -- writing -------------------------------------------------------------------
    def write(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        assert self.engine is not None
        self._ensure_table(records)
        assert self.table is not None
        cols = list(self.table.columns.keys())
        rows = [{c: r.get(c) for c in cols} for r in records]
        if self.key:  # last write wins for duplicate keys inside one batch
            dedup: dict[tuple, dict] = {}
            for r in rows:
                dedup[tuple(r[k] for k in self.key)] = r
            rows = list(dedup.values())

        with self.engine.begin() as conn:
            if self.mode == "upsert":
                self._upsert(conn, rows, cols)
            else:
                conn.execute(sa.insert(self.table), rows)
        self.rows_written += len(rows)
        return len(rows)

    def _upsert(self, conn: sa.Connection, rows: list[dict], cols: list[str]) -> None:
        assert self.table is not None
        dialect = self.engine.dialect.name  # type: ignore[union-attr]
        update_cols = [c for c in cols if c not in self.key]
        if dialect in ("sqlite", "postgresql"):
            mod = __import__(f"sqlalchemy.dialects.{dialect}", fromlist=["insert"])
            stmt = mod.insert(self.table)
            if update_cols:
                stmt = stmt.on_conflict_do_update(
                    index_elements=self.key, set_={c: stmt.excluded[c] for c in update_cols})
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=self.key)
            conn.execute(stmt, rows)
        elif dialect in ("mysql", "mariadb"):
            from sqlalchemy.dialects.mysql import insert as my_insert
            stmt = my_insert(self.table)
            stmt = stmt.on_duplicate_key_update({c: stmt.inserted[c] for c in update_cols or self.key})
            conn.execute(stmt, rows)
        else:  # portable fallback
            cond = sa.and_(*[self.table.c[k] == sa.bindparam(f"_k_{k}") for k in self.key])
            conn.execute(sa.delete(self.table).where(cond),
                         [{f"_k_{k}": r[k] for k in self.key} for r in rows])
            conn.execute(sa.insert(self.table), rows)

    def close(self, success: bool) -> None:
        if self.engine is None:
            return
        try:
            if self.mode == "replace":
                insp = sa.inspect(self.engine)
                staged = insp.has_table(self.write_table_name, schema=self.db_schema)
                md = sa.MetaData(schema=self.db_schema)
                if success and staged:
                    prep = self.engine.dialect.identifier_preparer
                    with self.engine.begin() as conn:
                        if insp.has_table(self.table_name, schema=self.db_schema):
                            sa.Table(self.table_name, md, autoload_with=conn).drop(conn)
                        staging = prep.quote(self.write_table_name)
                        if self.db_schema:
                            staging = f"{prep.quote_schema(self.db_schema)}.{staging}"
                        conn.execute(sa.text(
                            f"ALTER TABLE {staging} RENAME TO {prep.quote(self.table_name)}"))
                elif staged:
                    with self.engine.begin() as conn:
                        sa.Table(self.write_table_name, md, autoload_with=conn).drop(conn)
        finally:
            self.engine.dispose()
