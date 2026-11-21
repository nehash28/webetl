"""Data quality checks, run with DuckDB after every load (and on demand with `webetl check`).

  checks:
    - name: ids_unique
      type: unique
      table: silver               # run | silver | silver_all | any gold model name
      columns: [id]
    - {name: prices_valid, type: range, column: price, min: 0.01, max: 10000}
    - {name: names_present, type: not_null, columns: [name, category]}
    - {name: known_categories, type: accepted_values, column: category, values: [books, toys]}
    - {name: enough_rows, type: row_count, table: run, min: 1}
    - {name: fresh, type: freshness, column: _etl_loaded_at, max_age_hours: 24, severity: warn}
    - name: stock_not_above_total          # custom SQL: must return 0 (the number of bad rows)
      sql: SELECT COUNT(*) FROM category_summary WHERE in_stock > products

Tables you can check
  run         the records loaded by this run (only during `webetl run`)
  silver      latest row per key in the silver layer;  silver_all = full history
  <model>     any gold model (medallion pipelines)
  The default table is `silver` for medallion pipelines and `run` otherwise.

Severity
  error (default)  the run is marked failed and a `failure` alert is sent
  warn             the run stays successful; a `warning` alert is sent

Every check type also accepts `where:` (a SQL filter) to check a subset of rows.
Checks run after the data is committed, like dbt tests: they flag bad data loudly,
they do not undo the load. To keep bad rows out, use `schema` + `max_reject_ratio`.
"""

from __future__ import annotations

import logging
import re
from typing import Any

log = logging.getLogger(__name__)

TYPES = ("sql", "unique", "not_null", "accepted_values", "range", "row_count", "freshness")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class QualityConfigError(ValueError):
    pass


def _ident(name: Any, what: str) -> str:
    if not isinstance(name, str) or not _IDENT.match(name):
        raise QualityConfigError(f"invalid {what} {name!r}")
    return name


def check_type(check: dict[str, Any]) -> str:
    return check.get("type") or ("sql" if "sql" in check else "")


def validate_checks(checks: list[dict[str, Any]] | None) -> None:
    seen: set[str] = set()
    for i, c in enumerate(checks or []):
        if not isinstance(c, dict):
            raise QualityConfigError(f"check #{i + 1} must be a mapping")
        kind = check_type(c)
        if kind not in TYPES:
            raise QualityConfigError(f"check #{i + 1}: unknown type {kind!r}; use one of {TYPES}")
        name = c.get("name") or f"{kind}_{i + 1}"
        if name in seen:
            raise QualityConfigError(f"duplicate check name {name!r}")
        seen.add(name)
        if c.get("severity", "error") not in ("error", "warn"):
            raise QualityConfigError(f"check {name!r}: severity must be 'error' or 'warn'")
        if c.get("table"):
            _ident(c["table"], "table")
        if kind in ("unique", "not_null"):
            cols = c.get("columns") or ([c["column"]] if c.get("column") else [])
            if not cols:
                raise QualityConfigError(f"check {name!r} needs 'columns'")
            for col in cols:
                _ident(col, "column")
        if kind in ("accepted_values", "range", "freshness"):
            _ident(c.get("column"), "column")
        if kind == "accepted_values" and not c.get("values"):
            raise QualityConfigError(f"check {name!r} needs 'values'")
        if kind == "range" and c.get("min") is None and c.get("max") is None:
            raise QualityConfigError(f"check {name!r} needs 'min' and/or 'max'")
        if kind == "row_count" and c.get("min") is None and c.get("max") is None:
            raise QualityConfigError(f"check {name!r} needs 'min' and/or 'max'")
        if kind == "freshness" and c.get("max_age_hours") is None:
            raise QualityConfigError(f"check {name!r} needs 'max_age_hours'")


def _where(check: dict[str, Any], extra: str | None = None) -> str:
    parts = [p for p in (extra, check.get("where")) if p]
    return (" WHERE " + " AND ".join(f"({p})" for p in parts)) if parts else ""


def _evaluate(con, check: dict[str, Any], table: str) -> tuple[bool, Any, str]:
    """Return (passed, value, detail) for one check."""
    kind = check_type(check)
    q = lambda sql, params=None: con.execute(sql, params or []).fetchone()[0]  # noqa: E731

    if kind == "sql":
        value = q(check["sql"])
        return value in (0, None, False), value, "query must return 0"
    if kind == "row_count":
        value = int(q(f"SELECT COUNT(*) FROM {table}{_where(check)}"))
        lo, hi = check.get("min"), check.get("max")
        ok = (lo is None or value >= lo) and (hi is None or value <= hi)
        return ok, value, f"{value} rows (expected {lo if lo is not None else '-'}..{hi if hi is not None else '-'})"
    if kind == "unique":
        cols = ", ".join(check.get("columns") or [check["column"]])
        value = int(q(f"SELECT COUNT(*) FROM (SELECT {cols} FROM {table}{_where(check)} "
                      f"GROUP BY {cols} HAVING COUNT(*) > 1)"))
        return value == 0, value, f"{value} duplicated value(s) of ({cols})"
    if kind == "not_null":
        cols = check.get("columns") or [check["column"]]
        cond = " OR ".join(f"{c} IS NULL" for c in cols)
        value = int(q(f"SELECT COUNT(*) FROM {table}{_where(check, cond)}"))
        return value == 0, value, f"{value} row(s) with nulls in {', '.join(cols)}"
    if kind == "accepted_values":
        col, values = check["column"], list(check["values"])
        marks = ", ".join("?" for _ in values)
        value = int(q(f"SELECT COUNT(*) FROM {table}{_where(check, f'{col} IS NOT NULL AND {col} NOT IN ({marks})')}",
                      values))
        return value == 0, value, f"{value} row(s) with unexpected {col}"
    if kind == "range":
        col, lo, hi = check["column"], check.get("min"), check.get("max")
        conds, params = [], []
        if lo is not None:
            conds.append(f"{col} < ?")
            params.append(lo)
        if hi is not None:
            conds.append(f"{col} > ?")
            params.append(hi)
        value = int(q(f"SELECT COUNT(*) FROM {table}{_where(check, ' OR '.join(conds))}", params))
        return value == 0, value, f"{value} row(s) with {col} outside {lo if lo is not None else '-inf'}..{hi if hi is not None else 'inf'}"
    if kind == "freshness":
        col, limit = check["column"], float(check["max_age_hours"])
        latest = q(f"SELECT MAX(CAST({col} AS TIMESTAMPTZ)) FROM {table}{_where(check)}")
        if latest is None:
            return False, None, f"no {col} values found"
        age = q("SELECT (epoch(now()) - epoch(CAST(? AS TIMESTAMPTZ))) / 3600.0", [latest])
        age = round(float(age), 2)
        return age <= limit, age, f"newest {col} is {age} h old (max {limit:g} h)"
    raise QualityConfigError(f"unknown check type {kind!r}")


class QualityRunner:
    def __init__(self, pipeline: str, cfg: dict[str, Any], run_records: list[dict] | None = None,
                 schema: dict | None = None):
        self.pipeline = pipeline
        self.cfg = cfg
        self.checks = cfg.get("checks") or []
        validate_checks(self.checks)
        self.run_records = run_records
        self.schema = schema

    def _connect(self):
        import duckdb

        medallion = self.cfg.get("medallion")
        tables: set[str] = set()
        con = None
        if medallion:
            from webetl.medallion import GoldBuilder

            gb = GoldBuilder(self.pipeline, medallion)
            try:
                con = gb.connect()
                tables |= {"silver", "silver_all"}
            except FileNotFoundError:
                con = None
            if con is None:
                con = duckdb.connect()
            tables |= set(gb.attach_gold(con))
        con = con or duckdb.connect()
        if self.run_records is not None:
            if self.run_records:
                from webetl.loaders.lake import records_to_arrow

                run_table = records_to_arrow(self.run_records, self.schema)  # noqa: F841 - used by DuckDB
                con.execute("CREATE TABLE run AS SELECT * FROM run_table")
            else:
                con.execute("CREATE TABLE run AS SELECT NULL AS _empty WHERE false")
            tables.add("run")
        return con, tables

    def run(self) -> list[dict[str, Any]]:
        if not self.checks:
            return []
        con, tables = self._connect()
        default = "silver" if "silver" in tables else "run"
        results = []
        try:
            for i, check in enumerate(self.checks):
                kind = check_type(check)
                name = check.get("name") or f"{kind}_{i + 1}"
                table = check.get("table") or default
                res = {"name": name, "type": kind, "table": None if kind == "sql" else table,
                       "severity": check.get("severity", "error")}
                if table == "run" and kind != "sql" and self.run_records is None:
                    res.update(status="skip", value=None,
                               detail="skipped: the `run` table only exists during `webetl run`")
                    results.append(res)
                    continue
                try:
                    if kind != "sql" and table not in tables and not _exists(con, table):
                        hint = " (the `run` table only exists during `webetl run`)" if table == "run" else ""
                        raise LookupError(f"table {table!r} not found{hint}")
                    if kind not in ("sql", "row_count") and table == "run" and not self.run_records:
                        res.update(status="pass", value=0, detail="no rows loaded in this run")
                    else:
                        passed, value, detail = _evaluate(con, check, table)
                        res.update(status="pass" if passed else "fail", value=value, detail=detail)
                except Exception as exc:  # a broken check counts as a failed check
                    res.update(status="error", value=None, detail=f"{type(exc).__name__}: {exc}")
                log.info("check %-28s %s  %s", name, res["status"].upper(), res["detail"])
                results.append(res)
        finally:
            con.close()
        return results


def _exists(con, table: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table} LIMIT 0")
        return True
    except Exception:
        return False


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Compact summary stored in run stats and shown in alerts."""
    bad = [r for r in results if r["status"] in ("fail", "error")]
    return {
        "passed": sum(r["status"] == "pass" for r in results),
        "failed": len(bad),
        "skipped": sum(r["status"] == "skip" for r in results),
        "errors": [r["name"] for r in bad if r["severity"] == "error"],
        "warnings": [r["name"] for r in bad if r["severity"] == "warn"],
        "results": results,
    }
