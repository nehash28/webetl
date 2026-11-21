"""Write records to a data lake as partitioned Parquet / JSONL / CSV files.

Works on any fsspec filesystem: a local path, s3://bucket/prefix (pip install s3fs),
gs:// (gcsfs) or abfs:// (adlfs).

Target options
  path:          root of the lake, e.g. data/lake or s3://my-bucket/lake
  dataset:       sub-folder name (default: pipeline name)
  format:        parquet | jsonl | csv            (default parquet)
  partition_by:  [col, ...]  hive-style folders col=value/. The special column
                 `ingest_date` (YYYY-MM-DD of the run) is always available.
  compression:   parquet: snappy|zstd|gzip|none; jsonl/csv: gzip|none
  rows_per_file: buffer size before a file is flushed (default 100000)
  storage_options: {...} passed to fsspec (credentials, endpoint_url, ...)

Files are named part-<run_id>-<n>.<ext>. If the run fails, files written by
this run are deleted so readers never see half a load.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from webetl.loaders.base import LoadContext, Loader

log = logging.getLogger(__name__)

_ARROW_TYPES = {
    "str": pa.string(), "int": pa.int64(), "float": pa.float64(), "decimal": pa.decimal128(38, 9),
    "bool": pa.bool_(), "datetime": pa.timestamp("us", tz="UTC"), "date": pa.date32(), "json": pa.string(),
}


def _json_default(o: Any) -> Any:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    return str(o)


def _safe_partition_value(v: Any) -> str:
    s = "__HIVE_DEFAULT_PARTITION__" if v in (None, "") else (v.isoformat() if isinstance(v, (date, datetime)) else str(v))
    return s.replace("/", "_").replace("=", "_")


def records_to_arrow(rows: list[dict], declared: dict | None = None) -> pa.Table:
    """Convert records to an Arrow table: declared schema types win, else infer per column."""
    cols: list[str] = []
    for r in rows:
        for c in r:
            if c not in cols:
                cols.append(c)
    declared = declared or {}
    fields, arrays = [], []
    for c in cols:
        values = [r.get(c) for r in rows]
        if c in declared and declared[c]["type"] != "json":
            typ = _ARROW_TYPES[declared[c]["type"]]
        else:
            kinds = {type(v) for v in values if v is not None}
            if kinds & {dict, list} or declared.get(c, {}).get("type") == "json":
                values = [None if v is None else json.dumps(v, default=_json_default) for v in values]
                typ = pa.string()
            elif kinds <= {int} and kinds:
                typ = pa.int64()
            elif kinds <= {int, float} and kinds:
                typ, values = pa.float64(), [None if v is None else float(v) for v in values]
            elif kinds == {bool}:
                typ = pa.bool_()
            elif kinds == {datetime}:
                typ = pa.timestamp("us", tz="UTC")
            elif kinds == {date}:
                typ = pa.date32()
            else:  # strings, mixed types or all-null -> string (stable across files)
                typ = pa.string()
                values = [None if v is None else (v if isinstance(v, str) else str(v)) for v in values]
        fields.append(pa.field(c, typ))
        arrays.append(pa.array(values, type=typ))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))



class LakeLoader(Loader):
    def __init__(self, target: dict[str, Any], ctx: LoadContext):
        super().__init__(target, ctx)
        self.format = target.get("format", "parquet")
        if self.format not in ("parquet", "jsonl", "csv"):
            raise ValueError(f"Unknown lake format '{self.format}'")
        self.partition_by: list[str] = list(target.get("partition_by") or [])
        default_comp = "snappy" if self.format == "parquet" else "none"
        self.compression = target.get("compression", default_comp)
        self.rows_per_file = int(target.get("rows_per_file", 100_000))
        dataset = target.get("dataset", ctx.pipeline)
        self.fs, root = fsspec.core.url_to_fs(target["path"], **(target.get("storage_options") or {}))
        self.root = f"{root.rstrip('/')}/{dataset}"
        self.load_date = ctx.started_at.date().isoformat()
        self._buffers: dict[tuple, list[dict]] = defaultdict(list)
        self._file_no = 0
        self.files_written: list[str] = []

    # -- helpers ---------------------------------------------------------------------
    def _partition_key(self, rec: dict) -> tuple:
        return tuple(self.load_date if c == "ingest_date" and c not in rec else rec.get(c)
                     for c in self.partition_by)

    def _ext(self) -> str:
        ext = {"parquet": "parquet", "jsonl": "jsonl", "csv": "csv"}[self.format]
        if self.format != "parquet" and self.compression == "gzip":
            ext += ".gz"
        return ext

    def _arrow_table(self, rows: list[dict]) -> pa.Table:
        return records_to_arrow(rows, self.ctx.schema)

    def _write_file(self, key: tuple, rows: list[dict]) -> None:
        parts = [f"{c}={_safe_partition_value(v)}" for c, v in zip(self.partition_by, key)]
        folder = "/".join([self.root, *parts])
        self.fs.makedirs(folder, exist_ok=True)
        self._file_no += 1
        path = f"{folder}/part-{self.ctx.run_id}-{self._file_no:05d}.{self._ext()}"
        # Partition columns live in the folder names (hive style), not in the file.
        rows = [{k: v for k, v in r.items() if k not in self.partition_by} for r in rows]

        if self.format == "parquet":
            table = self._arrow_table(rows)
            comp = None if self.compression == "none" else self.compression
            with self.fs.open(path, "wb") as fh:
                pq.write_table(table, fh, compression=comp)
        else:
            buf = io.StringIO()
            if self.format == "jsonl":
                for r in rows:
                    buf.write(json.dumps(r, default=_json_default, ensure_ascii=False) + "\n")
            else:
                cols: list[str] = []
                for r in rows:
                    cols.extend(c for c in r if c not in cols)
                w = csv.DictWriter(buf, fieldnames=cols)
                w.writeheader()
                for r in rows:
                    w.writerow({k: (json.dumps(v, default=_json_default) if isinstance(v, (dict, list)) else v)
                                for k, v in r.items()})
            data = buf.getvalue().encode("utf-8")
            if self.compression == "gzip":
                data = gzip.compress(data)
            with self.fs.open(path, "wb") as fh:
                fh.write(data)
        self.files_written.append(path)
        log.info("wrote %d rows to %s", len(rows), path)

    # -- loader interface ----------------------------------------------------------------
    def write(self, records: list[dict[str, Any]]) -> int:
        for rec in records:
            key = self._partition_key(rec)
            buf = self._buffers[key]
            buf.append(rec)
            if len(buf) >= self.rows_per_file:
                self._write_file(key, buf)
                self._buffers[key] = []
        self.rows_written += len(records)
        return len(records)

    def close(self, success: bool) -> None:
        if success:
            for key, rows in self._buffers.items():
                if rows:
                    self._write_file(key, rows)
            self._buffers.clear()
            return
        self._buffers.clear()
        for path in self.files_written:  # roll back partial output
            try:
                self.fs.rm(path)
            except FileNotFoundError:
                pass
        if self.files_written:
            log.warning("run failed: removed %d partial files", len(self.files_written))
        self.files_written = []
