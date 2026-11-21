"""Record-level transformation chain plus schema validation.

Steps are listed in order under `transform:` in the pipeline config. Each step is
either a bare name ("flatten") or a one-key mapping ({rename: {a: b}}).

Available steps
  flatten        {sep: "_", max_depth: null}     nested dicts -> top-level columns
  rename         {old: new, ...}
  select         [col, ...]                      keep only these columns
  drop           [col, ...]
  strip          true | [col, ...]               trim whitespace on strings
  default        {col: value, ...}               fill missing / null values
  map_values     {col: {from: to, ...}}          e.g. "Three" -> 3
  regex_extract  {col: pattern}                  keep first capture group
  cast           {col: type, ...}                str|int|float|decimal|bool|datetime|date|json
  derive         {new_col: "{a}-{b}"}            str.format template over the record
  filter         {field, op, value}              eq|ne|gt|gte|lt|lte|in|not_in|contains|exists|not_null
  require        [col, ...]                      reject records where any is null/missing
  dedupe         {keys: [col, ...]}              drop repeats within a run

Records that fail a step are not dropped silently: they are rejected with a reason
and written to the rejects sink by the pipeline.
"""

from __future__ import annotations

import re
from typing import Any, Callable

from webetl.types import CastError, convert

Step = Callable[[dict[str, Any]], "dict[str, Any] | None"]


class RecordRejected(Exception):
    pass


# --- step factories ---------------------------------------------------------

def _flatten(arg: Any) -> Step:
    arg = arg if isinstance(arg, dict) else {}
    sep = arg.get("sep", "_")
    max_depth = arg.get("max_depth")

    def walk(obj: dict, prefix: str, depth: int, out: dict) -> None:
        for k, v in obj.items():
            key = f"{prefix}{sep}{k}" if prefix else str(k)
            if isinstance(v, dict) and v and (max_depth is None or depth < max_depth):
                walk(v, key, depth + 1, out)
            else:
                out[key] = v

    def step(rec: dict) -> dict:
        out: dict = {}
        walk(rec, "", 0, out)
        return out
    return step


def _rename(arg: dict) -> Step:
    def step(rec: dict) -> dict:
        return {arg.get(k, k): v for k, v in rec.items()}
    return step


def _select(arg: list) -> Step:
    cols = list(arg)
    return lambda rec: {c: rec.get(c) for c in cols}


def _drop(arg: list) -> Step:
    cols = set(arg)
    return lambda rec: {k: v for k, v in rec.items() if k not in cols}


def _strip(arg: Any) -> Step:
    cols = None if arg is True or arg is None else set(arg)

    def step(rec: dict) -> dict:
        for k, v in rec.items():
            if isinstance(v, str) and (cols is None or k in cols):
                rec[k] = " ".join(v.split())
        return rec
    return step


def _default(arg: dict) -> Step:
    def step(rec: dict) -> dict:
        for k, v in arg.items():
            if rec.get(k) in (None, ""):
                rec[k] = v
        return rec
    return step


def _map_values(arg: dict) -> Step:
    def step(rec: dict) -> dict:
        for col, mapping in arg.items():
            if col in rec and rec[col] in mapping:
                rec[col] = mapping[rec[col]]
        return rec
    return step


def _regex_extract(arg: dict) -> Step:
    compiled = {c: re.compile(p) for c, p in arg.items()}

    def step(rec: dict) -> dict:
        for col, rx in compiled.items():
            if rec.get(col) is not None:
                m = rx.search(str(rec[col]))
                rec[col] = (m.group(1) if m.groups() else m.group(0)) if m else None
        return rec
    return step


def _cast(arg: dict) -> Step:
    def step(rec: dict) -> dict:
        for col, typ in arg.items():
            if col in rec:
                try:
                    rec[col] = convert(rec[col], typ)
                except CastError as exc:
                    raise RecordRejected(f"cast {col}: {exc}") from exc
        return rec
    return step


class _SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return ""


def _derive(arg: dict) -> Step:
    def step(rec: dict) -> dict:
        for col, template in arg.items():
            rec[col] = str(template).format_map(_SafeDict({k: ("" if v is None else v) for k, v in rec.items()}))
        return rec
    return step


_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "gt": lambda a, b: a is not None and a > b,
    "gte": lambda a, b: a is not None and a >= b,
    "lt": lambda a, b: a is not None and a < b,
    "lte": lambda a, b: a is not None and a <= b,
    "in": lambda a, b: a in b,
    "not_in": lambda a, b: a not in b,
    "contains": lambda a, b: a is not None and b in a,
    "not_null": lambda a, b: a is not None,
}


def _filter(arg: dict) -> Step:
    field, op, value = arg["field"], arg.get("op", "eq"), arg.get("value")
    if op != "exists" and op not in _OPS:
        raise ValueError(f"Unknown filter op '{op}'")

    def step(rec: dict) -> dict | None:
        if op == "exists":
            return rec if field in rec else None
        try:
            return rec if _OPS[op](rec.get(field), value) else None
        except TypeError:
            return None
    return step


def _require(arg: list) -> Step:
    def step(rec: dict) -> dict:
        missing = [c for c in arg if rec.get(c) in (None, "")]
        if missing:
            raise RecordRejected(f"missing required: {', '.join(missing)}")
        return rec
    return step


def _dedupe(arg: Any) -> Step:
    keys = arg.get("keys") if isinstance(arg, dict) else arg
    seen: set = set()

    def step(rec: dict) -> dict | None:
        k = tuple(str(rec.get(c)) for c in keys) if keys else repr(sorted(rec.items(), key=lambda x: x[0]))
        if k in seen:
            return None
        seen.add(k)
        return rec
    return step


STEPS: dict[str, Callable[[Any], Step]] = {
    "flatten": _flatten, "rename": _rename, "select": _select, "drop": _drop,
    "strip": _strip, "default": _default, "map_values": _map_values,
    "regex_extract": _regex_extract, "cast": _cast, "derive": _derive,
    "filter": _filter, "require": _require, "dedupe": _dedupe,
}


def build_step(spec: Any) -> tuple[str, Step]:
    if isinstance(spec, str):
        name, arg = spec, None
    elif isinstance(spec, dict) and len(spec) == 1:
        name, arg = next(iter(spec.items()))
    else:
        raise ValueError(f"Invalid transform step: {spec!r}")
    if name not in STEPS:
        raise ValueError(f"Unknown transform step '{name}'. Available: {sorted(STEPS)}")
    return name, STEPS[name](arg)


# --- schema -----------------------------------------------------------------

def normalize_schema(schema: dict | None) -> dict[str, dict[str, Any]] | None:
    """Accept {columns: {col: type | {type, required}}, extra: keep|drop}."""
    if not schema:
        return None
    cols = schema.get("columns", schema)
    out: dict[str, dict[str, Any]] = {}
    for col, spec in cols.items():
        if col == "extra":
            continue
        spec = {"type": spec} if isinstance(spec, str) else dict(spec)
        spec.setdefault("type", "str")
        spec.setdefault("required", False)
        out[col] = spec
    return out


class Transformer:
    def __init__(self, steps: list[Any], schema: dict | None = None):
        self.steps = [build_step(s) for s in steps or []]
        self.schema = normalize_schema(schema)
        self.drop_extra = bool(schema and schema.get("extra") == "drop")
        self.stats = {"in": 0, "out": 0, "filtered": 0, "rejected": 0}

    def _apply_schema(self, rec: dict) -> dict:
        assert self.schema is not None
        out = {} if self.drop_extra else dict(rec)
        errors = []
        for col, spec in self.schema.items():
            try:
                out[col] = convert(rec.get(col), spec["type"])
            except CastError as exc:
                errors.append(f"{col}: {exc}")
                continue
            if spec["required"] and out[col] is None:
                errors.append(f"{col}: required value is missing")
        if errors:
            raise RecordRejected("schema: " + "; ".join(errors))
        return out

    def apply(self, records: list[dict]) -> tuple[list[dict], list[dict]]:
        good: list[dict] = []
        rejected: list[dict] = []
        for raw in records:
            self.stats["in"] += 1
            rec: dict | None = dict(raw)  # never mutate the raw record
            step_name = "schema"
            try:
                for step_name, fn in self.steps:
                    rec = fn(rec)
                    if rec is None:
                        break
                if rec is None:
                    self.stats["filtered"] += 1
                    continue
                if self.schema:
                    step_name = "schema"
                    rec = self._apply_schema(rec)
            except RecordRejected as exc:
                self.stats["rejected"] += 1
                rejected.append({"_reason": str(exc), "_step": step_name, "_record": raw})
                continue
            good.append(rec)
            self.stats["out"] += 1
        return good, rejected
