"""Type coercion shared by the cast step, schema validation and the loaders."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from dateutil import parser as dtparser

TYPES = ("str", "int", "float", "decimal", "bool", "datetime", "date", "json")

_TRUE = {"true", "t", "yes", "y", "1", "on"}
_FALSE = {"false", "f", "no", "n", "0", "off"}
_NUM_CLEAN = re.compile(r"[^0-9eE+\-.]")


class CastError(ValueError):
    pass


def _clean_numeric(s: str) -> str:
    """'£1,234.50' -> '1234.50'. Raises CastError if nothing numeric is left."""
    cleaned = _NUM_CLEAN.sub("", s.strip().replace(",", ""))
    if not any(ch.isdigit() for ch in cleaned):
        raise CastError(f"{s!r} is not a number")
    return cleaned


def to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = value / 1000 if value > 1e11 else value  # epoch ms vs s
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    else:
        dt = dtparser.parse(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def convert(value: Any, type_name: str) -> Any:
    """Convert a value to the named type. None and '' become None. Raises CastError."""
    if value is None or (isinstance(value, str) and value.strip() == "" and type_name != "str"):
        return None
    try:
        if type_name == "str":
            return value if isinstance(value, str) else (
                json.dumps(value, default=str) if isinstance(value, (dict, list)) else str(value))
        if type_name == "int":
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, (int, float, Decimal)):
                if isinstance(value, float) and not value.is_integer():
                    raise CastError(f"{value!r} is not a whole number")
                return int(value)
            f = float(_clean_numeric(str(value)))
            if not f.is_integer():
                raise CastError(f"{value!r} is not a whole number")
            return int(f)
        if type_name == "float":
            if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
                return float(value)
            return float(_clean_numeric(str(value)))
        if type_name == "decimal":
            return Decimal(str(value) if isinstance(value, (int, float)) else _clean_numeric(str(value)))
        if type_name == "bool":
            if isinstance(value, bool):
                return value
            s = str(value).strip().lower()
            if s in _TRUE:
                return True
            if s in _FALSE:
                return False
            raise CastError(f"{value!r} is not a boolean")
        if type_name == "datetime":
            return to_datetime(value)
        if type_name == "date":
            return to_datetime(value).date() if not (isinstance(value, date) and not isinstance(value, datetime)) else value
        if type_name == "json":
            return json.loads(value) if isinstance(value, str) else value
    except CastError:
        raise
    except (ValueError, TypeError, OverflowError, InvalidOperation) as exc:
        raise CastError(f"cannot convert {value!r} to {type_name}: {exc}") from exc
    raise CastError(f"unknown type '{type_name}' (expected one of {TYPES})")
