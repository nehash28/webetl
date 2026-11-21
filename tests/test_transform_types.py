from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from webetl.config import ConfigError, _interpolate
from webetl.extractors.base import watermark_key
from webetl.transform import Transformer
from webetl.types import CastError, convert
from webetl.utils import get_path


@pytest.mark.parametrize("value,typ,expected", [
    ("£51.77", "float", 51.77),
    ("1,234", "int", 1234),
    (3.0, "int", 3),
    ("yes", "bool", True),
    ("0", "bool", False),
    ("", "int", None),
    ("12.50", "decimal", Decimal("12.50")),
    ("2026-11-01T10:00:00Z", "datetime", datetime(2026, 11, 1, 10, tzinfo=timezone.utc)),
    (1762000000, "datetime", datetime.fromtimestamp(1762000000, tz=timezone.utc)),
    ("2026-11-01T23:30:00+02:00", "date", date(2026, 11, 1)),
    ({"a": 1}, "str", '{"a": 1}'),
    ('{"a": 1}', "json", {"a": 1}),
])
def test_convert(value, typ, expected):
    assert convert(value, typ) == expected


@pytest.mark.parametrize("value,typ", [("N/A", "float"), ("3.5", "int"), ("maybe", "bool"), ("x", "nope")])
def test_convert_errors(value, typ):
    with pytest.raises(CastError):
        convert(value, typ)


def test_get_path():
    body = {"data": {"items": [{"id": 1}, {"id": 2}]}}
    assert get_path(body, "data.items.1.id") == 2
    assert get_path(body, "data.missing", "d") == "d"
    assert get_path(body, None) is body


def test_watermark_ordering():
    assert watermark_key("2026-11-01T10:00:00Z") < watermark_key("2026-11-01T12:00:00+01:00")
    assert watermark_key(9) < watermark_key("10")


def test_env_interpolation(monkeypatch):
    monkeypatch.setenv("TOK", "abc")
    assert _interpolate({"a": ["${TOK}", "${MISSING:-dflt}"]}) == {"a": ["abc", "dflt"]}
    with pytest.raises(ConfigError):
        _interpolate("${DEFINITELY_NOT_SET_123}")


def test_transform_chain_and_rejects():
    t = Transformer(
        [
            {"flatten": {"sep": "_"}},
            {"rename": {"p_amount": "price"}},
            {"strip": True},
            {"filter": {"field": "kind", "op": "ne", "value": "skip"}},
            {"derive": {"label": "{name} ({kind})"}},
            {"dedupe": {"keys": ["id"]}},
            {"require": ["name"]},
        ],
        {"columns": {"id": {"type": "int", "required": True}, "price": "float"}},
    )
    raw = [
        {"id": "1", "name": "  A  ", "kind": "x", "p": {"amount": "9.99"}},
        {"id": "1", "name": "dup", "kind": "x", "p": {"amount": "1"}},
        {"id": "2", "name": "B", "kind": "skip", "p": {"amount": "1"}},
        {"id": "3", "name": "", "kind": "x", "p": {"amount": "1"}},
        {"id": "4", "name": "D", "kind": "x", "p": {"amount": "bad"}},
    ]
    good, bad = t.apply(raw)
    assert good == [{"id": 1, "name": "A", "kind": "x", "price": 9.99, "label": "A (x)"}]
    assert [b["_step"] for b in bad] == ["require", "schema"]
    assert t.stats == {"in": 5, "out": 1, "filtered": 2, "rejected": 2}
    assert raw[0]["name"] == "  A  "  # raw records are never mutated


def test_schema_extra_drop():
    t = Transformer([], {"columns": {"id": "int"}, "extra": "drop"})
    good, _ = t.apply([{"id": "5", "other": 1}])
    assert good == [{"id": 5}]


def test_unknown_step():
    with pytest.raises(ValueError, match="Unknown transform step"):
        Transformer(["explode"])


def test_env_interpolation_empty_value_uses_default(monkeypatch):
    monkeypatch.setenv("EMPTY_ONE", "")
    assert _interpolate("${EMPTY_ONE:-fallback}") == "fallback"
    assert _interpolate("${EMPTY_ONE}") == ""
