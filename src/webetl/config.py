"""Pipeline configuration: YAML loading, ${ENV} interpolation and basic validation."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class ConfigError(ValueError):
    """Raised when a pipeline config is invalid."""


def _interpolate(value: Any) -> Any:
    """Recursively replace ${VAR} / ${VAR:-default} with environment values."""
    if isinstance(value, str):
        def repl(m: re.Match) -> str:
            name, default = m.group(1), m.group(2)
            # Shell semantics: ${VAR:-default} also uses the default when VAR is set but empty
            # (e.g. `SMTP_HOST=` in a .env file), so an empty value never wins over a default.
            if os.environ.get(name):
                return os.environ[name]
            if default is not None:
                return default
            if name in os.environ:
                return ""
            raise ConfigError(f"Environment variable '{name}' is not set and has no default")

        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def validate_config(cfg: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(cfg, dict):
        raise ConfigError("Config must be a mapping")
    for key in ("name", "source", "load"):
        if key not in cfg:
            raise ConfigError(f"Config is missing required key '{key}'")
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", str(cfg["name"])):
        raise ConfigError("'name' may only contain letters, digits, '_' and '-'")
    if "type" not in cfg["source"]:
        raise ConfigError("'source.type' is required (api | html)")
    loads = cfg["load"]
    if isinstance(loads, dict):
        loads = [loads]
        cfg["load"] = loads
    if loads is None:
        loads = []
        cfg["load"] = loads
    if not loads and not cfg.get("medallion"):
        raise ConfigError("'load' needs at least one target (or a 'medallion' block)")
    for target in loads:
        if "type" not in target:
            raise ConfigError("Every load target needs a 'type' (sql | lake)")
    cfg.setdefault("transform", [])
    cfg.setdefault("schema", None)
    cfg.setdefault("batch_size", 500)
    return cfg


def load_config(path: str | Path) -> dict[str, Any]:
    """Read a YAML pipeline config, interpolate env vars, and validate it."""
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return validate_config(_interpolate(raw))
