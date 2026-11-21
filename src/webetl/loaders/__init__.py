"""Loaders write transformed records to databases or data lakes."""

from __future__ import annotations

from typing import Any

from webetl.loaders.base import LoadContext, Loader
from webetl.loaders.lake import LakeLoader
from webetl.loaders.sql import SqlLoader

LOADERS: dict[str, type[Loader]] = {
    "sql": SqlLoader,
    "lake": LakeLoader,
}


def build_loader(target: dict[str, Any], ctx: LoadContext) -> Loader:
    kind = target["type"]
    if kind not in LOADERS:
        raise ValueError(f"Unknown load type '{kind}'. Available: {sorted(LOADERS)}")
    return LOADERS[kind](target, ctx)


__all__ = ["Loader", "LoadContext", "SqlLoader", "LakeLoader", "build_loader", "LOADERS"]
