"""Pipeline orchestration.

extract -> bronze (raw) -> transform -> load (SQL / silver lake) -> commit watermark
        -> build gold models -> record run -> send alerts
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from webetl.alerts import notify
from webetl.config import load_config, validate_config
from webetl.extractors import build_extractor
from webetl.loaders import LakeLoader, LoadContext, Loader, build_loader
from webetl.medallion import GoldBuilder, expand_medallion
from webetl.quality import QualityRunner, summarize, validate_checks
from webetl.state import StateStore, mirror_run
from webetl.transform import Transformer
from webetl.utils import batched

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass
class RunResult:
    pipeline: str
    run_id: str
    status: str
    started_at: str
    finished_at: str
    duration_s: float
    stats: dict[str, Any] = field(default_factory=dict)
    sample: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    alerts: dict[str, str] = field(default_factory=dict)


class Pipeline:
    def __init__(self, config: dict[str, Any], state_path: str | Path = ".webetl/state.db",
                 alerts: dict[str, Any] | None = None):
        self.cfg = expand_medallion(validate_config(config))
        self.name: str = self.cfg["name"]
        validate_checks(self.cfg.get("checks"))
        self.state = StateStore(state_path)
        # Pipeline-level alerts win over shared ones passed in (e.g. from `--alerts`).
        self.alerts = self.cfg.get("alerts") or alerts

    @classmethod
    def from_file(cls, path: str | Path, **kw: Any) -> "Pipeline":
        return cls(load_config(path), **kw)

    def _aux_loaders(self, ctx: LoadContext) -> tuple[Loader | None, Loader]:
        """Raw/bronze landing zone (optional) and the rejects/dead-letter sink."""
        raw_cfg = self.cfg.get("raw")
        raw = None
        if raw_cfg:
            raw = LakeLoader({"type": "lake", "name": "bronze", "format": "jsonl", "compression": "gzip",
                              "partition_by": ["ingest_date"], "dataset": self.name, **raw_cfg},
                             LoadContext(ctx.pipeline, ctx.run_id, ctx.started_at))
        rej_cfg = self.cfg.get("rejects") or {"path": ".webetl/rejects"}
        rejects = LakeLoader({"type": "lake", "name": "rejects", "format": "jsonl",
                              "partition_by": ["ingest_date"], "dataset": self.name, **rej_cfg},
                             LoadContext(ctx.pipeline, ctx.run_id, ctx.started_at))
        return raw, rejects

    def build_gold(self, run_id: str = "manual") -> dict[str, int]:
        m = self.cfg.get("medallion")
        if not m or not (m.get("gold") or {}).get("models"):
            return {}
        return GoldBuilder(self.name, m, run_id).build()

    def run_checks(self, run_records: list[dict[str, Any]] | None = None,
                   schema: dict | None = None) -> dict[str, Any]:
        """Run the `checks:` block against the lake (and this run's records, if given)."""
        if not self.cfg.get("checks"):
            return {}
        return summarize(QualityRunner(self.name, self.cfg, run_records, schema).run())

    def run(self, *, full_refresh: bool = False, dry_run: bool = False,
            limit: int | None = None, alert: bool = True) -> RunResult:
        run_id = uuid.uuid4().hex
        started = datetime.now(timezone.utc)
        t0 = time.monotonic()
        cfg = self.cfg

        watermark = None if full_refresh else self.state.get_watermark(self.name)
        extractor = None
        transformer = None
        loaders: list[Loader] = []
        aux: list[Loader] = []
        raw = rejects = None
        sample: list[dict[str, Any]] = []
        error: str | None = None
        status = "success"
        gold: dict[str, int] = {}
        quality: dict[str, Any] = {}
        keep_records = bool(cfg.get("checks")) and not dry_run
        run_records: list[dict[str, Any]] = []

        log.info("run %s: pipeline=%s watermark=%s full_refresh=%s dry_run=%s",
                 run_id[:8], self.name, watermark, full_refresh, dry_run)
        try:
            # Setup lives inside the try so that bad config / unreachable storage also alerts.
            extractor = build_extractor(cfg["source"], watermark=watermark)
            transformer = Transformer(cfg.get("transform") or [], cfg.get("schema"))
            ctx = LoadContext(self.name, run_id, started, schema=transformer.schema)
            if not dry_run:
                loaders = [build_loader(t, ctx) for t in cfg["load"]]
                raw, rejects = self._aux_loaders(ctx)
                aux = [x for x in (raw, rejects) if x is not None]
            add_meta = cfg.get("metadata", True)

            for ld in loaders + aux:
                ld.open()
            records = extractor.extract()
            if limit:
                records = (r for i, r in enumerate(records) if i < limit)
            for batch in batched(records, int(cfg["batch_size"])):
                if raw:
                    raw.write(batch)
                good, bad = transformer.apply(batch)
                if add_meta:
                    loaded_at = datetime.now(timezone.utc)
                    for rec in good:
                        rec["_etl_run_id"] = run_id
                        rec["_etl_loaded_at"] = loaded_at
                if bad and rejects:
                    rejects.write(bad)
                if len(sample) < 5:
                    sample.extend(good[: 5 - len(sample)])
                for ld in loaders:
                    ld.write(good)
                if keep_records:
                    run_records.extend(good)
                log.info("batch: in=%d loaded=%d rejected=%d", len(batch), len(good), len(bad))

            max_ratio = cfg.get("max_reject_ratio")
            seen = transformer.stats["in"]
            if max_ratio is not None and seen and transformer.stats["rejected"] / seen > float(max_ratio):
                raise DataQualityError(
                    f"{transformer.stats['rejected']}/{seen} records rejected, above max_reject_ratio={max_ratio}")
            if cfg.get("min_records") is not None and transformer.stats["out"] < int(cfg["min_records"]):
                raise DataQualityError(
                    f"only {transformer.stats['out']} records loaded, below min_records={cfg['min_records']}")
        except Exception as exc:
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            log.exception("run %s failed", run_id[:8])

        for ld in loaders + aux:
            try:
                ld.close(success=status == "success" or ld is rejects)
            except Exception as exc:  # a failed commit fails the run
                if status == "success":
                    status, error = "failed", f"{ld.name} close: {type(exc).__name__}: {exc}"
                log.exception("closing loader %s failed", ld.name)
        if extractor is not None:
            extractor.close()

        if status == "success" and not dry_run and extractor is not None and extractor.max_watermark is not None:
            self.state.set_watermark(self.name, extractor.max_watermark)

        # Gold is rebuilt from silver after the data is safely committed. A gold failure fails
        # the run (and alerts) but does not undo the load; fix the SQL and run `webetl build-gold`.
        if status == "success" and not dry_run:
            try:
                gold = self.build_gold(run_id)
            except Exception as exc:
                status, error = "failed", f"gold build: {type(exc).__name__}: {exc}"
                log.exception("gold build failed")

        # Data quality checks run last, against what was just committed (like dbt tests).
        if status == "success" and keep_records:
            try:
                quality = self.run_checks(run_records, transformer.schema if transformer else None)
            except Exception as exc:
                status, error = "failed", f"quality checks: {type(exc).__name__}: {exc}"
                log.exception("quality checks crashed")
            if quality.get("errors"):
                status = "failed"
                error = "data quality checks failed: " + ", ".join(quality["errors"])
        run_records = []

        finished = datetime.now(timezone.utc)
        stats: dict[str, Any] = {
            "extract": extractor.stats if extractor else {},
            "transform": transformer.stats if transformer else {},
            "load": {ld.name: ld.rows_written for ld in loaders},
            "watermark": {"before": watermark,
                          "after": None if extractor is None or extractor.max_watermark is None
                          else str(extractor.max_watermark)},
        }
        if gold:
            stats["gold"] = gold
        if quality:
            stats["quality"] = quality
        if rejects is not None:
            stats["rejects_files"] = getattr(rejects, "files_written", [])
        result = RunResult(self.name, run_id, status, started.isoformat(), finished.isoformat(),
                           round(time.monotonic() - t0, 3), stats, sample, error)
        if not dry_run:
            self.state.record_run(run_id, self.name, result.started_at, result.finished_at,
                                  status, stats, error)
            if os.environ.get("RUN_HISTORY_URL"):
                mirror_run(os.environ["RUN_HISTORY_URL"], run_id, self.name, result.started_at,
                           result.finished_at, status, stats, error)
            if alert:
                result.alerts = notify(self.alerts, result)
        return result


def run_pipeline(path: str | Path, **kwargs: Any) -> RunResult:
    state_path = kwargs.pop("state_path", ".webetl/state.db")
    alerts = kwargs.pop("alerts", None)
    return Pipeline.from_file(path, state_path=state_path, alerts=alerts).run(**kwargs)
