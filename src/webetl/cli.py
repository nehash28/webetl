"""Command-line interface.

  webetl [--alerts alerts.yaml] run CONFIG [CONFIG ...] [--full-refresh] [--dry-run] [--limit N]
  webetl validate CONFIG [CONFIG ...]
  webetl build-gold CONFIG [CONFIG ...]     # rebuild gold tables from silver, no extraction
  webetl check CONFIG [CONFIG ...]          # run data quality checks against the lake now
  webetl test-alert CONFIG                  # send a test alert through every configured channel
  webetl history [PIPELINE]
  webetl reset PIPELINE                     # forget the incremental watermark
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from webetl.alerts import notify
from webetl.config import ConfigError, _interpolate, load_config
from webetl.medallion import expand_medallion
from webetl.pipeline import Pipeline, RunResult
from webetl.state import StateStore
from webetl.transform import Transformer


def _default(o: Any) -> Any:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    return str(o)


def _expand(paths: list[str]) -> list[Path]:
    out: list[Path] = []
    for p in map(Path, paths):
        out.extend(sorted(p.glob("*.y*ml")) if p.is_dir() else [p])
    return out


def _load_alerts(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    import yaml

    with open(path, encoding="utf-8") as fh:
        data = _interpolate(yaml.safe_load(fh) or {})
    return data.get("alerts", data)


def _failed_result(name: str, error: str) -> RunResult:
    now = datetime.now(timezone.utc).isoformat()
    return RunResult(name, uuid.uuid4().hex, "failed", now, now, 0.0, {}, [], error)


def cmd_run(args: argparse.Namespace) -> int:
    failed = 0
    shared_alerts = _load_alerts(args.alerts)
    for path in _expand(args.configs):
        try:
            pipe = Pipeline.from_file(path, state_path=args.state, alerts=shared_alerts)
        except Exception as exc:  # config could not even be loaded: still alert
            result = _failed_result(path.stem, f"config error: {type(exc).__name__}: {exc}")
            result.alerts = notify(shared_alerts, result)
        else:
            result = pipe.run(full_refresh=args.full_refresh, dry_run=args.dry_run, limit=args.limit)
        payload = {k: v for k, v in result.__dict__.items() if k != "sample" or args.dry_run}
        print(json.dumps(payload, indent=2, default=_default))
        failed += result.status != "success"
    return 1 if failed else 0


def cmd_validate(args: argparse.Namespace) -> int:
    bad = 0
    for path in _expand(args.configs):
        try:
            cfg = expand_medallion(load_config(path))
            Transformer(cfg.get("transform") or [], cfg.get("schema"))  # compiles steps
            targets = ", ".join(t.get("name") or t["type"] for t in cfg["load"])
            gold = len(((cfg.get("medallion") or {}).get("gold") or {}).get("models") or [])
            extra = f", gold models: {gold}" if gold else ""
            if cfg.get("checks"):
                from webetl.quality import validate_checks
                validate_checks(cfg["checks"])
                extra += f", checks: {len(cfg['checks'])}"
            print(f"OK    {path}  ({cfg['source']['type']} -> {targets}{extra})")
        except (ConfigError, ValueError, KeyError, OSError) as exc:
            bad += 1
            print(f"ERROR {path}: {exc}")
    return 1 if bad else 0


def cmd_build_gold(args: argparse.Namespace) -> int:
    bad = 0
    for path in _expand(args.configs):
        try:
            built = Pipeline.from_file(path, state_path=args.state).build_gold()
            print(f"OK    {path}: " + (", ".join(f"{k}={v} rows" for k, v in built.items()) or "no gold models"))
        except Exception as exc:
            bad += 1
            print(f"ERROR {path}: {type(exc).__name__}: {exc}")
    return 1 if bad else 0


def cmd_check(args: argparse.Namespace) -> int:
    bad = 0
    for path in _expand(args.configs):
        try:
            summary = Pipeline.from_file(path, state_path=args.state).run_checks()
        except Exception as exc:
            bad += 1
            print(f"ERROR {path}: {type(exc).__name__}: {exc}")
            continue
        if not summary:
            print(f"{path}: no checks configured")
            continue
        print(f"{path}: {summary['passed']} passed, {summary['failed']} failed")
        for r in summary["results"]:
            mark = {"pass": "PASS", "fail": "FAIL", "error": "ERROR", "skip": "SKIP"}[r["status"]]
            sev = f" [{r['severity']}]" if r["status"] in ("fail", "error") else ""
            print(f"  {mark:<5} {r['name']:<28}{sev}  {r['detail']}")
        bad += bool(summary["errors"])
    return 1 if bad else 0


def cmd_test_alert(args: argparse.Namespace) -> int:
    pipe = Pipeline.from_file(args.config, state_path=args.state, alerts=_load_alerts(args.alerts))
    if not pipe.alerts:
        print("No alerts configured (add an `alerts:` block or pass --alerts FILE).")
        return 1
    result = _failed_result(pipe.name, "This is a test alert from `webetl test-alert`. No action needed.")
    outcome = notify(pipe.alerts, result, force=True)
    if not outcome:
        print("No alert channels are enabled (is the webhook URL / SMTP host set?).")
        return 1
    for channel, status in outcome.items():
        print(f"{channel}: {status}")
    return 0 if all(v == "sent" for v in outcome.values()) else 1


def cmd_history(args: argparse.Namespace) -> int:
    for run in StateStore(args.state).history(args.pipeline, args.limit):
        loaded = run["stats"].get("transform", {}).get("out")
        print(f"{run['started_at'][:19]}  {run['pipeline']:<24} {run['status']:<8} "
              f"loaded={loaded}  run={run['run_id'][:8]}  {run['error'] or ''}")
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    StateStore(args.state).reset(args.pipeline)
    print(f"watermark cleared for {args.pipeline}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="webetl", description="Web ETL: APIs/HTML -> SQL / data lake")
    ap.add_argument("--state", default=".webetl/state.db", help="state database path")
    ap.add_argument("--alerts", help="YAML file with shared alert settings (used when a config has none)")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("run", help="run one or more pipelines (files or folders of YAML)")
    p.add_argument("configs", nargs="+")
    p.add_argument("--full-refresh", action="store_true", help="ignore the stored watermark")
    p.add_argument("--dry-run", action="store_true", help="extract + transform only, print a sample")
    p.add_argument("--limit", type=int, help="stop after N extracted records")
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("validate", help="check configs without running them")
    p.add_argument("configs", nargs="+")
    p.set_defaults(fn=cmd_validate)

    p = sub.add_parser("build-gold", help="rebuild gold tables from silver without extracting")
    p.add_argument("configs", nargs="+")
    p.set_defaults(fn=cmd_build_gold)

    p = sub.add_parser("check", help="run data quality checks against the lake")
    p.add_argument("configs", nargs="+")
    p.set_defaults(fn=cmd_check)

    p = sub.add_parser("test-alert", help="send a test alert to every configured channel")
    p.add_argument("config")
    p.set_defaults(fn=cmd_test_alert)

    p = sub.add_parser("history", help="show recent runs")
    p.add_argument("pipeline", nargs="?")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(fn=cmd_history)

    p = sub.add_parser("reset", help="clear a pipeline's incremental watermark")
    p.add_argument("pipeline")
    p.set_defaults(fn=cmd_reset)

    args = ap.parse_args(argv)
    level = logging.WARNING - 10 * min(args.verbose, 2)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", stream=sys.stderr)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
