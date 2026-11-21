"""One Airflow DAG per webetl pipeline config.

Every YAML file in the configs folder becomes a DAG called `webetl_<name>`:

    validate_config  ->  extract_transform_load  ->  data_quality_checks

* The schedule comes from the config's `schedule:` key (a cron string such as
  "*/30 * * * *"). Configs without one get a manual-only DAG you trigger from the UI.
* webetl runs in its own virtualenv (/opt/webetl-venv), so its dependencies never
  clash with Airflow's. Tasks call the `webetl` CLI, which exits non-zero on failure,
  so a failed pipeline shows up red in Airflow.
* Failure alerts (email/Slack) are sent by webetl itself, using the pipeline's
  `alerts:` block and the SMTP_* / SLACK_* variables from your .env file.
"""

from __future__ import annotations

import glob
import os
from datetime import timedelta

import pendulum
import yaml
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

WEBETL_BIN = os.environ.get("WEBETL_BIN", "/opt/webetl-venv/bin/webetl")
CONFIG_DIR = os.environ.get("WEBETL_CONFIG_DIR", "/opt/webetl/configs")
DATA_DIR = os.environ.get("WEBETL_DATA_DIR", "/data")
STATE = f"{DATA_DIR}/state/state.db"


def _read(path: str) -> dict | None:
    """Read just enough of a config to build the DAG (no env interpolation needed)."""
    try:
        with open(path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
    except (OSError, yaml.YAMLError):
        return None
    return cfg if isinstance(cfg, dict) and cfg.get("name") else None


def build_dag(path: str, cfg: dict) -> DAG:
    name = cfg["name"]
    source = (cfg.get("source") or {}).get("type", "?")
    gold = len((((cfg.get("medallion") or {}).get("gold") or {}).get("models")) or [])
    checks = len(cfg.get("checks") or [])
    run = f"cd {DATA_DIR} && {WEBETL_BIN} --state {STATE}"

    with DAG(
        dag_id=f"webetl_{name}",
        description=f"webetl pipeline {name} ({source} source)",
        schedule=cfg.get("schedule"),
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,            # don't backfill every missed interval
        max_active_runs=1,        # never run the same pipeline twice at once
        tags=["webetl", source] + (["medallion"] if cfg.get("medallion") else []),
        default_args={
            "owner": "webetl",
            "retries": 0,         # webetl already retries HTTP calls; avoids duplicate alerts
            "execution_timeout": timedelta(minutes=int(cfg.get("timeout_minutes", 30))),
        },
        doc_md=(f"### {name}\n\nConfig: `{os.path.basename(path)}`  \n"
                f"Gold models: {gold} · Quality checks: {checks}  \n"
                f"Schedule: `{cfg.get('schedule') or 'manual'}`"),
    ) as dag:
        validate = BashOperator(
            task_id="validate_config",
            bash_command=f"{WEBETL_BIN} validate {path}",
        )
        load = BashOperator(
            task_id="extract_transform_load",
            bash_command=f"{run} run {path}",
            doc_md="Extract -> bronze -> transform -> silver/warehouse -> gold -> checks -> alerts.",
        )
        validate >> load
        if checks:
            quality = BashOperator(
                task_id="data_quality_checks",
                bash_command=f"{run} check {path}",
                doc_md="Re-runs the `checks:` block against the lake and prints a pass/fail report.",
            )
            load >> quality
    return dag


for _path in sorted(glob.glob(os.path.join(CONFIG_DIR, "*.y*ml"))):
    _cfg = _read(_path)
    if _cfg:
        globals()[f"webetl_{_cfg['name']}"] = build_dag(_path, _cfg)
