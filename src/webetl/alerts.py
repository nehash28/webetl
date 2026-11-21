"""Run alerts to Slack (incoming webhook) and email (SMTP).

Config (per pipeline, or shared across pipelines with `webetl --alerts alerts.yaml run ...`):

  alerts:
    notify_on: [failure]          # any of: failure | warning | rejects | success
    slack:
      webhook_url: ${SLACK_WEBHOOK_URL:-}     # empty -> channel skipped
      mention: "<!channel>"       # optional, added to failure alerts
    email:
      smtp_host: ${SMTP_HOST:-}
      smtp_port: 587
      security: starttls          # starttls | ssl | none
      username: ${SMTP_USER:-}
      password: ${SMTP_PASSWORD:-}
      from: etl@example.com
      to: [data-team@example.com]
      subject_prefix: "[webetl]"

Triggers
  failure  the run failed (source error, data-quality gate, load error, gold build,
           a data quality check with severity: error ...)
  warning  the run succeeded but a data quality check with severity: warn failed
  rejects  the run succeeded but some records were rejected
  success  every successful run (useful for a daily heartbeat channel)

Sending an alert never fails the pipeline: delivery errors are logged and returned.
"""

from __future__ import annotations

import logging
import smtplib
import ssl
import time
from email.message import EmailMessage
from typing import Any

import requests

log = logging.getLogger(__name__)

TRIGGERS = ("failure", "warning", "rejects", "success")


def classify(result: Any) -> str | None:
    if result.status != "success":
        return "failure"
    if ((result.stats or {}).get("quality") or {}).get("warnings"):
        return "warning"
    if (result.stats.get("transform") or {}).get("rejected"):
        return "rejects"
    return "success"


def _summary_lines(result: Any) -> list[str]:
    s = result.stats or {}
    t = s.get("transform") or {}
    e = s.get("extract") or {}
    lines = [
        f"Pipeline: {result.pipeline}",
        f"Status:   {result.status.upper()}",
        f"Run ID:   {result.run_id}",
        f"Started:  {result.started_at}",
        f"Duration: {result.duration_s}s",
    ]
    if e:
        lines.append(f"Extract:  {e.get('records', 0)} records from {e.get('pages', 0)} pages")
    if t:
        lines.append(f"Transform: {t.get('out', 0)} loaded, {t.get('rejected', 0)} rejected, "
                     f"{t.get('filtered', 0)} filtered")
    if s.get("load"):
        lines.append("Load:     " + ", ".join(f"{k}={v}" for k, v in s["load"].items()))
    if s.get("gold"):
        lines.append("Gold:     " + ", ".join(f"{k}={v}" for k, v in s["gold"].items()))
    q = s.get("quality") or {}
    if q:
        lines.append(f"Quality:  {q.get('passed', 0)} checks passed, {q.get('failed', 0)} failed")
        for r in q.get("results", []):
            if r["status"] in ("fail", "error"):
                lines.append(f"  - [{r['severity']}] {r['name']}: {r['detail']}")
    if result.error:
        lines.append(f"Error:    {result.error}")
    return lines


_ICON = {"failure": ":red_circle:", "warning": ":large_yellow_circle:", "rejects": ":warning:",
         "success": ":white_check_mark:"}
_TITLE = {"failure": "ETL run FAILED", "warning": "ETL run passed with data quality warnings",
          "rejects": "ETL run finished with rejected records", "success": "ETL run succeeded"}


def send_slack(cfg: dict[str, Any], result: Any, kind: str) -> None:
    url = cfg.get("webhook_url")
    title = f"{_ICON[kind]} *{_TITLE[kind]}: `{result.pipeline}`*"
    if kind == "failure" and cfg.get("mention"):
        title = f"{cfg['mention']} {title}"
    body = "\n".join(_summary_lines(result))
    payload = {
        "text": f"{_TITLE[kind]}: {result.pipeline}",  # notification fallback
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": title}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"```{body}```"}},
        ],
    }
    if cfg.get("channel"):
        payload["channel"] = cfg["channel"]
    last: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=float(cfg.get("timeout", 10)))
            if resp.status_code < 400:
                return
            last = RuntimeError(f"Slack returned HTTP {resp.status_code}: {resp.text[:200]}")
            if resp.status_code < 500 and resp.status_code != 429:
                break  # bad URL / payload: retrying will not help
        except requests.RequestException as exc:
            last = exc
        time.sleep(0.5 * 2 ** attempt)
    raise last  # type: ignore[misc]


def send_email(cfg: dict[str, Any], result: Any, kind: str) -> None:
    to = cfg.get("to")
    to = [to] if isinstance(to, str) else list(to or [])
    msg = EmailMessage()
    msg["Subject"] = f"{cfg.get('subject_prefix', '[webetl]')} {_TITLE[kind]}: {result.pipeline}"
    msg["From"] = cfg.get("from") or cfg.get("username") or "webetl@localhost"
    msg["To"] = ", ".join(to)
    msg.set_content("\n".join(_summary_lines(result)) +
                    "\n\nSee `webetl history` and the rejects folder for details.\n")

    host, port = cfg["smtp_host"], int(cfg.get("smtp_port", 587))
    security = str(cfg.get("security", "starttls")).lower()
    timeout = float(cfg.get("timeout", 20))
    if security == "ssl":
        server: smtplib.SMTP = smtplib.SMTP_SSL(host, port, timeout=timeout,
                                                context=ssl.create_default_context())
    else:
        server = smtplib.SMTP(host, port, timeout=timeout)
    with server:
        if security == "starttls":
            server.starttls(context=ssl.create_default_context())
        if cfg.get("username"):
            server.login(cfg["username"], cfg.get("password", ""))
        server.send_message(msg)


def notify(alerts: dict[str, Any] | None, result: Any, force: bool = False) -> dict[str, str]:
    """Send alerts for a finished run. Returns {channel: "sent" | "error: ..."}."""
    if not alerts:
        return {}
    kind = classify(result)
    # `notify_on` is the documented key. A bare `on:` is read by YAML as the boolean True,
    # so accept that (and "on") too rather than silently falling back to the default.
    triggers = alerts.get("notify_on") or alerts.get("on") or alerts.get(True) or ["failure"]
    triggers = [triggers] if isinstance(triggers, str) else triggers
    unknown = set(triggers) - set(TRIGGERS)
    if unknown:
        log.warning("unknown alert triggers ignored: %s", sorted(unknown))
    if not force and kind not in triggers:
        return {}

    outcome: dict[str, str] = {}
    channels = [
        ("slack", send_slack, (alerts.get("slack") or {}).get("webhook_url")),
        ("email", send_email, (alerts.get("email") or {}).get("smtp_host")
         and (alerts.get("email") or {}).get("to")),
    ]
    for name, fn, enabled in channels:
        if not enabled:
            continue
        try:
            fn(alerts[name], result, kind)
            outcome[name] = "sent"
            log.info("%s alert sent for %s (%s)", name, result.pipeline, kind)
        except Exception as exc:  # never let alerting break the pipeline
            outcome[name] = f"error: {type(exc).__name__}: {exc}"
            log.error("%s alert failed for %s: %s", name, result.pipeline, exc)
    return outcome
