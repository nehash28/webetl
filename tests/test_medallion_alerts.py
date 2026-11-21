import json
import sqlite3
from pathlib import Path

import duckdb
import pytest

from webetl import alerts as alerts_mod
from webetl.cli import main as cli_main
from webetl.config import load_config
from webetl.pipeline import Pipeline

CONFIGS = Path(__file__).resolve().parents[1] / "configs"


class FakeSMTP:
    sent: list = []
    calls: list = []

    def __init__(self, host, port, timeout=None, **kw):
        FakeSMTP.calls.append(("connect", host, port))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def starttls(self, context=None):
        FakeSMTP.calls.append(("starttls",))

    def login(self, user, pwd):
        FakeSMTP.calls.append(("login", user))

    def send_message(self, msg):
        FakeSMTP.sent.append(msg)


@pytest.fixture
def fake_smtp(monkeypatch):
    FakeSMTP.sent, FakeSMTP.calls = [], []
    monkeypatch.setattr(alerts_mod.smtplib, "SMTP", FakeSMTP)
    return FakeSMTP


@pytest.fixture
def medallion_cfg(server, paths, monkeypatch):
    monkeypatch.setenv("API_BASE_URL", server)
    monkeypatch.setenv("DATABASE_URL", paths["db"])
    monkeypatch.setenv("LAKE_PATH", paths["lake"])
    monkeypatch.setenv("SLACK_WEBHOOK_URL", f"{server}/slack/ok")
    monkeypatch.setenv("SMTP_HOST", "smtp.test")
    monkeypatch.setenv("SMTP_USER", "bot@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "x")
    monkeypatch.setenv("ALERT_EMAIL_TO", "oncall@example.com")
    cfg = load_config(CONFIGS / "products_medallion.yaml")
    cfg["rejects"] = {"path": paths["rejects"]}
    return cfg


def test_medallion_layers_and_gold(medallion_cfg, paths, fake_smtp, slack_posts):
    res = Pipeline(medallion_cfg, state_path=paths["state"]).run()
    assert res.status == "success", res.error
    lake = Path(paths["lake"])
    assert len(list((lake / "bronze" / "products_medallion").rglob("*.jsonl.gz"))) == 1
    assert len(list((lake / "silver" / "products_medallion").rglob("*.parquet"))) == 1
    gold_dir = lake / "gold" / "products_medallion"
    assert sorted(p.name for p in gold_dir.iterdir()) == [
        "category_summary.parquet", "daily_loads.parquet", "out_of_stock.parquet", "price_bands.parquet"]
    assert res.stats["gold"]["category_summary"] == 4
    assert res.stats["load"] == {"warehouse": 56, "silver": 56}

    summary = duckdb.sql(f"SELECT SUM(products), SUM(in_stock) FROM '{gold_dir}/category_summary.parquet'").fetchall()[0]
    assert summary == (56, 37)

    con = sqlite3.connect(paths["db"].replace("sqlite:///", ""))
    tables = {r[0] for r in con.execute("select name from sqlite_master where type='table'")}
    assert {"products", "gold_category_summary", "gold_price_bands", "gold_out_of_stock",
            "gold_daily_loads"} <= tables
    assert con.execute("select count(*) from gold_out_of_stock").fetchall()[0][0] == 19

    # 1 rejected record + `on: [failure, rejects]` -> a warning alert on both channels
    assert res.alerts == {"slack": "sent", "email": "sent"}
    assert ":warning:" in json.dumps(slack_posts[0])
    assert "1 rejected" in fake_smtp.sent[0].get_content()
    assert ("starttls",) in fake_smtp.calls and ("login", "bot@example.com") in fake_smtp.calls

    # Second (incremental) run: silver keeps history, the `silver` view keeps the latest per key.
    res2 = Pipeline(medallion_cfg, state_path=paths["state"]).run(full_refresh=True)
    assert res2.status == "success"
    assert len(list((lake / "silver" / "products_medallion").rglob("*.parquet"))) == 2
    assert res2.stats["gold"]["category_summary"] == 4
    loads = duckdb.sql(f"SELECT rows_loaded, products FROM '{gold_dir}/daily_loads.parquet'").fetchall()
    assert loads == [(112, 56)]          # silver_all has both loads, 56 distinct products
    assert duckdb.sql(f"SELECT SUM(products) FROM '{gold_dir}/category_summary.parquet'").fetchall()[0] == (56,)


def test_failure_alert_on_source_error(medallion_cfg, paths, fake_smtp, slack_posts):
    medallion_cfg["source"]["auth"]["token"] = "wrong"
    medallion_cfg["source"]["retries"] = 0
    res = Pipeline(medallion_cfg, state_path=paths["state"]).run()
    assert res.status == "failed"
    assert res.alerts == {"slack": "sent", "email": "sent"}
    post = json.dumps(slack_posts[0])
    assert "FAILED" in post and "<!channel>" in post and "401" in post
    msg = fake_smtp.sent[0]
    assert "FAILED" in msg["Subject"] and msg["To"] == "oncall@example.com"
    assert not (Path(paths["lake"]) / "gold").exists()  # no gold built from a failed run


def test_gold_sql_error_fails_run_but_keeps_load(medallion_cfg, paths, fake_smtp, slack_posts):
    medallion_cfg["medallion"]["gold"]["models"].append({"name": "broken", "sql": "SELECT nope FROM silver"})
    res = Pipeline(medallion_cfg, state_path=paths["state"]).run()
    assert res.status == "failed" and res.error.startswith("gold build")
    assert "FAILED" in json.dumps(slack_posts[0])
    # the silver/warehouse load was committed and the watermark advanced
    assert Pipeline(medallion_cfg, state_path=paths["state"]).state.get_watermark("products_medallion")
    # after fixing the SQL, gold can be rebuilt without re-extracting
    medallion_cfg["medallion"]["gold"]["models"].pop()
    assert Pipeline(medallion_cfg, state_path=paths["state"]).build_gold()["category_summary"] == 4


def test_no_alert_when_trigger_not_matched(medallion_cfg, paths, fake_smtp, slack_posts):
    medallion_cfg["alerts"]["notify_on"] = ["failure"]
    medallion_cfg["max_reject_ratio"] = None
    res = Pipeline(medallion_cfg, state_path=paths["state"]).run()
    assert res.status == "success" and res.alerts == {} and not slack_posts and not fake_smtp.sent


def test_alert_delivery_error_does_not_break_run(medallion_cfg, paths, server, fake_smtp):
    medallion_cfg["alerts"]["slack"]["webhook_url"] = f"{server}/slack/bad"
    res = Pipeline(medallion_cfg, state_path=paths["state"]).run()
    assert res.status == "success"
    assert res.alerts["slack"].startswith("error") and res.alerts["email"] == "sent"


def test_unset_channels_are_skipped(medallion_cfg, paths, monkeypatch):
    medallion_cfg["alerts"]["slack"]["webhook_url"] = ""
    medallion_cfg["alerts"]["email"]["smtp_host"] = ""
    res = Pipeline(medallion_cfg, state_path=paths["state"]).run()
    assert res.status == "success" and res.alerts == {}


def test_cli_shared_alerts_config_error_and_test_alert(server, paths, tmp_path, slack_posts, capsys):
    alerts_file = tmp_path / "alerts.yaml"
    alerts_file.write_text(f"alerts:\n  notify_on: [failure]\n  slack:\n    webhook_url: {server}/slack/ok\n")
    bad_cfg = tmp_path / "broken.yaml"
    bad_cfg.write_text("name: broken\nsource: {type: api}\n")  # no load target -> config error
    code = cli_main(["--state", str(paths["state"]), "--alerts", str(alerts_file), "run", str(bad_cfg)])
    assert code == 1
    assert "config error" in json.dumps(slack_posts[0])

    good_cfg = tmp_path / "ok.yaml"
    good_cfg.write_text(f"name: ok\nsource: {{type: api, url: {server}/api/flaky}}\n"
                        f"load: [{{type: sql, url: '{paths['db']}', table: t}}]\n")
    assert cli_main(["--state", str(paths["state"]), "--alerts", str(alerts_file),
                     "test-alert", str(good_cfg)]) == 0
    assert "test alert" in json.dumps(slack_posts[-1])
    assert "slack: sent" in capsys.readouterr().out


def test_medallion_config_validation():
    from webetl.medallion import expand_medallion
    base = {"name": "x", "source": {"type": "api"}, "load": []}
    with pytest.raises(ValueError):
        expand_medallion({**base, "medallion": {"root": "l", "gold": {"models": [{"name": "drop table", "sql": "x"}]}}})
    cfg = expand_medallion({**base, "medallion": {"root": "s3://bucket/lake"}})
    assert cfg["raw"]["path"] == "s3://bucket/lake/bronze"
    assert cfg["load"][0]["path"] == "s3://bucket/lake/silver"


def test_yaml_bare_on_key_still_works():
    import yaml
    from types import SimpleNamespace
    cfg = yaml.safe_load("on: [rejects]\nslack: {webhook_url: ''}\n")
    assert True in cfg  # the YAML 1.1 gotcha this guards against
    res = SimpleNamespace(status="success", stats={"transform": {"rejected": 2}})
    assert alerts_mod.classify(res) == "rejects"
    # rejects is a configured trigger, but no channel is enabled -> nothing sent, no error
    assert alerts_mod.notify(cfg, res) == {}
