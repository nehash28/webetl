# webetl — config-driven web ETL pipeline

**🔗 Live dashboard: [webetl on Streamlit](https://webetl-j9wfuap3a4jsou6unnvvkz.streamlit.app)** · Python · Airflow · PostgreSQL · DuckDB · Docker

## What I built

**webetl** is an end-to-end data pipeline. It collects data from web APIs and websites, cleans and validates it, stores it in a layered data lake and a cloud warehouse, and publishes it to a public dashboard. It runs on a schedule, checks its own data quality, and emails me when something goes wrong.

```mermaid
flowchart LR
    A["Web sources<br/>CoinGecko API, REST APIs,<br/>scraped web pages"] --> B["Extract<br/>pagination, auth,<br/>retries, rate limits"]
    B --> C[("Bronze<br/>raw JSON")]
    C --> D["Transform<br/>clean, type, validate"]
    D --> E[("Silver<br/>typed Parquet")]
    D -. bad rows .-> R["Rejects file"]
    E --> F[("Gold<br/>SQL summary tables")]
    F --> G["Data quality<br/>checks"]
    F --> H[("Neon<br/>PostgreSQL")]
    H --> I["Public Streamlit<br/>dashboard"]
    G -. failure .-> J["Email / Slack<br/>alert"]
    K["Apache Airflow<br/>every 30 min"] -. schedules .-> B
```

### Highlights

- **Config-driven pipelines.** Adding a new data source means writing a YAML file, not new code. The same engine handles REST APIs (page, offset, cursor, next-URL and link-header pagination; bearer, basic and API-key auth) and HTML scraping (CSS selectors, next-page links, detail pages, robots.txt).
- **Medallion data lake.** Raw data is kept in **bronze**, so it can be reprocessed without calling the source again. **Silver** holds typed, validated Parquet with full history. **Gold** tables are defined in SQL and built with DuckDB.
- **Data quality checks.** Rules like *unique IDs*, *prices in range* or *data loaded in the last 24 hours* run after every load. A failed `error` rule fails the run and sends an alert; a failed `warn` rule sends a warning. Bad records go to a rejects file with the reason, instead of being silently dropped.
- **Reliable by design.** Incremental loading with watermarks, idempotent upserts (re-running never creates duplicates), all-or-nothing writes, and HTTP retries with backoff.
- **Orchestration.** Apache Airflow turns every config into a DAG automatically and runs it on its schedule. A failed pipeline shows up red in Airflow and sends an email.
- **Production-style deployment.** The whole stack (Airflow, PostgreSQL, dashboard, demo API) starts with one `docker compose` command. The warehouse runs on **Neon** (cloud PostgreSQL), and the public dashboard connects with a **read-only** database login.
- **Tested.** 60 automated tests, including end-to-end runs against a mock API, a fake Slack webhook and SMTP server, and a real PostgreSQL database.

### Tech stack

| Area | Tools |
|---|---|
| Language | Python 3.12 |
| Extraction | requests (retries, rate limiting), BeautifulSoup, lxml |
| Storage | Parquet (PyArrow), JSONL, DuckDB, PostgreSQL (Neon), SQLite, SQLAlchemy 2 |
| Orchestration | Apache Airflow 3 |
| Infrastructure | Docker, Docker Compose |
| Dashboard | Streamlit, Altair, hosted on Streamlit Community Cloud |
| Alerts | SMTP email, Slack webhooks |
| Testing | pytest |

### What I learned

- **Designing a medallion architecture:** why keeping raw data matters, and how separating cleaning (silver) from business logic (gold) makes a pipeline easier to change.
- **Building pipelines that fail safely:** watermarks only advance after a successful load, partial files are rolled back, and data quality problems are loud instead of silent.
- **Running real infrastructure:** containerising a multi-service stack, scheduling it with Airflow, and connecting a laptop pipeline to a cloud database.
- **Security basics:** keeping secrets out of Git with `.env`, and giving a public app a read-only database login instead of full access.

---

## Documentation

Extract data from **REST APIs** and **web pages (scraping)**, transform it into **typed, validated records**, and load it into **SQL databases** and **data lakes**. Each pipeline is one YAML file, so you don't write code for each new source.

It can organise the lake as a **bronze → silver → gold medallion architecture** with SQL-defined gold tables, run **data quality checks**, send **Slack and email alerts**, and run on a schedule with **Apache Airflow + PostgreSQL in Docker Compose**, with a **Streamlit dashboard** on top.

## Run the whole stack with Docker (recommended)

```bash
cp .env.example .env          # Windows: copy .env.example .env   (then add your email settings)
docker compose up -d --build  # first build takes a few minutes
```

| Service | URL | What it is |
|---|---|---|
| Airflow | http://localhost:8080 | one DAG per pipeline config, scheduled from its `schedule:` key |
| Dashboard | http://localhost:8501 | gold tables, run history, data quality results |
| PostgreSQL | `localhost:5432`, user `webetl` / password `webetl`, db `warehouse` | the warehouse (connect DBeaver, Power BI, ...) |
| Mock API | http://localhost:8765 | demo source used by `products_medallion` |

**Public web address:** to publish the dashboard at a free `https://<name>.streamlit.app` link (with a free Neon cloud database and a read-only login), follow **[DEPLOY_DASHBOARD.md](DEPLOY_DASHBOARD.md)**. Setting `WAREHOUSE_URL` in `.env` points Airflow at the cloud database, and every run is also recorded in its `etl_runs` table so the hosted dashboard can show run history.

Stop with `docker compose down` (data is kept in Docker volumes; `docker compose down -v` deletes it).
The stack is set up for a laptop/demo: fixed local database passwords and no Airflow login. Don't expose these ports to the internet.

```
              ┌──────────────────────── docker compose ────────────────────────┐
 configs/*.yaml ─▶ Airflow (scheduler + UI :8080)                              │
              │     DAG webetl_<name>: validate_config ─▶ extract_transform_load ─▶ data_quality_checks
              │        │ runs the webetl CLI (own virtualenv)                   │
              │        ├──▶ lake volume  /data/lake/{bronze,silver,gold}        │
              │        ├──▶ PostgreSQL :5432  warehouse + gold_* tables ◀── Dashboard :8501
              │        └──▶ email / Slack alerts on failure or quality warnings │
              └────────────────────────────────────────────────────────────────┘
```

```
 ┌──────────── EXTRACT ────────────┐   ┌───────── TRANSFORM ─────────┐   ┌──────────── LOAD ─────────────┐
 │ api : REST/JSON                 │   │ flatten, rename, select,    │   │ sql  : SQLite / PostgreSQL /  │
 │   auth (bearer/basic/api key)   │   │ cast, map_values, regex,    │   │        MySQL / any SQLAlchemy │
 │   page/offset/cursor/next_url/  │──▶│ derive, filter, dedupe,     │──▶│        append | upsert |      │
 │   link-header pagination        │   │ require  + typed schema     │   │        replace (atomic swap)  │
 │ html: CSS selectors, next-page, │   │ validation                  │   │ lake : Parquet / JSONL / CSV, │
 │   detail pages, robots.txt      │   └──────────────┬──────────────┘   │        hive partitions, local │
 │ retries · backoff · rate limit  │                  │ bad rows         │        or s3:// gs:// abfs:// │
 └───────────────┬─────────────────┘                  ▼                  └───────────────────────────────┘
                 │ raw records              rejects (dead-letter JSONL)
                 ▼
        raw landing zone (bronze, JSONL.gz)        state: incremental watermarks + run history
```

## Quick start

```bash
pip install -e ".[dev]"            # add [postgres], [s3], [gcs], [azure] as needed

# start a local mock API + website (imitates real sources) in another terminal
python examples/mock_server.py --port 8765

webetl validate configs/
webetl run configs/products_api.yaml                     # API  -> SQLite + Parquet lake
BOOKS_BASE_URL=http://127.0.0.1:8765 \
  webetl run configs/books_scrape.yaml                   # HTML -> SQLite + Parquet lake
webetl run configs/products_api.yaml                     # incremental: only new/changed rows
webetl history

# medallion lake + alerts (see the sections below)
export SLACK_WEBHOOK_URL=http://127.0.0.1:8765/slack/ok   # the mock server fakes Slack too
webetl run configs/products_medallion.yaml
webetl test-alert configs/products_medallion.yaml
```

To use real sources, run `webetl run configs/books_scrape.yaml` against books.toscrape.com, a public scraping sandbox. For GitHub, run `GITHUB_TOKEN=... webetl run configs/github_repos.yaml`.

### CLI

| Command | What it does |
|---|---|
| `webetl run CONFIG… [--full-refresh] [--dry-run] [--limit N]` | Run pipelines (files or folders). `--dry-run` extracts and transforms, prints a sample, writes nothing. |
| `webetl validate CONFIG…` | Parse configs and compile transform steps without running them. |
| `webetl build-gold CONFIG…` | Rebuild gold tables from silver without extracting anything (e.g. after editing gold SQL). |
| `webetl check CONFIG…` | Run the data quality checks against the lake now and print a pass/fail report. |
| `webetl test-alert CONFIG` | Send a test alert through every configured channel. |
| `--alerts FILE` | Shared alert settings for pipelines that have no `alerts:` block, e.g. `webetl --alerts alerts.example.yaml run configs/`. |
| `webetl history [PIPELINE]` | Recent runs with status, row counts and errors. |
| `webetl reset PIPELINE` | Forget the incremental watermark. |
| `--state PATH`, `-v/-vv` | State DB location (default `.webetl/state.db`), log verbosity. |

The exit code is non-zero if any pipeline fails, so cron, CI and Airflow can detect failures.

### Python API

```python
from webetl import Pipeline
result = Pipeline.from_file("configs/products_api.yaml").run(full_refresh=False)
print(result.status, result.stats)
```

## Pipeline config reference

```yaml
name: my_pipeline              # letters, digits, _ and -
batch_size: 500                # records per transform/load batch
metadata: true                 # add _etl_run_id and _etl_loaded_at columns
max_reject_ratio: 0.05         # optional data-quality gate: fail if >5% rejected
min_records: 1                 # optional: fail if fewer rows were loaded
raw:     {path: data/lake/raw}         # optional raw landing zone (JSONL.gz)
rejects: {path: .webetl/rejects}       # dead-letter output (default shown)
```

Any value can use `${ENV_VAR}` or `${ENV_VAR:-default}`. Keep secrets in environment variables, not in YAML.

### Source: `type: api`

| Key | Notes |
|---|---|
| `url`, `method` (GET/POST), `params`, `headers`, `body` | Request definition. `body` is sent as JSON for POST. |
| `auth` | `{type: bearer, token}` · `{type: basic, username, password}` · `{type: api_key, name, value, in: header\|query}` |
| `records_path` | Dotted path to the list of records, e.g. `data.items`. Leave it out if the body is the list. |
| `pagination.type` | `none` · `page` (`page_param`, `size_param`, `size`, `start`) · `offset` (`offset_param`, `limit_param`, `limit`) · `cursor` (`cursor_param`, `cursor_path`) · `next_url` (`next_url_path`) · `link_header` |
| `pagination.max_pages` | Safety limit on the number of pages. |
| `incremental` | `field` is the record field used as the watermark. `param` is the request parameter that receives the last watermark (optional). `initial` is the starting value. `client_filter` (default true) drops records older than the watermark. |
| `rate_limit_per_sec`, `retries`, `backoff_factor`, `timeout`, `verify_ssl` | Resilience. Retries cover 429 and 5xx responses and honour `Retry-After`. |

### Source: `type: html`

| Key | Notes |
|---|---|
| `url` / `urls` | One or more start pages. |
| `item_selector` | CSS selector for each record, e.g. `article.product_pod`. |
| `fields` | `name: "css"` (text) or `{selector, attr: text\|html\|<attribute>, all: true, regex, default, absolute}`. Use selector `.` for the item itself. `href`/`src` become absolute URLs. |
| `pagination` | `{next_selector: "li.next a", max_pages: 50}` |
| `follow` | `{url_field, fields, root_selector}` visits each record's detail page and merges in more fields. |
| `respect_robots` | Default **true** for HTML. A disallowed URL fails the run. |
| `rate_limit_per_sec` | Default **1 request/sec** for HTML. Set a descriptive `user_agent` with contact details. |

### Transform steps (in order)

`flatten`, `rename`, `select`, `drop`, `strip`, `default`, `map_values`, `regex_extract`, `cast`, `derive` (`"{a}-{b}"` templates), `filter` (`eq ne gt gte lt lte in not_in contains exists not_null`), `require`, `dedupe`. The docstring in `src/webetl/transform.py` documents each step.

`schema:` runs after the steps. It casts each column to `str | int | float | decimal | bool | datetime | date | json` and enforces `required`. With `extra: drop`, undeclared columns are removed. Records that fail are **never silently dropped**: they go to the rejects sink with the original record, the failing step and the reason. Numeric casts handle scraped strings like `£1,234.50`. Datetimes are normalized to UTC, and epoch seconds and milliseconds are accepted.

### Load targets (one or more)

**`type: sql`**: `url` (SQLAlchemy URL), `table`, `db_schema`, `key: [cols]`, `mode`

- `upsert` (default when `key` is set) uses native `INSERT … ON CONFLICT` on SQLite and PostgreSQL, `ON DUPLICATE KEY` on MySQL, and delete-then-insert elsewhere. This makes re-runs idempotent.
- `append` inserts rows.
- `replace` loads into a staging table and swaps it in only if the whole run succeeds.
- The table is created on first load, with types taken from `schema` or inferred from the data. New columns are added automatically with `ALTER TABLE` (additive schema evolution). If you upsert into a table you created yourself, it needs a unique or primary key on `key`.

**`type: lake`**: `path` (local, `s3://`, `gs://`, `abfs://`), `dataset`, `format: parquet|jsonl|csv`, `partition_by: [cols]` (hive-style `col=value/`; the built-in `ingest_date` column is the run date), `compression`, `rows_per_file`, `storage_options` (credentials and endpoint, passed to fsspec).

- Files are named `part-<run_id>-<n>.<ext>`. If the run fails, files already written by that run are deleted.
- Lake loads are **append-only**, so each run adds new files. Deduplicate when reading, using the key and the latest `_etl_loaded_at`, or partition by `ingest_date` and read the latest partition.

## Medallion data lake (bronze → silver → gold)

```
 source ──▶ BRONZE  raw records, exactly as received         <root>/bronze/<pipeline>/ingest_date=…/part-*.jsonl.gz
              │      (replayable: re-transform without hitting the source again)
              ▼ transform + schema (bad rows → rejects)
            SILVER  typed, validated, deduplicated Parquet    <root>/silver/<pipeline>/ingest_date=…/part-*.parquet
              │      append-only history of every load
              ▼ DuckDB SQL models, rebuilt after every successful run
            GOLD    business-ready tables                      <root>/gold/<pipeline>/<model>.parquet
                     + optionally published to a SQL warehouse  gold_<model> tables for BI / dashboards
```

Add a `medallion:` block to any pipeline. It sets up the bronze landing zone and the silver lake target for you:

```yaml
medallion:
  root: data/lake                  # or s3://bucket/lake, gs://…, abfs://…
  storage_options: {}              # fsspec credentials / endpoint_url (e.g. MinIO)
  bronze: true
  silver:
    partition_by: [ingest_date]
    key: [id]                      # the `silver` view keeps only the latest row per key
  gold:
    warehouse: sqlite:///data/warehouse.db   # optional: also write gold_<model> tables here
    table_prefix: gold_
    models:                        # built in order; later models can query earlier ones
      - name: category_summary
        sql: |
          SELECT category, COUNT(*) AS products, ROUND(AVG(price), 2) AS avg_price
          FROM silver GROUP BY category
      - name: daily_loads
        sql: SELECT ingest_date, COUNT(*) AS rows_loaded FROM silver_all GROUP BY 1
```

Gold SQL is DuckDB SQL and can read:

| Name | Contents |
|---|---|
| `silver_all` | every silver row ever loaded: the full history, including the `ingest_date` partition column |
| `silver` | the latest version of each `silver.key` (by `_etl_loaded_at`); same as `silver_all` without a key |
| `<model>` | any gold model defined earlier in the list |

How it behaves:

- **Gold is built only after the load has committed.** If gold SQL fails, the run is marked failed and alerts are sent, but the silver data and watermark are kept. Fix the SQL and run `webetl build-gold CONFIG`.
- **Each gold file is replaced atomically**, so readers never see a half-written table. Gold tables published to the warehouse use the staging-swap `replace` mode.
- Silver files with different columns over time are fine: they are read with `union_by_name`.
- `load:` can be empty (`load: []`) when the lake is the only destination.

Query any layer directly:

```bash
python -c "import duckdb; print(duckdb.sql(\"SELECT * FROM 'data/lake/gold/products_medallion/category_summary.parquet'\"))"
```

Examples: `configs/products_medallion.yaml` (mock API, 4 gold models, warehouse publishing) and `configs/crypto_medallion.yaml` (CoinGecko top 250 coins: top movers, market overview, daily price history).

## Alerts (Slack and email)

```yaml
alerts:
  notify_on: [failure]             # failure | rejects | success
  slack:
    webhook_url: ${SLACK_WEBHOOK_URL:-}
    mention: "<!channel>"          # optional, added to failure alerts
  email:
    smtp_host: ${SMTP_HOST:-}
    smtp_port: 587
    security: starttls             # starttls | ssl | none
    username: ${SMTP_USER:-}
    password: ${SMTP_PASSWORD:-}
    from: etl@example.com
    to: [data-team@example.com]
```

| Trigger | Sent when |
|---|---|
| `failure` | the run failed for any reason: source down or 401, bad config, data-quality gate, load error, gold SQL error |
| `rejects` | the run succeeded but some records were rejected |
| `success` | every successful run (a daily heartbeat) |

Each alert includes the pipeline, status, run ID, duration, record counts, rows per target, gold row counts and the error message.

- A channel whose URL or host is empty is skipped, so `${VAR:-}` lets you turn channels on per environment.
- **Alert delivery never fails the pipeline.** A delivery error is logged and shown in the run result's `alerts` field.
- Put shared settings in one file and pass `--alerts alerts.example.yaml`. This also alerts when a config file can't be loaded at all.
- Use `notify_on`, not `on`: YAML reads a bare `on` as the boolean `true`. A bare `on` is still accepted.

**Slack setup:** create a Slack app, enable *Incoming Webhooks*, add a webhook to your channel and copy the URL into `SLACK_WEBHOOK_URL`.
**Email setup (Gmail example):** turn on 2-step verification, create an *App password*, then set `SMTP_HOST=smtp.gmail.com`, `SMTP_PORT=587`, `SMTP_USER=you@gmail.com` and `SMTP_PASSWORD=<app password>`. Outlook/Office 365 uses `smtp.office365.com:587`, and SendGrid and Mailgun work the same way.

Check your setup with `webetl test-alert configs/products_medallion.yaml`.

## Data quality checks

Add a `checks:` block to any pipeline. Checks run after every load (and in Airflow's `data_quality_checks` task), using DuckDB over the lake:

```yaml
checks:
  - {name: ids_unique, type: unique, table: silver, columns: [id]}
  - {name: names_present, type: not_null, columns: [name, category]}
  - {name: prices_in_range, type: range, column: price, min: 0.01, max: 10000}
  - {name: known_categories, type: accepted_values, column: category, values: [books, toys], severity: warn}
  - {name: run_not_empty, type: row_count, table: run, min: 1}
  - {name: loaded_recently, type: freshness, column: _etl_loaded_at, max_age_hours: 24}
  - name: stock_not_above_total            # custom SQL: returns the number of bad rows, must be 0
    sql: SELECT COUNT(*) FROM category_summary WHERE in_stock > products
```

| Type | Fails when |
|---|---|
| `unique` | any combination of `columns` appears more than once |
| `not_null` | any of `columns` is null |
| `range` | `column` is below `min` or above `max` |
| `accepted_values` | `column` has a value not in `values` |
| `row_count` | the table has fewer than `min` or more than `max` rows |
| `freshness` | the newest `column` value is older than `max_age_hours` |
| `sql` | your query returns anything other than 0 |

- **Tables:** `run` (records loaded by this run), `silver` (latest row per key), `silver_all` (full history), or any gold model. Defaults to `silver` for medallion pipelines, `run` otherwise. Every check also accepts `where:` to test a subset of rows.
- **Severity:** `error` (default) marks the run **failed** and sends a `failure` alert. `warn` keeps the run successful and sends a `warning` alert (add `warning` to `alerts.notify_on`).
- Like dbt tests, checks run **after** the data is committed. They make bad data loud; they don't undo the load. To keep bad rows out in the first place, use `schema:` and `max_reject_ratio`.
- A check that can't run (a typo in a column name, say) counts as failed, so a broken check never passes silently.
- Results are stored with every run: in `webetl history`, in alerts, and in the dashboard's **Pipeline runs** tab.
- During a run, the `run` table holds that run's records in memory. For very large loads, check `silver` instead.

## PostgreSQL

Every SQL target and the gold `warehouse` accept any SQLAlchemy URL. For PostgreSQL install the extra and set `DATABASE_URL`:

```bash
pip install -e ".[postgres]"
export DATABASE_URL=postgresql+psycopg2://webetl:webetl@localhost:5432/warehouse
```

Upserts use native `INSERT … ON CONFLICT`, `replace` swaps a staging table in one transaction, and new columns are added with `ALTER TABLE`. The test suite runs the medallion pipeline against a real PostgreSQL when `WEBETL_TEST_POSTGRES=<url>` is set. The dashboard reads `DATABASE_URL` too.

## Airflow

`airflow/dags/webetl_dags.py` turns every YAML file in `configs/` into a DAG named `webetl_<name>`:

- The schedule comes from the config's `schedule:` key, a cron string such as `"*/30 * * * *"`. Configs without one become manual-only DAGs you can trigger from the UI.
- Tasks: `validate_config` → `extract_transform_load` → `data_quality_checks` (the last one only if the config has checks).
- webetl runs in its own virtualenv inside the Airflow image, so its dependencies never clash with Airflow's. Tasks call the CLI, which exits non-zero on failure, so failed runs turn red in Airflow.
- `catchup=False` and `max_active_runs=1`: no backfilling of missed intervals, and a pipeline never runs twice at the same time. Task retries are off because webetl already retries HTTP calls (and a retry would send a second alert).
- Add a pipeline by dropping a YAML file into `configs/`. Airflow picks it up within a minute or so.

## Dashboard

`dashboard.py` is a Streamlit app over the warehouse and run history. In Docker it runs at http://localhost:8501; locally run `pip install -e ".[dashboard]"` then `python -m streamlit run dashboard.py`.
Tabs: **Products** (KPIs and charts from the gold tables), **Crypto** (appears once `crypto_medallion` has run), **Pipeline runs** (status, rows, rejected, checks passed, errors, and the latest quality report) and **Explore tables** (browse and download any table as CSV).

## Reliability built in

- **Idempotent and incremental.** The watermark is committed *only after* every loader has closed successfully. Combined with upserts, a failed or repeated run never creates duplicates or skips data.
- **All-or-nothing outputs.** `replace` uses a staging swap, lake files are rolled back on failure, and SQL batches are transactional.
- **Raw zone.** The untouched source data is kept, so you can re-transform later without hitting the source again.
- **Observability.** Every run is stored with its stats (requests, pages, records in/out/filtered/rejected, rows per loader, watermark before and after). View them with `webetl history`.

## Scheduling

The CLI is stateless apart from `--state`, so any scheduler works:

```cron
*/30 * * * * cd /opt/webetl && webetl run configs/ >> logs/etl.log 2>&1
```

Airflow is included (see above). For Dagster or Prefect, call `Pipeline.from_file(...).run()` in a task and fail the task when `result.status != "success"`.

## Extending

Add a source or a destination by subclassing `Extractor` (implement `_records()`) or `Loader` (implement `write()`, and optionally `open()` and `close(success)`), then register it in `EXTRACTORS` or `LOADERS`. Add a transform step by adding a factory to `STEPS` in `transform.py`.

## Project layout

```
src/webetl/
  config.py        YAML + ${ENV} interpolation + validation
  http.py          session with retries, backoff, Retry-After, rate limiter, robots.txt
  extractors/      api.py (REST), html.py (scraper), base.py (incremental/watermarks)
  transform.py     step chain, schema validation, rejects
  types.py         type coercion
  loaders/         sql.py (SQLAlchemy upsert/append/replace), lake.py (Parquet/JSONL/CSV via fsspec)
  pipeline.py      orchestration, data-quality gates, state commit
  medallion.py     bronze/silver/gold layout, DuckDB gold models, warehouse publishing
  alerts.py        Slack webhook + SMTP email alerts
  quality.py       data quality checks (DuckDB)
  state.py         watermarks + run history (SQLite)
  cli.py           webetl command
configs/           example pipelines (mock API, books.toscrape.com, GitHub, medallion, CoinGecko)
alerts.example.yaml  shared alert settings for --alerts
airflow/dags/      webetl_dags.py: one Airflow DAG per config
docker/            Airflow image, Postgres init script
docker-compose.yml Postgres + Airflow + dashboard + mock API
dashboard.py       Streamlit dashboard (requirements.txt = its packages, for Streamlit Cloud)
DEPLOY_DASHBOARD.md  publish the dashboard on a real web address
.env.example       template for your secrets (copy to .env)
examples/          mock_server.py: a local API, website and fake Slack webhook for demos and tests
tests/             57 tests, end-to-end against the mock server (fake Slack + SMTP, optional real PostgreSQL)
```

## Responsible scraping

Check a site's terms of service and robots.txt before scraping it. Use the API when one exists. Keep the default rate limits, identify yourself in `user_agent`, and don't collect personal data you don't need.
