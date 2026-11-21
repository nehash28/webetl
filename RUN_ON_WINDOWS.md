# Running the full stack on Windows

This runs Airflow, PostgreSQL, the dashboard and the demo API together using Docker Desktop.

## 1. Install Docker Desktop (one time)

1. Download **Docker Desktop for Windows** from docker.com and install it. Keep the **WSL 2** option ticked.
2. Restart your computer if the installer asks.
3. Open Docker Desktop and wait until the bottom-left corner says **Engine running**.
4. Recommended: in Docker Desktop, **Settings → Resources**, give Docker at least **4 GB of memory**. Airflow needs it.

Check it works. In PowerShell:

```powershell
docker --version
docker compose version
```

## 2. Add your email settings

```powershell
cd ~\Downloads\webetl
copy .env.example .env
notepad .env
```

Fill in your Gmail details, then save and close Notepad:

```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=your16letterapppassword
ALERT_EMAIL_FROM=you@gmail.com
ALERT_EMAIL_TO=you@gmail.com
```

The `.env` file holds your password. Don't share it or upload it to GitHub. It's already listed in `.gitignore`.

## 3. Start everything

```powershell
docker compose up -d --build
```

The first time takes 5–10 minutes, because it downloads Airflow and PostgreSQL. After that it starts in seconds.

Check that all four services are running:

```powershell
docker compose ps
```

## 4. Open it

| What | Open in your browser |
|---|---|
| Airflow | http://localhost:8080 |
| Dashboard | http://localhost:8501 |

Airflow takes about a minute to be ready after starting. If the page doesn't load, wait and refresh.

In Airflow you'll see one DAG per pipeline. `webetl_products_medallion` and `webetl_crypto_markets` run every 30 minutes by themselves. To run one now, click it and press the **▶ Trigger** button. Then open the run to see the three tasks turn green: `validate_config → extract_transform_load → data_quality_checks`. Click a task and open **Logs** to see what it did.

## 5. Everyday commands

| Task | Command |
|---|---|
| Stop everything (data is kept) | `docker compose down` |
| Start again | `docker compose up -d` |
| See Airflow's logs | `docker compose logs -f airflow` |
| Rebuild after changing code | `docker compose up -d --build` |
| Run a webetl command inside Airflow | `docker compose exec airflow /opt/webetl-venv/bin/webetl --state /data/state/state.db history` |
| Delete all data and start fresh | `docker compose down -v` |

## 6. Connect other tools to the warehouse

PostgreSQL is available at `localhost:5432`. The database is `warehouse`, the user `webetl` and the password `webetl`. You can connect DBeaver, pgAdmin or **Power BI** (Get data → PostgreSQL database) and build reports on the `gold_*` tables.

## Troubleshooting

- **"port is already allocated":** something else is using 8080, 8501, 5432 or 8765. Close it, or change the left-hand port number in `docker-compose.yml` (for example `"8081:8080"`).
- **Airflow page keeps loading:** run `docker compose logs airflow` and look for errors. Usually it just needs another minute.
- **No emails:** check `.env`, then run `docker compose up -d` again so Airflow picks up the changes. Send a test with:
  `docker compose exec airflow /opt/webetl-venv/bin/webetl test-alert /opt/webetl/configs/products_medallion.yaml`
- **The computer gets slow:** stop the stack with `docker compose down` when you're not using it.
