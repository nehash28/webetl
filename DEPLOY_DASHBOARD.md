# Put the dashboard on a real web address (free)

The result is a public link like **https://webetl-yourname.streamlit.app** that anyone can open.

```
 your laptop (Docker)                     cloud (free)
 ┌──────────────────────────┐            ┌──────────────────────────┐
 │ Airflow runs pipelines   │ ─ writes ─▶│ Neon PostgreSQL          │
 │ every 30 min             │            │ gold_* tables + etl_runs │
 └──────────────────────────┘            └────────────┬─────────────┘
                                                      │ reads (read-only login)
                                         ┌────────────▼─────────────┐
                              anyone ──▶ │ Streamlit Community Cloud│
                                         │ your public dashboard    │
                                         └──────────────────────────┘
```

Airflow and the full-access database password stay private on your laptop. The public dashboard only gets a **read-only** login.

There are 5 parts. It takes about 30–45 minutes the first time.

---

## Part 1: Create a free cloud database (Neon)

1. Go to **neon.tech** and click **Sign up**. Signing in with GitHub is easiest.
2. Create a project:
   - **Project name:** `webetl`
   - **Region:** pick the one closest to you (for Dubai, a Europe region such as Frankfurt is a good choice)
   - Leave the other settings as they are.
3. On the project dashboard, click **Connect**. Copy the **connection string**. It looks like:
   ```
   postgresql://neondb_owner:AbC123xyz@ep-cool-name-123456.eu-central-1.aws.neon.tech/neondb?sslmode=require
   ```
   This is your **owner** (full-access) connection string. Keep it private: it only goes into your `.env` file.

## Part 2: Send your pipeline data to Neon

1. In PowerShell:
   ```powershell
   cd ~\Downloads\webetl-v2\webetl
   notepad .env
   ```
2. At the bottom, paste your Neon connection string after `WAREHOUSE_URL=`, with no quotes and no spaces:
   ```
   WAREHOUSE_URL=postgresql://neondb_owner:AbC123xyz@ep-cool-name-123456.eu-central-1.aws.neon.tech/neondb?sslmode=require
   ```
   Save with **Ctrl + S** and close Notepad.
3. Restart the stack so it picks up the change (and the new code):
   ```powershell
   docker compose up -d --build
   ```
4. Open Airflow at **http://localhost:8080** and **▶ Trigger** `webetl_products_medallion`. Wait for all three tasks to turn green. `webetl_crypto_markets` will also run by itself within 30 minutes; trigger it too if you like.
5. Check it worked: in Neon, open **Tables** in the left menu. You should see `products`, `gold_category_summary`, `etl_runs` and the other tables.

## Part 3: Create a read-only login for the public dashboard

The public dashboard should only be able to **read** data, never change or delete it.

1. In Neon, open the **SQL Editor** in the left menu.
2. Paste the following. **Replace `CHOOSE_A_LONG_PASSWORD`** with a long password: 20 or more letters and numbers, **no symbols** (symbols cause problems in web addresses).
   ```sql
   CREATE ROLE dashboard_reader WITH LOGIN PASSWORD 'CHOOSE_A_LONG_PASSWORD';
   GRANT USAGE ON SCHEMA public TO dashboard_reader;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_reader;
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dashboard_reader;
   ```
   Click **Run**.
   - If Neon rejects the password as too weak, make it longer.
   - The last line makes sure the reader can also read tables that pipelines create later.
3. Build the **reader connection string**: take your owner string from Part 1 and replace only the username and password:
   ```
   postgresql://dashboard_reader:CHOOSE_A_LONG_PASSWORD@ep-cool-name-123456.eu-central-1.aws.neon.tech/neondb?sslmode=require
   ```
   Keep this handy for Part 5.

## Part 4: Put the project on GitHub

Streamlit Cloud deploys from a GitHub repository.

1. Check Git is installed:
   ```powershell
   git --version
   ```
   If it says "not recognized", install **Git for Windows** from git-scm.com (the default options are fine), then open a new PowerShell window.
2. Tell Git who you are (one time, use your own name and GitHub email):
   ```powershell
   git config --global user.name "Your Name"
   git config --global user.email "you@example.com"
   ```
3. On **github.com**, click **+** (top right) → **New repository**:
   - **Name:** `webetl`
   - **Public**
   - Do **not** add a README, .gitignore or license (the project already has them).
   - Click **Create repository**.
4. In PowerShell, inside the project folder:
   ```powershell
   cd ~\Downloads\webetl-v2\webetl
   git init
   git add .
   git status
   ```
   **⚠️ Check the `git status` list carefully.** `.env` must **not** appear. It holds your passwords, and `.gitignore` keeps it out. If you do see `.env` in the list, stop and ask for help before going further.
5. Commit and upload. Replace `YOUR-GITHUB-USERNAME`:
   ```powershell
   git commit -m "webetl: web ETL pipeline with medallion lake, Airflow, quality checks and dashboard"
   git branch -M main
   git remote add origin https://github.com/YOUR-GITHUB-USERNAME/webetl.git
   git push -u origin main
   ```
   A browser window may open asking you to sign in to GitHub. Approve it. Then refresh your repository page on GitHub to see the files.

## Part 5: Deploy the dashboard on Streamlit Community Cloud

1. Go to **share.streamlit.io** and click **Continue with GitHub**. Allow access.
2. Click **Create app**, then **Deploy a public app from GitHub**.
3. Fill in:
   - **Repository:** `YOUR-GITHUB-USERNAME/webetl`
   - **Branch:** `main`
   - **Main file path:** `dashboard.py`
   - **App URL:** choose your web address, for example `webetl-neha`, which gives you **webetl-neha.streamlit.app**
4. Click **Advanced settings**:
   - **Python version:** 3.12
   - **Secrets:** paste this, using your **reader** string from Part 3 (keep the quotes):
     ```toml
     DATABASE_URL = "postgresql://dashboard_reader:CHOOSE_A_LONG_PASSWORD@ep-cool-name-123456.eu-central-1.aws.neon.tech/neondb?sslmode=require"
     DASHBOARD_PUBLIC = "true"
     ```
   - Click **Save**.
5. Click **Deploy**. The first start takes a few minutes. Then your dashboard is live at your new web address. 🎉

`DASHBOARD_PUBLIC = "true"` hides the database address on the page and shows a short project description instead.

---

## Good to know

- **Fresh data:** the dashboard shows whatever is in Neon. While Docker and Airflow run on your laptop, the data updates every 30 minutes. When your laptop is off, visitors still see the last data loaded.
- **Sleeping app:** free Streamlit apps go to sleep after a while without visitors. The next visitor sees a "wake up" button, and it starts in under a minute.
- **Updating the dashboard:** change the code, then `git add .`, `git commit -m "describe the change"` and `git push`. Streamlit Cloud redeploys automatically.
- **Back to the local database:** clear `WAREHOUSE_URL=` in `.env` and run `docker compose up -d`.

## Keep it safe

| Secret | Where it lives | Never put it in |
|---|---|---|
| Neon **owner** string | `.env` on your laptop | GitHub, Streamlit, screenshots |
| Neon **reader** string | Streamlit Cloud → app → Settings → Secrets | GitHub, screenshots |
| Gmail App password | `.env` on your laptop | anywhere else |

If a secret is ever exposed, reset it: in Neon, **Roles → Reset password** (then update `.env` or the Streamlit secret); for Gmail, delete the App password and create a new one.
