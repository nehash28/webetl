"""webetl dashboard: browse the gold tables and pipeline runs in your browser.

    pip install streamlit
    python -m streamlit run dashboard.py

Reads the warehouse (gold_* tables published by the medallion pipelines) and the
run history.

  Warehouse    DATABASE_URL (env var or Streamlit secret), else data/warehouse.db
  Run history  the warehouse's `etl_runs` table if it exists (pipelines write it when
               RUN_HISTORY_URL is set), else the local WEBETL_STATE / .webetl/state.db

On Streamlit Community Cloud, put DATABASE_URL in the app's Secrets.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import altair as alt
import pandas as pd
import sqlalchemy as sa
import streamlit as st

st.set_page_config(page_title="webetl dashboard", page_icon=":bar_chart:", layout="wide")

HERE = Path(__file__).resolve().parent
DB = Path(os.environ.get("WAREHOUSE_DB", HERE / "data" / "warehouse.db"))


def _secret(name: str) -> str | None:
    """Streamlit Cloud secrets; quietly None when no secrets file exists (local runs)."""
    try:
        value = st.secrets.get(name)
    except Exception:
        return None
    return str(value) if value else None


DB_URL = os.environ.get("DATABASE_URL") or _secret("DATABASE_URL") or f"sqlite:///{DB}"
for _prefix in ("postgresql://", "postgres://"):  # hosted Postgres URLs -> psycopg2 driver
    if DB_URL.startswith(_prefix):
        DB_URL = "postgresql+psycopg2://" + DB_URL[len(_prefix):]
PUBLIC = (os.environ.get("DASHBOARD_PUBLIC") or _secret("DASHBOARD_PUBLIC") or "").lower() in ("1", "true", "yes")
STATE = Path(os.environ.get("WEBETL_STATE", HERE / ".webetl" / "state.db"))

# Validated categorical palette (slot order is fixed; never cycled).
BLUE, ORANGE = "#2a78d6", "#eb6834"
GRID = "#e4e3df"



# ---------------------------------------------------------------- data access
@st.cache_resource
def engine(url: str) -> sa.Engine:
    return sa.create_engine(url)


def warehouse_exists() -> bool:
    if DB_URL.startswith("sqlite:///"):
        return Path(DB_URL[len("sqlite:///"):]).exists()
    try:
        with engine(DB_URL).connect():
            return True
    except Exception as exc:
        st.error(f"Cannot connect to the warehouse: {exc}")
        return False


@st.cache_data(ttl=30)
def tables(url: str) -> list[str]:
    return sorted(sa.inspect(engine(url)).get_table_names())


@st.cache_data(ttl=30)
def read(url: str, sql: str) -> pd.DataFrame:
    with engine(url).connect() as con:
        return pd.read_sql_query(sa.text(sql), con)


def has(name: str) -> bool:
    return name in tables(DB_URL)


def gold(name: str) -> pd.DataFrame:
    return read(DB_URL, f'SELECT * FROM "{name}"')


def _ts(col: pd.Series) -> pd.Series:
    """Timestamps from SQLite (ISO strings) or PostgreSQL (datetimes) -> UTC datetimes."""
    if pd.api.types.is_datetime64_any_dtype(col):
        return col.dt.tz_convert("UTC") if col.dt.tz is not None else col.dt.tz_localize("UTC")
    return pd.to_datetime(col, utc=True, format="ISO8601")


def style(chart: alt.Chart, height: int = 280) -> alt.Chart:
    """Recessive axes and grid, no chart border."""
    return (chart.properties(height=height)
            .configure_view(strokeWidth=0)
            .configure_axis(gridColor=GRID, domain=False, tickColor=GRID, labelFontSize=12, titleFontSize=12)
            .configure_legend(orient="top", title=None, labelFontSize=12))


def bar(df: pd.DataFrame, x: str, y: str, *, horizontal: bool = False, sort=None, size: int = 22,
        x_title: str | None = None, y_title: str | None = None, tooltip=None) -> alt.Chart:
    enc_cat = alt.X if not horizontal else alt.Y
    enc_val = alt.Y if not horizontal else alt.X
    return alt.Chart(df).mark_bar(color=BLUE, cornerRadiusEnd=4, size=size).encode(
        enc_cat(f"{x}:N", sort=sort, title=x_title or "", axis=alt.Axis(labelAngle=0)),
        enc_val(f"{y}:Q", title=y_title or y),
        tooltip=tooltip or [x, y],
    )


# ---------------------------------------------------------------- header
st.title("webetl dashboard")
if PUBLIC:
    st.caption("Live data from the webetl pipeline: API → bronze → silver → gold, scheduled by Airflow, "
               "with data quality checks. Source code on GitHub.")
else:
    st.caption(f"Warehouse: `{sa.make_url(DB_URL).render_as_string(hide_password=True)}`")
if st.button("Refresh data"):
    st.cache_data.clear()

if not warehouse_exists():
    st.warning("No warehouse found yet. Run a pipeline first, for example:\n\n"
               "`python -m webetl.cli run configs\\products_medallion.yaml`")
    st.stop()

sections = []
if has("gold_category_summary"):
    sections.append("Products")
if has("gold_market_overview") or has("gold_top_movers_24h"):
    sections.append("Crypto")
sections += ["Pipeline runs", "Explore tables"]
tabs = dict(zip(sections, st.tabs(sections)))

# ---------------------------------------------------------------- products
if "Products" in tabs:
    with tabs["Products"]:
        cats = gold("gold_category_summary")
        total = int(cats["products"].sum())
        in_stock = int(cats["in_stock"].sum())
        avg_price = (cats["avg_price"] * cats["products"]).sum() / total if total else 0
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Products", f"{total:,}")
        k2.metric("In stock", f"{in_stock:,} ({in_stock / total:.0%})" if total else "0")
        k3.metric("Out of stock", f"{total - in_stock:,}")
        k4.metric("Average price", f"{avg_price:,.2f}")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Stock by category")
            stock = cats.assign(out_of_stock=cats["products"] - cats["in_stock"]).melt(
                id_vars="category", value_vars=["in_stock", "out_of_stock"], var_name="status", value_name="count")
            stock["status"] = stock["status"].map({"in_stock": "In stock", "out_of_stock": "Out of stock"})
            chart = alt.Chart(stock).mark_bar(cornerRadiusEnd=4, size=22, stroke="white", strokeWidth=2).encode(
                y=alt.Y("category:N", title="", sort="-x"),
                x=alt.X("sum(count):Q", title="Products"),
                color=alt.Color("status:N", scale=alt.Scale(domain=["In stock", "Out of stock"],
                                                            range=[BLUE, ORANGE])),
                order=alt.Order("status:N"),
                tooltip=["category", "status", "count"],
            )
            st.altair_chart(style(chart, 240), width="stretch")
        with c2:
            st.subheader("Average price by category")
            chart = bar(cats, "category", "avg_price", horizontal=True, sort="-x", y_title="Average price",
                        tooltip=["category", "avg_price", "min_price", "max_price"])
            st.altair_chart(style(chart, 240), width="stretch")

        c3, c4 = st.columns(2)
        if has("gold_price_bands"):
            with c3:
                st.subheader("Products by price band")
                bands = gold("gold_price_bands")
                bands["band"] = bands["price_band"].str.replace(r"^\d+\.\s*", "", regex=True)
                chart = bar(bands, "band", "products", sort=list(bands["band"]), y_title="Products", size=48,
                            tooltip=["band", "products"])
                st.altair_chart(style(chart, 240), width="stretch")
        if has("gold_out_of_stock"):
            with c4:
                st.subheader("Out-of-stock products")
                st.dataframe(gold("gold_out_of_stock"), hide_index=True, width="stretch", height=240)

        with st.expander("Category summary table"):
            st.dataframe(cats, hide_index=True, width="stretch")

# ---------------------------------------------------------------- crypto
if "Crypto" in tabs:
    with tabs["Crypto"]:
        if has("gold_market_overview"):
            ov = gold("gold_market_overview").iloc[0]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total market cap", f"${ov['total_market_cap_usd'] / 1e12:,.2f}T")
            k2.metric("24h volume", f"${ov['total_volume_usd'] / 1e9:,.1f}B")
            k3.metric("Average 24h change", f"{ov['avg_change_24h_pct']:+.2f}%")
            k4.metric("Gainers / losers", f"{int(ov['gainers'])} / {int(ov['losers'])}")
        if has("gold_top_movers_24h"):
            st.subheader("Top movers, last 24h (top 100 coins)")
            mv = gold("gold_top_movers_24h")
            mv["direction"] = mv["change_24h_pct"].apply(lambda v: "Up" if v >= 0 else "Down")
            chart = alt.Chart(mv).mark_bar(cornerRadiusEnd=4, size=14).encode(
                y=alt.Y("name:N", sort=alt.EncodingSortField("change_24h_pct", order="descending"), title=""),
                x=alt.X("change_24h_pct:Q", title="24h change (%)"),
                color=alt.Color("direction:N", scale=alt.Scale(domain=["Up", "Down"], range=[BLUE, ORANGE])),
                tooltip=["name", "symbol", "price_usd", "change_24h_pct", "market_cap_rank"],
            )
            st.altair_chart(style(chart, 420), width="stretch")
        if has("gold_daily_price_history"):
            st.subheader("Daily price history")
            hist = gold("gold_daily_price_history")
            coin = st.selectbox("Coin", sorted(hist["id"].unique()),
                                index=sorted(hist["id"].unique()).index("bitcoin")
                                if "bitcoin" in set(hist["id"]) else 0)
            one = hist[hist["id"] == coin]
            if len(one) < 2:
                st.info("Only one day of data so far. Schedule the pipeline and this chart fills in over time.")
            chart = alt.Chart(one).mark_line(color=BLUE, strokeWidth=2, point=alt.OverlayMarkDef(size=60)).encode(
                x=alt.X("day:T", title=""), y=alt.Y("close:Q", title="Close (USD)", scale=alt.Scale(zero=False)),
                tooltip=["day", "open", "high", "low", "close"])
            st.altair_chart(style(chart, 280), width="stretch")

# ---------------------------------------------------------------- runs
with tabs["Pipeline runs"]:
    RUNS_SQL = ("SELECT run_id, pipeline, started_at, finished_at, status, stats, error "
                "FROM {table} ORDER BY started_at DESC")
    if has("etl_runs"):
        runs = read(DB_URL, RUNS_SQL.format(table="etl_runs"))
    elif STATE.exists():
        runs = read(f"sqlite:///{STATE}", RUNS_SQL.format(table="runs"))
    else:
        runs = None
    if runs is None or runs.empty:
        st.info("No run history yet.")
    else:
        stats = runs["stats"].apply(lambda s: json.loads(s or "{}"))
        runs["loaded"] = stats.apply(lambda s: (s.get("transform") or {}).get("out"))
        runs["rejected"] = stats.apply(lambda s: (s.get("transform") or {}).get("rejected"))
        runs["quality"] = stats.apply(lambda s: s.get("quality") or {})
        runs["checks"] = runs["quality"].apply(
            lambda q: f"{q.get('passed', 0)}/{q.get('passed', 0) + q.get('failed', 0)} passed" if q else "")
        runs["started"] = _ts(runs["started_at"])
        runs["duration_s"] = (_ts(runs["finished_at"]) - runs["started"]).dt.total_seconds().round(1)
        runs["result"] = runs["status"].map({"success": "✅ success", "failed": "❌ failed"}).fillna(runs["status"])

        pipes = ["All pipelines"] + sorted(runs["pipeline"].unique())
        pick = st.selectbox("Pipeline", pipes)
        view = runs if pick == "All pipelines" else runs[runs["pipeline"] == pick]

        ok = int((view["status"] == "success").sum())
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Runs", len(view))
        k2.metric("Success rate", f"{ok / len(view):.0%}" if len(view) else "n/a")
        k3.metric("Last run", view["result"].iloc[0] if len(view) else "n/a")
        k4.metric("Rows loaded (last success)",
                  f"{int(view.loc[view['status'] == 'success', 'loaded'].iloc[0]):,}" if ok else "n/a")

        st.subheader("Recent runs")
        st.dataframe(
            view[["started", "pipeline", "result", "loaded", "rejected", "checks", "duration_s", "error"]],
            hide_index=True, width="stretch",
            column_config={"started": st.column_config.DatetimeColumn("Started (UTC)", format="YYYY-MM-DD HH:mm"),
                           "duration_s": st.column_config.NumberColumn("Duration (s)"),
                           "error": st.column_config.TextColumn("Error", width="large")})

        with_checks = view[view["quality"].apply(bool)]
        if len(with_checks):
            latest = with_checks.iloc[0]
            st.subheader("Data quality, latest checked run")
            st.caption(f"{latest['pipeline']} at {latest['started']:%Y-%m-%d %H:%M} UTC")
            marks = {"pass": "✅ pass", "fail": "❌ fail", "error": "❌ error", "skip": "⏭ skipped"}
            checks = pd.DataFrame(latest["quality"]["results"])
            checks["result"] = checks["status"].map(marks)
            st.dataframe(checks[["result", "name", "severity", "table", "detail"]], hide_index=True, width="stretch",
                         column_config={"detail": st.column_config.TextColumn("Detail", width="large")})

# ---------------------------------------------------------------- explore
with tabs["Explore tables"]:
    names = [t for t in tables(DB_URL) if not (PUBLIC and t == "etl_runs")]
    name = st.selectbox("Table", names)
    if name:
        df = read(DB_URL, f'SELECT * FROM "{name}"')
        st.caption(f"{len(df):,} rows, {len(df.columns)} columns")
        st.dataframe(df, hide_index=True, width="stretch")
        st.download_button("Download as CSV", df.to_csv(index=False).encode("utf-8"),
                           file_name=f"{name}.csv", mime="text/csv")
