"""
Overview content (the landing page).

This file is registered as a page by `dashboard/app.py` via `st.navigation` —
do NOT call `st.set_page_config` here; the router already did. We still call
`apply_theme()` so charts get the YT palette.
"""

from __future__ import annotations

import streamlit as st

from lib.charts import freshness_badge, kpi_row, section, time_series_with_rolling
from lib.db import has_table, q, warehouse_exists
from lib.theme import apply_theme


apply_theme()

st.title("📺 YouTube Pipeline · Overview")
st.caption(
    "Live state of the medallion. Read-only barometer over `data/warehouse/dev.duckdb`. "
    "Pick a page from the left sidebar to drill down."
)

if not warehouse_exists():
    st.error(
        "**No warehouse found.** Build it first:\n\n"
        "```\ncd dbt_youtube && DBT_PROFILES_DIR=. dbt run\n```"
    )
    st.stop()


# ── Headline KPIs ────────────────────────────────────────────────────────────

with section("Pipeline freshness", icon="🩺"):
    if has_table("main_silver", "stg_video_stats"):
        fresh = q(
            """
            SELECT
                max(fetched_date)                          AS latest_snapshot,
                (current_date - max(fetched_date))::int    AS days_since,
                count(DISTINCT fetched_date)::int          AS distinct_dates,
                count(DISTINCT channel_id)::int            AS distinct_channels,
                count(*)::bigint                            AS silver_rows
            FROM main_silver.stg_video_stats
            """
        ).iloc[0]

        days = int(fresh["days_since"]) if fresh["days_since"] is not None else None
        kpi_row([
            {"label": "Freshness",        "value": freshness_badge(days),
             "help": "Green ≤ 1 day · Amber ≤ 3 · Red > 3"},
            {"label": "Latest snapshot",  "value": str(fresh["latest_snapshot"])[:10]},
            {"label": "Snapshot dates",   "value": int(fresh["distinct_dates"])},
            {"label": "Channels covered", "value": int(fresh["distinct_channels"])},
            {"label": "Silver rows",      "value": f"{int(fresh['silver_rows']):,}"},
        ])
    else:
        st.warning(
            "Silver layer not built yet — "
            "`dbt run --project-dir dbt_youtube --profiles-dir dbt_youtube`."
        )


# ── Registry KPIs ────────────────────────────────────────────────────────────

with section("Channel registry", icon="📇"):
    if has_table("main_silver", "dim_channel"):
        s = q(
            """
            SELECT
                count(*)::int                            AS total,
                count(*) FILTER (WHERE active)::int      AS active_count,
                count(*) FILTER (WHERE NOT active)::int  AS inactive_count,
                count(DISTINCT niche) FILTER (WHERE active)::int AS niche_count
            FROM main_silver.dim_channel
            """
        ).iloc[0]
        kpi_row([
            {"label": "Total tracked", "value": int(s["total"])},
            {"label": "Active",        "value": int(s["active_count"])},
            {"label": "Inactive",      "value": int(s["inactive_count"])},
            {"label": "Distinct niches (active)", "value": int(s["niche_count"])},
        ])
    else:
        st.info("`dim_channel` not built yet.")


# ── Curator pulse ────────────────────────────────────────────────────────────

with section("Curator pulse", icon="🛒"):
    from pathlib import Path
    import pandas as pd

    curator_dir = Path(__file__).resolve().parent.parent / "data" / "curator"
    stale_csv = curator_dir / "stale_channels.csv"
    cand_csv = curator_dir / "candidates.csv"

    cols = []
    if stale_csv.exists():
        sdf = pd.read_csv(stale_csv)
        n_active = len(sdf)
        n_stale = int(sdf["stale"].fillna(False).astype(bool).sum())
        rate = (n_stale / n_active * 100) if n_active else 0.0
        cols.append({"label": "Active channels", "value": n_active})
        cols.append({"label": "Stale (>14d)",    "value": n_stale,
                     "help": f"{rate:.1f}% of active"})
    if cand_csv.exists():
        cdf = pd.read_csv(cand_csv)
        status = cdf["status"].fillna("pending").value_counts()
        cols.append({"label": "Pending review", "value": int(status.get("pending", 0))})
        cols.append({"label": "Accepted",       "value": int(status.get("accepted", 0))})
        cols.append({"label": "Promoted",       "value": int(status.get("promoted", 0))})
    if cols:
        kpi_row(cols)
        st.caption("→ Open **Curator** in the left sidebar for the full funnel + cohort breakdown.")
    else:
        st.info("Curator outputs not present yet — run `make curator`.")


# ── Snapshot activity ────────────────────────────────────────────────────────

with section("Collector activity", icon="📈"):
    if has_table("main_silver", "stg_video_stats"):
        activity = q(
            """
            SELECT
                fetched_date,
                count(*)::int AS rows
            FROM main_silver.stg_video_stats
            GROUP BY fetched_date
            ORDER BY fetched_date
            """
        )
        chart = time_series_with_rolling(
            activity, x="fetched_date", y="rows",
            x_title="Snapshot date", y_title="Rows captured",
            window=7,
        )
        st.altair_chart(chart, use_container_width=True)
        st.caption(
            "Bars: silver rows per snapshot date. Dashed line: 7-day rolling mean. "
            "Red bars = zero-row days (likely a missed collector run, not a quiet day)."
        )
    else:
        st.info("Silver activity will appear here once `stg_video_stats` is built.")


st.markdown("---")
st.caption(
    "Built on DuckDB + dbt + Streamlit. "
    "Filter state on each page persists in the URL — copy & paste to share."
)
