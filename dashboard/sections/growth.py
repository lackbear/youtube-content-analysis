"""Growth tab — analytical view over `fct_video_growth_7d`.

Composed into the Analytics page. Uses the date / channel / niche / tier
filters from the Analytics-level sidebar.
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from lib.charts import kpi_row, perf_scatter, section
from lib.db import has_table, q
from lib.filters import Filters
from lib.theme import channel_color_scale


def _safe_int(v) -> int:
    """Coalesce DuckDB's pd.NA / None to 0 without tripping the `or`-on-NA
    TypeError that bit us before. (`pd.NA or 0` raises because NA's __bool__
    is undefined.)"""
    return int(v) if pd.notna(v) else 0


def render(filters: Filters) -> None:
    if not has_table("main_gold", "fct_video_growth_7d"):
        st.info("`fct_video_growth_7d` not built yet.")
        return

    date_sql,  date_p  = filters.date_clause("g.snapshot_date")
    chan_sql,  chan_p  = filters.channel_clause("g.channel_id")
    niche_sql, niche_p = filters.niche_clause("d.niche")
    tier_sql,  tier_p  = filters.tier_clause("d.tier")
    where = f"{date_sql} AND {chan_sql} AND {niche_sql} AND {tier_sql}"
    params = date_p + chan_p + niche_p + tier_p

    base_sql = f"""
        FROM main_gold.fct_video_growth_7d g
        LEFT JOIN main_silver.dim_channel d USING (channel_id)
        WHERE {where}
    """

    with section("Window summary", icon="📊"):
        s = q(
            f"""
            SELECT
                count(*)::int                              AS rows,
                count(DISTINCT g.video_id)::int            AS videos,
                count(DISTINCT g.channel_id)::int          AS channels,
                sum(g.views_added_window)::bigint          AS views_added,
                sum(g.likes_added_window)::bigint          AS likes_added,
                sum(g.comments_added_window)::bigint       AS comments_added
            {base_sql}
            """,
            params=tuple(params),
        ).iloc[0]
        kpi_row([
            {"label": "Rows",            "value": _safe_int(s["rows"])},
            {"label": "Distinct videos", "value": _safe_int(s["videos"])},
            {"label": "Channels",        "value": _safe_int(s["channels"])},
            {"label": "Views added",     "value": f"{_safe_int(s['views_added']):,}"},
            {"label": "Likes added",     "value": f"{_safe_int(s['likes_added']):,}"},
        ])

    with section("Performance · 7-day window vs baseline", icon="🎯"):
        df = q(
            f"""
            SELECT
                g.video_id,
                g.channel_id,
                g.channel_name,
                substr(g.title, 1, 60) AS title,
                g.snapshot_date,
                g.days_since_baseline,
                g.views_baseline,
                g.views_added_window,
                g.likes_added_window,
                g.comments_added_window
            {base_sql}
            ORDER BY g.views_added_window DESC NULLS LAST
            LIMIT 5000
            """,
            params=tuple(params),
        )
        if df.empty:
            st.info("No rows for this filter window.")
        else:
            chart = perf_scatter(
                df,
                x="views_baseline",
                y="views_added_window",
                size="likes_added_window",
                color="channel_name",
                color_domain=df["channel_name"].dropna().unique().tolist(),
                x_title="Views at baseline (start of window)",
                y_title="Views added in 7-day window",
                tooltip=[
                    "channel_name", "title", "snapshot_date", "days_since_baseline",
                    "views_baseline", "views_added_window", "likes_added_window",
                ],
                height=420,
            )
            st.altair_chart(chart, use_container_width=True)
            st.caption(
                "Each dot is one (video, snapshot) pair. The dashed diagonal "
                "is y = x — videos *above* the line added more views in the "
                "window than they had at baseline. Bubble size = likes added."
            )

    with section("Per-channel · views added over time", icon="📈"):
        df = q(
            f"""
            SELECT
                g.snapshot_date,
                g.channel_name,
                sum(g.views_added_window)::bigint AS views_added
            {base_sql}
            GROUP BY 1, 2
            ORDER BY 1
            """,
            params=tuple(params),
        )
        if df.empty:
            st.info("No data.")
        else:
            scale = channel_color_scale(df["channel_name"].unique().tolist())
            line = (
                alt.Chart(df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("snapshot_date:T", title="Snapshot date"),
                    y=alt.Y("views_added:Q", title="Views added (7-day window)"),
                    color=alt.Color("channel_name:N", scale=scale, title="Channel"),
                    tooltip=["snapshot_date", "channel_name", "views_added"],
                )
                .properties(height=320)
            )
            st.altair_chart(line, use_container_width=True)

    with section("Top 25 videos by views added", icon="🏆"):
        top = q(
            f"""
            SELECT
                g.channel_name,
                g.title,
                g.snapshot_date,
                g.days_since_baseline,
                g.views_baseline,
                g.views_added_window,
                g.likes_added_window,
                g.comments_added_window
            {base_sql}
            ORDER BY g.views_added_window DESC NULLS LAST
            LIMIT 25
            """,
            params=tuple(params),
        )
        st.dataframe(top, use_container_width=True, hide_index=True)
