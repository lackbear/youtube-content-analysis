"""Channels tab — the registry, by tier / niche.

Composed into the Analytics page as one of three tabs. Filters come from the
Analytics-level sidebar (`render_sidebar`); we ignore the date filter since
the channel dimension is a static registry, not time-series.
"""

from __future__ import annotations

import altair as alt
import streamlit as st

from lib.charts import bar_horizontal, kpi_row, section
from lib.db import has_table, q
from lib.filters import Filters


def render(filters: Filters) -> None:
    if not has_table("main_silver", "dim_channel"):
        st.info("`dim_channel` not built yet.")
        return

    niche_sql, niche_p = filters.niche_clause()
    tier_sql,  tier_p  = filters.tier_clause()
    chan_sql,  chan_p  = filters.channel_clause()
    where  = f"active AND {niche_sql} AND {tier_sql} AND {chan_sql}"
    where_all = f"{niche_sql} AND {tier_sql} AND {chan_sql}"
    params = niche_p + tier_p + chan_p

    with section("Summary", icon="📊"):
        s = q(
            f"""
            SELECT
                count(*)::int                                     AS total,
                count(*) FILTER (WHERE active)::int               AS active,
                count(*) FILTER (WHERE NOT active)::int           AS inactive,
                count(DISTINCT niche) FILTER (WHERE active)::int  AS niches,
                count(DISTINCT tier)  FILTER (WHERE active)::int  AS tiers
            FROM main_silver.dim_channel
            WHERE {where_all}
            """,
            params=tuple(params),
        ).iloc[0]
        kpi_row([
            {"label": "Total tracked", "value": int(s["total"])},
            {"label": "Active",        "value": int(s["active"])},
            {"label": "Inactive",      "value": int(s["inactive"])},
            {"label": "Niches",        "value": int(s["niches"])},
            {"label": "Tiers",         "value": int(s["tiers"])},
        ])

    col_a, col_b = st.columns(2)
    with col_a:
        with section("Active by tier", icon="🏷️"):
            df = q(
                f"""
                SELECT
                    coalesce(nullif(tier, ''), '(unset)') AS tier,
                    count(*)::int AS n
                FROM main_silver.dim_channel
                WHERE {where}
                GROUP BY 1
                ORDER BY n DESC
                """,
                params=tuple(params),
            )
            st.altair_chart(
                bar_horizontal(df, x="n", y="tier", x_title="Channels", height=240),
                use_container_width=True,
            )
    with col_b:
        with section("Active by niche", icon="🎯"):
            df = q(
                f"""
                SELECT
                    coalesce(nullif(niche, ''), '(unset)') AS niche,
                    count(*)::int AS n
                FROM main_silver.dim_channel
                WHERE {where}
                GROUP BY 1
                ORDER BY n DESC
                """,
                params=tuple(params),
            )
            st.altair_chart(
                bar_horizontal(df, x="n", y="niche", x_title="Channels", height=300),
                use_container_width=True,
            )

    with section("Subscribers at addition · distribution", icon="📐"):
        df = q(
            f"""
            SELECT
                handle, name, tier, niche, subscribers_at_addition
            FROM main_silver.dim_channel
            WHERE {where} AND subscribers_at_addition IS NOT NULL
            """,
            params=tuple(params),
        )
        if df.empty:
            st.info("No channels match the current filters.")
        else:
            hist = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "subscribers_at_addition:Q",
                        bin=alt.Bin(maxbins=30),
                        title="Subscribers at addition",
                    ),
                    y=alt.Y("count():Q", title="Channels"),
                    color=alt.Color("tier:N", title="Tier"),
                    tooltip=[alt.Tooltip("count():Q", title="channels"), "tier:N"],
                )
                .properties(height=240)
            )
            st.altair_chart(hist, use_container_width=True)

    with section("Registry", icon="📇"):
        show_inactive = st.toggle("Include inactive", value=False, key="ch_show_inactive")
        base_where = where if not show_inactive else where_all
        df = q(
            f"""
            SELECT
                handle, name, niche, tier,
                subscribers_at_addition, added_date,
                active, deactivated_date, deactivated_reason
            FROM main_silver.dim_channel
            WHERE {base_where}
            ORDER BY active DESC, tier, name
            """,
            params=tuple(params),
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"{len(df)} rows.")
