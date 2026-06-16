"""Diagnostics tab — engineering views.

Bronze parquet inventory now joins to `dim_channel` so you see channel names
alongside the UCxxx ids — the per-file table used to show only the partition
folder name (channel_id), which is unreadable at a glance.
"""

from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from lib.charts import section
from lib.db import WAREHOUSE, has_table, list_tables, q, warehouse_exists


def render() -> None:
    with section("Video-id overlap matrix", icon="🧮"):
        st.markdown(
            "Each cell = `video_id`s present in BOTH snapshot dates. "
            "Bronze pulls *latest 10 videos per channel*, so high-frequency "
            "posters turn over their latest-10 within a week. The ASOF JOIN "
            "for 7-day growth can only fire on videos in BOTH endpoints."
        )
        if not has_table("main_silver", "stg_video_stats"):
            st.info("Silver not built.")
        else:
            overlap = q(
                """
                WITH dates AS (
                    SELECT DISTINCT fetched_date FROM main_silver.stg_video_stats
                ),
                pairs AS (
                    SELECT a.fetched_date AS date_a, b.fetched_date AS date_b
                    FROM dates a CROSS JOIN dates b
                    WHERE a.fetched_date <= b.fetched_date
                )
                SELECT
                    p.date_a, p.date_b,
                    (SELECT count(DISTINCT s_a.video_id)
                       FROM main_silver.stg_video_stats s_a
                       JOIN main_silver.stg_video_stats s_b USING (video_id)
                      WHERE s_a.fetched_date = p.date_a
                        AND s_b.fetched_date = p.date_b
                    ) AS shared
                FROM pairs p
                ORDER BY p.date_a, p.date_b
                """
            )
            heat = (
                alt.Chart(overlap)
                .mark_rect()
                .encode(
                    x=alt.X("date_b:O", title="Date B (later)"),
                    y=alt.Y("date_a:O", title="Date A (earlier)"),
                    color=alt.Color("shared:Q", title="Shared video_ids",
                                    scale=alt.Scale(scheme="blues")),
                    tooltip=["date_a", "date_b", "shared"],
                )
                .properties(height=320)
            )
            text = (
                alt.Chart(overlap)
                .mark_text(fontSize=11)
                .encode(
                    x="date_b:O", y="date_a:O", text="shared:Q",
                    color=alt.condition(
                        "datum.shared > 8", alt.value("white"), alt.value("#222"),
                    ),
                )
            )
            st.altair_chart(heat + text, use_container_width=True)

    with section("Warehouse tables", icon="🗂️"):
        if not warehouse_exists():
            st.error("No warehouse.")
        else:
            st.code(str(WAREHOUSE), language="text")
            rows = []
            for schema, name in list_tables():
                try:
                    n = q(f"SELECT count(*)::bigint AS n FROM {schema}.{name}").iloc[0]["n"]
                except Exception:
                    n = None
                rows.append({"schema": schema, "table": name,
                             "rows": int(n) if n is not None else None})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with section("Bronze parquet inventory", icon="🪨"):
        bronze_root = Path(__file__).resolve().parents[2] / "data" / "raw" / "video_stats"
        if not bronze_root.exists():
            st.info("No bronze yet.")
            return

        files = sorted(bronze_root.glob("date=*/**/*.parquet"))
        rows = []
        for f in files:
            try:
                d = f.relative_to(bronze_root).parts[0].replace("date=", "")
                channel_id = next(
                    (p.replace("channel_id=", "")
                     for p in f.relative_to(bronze_root).parts
                     if p.startswith("channel_id=")),
                    "(no-channel-partition)",
                )
            except Exception:
                d, channel_id = "?", "?"
            rows.append({"date": d, "channel_id": channel_id,
                         "size_kb": round(f.stat().st_size / 1024, 1)})
        df = pd.DataFrame(rows)

        # Join channel name from dim_channel so the per-file table is readable
        # — UCxxxxx alone is opaque. Falls back to the raw id if dim isn't built.
        if has_table("main_silver", "dim_channel") and not df.empty:
            try:
                names = q(
                    "SELECT channel_id, name FROM main_silver.dim_channel"
                )
                df = df.merge(names, on="channel_id", how="left")
                df["channel"] = df.apply(
                    lambda r: f"{r['name']} ({r['channel_id']})"
                    if pd.notna(r.get("name")) else r["channel_id"],
                    axis=1,
                )
                df = df[["date", "channel", "size_kb"]]
            except Exception:
                pass

        st.write(f"**{len(df)} parquet file(s)** under `{bronze_root}`")
        if not df.empty:
            by_date = df.groupby("date").agg(
                files=(df.columns[1], "count"),
                total_kb=("size_kb", "sum"),
            ).reset_index()
            st.dataframe(by_date, use_container_width=True, hide_index=True)
            with st.expander("Per-file detail"):
                st.dataframe(df, use_container_width=True, hide_index=True)
