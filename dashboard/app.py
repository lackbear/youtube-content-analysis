"""
dashboard/app.py — read-only barometer for the YouTube data pipeline.

Reads data/warehouse/dev.duckdb directly and renders whatever the
medallion currently contains. Designed to grow with the project:
empty-state cards for models that don't exist yet make future progress
visible at a glance — when chapter 5.5 ships dim_channel, the
"coming soon" placeholder flips to a populated card automatically.

Composable Block 4 in the project's layered design:
  Block 1 (collector) -> Block 2 (Airflow) -> Block 3 (dbt) -> Block 4 (this).
Works whether the warehouse was built by `dbt run` invoked manually,
by Airflow on a schedule, or by anything else.

Run:
    venv/Scripts/streamlit run dashboard/app.py

Open: http://localhost:8501
"""

from datetime import date
from pathlib import Path

import altair as alt
import duckdb
import streamlit as st


WAREHOUSE = Path(__file__).resolve().parent.parent / "data" / "warehouse" / "dev.duckdb"


st.set_page_config(
    page_title="YT Pipeline · Live State",
    page_icon="📺",
    layout="wide",
)


# ── Helpers ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=10)
def list_tables():
    """All non-system tables in the warehouse, as (schema, name) tuples."""
    if not WAREHOUSE.exists():
        return []
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        return con.sql(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'main', 'pg_catalog')
            ORDER BY 1, 2
            """
        ).fetchall()


def has_table(schema: str, name: str) -> bool:
    return (schema, name) in list_tables()


@st.cache_data(ttl=10)
def q(sql: str):
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        return con.sql(sql).df()


def coming_soon(label: str, reason: str) -> None:
    """Placeholder card for a model that doesn't exist yet."""
    st.info(f"**{label}** — *coming soon.* {reason}")


# ── Header ───────────────────────────────────────────────────────────────────

st.title("📺 YouTube Pipeline · live state")
st.caption(
    "Read-only barometer. Reads `data/warehouse/dev.duckdb` directly. "
    "Block 4 in the composable stack — agnostic to whether dbt was "
    "triggered by `make run`, by Airflow, or by hand."
)

if not WAREHOUSE.exists():
    st.error(
        f"No warehouse found at `{WAREHOUSE}`.\n\n"
        "Build it:\n```\ncd dbt_youtube && DBT_PROFILES_DIR=. dbt run\n```"
    )
    st.stop()


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.subheader("Warehouse")
    project_root = WAREHOUSE.parents[2]
    st.code(str(WAREHOUSE.relative_to(project_root)), language="text")

    tables = list_tables()
    st.markdown(f"**Tables present:** {len(tables)}")
    for schema, name in tables:
        st.code(f"{schema}.{name}", language="text")

    st.markdown("---")
    st.caption(
        "Cards labelled *coming soon* show models the project plans to "
        "build. They will populate automatically once the table exists."
    )


# ── Pipeline freshness ───────────────────────────────────────────────────────

st.header("Pipeline freshness")

if has_table("main_silver", "stg_video_stats"):
    # Compute the day delta in SQL — DuckDB returns a clean integer.
    # Doing it in pandas requires Timestamp/date juggling that's easy to
    # get wrong (max(fetched_date) comes back as a pd.Timestamp).
    fresh = q(
        """
        SELECT
            max(fetched_date)                  AS latest_snapshot,
            (current_date - max(fetched_date)) AS days_since,
            count(DISTINCT fetched_date)       AS distinct_dates,
            count(DISTINCT channel_id)         AS distinct_channels,
            count(*)                           AS silver_rows
        FROM main_silver.stg_video_stats
        """
    ).iloc[0]

    days_old = int(fresh["days_since"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest snapshot", str(fresh["latest_snapshot"])[:10])
    c2.metric("Days since", days_old)
    c3.metric("Snapshot dates", int(fresh["distinct_dates"]))
    c4.metric("Channels covered", int(fresh["distinct_channels"]))
else:
    st.warning(
        "Silver layer not built yet — run "
        "`dbt run --project-dir dbt_youtube --profiles-dir dbt_youtube`."
    )


# ── Channels — dim_channel ───────────────────────────────────────────────────

st.header("Channels · dim_channel")

if has_table("main_silver", "dim_channel"):
    summary = q(
        """
        SELECT
            count(*)                            AS total,
            count(*) FILTER (WHERE active)      AS active_count,
            count(*) FILTER (WHERE NOT active)  AS inactive_count
        FROM main_silver.dim_channel
        """
    ).iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total tracked", int(summary["total"]))
    c2.metric("Active",        int(summary["active_count"]))
    c3.metric("Inactive",      int(summary["inactive_count"]))

    col_a, col_b = st.columns(2)

    by_tier = q(
        """
        SELECT
            coalesce(nullif(tier, ''), '(unset)') AS tier,
            count(*)                              AS n
        FROM main_silver.dim_channel
        WHERE active
        GROUP BY tier
        ORDER BY n DESC
        """
    )
    with col_a:
        st.markdown("**Active by tier**")
        st.bar_chart(by_tier.set_index("tier"))

    by_niche = q(
        """
        SELECT
            coalesce(nullif(niche, ''), '(unset)') AS niche,
            count(*)                                AS n
        FROM main_silver.dim_channel
        WHERE active
        GROUP BY niche
        ORDER BY n DESC
        """
    )
    with col_b:
        st.markdown("**Active by niche**")
        st.bar_chart(by_niche.set_index("niche"))

    with st.expander("Full channel registry"):
        full = q(
            """
            SELECT
                handle, name, niche, tier,
                subscribers_at_addition, added_date,
                active, deactivated_reason
            FROM main_silver.dim_channel
            ORDER BY active DESC, tier, name
            """
        )
        st.dataframe(full, use_container_width=True, hide_index=True)
else:
    coming_soon(
        "`dim_channel`",
        "Chapter 6 commit 1 builds this from `competitors.csv`."
    )


# ── Silver — snapshot activity ───────────────────────────────────────────────

st.header("Silver · snapshot activity")

if has_table("main_silver", "stg_video_stats"):
    silver_activity = q(
        """
        SELECT
            fetched_date,
            count(*)                    AS rows,
            count(DISTINCT video_id)    AS distinct_videos,
            count(DISTINCT channel_id)  AS distinct_channels
        FROM main_silver.stg_video_stats
        GROUP BY fetched_date
        ORDER BY fetched_date
        """
    )

    chart = (
        alt.Chart(silver_activity)
        .mark_bar()
        .encode(
            x=alt.X("fetched_date:T", title="Snapshot date"),
            y=alt.Y("rows:Q", title="Rows captured"),
            tooltip=list(silver_activity.columns),
        )
        .properties(height=220)
    )
    st.altair_chart(chart, use_container_width=True)

    with st.expander("Per-date detail"):
        st.dataframe(silver_activity, use_container_width=True, hide_index=True)
else:
    coming_soon("Silver activity", "Builds the moment `dbt run --select tag:silver` succeeds.")


# ── Gold — 7-day video growth ────────────────────────────────────────────────

st.header("Gold · 7-day video growth")

if has_table("main_gold", "fct_video_growth_7d"):
    gold = q(
        """
        SELECT
            channel_name,
            substr(title, 1, 60)        AS title,
            snapshot_date,
            days_since_baseline,
            views_added_window,
            likes_added_window,
            comments_added_window
        FROM main_gold.fct_video_growth_7d
        ORDER BY views_added_window DESC NULLS LAST
        """
    )

    st.metric("Rows", len(gold))

    if len(gold) == 0:
        st.info(
            "Table exists but is empty. Likely the bronze 'latest 10' "
            "limitation — see the overlap heatmap below."
        )
    else:
        head = gold.head(10).copy()
        head["label"] = head["channel_name"] + " — " + head["title"]
        chart = (
            alt.Chart(head)
            .mark_bar()
            .encode(
                x=alt.X("views_added_window:Q", title="Views added in 7-day window"),
                y=alt.Y("label:N", sort="-x", title=None),
                color=alt.Color("channel_name:N", legend=alt.Legend(title="Channel")),
                tooltip=["channel_name", "title", "snapshot_date",
                         "days_since_baseline", "views_added_window",
                         "likes_added_window", "comments_added_window"],
            )
            .properties(height=max(40 * min(len(gold), 10), 180))
        )
        st.altair_chart(chart, use_container_width=True)

        with st.expander("Full gold table"):
            st.dataframe(gold, use_container_width=True, hide_index=True)
else:
    coming_soon("Gold growth", "Built by chapter 5 commit 1.")


# ── Why gold is small — channel-overlap matrix ───────────────────────────────

st.header("Why gold is small · video-id overlap matrix")
st.caption(
    "Each cell is the count of `video_id`s present in BOTH snapshot dates. "
    "Bronze pulls 'latest 10 videos per channel' — so high-frequency posters "
    "completely turn over their latest-10 within a week. The ASOF JOIN that "
    "computes 7-day growth can only fire on videos present in BOTH endpoints. "
    "Watch this matrix fill in (or, better: bump `max_videos_per_channel` "
    "so the bronze captures longer per-video history)."
)

if has_table("main_silver", "stg_video_stats"):
    overlap = q(
        """
        WITH dates AS (
            SELECT DISTINCT fetched_date
            FROM main_silver.stg_video_stats
        ),
        pairs AS (
            SELECT a.fetched_date AS date_a, b.fetched_date AS date_b
            FROM dates a CROSS JOIN dates b
            WHERE a.fetched_date <= b.fetched_date
        )
        SELECT
            p.date_a,
            p.date_b,
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
            color=alt.Color(
                "shared:Q",
                title="Shared video_ids",
                scale=alt.Scale(scheme="blues"),
            ),
            tooltip=["date_a", "date_b", "shared"],
        )
        .properties(height=320)
    )
    text = (
        alt.Chart(overlap)
        .mark_text(fontSize=11)
        .encode(
            x="date_b:O",
            y="date_a:O",
            text="shared:Q",
            color=alt.condition(
                "datum.shared > 8", alt.value("white"), alt.value("#222")
            ),
        )
    )
    st.altair_chart(heat + text, use_container_width=True)


# ── Coming-soon cards ────────────────────────────────────────────────────────

st.header("What's next · empty cards light up as the project ships")

left, right = st.columns(2)

with left:
    # dim_channel has its own dedicated section above — no need to duplicate
    # the coming-soon card here. Keep the slot for the next big silver model.
    if has_table("main_gold", "fct_channel_velocity"):
        st.success("**`fct_channel_velocity`** ✓ shipped.")
    else:
        coming_soon(
            "`fct_channel_velocity`",
            "Channel-grain rollup of growth. Gap-tolerant — replaces the "
            "per-video model when the bronze can't hold a full 7-day cohort.",
        )

with right:
    if has_table("main_gold", "fct_video_leaderboard"):
        st.success("**`fct_video_leaderboard`** ✓ shipped.")
    else:
        coming_soon(
            "`fct_video_leaderboard`",
            "Niche-segmented top-N videos by growth. Future chapter.",
        )

    coming_soon(
        "Airflow-driven daily refresh",
        "Chapter 5 commit 2 — the DAG runs `dbt run` after `collect` lands "
        "fresh bronze, so this dashboard stays current automatically.",
    )
