"""Curator tab — funnel, cohort, time-to-promotion.

Now opens with a 4-bullet legend explaining what each chart block means —
curator analytics are not self-explanatory and the previous version asked
the reader to figure it out.
"""

from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from lib.charts import kpi_row, section


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
COMPETITORS  = PROJECT_ROOT / "competitors.csv"
STALE_CSV    = PROJECT_ROOT / "data" / "curator" / "stale_channels.csv"
CAND_CSV     = PROJECT_ROOT / "data" / "curator" / "candidates.csv"


@st.cache_data(ttl=300, show_spinner=False)
def _load_all():
    comp  = pd.read_csv(COMPETITORS, dtype=str, keep_default_na=False) if COMPETITORS.exists() else pd.DataFrame()
    stale = pd.read_csv(STALE_CSV,   dtype=str, keep_default_na=False) if STALE_CSV.exists()   else pd.DataFrame()
    cand  = pd.read_csv(CAND_CSV,    dtype=str, keep_default_na=False) if CAND_CSV.exists()    else pd.DataFrame()
    return comp, stale, cand


def render() -> None:
    st.info(
        "**📚 What you're looking at, in one minute:**\n\n"
        "- **Funnel** — of all candidates ever discovered, how many got accepted, then promoted (= live in your registry).\n"
        "- **Discovery cohorts** — when candidates arrived (by week), colored by their current status.\n"
        "- **Niche conversion** — which niches actually convert from *discovered* → *promoted*.\n"
        "- **Time to promotion** — days between a candidate appearing and being added to the live registry.",
        icon="ℹ️",
    )

    comp, stale, cand = _load_all()
    if cand.empty and stale.empty:
        st.warning(
            "Curator outputs not present yet. Run the curator DAG or "
            "`python scripts/discover.py` to populate them."
        )
        return

    # ── KPIs ────────────────────────────────────────────────────────────────
    with section("Funnel summary", icon="📊"):
        n_active = len(stale) if not stale.empty else 0
        n_stale = (
            int((stale["stale"].str.lower() == "true").sum()) if not stale.empty else 0
        )
        stale_rate = (n_stale / n_active * 100) if n_active else 0.0

        status_counts = (
            cand["status"].fillna("pending").value_counts().to_dict()
            if not cand.empty else {}
        )
        n_total    = len(cand)
        n_pending  = int(status_counts.get("pending", 0))
        n_accepted = int(status_counts.get("accepted", 0))
        n_promoted = int(status_counts.get("promoted", 0))
        n_rejected = int(status_counts.get("rejected", 0))

        kpi_row([
            {"label": "Stale rate",
             "value": f"{stale_rate:.1f}%",
             "help": f"{n_stale} of {n_active} active channels (>14d no posts)"},
            {"label": "Candidates · total", "value": n_total},
            {"label": "Pending",  "value": n_pending},
            {"label": "Accepted", "value": n_accepted,
             "help": "Awaiting promotion"},
            {"label": "Promoted", "value": n_promoted,
             "help": "Live in competitors.csv"},
            {"label": "Rejected", "value": n_rejected},
        ])

    # ── Funnel ──────────────────────────────────────────────────────────────
    with section("Acceptance funnel", icon="🪜"):
        funnel = pd.DataFrame({
            "stage": ["Discovered", "Accepted", "Promoted"],
            "count": [n_total, n_accepted + n_promoted, n_promoted],
        })
        funnel["pct"] = funnel["count"] / max(funnel["count"].iloc[0], 1) * 100

        bar = (
            alt.Chart(funnel)
            .mark_bar(cornerRadiusEnd=4)
            .encode(
                x=alt.X("count:Q", title="Candidates"),
                y=alt.Y("stage:N", sort=["Discovered", "Accepted", "Promoted"], title=None),
                tooltip=["stage", "count", alt.Tooltip("pct:Q", format=".1f", title="% of discovered")],
            )
        )
        text = (
            alt.Chart(funnel)
            .mark_text(align="left", dx=4, fontSize=12)
            .encode(
                x="count:Q",
                y=alt.Y("stage:N", sort=["Discovered", "Accepted", "Promoted"]),
                text=alt.Text("pct:Q", format=".1f"),
            )
        )
        st.altair_chart((bar + text).properties(height=200), use_container_width=True)
        st.caption(
            f"Discovered → Accepted: **{(n_accepted+n_promoted)/max(n_total,1)*100:.1f}%** of candidates pass review. "
            f"Accepted → Promoted: **{n_promoted/max(n_accepted+n_promoted,1)*100:.1f}%** make it into the live registry."
        )
        if n_rejected:
            st.caption(f"Rejected branch: {n_rejected} candidate(s) rejected outright.")

    # ── Cohorts ─────────────────────────────────────────────────────────────
    with section("Discovery cohorts · weekly arrivals by status", icon="📅"):
        if "discovered_date" not in cand.columns or cand.empty:
            st.info("No `discovered_date` data yet.")
        else:
            c = cand.copy()
            c["discovered_date"] = pd.to_datetime(c["discovered_date"], errors="coerce")
            c["status"] = c["status"].fillna("pending").replace("", "pending")
            c = c.dropna(subset=["discovered_date"])
            if c.empty:
                st.info("All candidates have empty discovered_date.")
            else:
                stack = (
                    alt.Chart(c)
                    .mark_bar()
                    .encode(
                        x=alt.X("yearweek(discovered_date):T", title="Week discovered"),
                        y=alt.Y("count():Q", title="Candidates"),
                        color=alt.Color(
                            "status:N",
                            scale=alt.Scale(
                                domain=["pending", "accepted", "promoted", "rejected"],
                                range=["#BAB0AC", "#F58518", "#54A24B", "#E45756"],
                            ),
                            title="Status",
                        ),
                        tooltip=[
                            alt.Tooltip("yearweek(discovered_date):T", title="week"),
                            "status:N",
                            alt.Tooltip("count():Q", title="candidates"),
                        ],
                    )
                    .properties(height=260)
                )
                st.altair_chart(stack, use_container_width=True)
                st.caption("Bar height = candidates that arrived that week. Green = already promoted; orange = accepted (waiting); grey = pending review; red = rejected.")

    # ── Niche conversion ────────────────────────────────────────────────────
    with section("Niche conversion", icon="🎯"):
        if cand.empty or "niche" not in cand.columns:
            st.info("No niche data on candidates.")
        else:
            c = cand.copy()
            c["status"] = c["status"].fillna("pending").replace("", "pending")
            agg = c.groupby(["niche", "status"]).size().reset_index(name="n")
            if agg.empty:
                st.info("No data.")
            else:
                chart = (
                    alt.Chart(agg)
                    .mark_bar()
                    .encode(
                        x=alt.X("n:Q", title="Candidates", stack="zero"),
                        y=alt.Y("niche:N", sort="-x", title="Niche"),
                        color=alt.Color(
                            "status:N",
                            scale=alt.Scale(
                                domain=["pending", "accepted", "promoted", "rejected"],
                                range=["#BAB0AC", "#F58518", "#54A24B", "#E45756"],
                            ),
                        ),
                        tooltip=["niche", "status", "n"],
                    )
                    .properties(height=max(28 * agg["niche"].nunique(), 200))
                )
                st.altair_chart(chart, use_container_width=True)
                st.caption(
                    "Stacked by status — green block size relative to the bar = the niche's promotion rate. "
                    "Niches with mostly grey are discovery-rich but not yet curated."
                )

    # ── Time to promotion ───────────────────────────────────────────────────
    with section("Time to promotion · discovered → live", icon="⏱️"):
        if comp.empty or cand.empty:
            st.info("Need both `competitors.csv` and `candidates.csv` to compute.")
        else:
            promoted = cand[cand["status"] == "promoted"].copy()
            promoted["discovered_date"] = pd.to_datetime(promoted["discovered_date"], errors="coerce")
            c2 = comp.copy()
            c2["added_date"] = pd.to_datetime(c2["added_date"], errors="coerce")
            joined = promoted.merge(c2[["channel_id", "added_date"]], on="channel_id", how="left")
            joined["days_to_promote"] = (joined["added_date"] - joined["discovered_date"]).dt.days
            joined = joined.dropna(subset=["days_to_promote"])
            if joined.empty:
                st.info("No promoted candidates with a discovered_date yet.")
            else:
                median = joined["days_to_promote"].median()
                mean = joined["days_to_promote"].mean()
                kpi_row([
                    {"label": "Promoted (computable)",   "value": int(len(joined))},
                    {"label": "Median days to promote",  "value": f"{median:.1f}"},
                    {"label": "Mean days to promote",    "value": f"{mean:.1f}"},
                    {"label": "Max days",                "value": int(joined["days_to_promote"].max())},
                ])
                hist = (
                    alt.Chart(joined)
                    .mark_bar()
                    .encode(
                        x=alt.X("days_to_promote:Q", bin=alt.Bin(maxbins=20),
                                title="Days from discovered → added to registry"),
                        y=alt.Y("count():Q", title="Channels"),
                        tooltip=[alt.Tooltip("count():Q", title="channels")],
                    )
                    .properties(height=240)
                )
                st.altair_chart(hist, use_container_width=True)

    # ── Tables ──────────────────────────────────────────────────────────────
    with section("Stale channels — needs attention", icon="🚨"):
        if stale.empty:
            st.info("No stale_channels.csv yet.")
        else:
            s = stale.copy()
            mask_stale = s.get("stale", "").astype(str).str.lower() == "true"
            mask_unfetched = s.get("last_published_at", "").astype(str).eq("")
            focus = s[mask_stale | mask_unfetched]
            cols = ["handle", "name", "tier", "niche",
                    "last_published_at", "days_since_last_post"]
            cols = [c for c in cols if c in focus.columns]
            st.dataframe(focus[cols], use_container_width=True, hide_index=True)
            st.caption(f"{len(focus)} channels need attention (of {len(s)} active).")

    with section("Pending candidates · review queue", icon="📥"):
        if cand.empty:
            st.info("No candidates.csv yet.")
        else:
            pending = cand[cand["status"].fillna("pending").isin(["pending", ""])].copy()
            if pending.empty:
                st.success("Inbox zero on the candidate queue.")
            else:
                cols = ["handle", "name", "niche", "tier",
                        "subscribers", "last_published_at",
                        "discovered_date", "notes"]
                cols = [c for c in cols if c in pending.columns]
                if "subscribers" in pending.columns:
                    pending["_subs_num"] = pd.to_numeric(pending["subscribers"], errors="coerce")
                    pending = pending.sort_values(
                        "_subs_num", ascending=False, kind="mergesort",
                    ).drop(columns="_subs_num")
                st.dataframe(pending[cols], use_container_width=True, hide_index=True)
                st.caption(
                    f"{len(pending)} pending. Edit `data/curator/candidates.csv` "
                    "to flip `status` → `accepted` / `rejected`, then re-run "
                    "`scripts/promote_candidates.py`."
                )
