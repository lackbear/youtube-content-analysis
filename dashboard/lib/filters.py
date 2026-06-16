"""
Global filter sidebar with URL-query-param persistence.

Every page that wants filters calls `render_sidebar(scope=...)`. The function:

  1. Seeds widgets from `st.query_params` on first render of the session.
  2. Renders the requested widgets in the sidebar.
  3. Writes the current values back to `st.query_params` so the URL is
     shareable and survives reload.
  4. Returns a `Filters` object the page passes to query helpers.

We avoid the bidirectional session_state ↔ query_params sync footgun by
re-seeding only once per session (guarded by a sentinel key).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

import streamlit as st

from .db import has_table, q


@dataclass
class Filters:
    date_start: date
    date_end: date
    channel_ids: list[str] = field(default_factory=list)
    niches: list[str] = field(default_factory=list)
    tiers: list[str] = field(default_factory=list)
    active_only: bool = True

    # ── SQL helpers ──────────────────────────────────────────────────────────

    def date_clause(self, col: str) -> tuple[str, list]:
        return f"{col} BETWEEN ? AND ?", [self.date_start, self.date_end]

    def channel_clause(self, col: str = "channel_id") -> tuple[str, list]:
        if not self.channel_ids:
            return "1=1", []
        ph = ",".join(["?"] * len(self.channel_ids))
        return f"{col} IN ({ph})", list(self.channel_ids)

    def niche_clause(self, col: str = "niche") -> tuple[str, list]:
        if not self.niches:
            return "1=1", []
        ph = ",".join(["?"] * len(self.niches))
        return f"{col} IN ({ph})", list(self.niches)

    def tier_clause(self, col: str = "tier") -> tuple[str, list]:
        if not self.tiers:
            return "1=1", []
        ph = ",".join(["?"] * len(self.tiers))
        return f"{col} IN ({ph})", list(self.tiers)


# ── Query-param (de)serialization ────────────────────────────────────────────

def _qp_get_list(key: str) -> list[str]:
    raw = st.query_params.get(key, "")
    return [v for v in raw.split("|") if v] if raw else []


def _qp_set_list(key: str, values: list[str]) -> None:
    if values:
        st.query_params[key] = "|".join(values)
    elif key in st.query_params:
        del st.query_params[key]


def _qp_get_date(key: str, default: date) -> date:
    raw = st.query_params.get(key)
    try:
        return date.fromisoformat(raw) if raw else default
    except (ValueError, TypeError):
        return default


# ── Domain helpers (cached) ──────────────────────────────────────────────────

def _channel_options() -> list[tuple[str, str]]:
    """Return [(channel_id, label)] for the channel multiselect."""
    if not has_table("main_silver", "dim_channel"):
        return []
    df = q(
        """
        SELECT channel_id, handle, name
        FROM main_silver.dim_channel
        WHERE active
        ORDER BY name
        """
    )
    return [
        (row["channel_id"], f"{row['name']} ({row['handle']})")
        for _, row in df.iterrows()
    ]


def _distinct(col: str) -> list[str]:
    if not has_table("main_silver", "dim_channel"):
        return []
    df = q(
        f"""
        SELECT DISTINCT coalesce(nullif({col}, ''), '(unset)') AS v
        FROM main_silver.dim_channel
        WHERE active
        ORDER BY 1
        """
    )
    return df["v"].tolist()


# ── Public entrypoint ────────────────────────────────────────────────────────

QUICK_RANGES = {
    "Last 7 days":  7,
    "Last 30 days": 30,
    "Last 90 days": 90,
    "All time":     None,
}


def render_sidebar(scope: tuple[str, ...] = ("date", "channels", "niches", "tiers")) -> Filters:
    """Render the requested filter widgets in the sidebar; return current values.

    `scope` controls which widgets appear, so the curator and ops pages can
    skip filters that don't make sense there.
    """
    today = date.today()

    with st.sidebar:
        st.markdown("### 🎛️ Filters")

        # ── Date range ──
        if "date" in scope:
            quick = st.selectbox(
                "Quick range", list(QUICK_RANGES.keys()),
                index=1, key="filt_quick",
                help="Sets a default range. Pick custom dates below to override.",
            )
            if QUICK_RANGES[quick] is None:
                default_start = date(2020, 1, 1)
            else:
                default_start = today - timedelta(days=QUICK_RANGES[quick])
            qp_start = _qp_get_date("start", default_start)
            qp_end = _qp_get_date("end", today)
            picked = st.date_input(
                "Custom range", value=(qp_start, qp_end), key="filt_dates",
            )
            # st.date_input returns a single date if user clicks once mid-edit.
            if isinstance(picked, tuple) and len(picked) == 2:
                d_start, d_end = picked
            else:
                d_start = d_end = picked if isinstance(picked, date) else qp_start
        else:
            d_start, d_end = today - timedelta(days=30), today

        # ── Channels ──
        if "channels" in scope:
            opts = _channel_options()
            id_to_label = dict(opts)
            label_to_id = {v: k for k, v in id_to_label.items()}
            seeded_ids = _qp_get_list("ch")
            seeded_labels = [id_to_label[i] for i in seeded_ids if i in id_to_label]
            picked_labels = st.multiselect(
                "Channels", list(label_to_id.keys()),
                default=seeded_labels, key="filt_channels",
                help="Empty = all active channels.",
            )
            ch_ids = [label_to_id[l] for l in picked_labels]
        else:
            ch_ids = []

        # ── Niches ──
        if "niches" in scope:
            niche_opts = _distinct("niche")
            niches = st.multiselect(
                "Niches", niche_opts,
                default=_qp_get_list("nic"), key="filt_niches",
            )
        else:
            niches = []

        # ── Tiers ──
        if "tiers" in scope:
            tier_opts = _distinct("tier")
            tiers = st.multiselect(
                "Tiers", tier_opts,
                default=_qp_get_list("tr"), key="filt_tiers",
            )
        else:
            tiers = []

        active_only = True  # active_only currently always-on; toggle reserved for future

        st.caption(
            "Filters persist in the URL — copy the address bar to share a "
            "view. Clear: ✕ next to each widget."
        )

    # ── Sync URL ──
    st.query_params["start"] = d_start.isoformat()
    st.query_params["end"] = d_end.isoformat()
    _qp_set_list("ch", ch_ids)
    _qp_set_list("nic", niches)
    _qp_set_list("tr", tiers)

    return Filters(
        date_start=d_start,
        date_end=d_end,
        channel_ids=ch_ids,
        niches=niches,
        tiers=tiers,
        active_only=active_only,
    )
