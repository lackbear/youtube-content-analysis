"""
Reusable Altair chart builders + small Streamlit helpers (KPI cards, traffic
lights, error boundaries). Page code stays declarative — `bar_horizontal(df,
...)` instead of 30 lines of Altair.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable

import altair as alt
import pandas as pd
import streamlit as st

from .theme import PALETTE, channel_color_scale


# ── KPI cards ────────────────────────────────────────────────────────────────

def kpi_row(metrics: list[dict]) -> None:
    """Render a row of `st.metric` cards.

    Each dict: {label, value, delta?, help?}. Metric layout deliberately uses
    one column per metric (no wrap) — the 5-up KPI strip is the visual
    signature of the overview page.
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        col.metric(
            m["label"],
            m["value"],
            delta=m.get("delta"),
            help=m.get("help"),
        )


# ── Traffic light ────────────────────────────────────────────────────────────

def freshness_badge(days_since: int | None,
                    green: int = 1, amber: int = 3) -> str:
    """Markdown badge string showing the freshness SLA bucket."""
    if days_since is None:
        return "⚪ unknown"
    if days_since <= green:
        return f"🟢 fresh ({days_since}d)"
    if days_since <= amber:
        return f"🟡 stale ({days_since}d)"
    return f"🔴 very stale ({days_since}d)"


# ── Bars ─────────────────────────────────────────────────────────────────────

def bar_horizontal(df: pd.DataFrame, x: str, y: str,
                   x_title: str | None = None, y_title: str | None = None,
                   color: str | None = None, height: int | None = None,
                   tooltip: list[str] | None = None) -> alt.Chart:
    """Sorted horizontal bar with value labels. Default sort is descending by x."""
    if df.empty:
        return alt.Chart(pd.DataFrame({y: [], x: []})).mark_bar()
    enc = dict(
        x=alt.X(f"{x}:Q", title=x_title or x),
        y=alt.Y(f"{y}:N", sort="-x", title=y_title),
        tooltip=tooltip or [y, x],
    )
    if color:
        enc["color"] = alt.Color(f"{color}:N", legend=None)
    bar = alt.Chart(df).mark_bar().encode(**enc)
    text = (
        alt.Chart(df)
        .mark_text(align="left", baseline="middle", dx=4, fontSize=11)
        .encode(
            x=f"{x}:Q",
            y=alt.Y(f"{y}:N", sort="-x"),
            text=alt.Text(f"{x}:Q", format=","),
        )
    )
    chart = (bar + text)
    if height:
        chart = chart.properties(height=height)
    return chart


# ── Time series with rolling mean ────────────────────────────────────────────

def time_series_with_rolling(
    df: pd.DataFrame, x: str, y: str,
    x_title: str | None = None, y_title: str | None = None,
    window: int = 7, height: int = 220,
) -> alt.Chart:
    """Bar of raw values + line of rolling mean. Days with 0 are colored red
    so missed-collector-runs jump out from genuinely quiet days."""
    if df.empty:
        return alt.Chart(pd.DataFrame({x: [], y: []})).mark_bar()
    df = df.copy()
    df["_rolling"] = df[y].rolling(window=window, min_periods=1).mean()
    df["_zero"] = df[y].fillna(0) == 0

    bars = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{x}:T", title=x_title or x),
            y=alt.Y(f"{y}:Q", title=y_title or y),
            color=alt.condition(
                alt.datum._zero,
                alt.value("#E45756"),
                alt.value(PALETTE[0]),
            ),
            tooltip=[x, y, alt.Tooltip("_rolling:Q", title=f"{window}d mean", format=",.0f")],
        )
    )
    line = (
        alt.Chart(df)
        .mark_line(strokeDash=[4, 3], color="#262730")
        .encode(x=f"{x}:T", y="_rolling:Q")
    )
    return (bars + line).properties(height=height)


# ── Scatter (gold's better chart) ────────────────────────────────────────────

def perf_scatter(
    df: pd.DataFrame, x: str, y: str, size: str | None = None,
    color: str | None = None, color_domain: list[str] | None = None,
    x_title: str | None = None, y_title: str | None = None,
    tooltip: list[str] | None = None, height: int = 360,
) -> alt.LayerChart:
    """Performance scatter with a y=x diagonal reference line."""
    if df.empty:
        return alt.Chart(pd.DataFrame({x: [], y: []})).mark_point()
    enc = dict(
        x=alt.X(f"{x}:Q", title=x_title or x, scale=alt.Scale(zero=False)),
        y=alt.Y(f"{y}:Q", title=y_title or y, scale=alt.Scale(zero=False)),
        tooltip=tooltip or list(df.columns),
    )
    if size:
        enc["size"] = alt.Size(f"{size}:Q", legend=alt.Legend(title=size))
    if color:
        scale = (
            channel_color_scale(color_domain) if color_domain
            else alt.Scale(scheme="tableau10")
        )
        enc["color"] = alt.Color(f"{color}:N", scale=scale,
                                 legend=alt.Legend(title=color))
    points = alt.Chart(df).mark_circle(opacity=0.75).encode(**enc)

    # Diagonal y=x: same view → no growth. Anything above the line is
    # over-performing for its baseline.
    max_v = max(df[x].max(), df[y].max())
    diag_df = pd.DataFrame({"a": [0, max_v]})
    diag = (
        alt.Chart(diag_df)
        .mark_rule(strokeDash=[6, 4], color="#999")
        .encode(x="a:Q", y="a:Q")
    )
    return (points + diag).properties(height=height).interactive()


# ── Error boundary ───────────────────────────────────────────────────────────

@contextmanager
def section(title: str, icon: str = "🧱"):
    """Wrap a page section in a header + try/except.

    Today a missing column or a partial silver layer can crash the whole page.
    `with section("Channels"):` isolates the failure to one block and renders
    a red error card while the rest of the page keeps rendering.
    """
    st.markdown(f"### {icon} {title}")
    try:
        yield
    except Exception as e:
        st.error(f"⚠️ Section failed: `{type(e).__name__}: {e}`")
        st.exception(e)
