"""
Analytics page — Channels, Growth, Curator as three tabs under one filter bar.

The previous design had each of these as its own page, which made nav heavy
for a 1-2 person tool. Tabs collapse them into a single nav entry while
keeping a clean per-section layout.

One filter sidebar at the page level applies to all tabs that consume it.
Curator ignores filters (it works on CSVs, not the warehouse) — that's
called out in the sidebar caption.
"""

from __future__ import annotations

import streamlit as st

from lib.db import warehouse_exists
from lib.filters import render_sidebar
from lib.theme import apply_theme
from sections import channels as channels_tab
from sections import curator as curator_tab
from sections import growth as growth_tab


apply_theme()

st.title("📊 Analytics")
st.caption(
    "Three views over the curated registry and gold facts. "
    "Filters on the left apply to **Channels** and **Growth**; **Curator** uses its own state."
)

if not warehouse_exists():
    st.error("No warehouse — run `dbt run` first.")
    st.stop()

filters = render_sidebar(scope=("date", "niches", "tiers", "channels"))

t_channels, t_growth, t_curator = st.tabs(["📺 Channels", "📈 Growth", "🛒 Curator"])

with t_channels:
    channels_tab.render(filters)

with t_growth:
    growth_tab.render(filters)

with t_curator:
    curator_tab.render()
