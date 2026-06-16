"""
dashboard/app.py — entrypoint + 3-page navigation router.

Three nav items, deliberately:
  • Overview   — landing KPIs
  • Analytics  — Channels / Growth / Curator (tabs inside)
  • Admin      — Ops / Diagnostics (tabs inside)

Earlier iteration had 6 pages (one per section). For a 1-2 person internal
tool that's too much chrome — collapsing into Analytics + Admin gives the
same content with one-third the nav surface.

Tradeoff: Streamlit `st.tabs` is not URL-routable, so a shared link won't
remember which tab was active. Filter state still persists in the URL.
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st


st.set_page_config(
    page_title="YT Pipeline",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded",
)


DASH = Path(__file__).resolve().parent

OVERVIEW  = st.Page(str(DASH / "_overview.py"),  title="Overview",  icon="📺", default=True)
ANALYTICS = st.Page(str(DASH / "_analytics.py"), title="Analytics", icon="📊")
ADMIN     = st.Page(str(DASH / "_admin.py"),     title="Admin",     icon="⚙️")

nav = st.navigation([OVERVIEW, ANALYTICS, ADMIN])
nav.run()
