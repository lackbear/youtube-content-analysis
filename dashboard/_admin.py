"""
Admin page — Ops + Diagnostics as two tabs.

Read-side first (ops dashboards, diagnostics), write-side embedded inside
the Ops tab with confirmation guards. No sidebar filters here — admin
actions don't filter, they trigger.
"""

from __future__ import annotations

import streamlit as st

from lib.theme import apply_theme
from sections import diagnostics as diagnostics_tab
from sections import ops as ops_tab


apply_theme()

st.title("⚙️ Admin")
st.caption(
    "Pipeline operations and engineering diagnostics. "
    "Manual triggers live inside **Ops** and ask for confirmation before firing."
)

t_ops, t_diag = st.tabs(["⚙️ Ops", "🔬 Diagnostics"])

with t_ops:
    ops_tab.render()

with t_diag:
    diagnostics_tab.render()
