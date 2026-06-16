"""
Global Altair theme + a shared color palette.

`apply_theme()` is called at the top of every page. Once a theme is registered
in a process it stays until the kernel is restarted, so the call is idempotent.

The palette is a small, deterministic set so the same channel keeps the same
color across charts on a page.
"""

from __future__ import annotations

import altair as alt
import streamlit as st


# Tableau-10 inspired palette, tuned for both light & dark Streamlit themes.
PALETTE = [
    "#4C78A8", "#F58518", "#54A24B", "#E45756", "#72B7B2",
    "#EECA3B", "#B279A2", "#FF9DA6", "#9D755D", "#BAB0AC",
]


def _theme():
    return {
        "config": {
            "view": {"continuousWidth": 480, "continuousHeight": 320, "stroke": None},
            "title": {"fontSize": 14, "anchor": "start", "color": "#262730"},
            "axis": {
                "labelFontSize": 11,
                "titleFontSize": 12,
                "labelColor": "#5A5A5A",
                "titleColor": "#262730",
                "grid": True,
                "gridColor": "#EAECEF",
                "domain": False,
                "tickSize": 0,
            },
            "legend": {
                "labelFontSize": 11,
                "titleFontSize": 12,
                "padding": 4,
                "symbolType": "circle",
            },
            "range": {"category": PALETTE, "ramp": PALETTE},
            "bar": {"cornerRadiusEnd": 2},
            "line": {"strokeWidth": 2.5},
            "point": {"size": 60, "filled": True},
        }
    }


def apply_theme() -> None:
    """Register & enable the YT theme. Idempotent.

    Altair 5.5 moved `alt.themes` → `alt.theme` and changed the registration
    API to a decorator. We support both so the requirements pin (`altair>=5.0`)
    keeps working across the migration window.
    """
    if hasattr(alt, "theme") and hasattr(alt.theme, "register"):
        # Altair 5.5+: decorator-style; calling it with our function works too.
        alt.theme.register("yt", enable=True)(_theme)
    else:
        # Altair 5.0–5.4
        alt.themes.register("yt", _theme)  # type: ignore[attr-defined]
        alt.themes.enable("yt")            # type: ignore[attr-defined]


def channel_color_scale(channel_names: list[str]) -> alt.Scale:
    """A deterministic color scale keyed by channel name. Pass the full domain
    once per page so the same channel gets the same color in every chart."""
    domain = sorted(set(channel_names))
    n = len(domain)
    palette = (PALETTE * ((n // len(PALETTE)) + 1))[:n] if n else PALETTE
    return alt.Scale(domain=domain, range=palette)


def page_setup(title: str, icon: str = "📺") -> None:
    """One-shot page header — applies theme + (best-effort) sets page config.

    When pages are loaded through `st.navigation()` (the new entrypoint pattern
    in app.py), the router has already called `set_page_config` and a second
    call raises StreamlitAPIException. We swallow that — it's the expected
    happy path. The set_page_config is only useful for direct-script runs
    (e.g., the smoke test or `streamlit run dashboard/pages/3_Curator.py`).
    """
    try:
        st.set_page_config(
            page_title=f"YT · {title}",
            page_icon=icon,
            layout="wide",
            initial_sidebar_state="expanded",
        )
    except Exception:
        pass
    apply_theme()
