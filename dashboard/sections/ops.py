"""Ops tab — Airflow REST trigger + freshness SLA.

If you see a 401 here: by default Airflow 2.10 only ships
`airflow.api.auth.backend.session` for the API. For Basic auth (which the
dashboard uses) you must enable `airflow.api.auth.backend.basic_auth`.
The fix is in docker-compose.yml — see the comment above the env block.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from lib.airflow_client import AirflowClient, AirflowConfig
from lib.charts import freshness_badge, kpi_row, section
from lib.db import clear_cache, has_table, q, warehouse_exists


KNOWN_DAGS = ["youtube_pipeline_python", "youtube_pipeline_bash", "youtube_curator"]


def _format_runs(runs: pd.DataFrame) -> pd.DataFrame:
    if runs.empty:
        return runs
    out = runs.copy()
    if "duration_s" in out.columns:
        out["duration"] = out["duration_s"].apply(
            lambda s: f"{int(s)}s" if pd.notna(s) else ""
        )
    badge = {
        "success": "🟢", "running": "🔵", "queued": "⏸️",
        "failed": "🔴", "up_for_retry": "🟠", "up_for_reschedule": "🟡",
    }
    out["state"] = out["state"].map(lambda s: f"{badge.get(s, '⚪')} {s}")
    keep = ["run_id", "state", "execution_date", "start_date", "end_date", "duration"]
    return out[[c for c in keep if c in out.columns]]


def render() -> None:
    # ── Freshness SLA ───────────────────────────────────────────────────────
    with section("Freshness SLA", icon="🩺"):
        if not warehouse_exists():
            st.error("Warehouse missing — `dbt run` first.")
        elif not has_table("main_silver", "stg_video_stats"):
            st.warning("Silver layer not built.")
        else:
            f = q(
                """
                SELECT
                    max(fetched_date)                       AS latest,
                    (current_date - max(fetched_date))::int AS days_since,
                    count(DISTINCT fetched_date)::int       AS distinct_dates,
                    count(DISTINCT channel_id)::int         AS distinct_channels
                FROM main_silver.stg_video_stats
                """
            ).iloc[0]
            days = int(f["days_since"]) if f["days_since"] is not None else None
            kpi_row([
                {"label": "Status",          "value": freshness_badge(days),
                 "help": "Green ≤ 1d · Amber ≤ 3d · Red > 3d"},
                {"label": "Latest snapshot", "value": str(f["latest"])[:10]},
                {"label": "Snapshot dates",  "value": int(f["distinct_dates"])},
                {"label": "Channels",        "value": int(f["distinct_channels"])},
            ])

    cfg = AirflowConfig.load()
    client = AirflowClient(cfg)

    with section("Airflow connection", icon="🔌"):
        health = client.health()
        if "error" in health:
            st.error(
                f"Cannot reach Airflow at `{cfg.base_url}` — `{health['error']}`. "
                "Configure via env (`AIRFLOW_BASE_URL`, `AIRFLOW_USER`, `AIRFLOW_PASSWORD`) "
                "or `.streamlit/secrets.toml`."
            )
            return
        metadb = health.get("metadatabase", {}).get("status", "?")
        sched  = health.get("scheduler", {}).get("status", "?")
        kpi_row([
            {"label": "Airflow URL",  "value": cfg.base_url},
            {"label": "Metadatabase", "value": f"🟢 {metadb}" if metadb == "healthy" else f"🔴 {metadb}"},
            {"label": "Scheduler",    "value": f"🟢 {sched}"  if sched  == "healthy" else f"🔴 {sched}"},
        ])

    # Probe the API once — if it 401s, show a friendly explainer up front
    # rather than 401 errors scattered across every DAG card.
    with section("DAGs · recent runs", icon="📜"):
        try:
            dags_df = client.list_dags()
            api_ok = True
        except Exception as e:
            msg = str(e)
            if "401" in msg or "Unauthorized" in msg:
                st.warning(
                    "**Airflow API auth not enabled** — `/health` works without auth, "
                    "but `/api/v1/*` is rejecting Basic auth.\n\n"
                    "**Fix:** add this to the `x-airflow-common.environment` block "
                    "in `docker-compose.yml`, then restart Airflow:\n"
                    "```\nAIRFLOW__API__AUTH_BACKENDS: \"airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth\"\n```"
                )
                st.caption("After editing: `docker compose up -d --force-recreate airflow-init airflow-webserver airflow-scheduler`")
                return
            st.error(f"Failed to list DAGs: `{e}`")
            return

        if not dags_df.empty:
            st.dataframe(dags_df, use_container_width=True, hide_index=True)

    for dag_id in KNOWN_DAGS:
        with section(dag_id, icon="🧬"):
            try:
                runs = client.list_runs(dag_id, limit=10)
            except Exception as e:
                st.error(f"Cannot fetch runs for `{dag_id}`: `{e}`")
                continue

            running = client.is_running(dag_id)

            col1, col2 = st.columns([3, 1])
            with col1:
                st.dataframe(_format_runs(runs), use_container_width=True, hide_index=True)
            with col2:
                disabled = running
                help_msg = (
                    "A run is currently active — wait for it to finish."
                    if running else
                    f"POSTs to /api/v1/dags/{dag_id}/dagRuns. Confirms first."
                )
                if st.button(f"▶ Trigger {dag_id}",
                             key=f"trigger_{dag_id}",
                             disabled=disabled,
                             help=help_msg,
                             use_container_width=True):
                    st.session_state[f"confirm_{dag_id}"] = True

                if st.session_state.get(f"confirm_{dag_id}"):
                    st.warning(f"Confirm trigger of `{dag_id}`?")
                    cy, cn = st.columns(2)
                    if cy.button("✅ Yes, trigger", key=f"yes_{dag_id}", use_container_width=True):
                        try:
                            result = client.trigger(dag_id)
                            st.success(f"Triggered: `{result.get('dag_run_id')}`")
                            st.session_state[f"confirm_{dag_id}"] = False
                        except Exception as e:
                            st.error(f"Trigger failed: `{e}`")
                    if cn.button("✖ Cancel", key=f"no_{dag_id}", use_container_width=True):
                        st.session_state[f"confirm_{dag_id}"] = False

    with section("Dashboard cache", icon="🧹"):
        st.write(
            "Queries are cached for 5 minutes. After a manual trigger lands "
            "new data, click below to force a refresh."
        )
        if st.button("🧹 Clear cache and reconnect"):
            clear_cache()
            st.success(
                f"Cache cleared at {datetime.now(timezone.utc).isoformat(timespec='seconds')}."
            )
