"""
Thin Airflow REST client used by the Pipeline Ops page.

Reads connection config from `st.secrets` first (production), falling back to
env vars (dev) and finally to the docker-compose POC defaults
(http://localhost:8080, admin/admin).

We deliberately use the **stable REST API** (`/api/v1/...`) and HTTP Basic
auth — Airflow 2.10's default. NEVER shell out to the `airflow` CLI from the
dashboard; that would couple the two services and turn a button click into a
remote-code-execution surface.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import pandas as pd
import requests
import streamlit as st


@dataclass
class AirflowConfig:
    base_url: str
    user: str
    password: str
    timeout: int = 10

    @classmethod
    def load(cls) -> "AirflowConfig":
        # st.secrets raises StreamlitSecretNotFoundError on access when
        # there's no secrets.toml file at all (it lazy-parses on first read,
        # not on attribute access). Wrap the whole probe so missing-secrets
        # is treated as "fall through to env vars".
        af: dict = {}
        try:
            af = dict(st.secrets.get("airflow", {})) if "airflow" in st.secrets else {}
        except Exception:
            af = {}
        return cls(
            base_url=(
                af.get("base_url")
                or os.environ.get("AIRFLOW_BASE_URL")
                or "http://localhost:8080"
            ).rstrip("/"),
            user=af.get("user") or os.environ.get("AIRFLOW_USER") or "admin",
            password=(
                af.get("password")
                or os.environ.get("AIRFLOW_PASSWORD")
                or "admin"
            ),
        )


class AirflowClient:
    def __init__(self, cfg: AirflowConfig | None = None):
        self.cfg = cfg or AirflowConfig.load()

    # ── Low-level ────────────────────────────────────────────────────────────

    def _get(self, path: str, **params) -> dict:
        r = requests.get(
            f"{self.cfg.base_url}/api/v1{path}",
            auth=(self.cfg.user, self.cfg.password),
            params=params,
            timeout=self.cfg.timeout,
        )
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, payload: dict) -> dict:
        r = requests.post(
            f"{self.cfg.base_url}/api/v1{path}",
            auth=(self.cfg.user, self.cfg.password),
            json=payload,
            timeout=self.cfg.timeout,
        )
        r.raise_for_status()
        return r.json()

    # ── Public ───────────────────────────────────────────────────────────────

    def health(self) -> dict:
        """Returns Airflow's `/health` JSON. Doesn't require auth on most
        installs but we send creds anyway for symmetry."""
        try:
            r = requests.get(
                f"{self.cfg.base_url}/health",
                auth=(self.cfg.user, self.cfg.password),
                timeout=self.cfg.timeout,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            return {"error": str(e)}

    def list_dags(self) -> pd.DataFrame:
        data = self._get("/dags", limit=100)
        rows = [
            {
                "dag_id": d["dag_id"],
                "is_paused": d["is_paused"],
                "schedule": d.get("schedule_interval", {}).get("value")
                if isinstance(d.get("schedule_interval"), dict) else d.get("schedule_interval"),
                "tags": ",".join(t["name"] for t in d.get("tags", [])),
            }
            for d in data.get("dags", [])
        ]
        return pd.DataFrame(rows)

    def list_runs(self, dag_id: str, limit: int = 10) -> pd.DataFrame:
        """Most-recent N runs of a DAG, with state + duration."""
        data = self._get(
            f"/dags/{dag_id}/dagRuns",
            order_by="-execution_date",
            limit=limit,
        )
        rows = []
        for r in data.get("dag_runs", []):
            start = r.get("start_date")
            end = r.get("end_date")
            duration = None
            if start and end:
                duration = (
                    pd.to_datetime(end) - pd.to_datetime(start)
                ).total_seconds()
            rows.append({
                "run_id": r["dag_run_id"],
                "state": r["state"],
                "execution_date": r.get("execution_date"),
                "start_date": start,
                "end_date": end,
                "duration_s": duration,
            })
        df = pd.DataFrame(rows)
        if not df.empty:
            for c in ("execution_date", "start_date", "end_date"):
                df[c] = pd.to_datetime(df[c], errors="coerce")
        return df

    def is_running(self, dag_id: str) -> bool:
        """Idempotency guard for the trigger button."""
        try:
            data = self._get(
                f"/dags/{dag_id}/dagRuns",
                state="running",
                limit=1,
            )
            return bool(data.get("dag_runs"))
        except Exception:
            # Surface it via list_runs for the operator instead of failing
            # silently — but don't block the button on a probe error.
            return False

    def trigger(self, dag_id: str, conf: dict | None = None) -> dict:
        """POST a manual run. Returns the new dag_run dict on success."""
        return self._post(f"/dags/{dag_id}/dagRuns", {"conf": conf or {}})
