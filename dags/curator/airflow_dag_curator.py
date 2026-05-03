"""
airflow_dag_curator.py — weekly curator DAG (chapter 6 commit 3).

Runs `scripts/discover.py` once a week. Reads
`data/curator/candidates_input.csv`, validates each handle against the
YouTube API, dedups against `competitors.csv` and the existing
`candidates.csv`, appends new validated rows. Manual review then flips
status from `pending` → `accepted` | `rejected` (a future commit will
auto-promote accepted candidates into `competitors.csv` to fill stale
slots flagged by `detect_stale.py`).

Schedule: Saturdays 09:00 UTC.

Single task DAG — `discover` is the only thing this DAG does.
`detect_stale` runs daily inside the main `youtube_pipeline_*` DAGs,
so no need to duplicate it here.

If `data/curator/candidates_input.csv` is missing or empty (no fresh
AI-sourced batch this week), discover.py raises FileNotFoundError /
exits cleanly — failure is acceptable and means "nothing to do."
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_discover(**context):
    """Validate AI-sourced candidates and write candidates.csv."""
    from scripts.discover import discover
    discover()


default_args = {
    "owner":             "tkhomsi",
    "retries":           1,
    "retry_delay":       timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=15),
    "depends_on_past":   False,
}


with DAG(
    dag_id="youtube_curator",
    description=(
        "Weekly — validates AI-sourced candidate channels against the YouTube API. "
        "Output drives manual curator review (candidates.csv)."
    ),
    start_date=datetime(2026, 4, 19),
    schedule="0 9 * * 6",          # Saturdays 09:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["youtube", "curator", "chapter-6"],
) as dag:

    discover_task = PythonOperator(
        task_id="discover",
        python_callable=run_discover,
    )
