"""
airflow_dag_v1.py — YouTube data pipeline orchestration (chapter 4)

First DAG for the project. Per the single-file-per-chapter convention,
future significant changes will land as airflow_dag_v2.py so the diff
narrates the evolution (adding DockerOperator, dbt integration, etc.).

Shape — Option C: one task per service boundary.

    collect  →  dbt_silver  →  dbt_gold  →  notify
    (Python)    (placeholder)  (placeholder)  (placeholder)

Only `collect` does real work today. The dbt and notify tasks are
placeholders that just print — chapters 5 and 6 will wire them up.
Keeping them in the DAG now means the shape is visible end-to-end and
the dependency chain is locked in.

Retry policy:
    Default              → retries=2, retry_delay=10min
    QuotaExhaustedError  → retries=0 (quota resets at PT midnight; retrying
                           10 minutes later just burns units and fails again)

Schedule: 0 8 * * *  (08:00 UTC) — safely past the YouTube API's PT-midnight
quota reset, which can fall anywhere between 07:00 and 08:00 UTC depending
on daylight saving.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator


# ── Task callables ───────────────────────────────────────────────────────────
# Imports of Collectorv2 happen INSIDE the callable, not at module level.
# Airflow re-parses every DAG file on every scheduler tick (~30s); keeping
# module-level code cheap means the scheduler stays responsive. Collectorv2's
# import side effects (load_config, DEFAULT_CHANNELS) only fire when the
# collect task actually runs.

def run_collector(**context):
    """
    Invoke Collectorv2.run() inside the Airflow worker.

    On QuotaExhaustedError we convert to AirflowFailException — this marks
    the task as failed but tells Airflow NOT to retry (the exception type
    is treated as terminal). We want the DAG to go red in the UI so oncall
    sees the issue, but we don't want 2 automatic retries that would just
    abort the same way.
    """
    from ingestion.Collectorv2 import run as collector_run, QuotaExhaustedError

    try:
        collector_run()
    except QuotaExhaustedError as e:
        raise AirflowFailException(
            f"Quota exhausted — not retrying until PT-midnight reset: {e}"
        ) from e


def run_dbt_silver(**context):
    """Placeholder — chapter 5 will replace with `dbt run --select tag:silver`."""
    print("[placeholder] dbt silver models will run here (chapter 5).")


def run_dbt_gold(**context):
    """Placeholder — chapter 5 will replace with `dbt run --select tag:gold`."""
    print("[placeholder] dbt gold models will run here (chapter 5).")


def notify(**context):
    """Placeholder — chapter 6 will post a daily summary (Slack / email)."""
    print("[placeholder] daily summary notification will post here (chapter 6).")


# ── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":             "tkhomsi",
    "retries":           2,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "sla":               timedelta(hours=2),
    "depends_on_past":   False,
    # on_failure_callback will be wired up in chapter 6 (Slack/email alerts).
}


with DAG(
    dag_id="youtube_pipeline",
    description="YouTube competitor data pipeline — collect, transform, notify.",
    start_date=datetime(2026, 4, 19),
    schedule="0 8 * * *",          # 08:00 UTC daily
    catchup=False,                 # never backfill: stale snapshots aren't useful
    max_active_runs=1,             # no concurrent runs; avoids quota double-spend
    default_args=default_args,
    tags=["youtube", "bronze", "chapter-4"],
) as dag:

    collect = PythonOperator(
        task_id="collect",
        python_callable=run_collector,
    )

    dbt_silver = PythonOperator(
        task_id="dbt_silver",
        python_callable=run_dbt_silver,
    )

    dbt_gold = PythonOperator(
        task_id="dbt_gold",
        python_callable=run_dbt_gold,
    )

    notify_task = PythonOperator(
        task_id="notify",
        python_callable=notify,
    )

    collect >> dbt_silver >> dbt_gold >> notify_task
