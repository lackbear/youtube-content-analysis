"""
youtube_pipeline (PythonOperator variant) — chapter 5 commit 2a

Sibling of dags/bash_operator/youtube_pipeline.py. Same shape, same
schedule, same default_args. The only difference: dbt_silver and dbt_gold
use PythonOperator (a Python callable that will subprocess-run the dbt
CLI in commit 2b) instead of BashOperator. Both DAGs produce identical
warehouse output.

Use whichever fits the situation — PythonOperator wins when you want
pre-flight logic, custom error handling, or programmatic stdout capture
around the dbt invocation; BashOperator (sibling DAG) wins for the most
idiomatic, least-layered dbt-on-Airflow pattern.

Shape — Option C: one task per service boundary.

    collect  →  dbt_silver  →  dbt_gold  →  notify

Only `collect` does real work today. The dbt and notify tasks are
placeholders that just print; commit 5.2b replaces dbt_silver/dbt_gold
with real `subprocess.run(["dbt", "run", "--select", "tag:..."])` calls.

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


def run_detect_stale(**context):
    """
    Flag channels with no new posts in N days (chapter 6 commit 2).

    Standalone branch — never blocks the collect chain. Reads bronze
    parquet directly + competitors.csv; writes data/curator/stale_channels.csv.
    """
    from scripts.detect_stale import detect_stale
    path = detect_stale()
    print(f"Wrote {path}")


def _run_dbt(select_arg: str) -> None:
    """
    Subprocess-run `dbt run --select <arg>` from /opt/airflow/dbt_youtube.

    PythonOperator-flavoured invocation. Pros over BashOperator (sibling DAG):
      - Pre-flight Python logic could land here (e.g. quota check, freshness
        gate, custom logging) without leaving Python.
      - Stdout/stderr captured programmatically — useful when we eventually
        want to parse `run_results.json` and surface model-level metrics.
      - Errors raise as Python exceptions, so AirflowFailException can pin
        retry semantics on specific failure modes.
    """
    import subprocess
    import sys

    result = subprocess.run(
        ["dbt", "run", "--select", select_arg],
        cwd="/opt/airflow/dbt_youtube",
        capture_output=True,
        text=True,
        check=False,
    )
    # Stream both streams to the Airflow task log. Order: stdout first
    # (success messages, model timings) then stderr (warnings, errors) —
    # easier to read top-down.
    sys.stdout.write(result.stdout or "")
    sys.stderr.write(result.stderr or "")
    if result.returncode != 0:
        raise AirflowFailException(
            f"dbt run --select {select_arg} failed (exit code {result.returncode})"
        )


def run_dbt_silver(**context):
    """Build the silver tier."""
    _run_dbt("tag:silver")


def run_dbt_gold(**context):
    """Build the gold tier."""
    _run_dbt("tag:gold")


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
    dag_id="youtube_pipeline_python",
    description="YouTube pipeline — PythonOperator dbt variant. Sibling: youtube_pipeline_bash.",
    start_date=datetime(2026, 4, 19),
    schedule="0 8 * * *",          # 08:00 UTC daily
    catchup=False,                 # never backfill: stale snapshots aren't useful
    max_active_runs=1,             # no concurrent runs; avoids quota double-spend
    default_args=default_args,
    tags=["youtube", "bronze", "chapter-5", "python_operator"],
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

    # Standalone parallel branch — informs the curator (chapter 6 commit 3)
    # without gating the collect chain. If detect_stale errors, collect still runs.
    detect_stale_task = PythonOperator(
        task_id="detect_stale",
        python_callable=run_detect_stale,
    )

    collect >> dbt_silver >> dbt_gold >> notify_task
