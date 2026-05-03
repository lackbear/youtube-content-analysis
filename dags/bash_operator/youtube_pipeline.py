"""
youtube_pipeline (BashOperator variant) — chapter 5 commit 2a

Sibling of dags/python_operator/youtube_pipeline.py. Same shape, same
schedule, same default_args. The only difference: dbt_silver and dbt_gold
use BashOperator (the dbt CLI runs in a shell that Airflow spawns)
instead of PythonOperator. Both DAGs produce identical warehouse output.

Use whichever fits the situation — BashOperator wins for the most
idiomatic, least-layered dbt-on-Airflow pattern (the textbook example
in dbt's own docs); PythonOperator (sibling DAG) wins when you want
pre-flight logic, custom error handling, or programmatic stdout capture
around the invocation.

Shape — Option C: one task per service boundary.

    collect  →  dbt_silver  →  dbt_gold  →  notify
    (Python)    (Bash)         (Bash)        (Python)

Only `collect` and `notify` are PythonOperator: `collect` calls
Collectorv2.run() in-process (Python-shaped work — wrapping it in bash
would require either subprocess or `python -c "..."`, neither of which
buys anything); `notify` will post a Slack/email summary in chapter 6
(also Python-shaped work).

The bash-vs-python contrast is concentrated on the two dbt tasks where
the difference actually matters. This commit (5.2a) ships them as
BashOperators with placeholder `echo` commands; commit 5.2b swaps the
echo for `cd /opt/airflow/dbt_youtube && dbt run --select tag:...`.

Retry policy: same as the python variant.

Schedule: 0 8 * * * UTC, past the YouTube API's PT-midnight quota reset.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ── Task callables ───────────────────────────────────────────────────────────
# `collect` and `notify` are Python-shaped work and use PythonOperator in
# both DAG variants. The two callables here are intentional copies of the
# python_operator/youtube_pipeline.py versions — keeping the diff between
# the two DAG files focused on the dbt tasks (the actual point of the
# comparison).

def run_collector(**context):
    """
    Invoke Collectorv2.run() inside the Airflow worker.

    On QuotaExhaustedError we convert to AirflowFailException — the task
    fails red in the UI but Airflow doesn't retry (the quota won't reset
    within the retry window).
    """
    from ingestion.Collectorv2 import run as collector_run, QuotaExhaustedError

    try:
        collector_run()
    except QuotaExhaustedError as e:
        raise AirflowFailException(
            f"Quota exhausted — not retrying until PT-midnight reset: {e}"
        ) from e


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
}


with DAG(
    dag_id="youtube_pipeline_bash",
    description="YouTube pipeline — BashOperator dbt variant. Sibling: youtube_pipeline_python.",
    start_date=datetime(2026, 4, 19),
    schedule="0 8 * * *",          # 08:00 UTC daily
    catchup=False,                 # never backfill: stale snapshots aren't useful
    max_active_runs=1,             # no concurrent runs; avoids quota double-spend
    default_args=default_args,
    tags=["youtube", "bronze", "chapter-5", "bash_operator"],
) as dag:

    collect = PythonOperator(
        task_id="collect",
        python_callable=run_collector,
    )

    # Placeholder dbt tasks — commit 5.2b replaces with the real invocation:
    #   bash_command="cd /opt/airflow/dbt_youtube && dbt run --select tag:silver"
    # Locking the operator type in NOW means the swap to real dbt is a
    # bash_command edit only — no DAG-shape change in 5.2b.
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command='echo "[placeholder] dbt silver models will run here (chapter 5.2b)."',
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command='echo "[placeholder] dbt gold models will run here (chapter 5.2b)."',
    )

    notify_task = PythonOperator(
        task_id="notify",
        python_callable=notify,
    )

    collect >> dbt_silver >> dbt_gold >> notify_task
