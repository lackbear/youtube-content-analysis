"""
airflow_dag_curator.py — weekly curator DAG (chapter 6 commits 3 + 3.5).

Two-task DAG. Both run weekly on Saturdays:

    discover  →  promote

`discover` reads `data/curator/candidates_input.csv`, validates each
handle against the YouTube API, dedups, appends new validated rows to
`data/curator/candidates.csv` with `status=pending`. Manual review then
flips status `pending → accepted | rejected`.

`promote` (chapter 6 commit 3.5) closes the curator loop. For each row
in `stale_channels.csv` flagged stale, find an `accepted` candidate in
the matching niche (legacy stales fall through to "any niche") and:
  - flip the stale row to `active=false` in competitors.csv
  - append the candidate as `active=true`
  - flip the candidate's status `accepted → promoted`

`detect_stale` itself runs daily inside the main `youtube_pipeline_*`
DAGs (so its output is fresh by Saturday); no need to duplicate it here.

Schedule: Saturdays 09:00 UTC.

If `candidates_input.csv` is missing or empty, discover errors out and
promote is skipped — acceptable for "nothing to do this week."
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_discover(**context):
    """Validate AI-sourced candidates and write candidates.csv."""
    from scripts.discover import discover
    discover()


def run_promote(**context):
    """
    Apply the queue replacement: deactivate stale channels in
    competitors.csv and activate accepted candidates in their place.
    """
    from scripts.promote_candidates import promote
    promote(dry_run=False)


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
        "Weekly — validates AI-sourced candidate channels against the YouTube API "
        "and promotes accepted candidates into competitors.csv."
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

    promote_task = PythonOperator(
        task_id="promote",
        python_callable=run_promote,
    )

    discover_task >> promote_task
