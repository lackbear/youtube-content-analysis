# Dashboard — live-state barometer

A read-only Streamlit app over the local DuckDB warehouse at
`data/warehouse/dev.duckdb`. Block 4 in the project's composable stack
— it doesn't care whether dbt was triggered by `make run`, by Airflow,
or by hand; it just reads whatever's there and renders it.

## Run it

```bash
# from the project root, in the same venv used for dbt
venv/Scripts/python -m pip install -r dashboard/requirements.txt
venv/Scripts/streamlit run dashboard/app.py
```

Open: <http://localhost:8501>

(See *Three local services* below for why this port and not 8080.)

## What each section shows

| Section | Source | When it fills out |
|---|---|---|
| **Pipeline freshness** | `main_silver.stg_video_stats` | After the first `dbt run` |
| **Channels · dim_channel** | `main_silver.dim_channel` | After chapter 6 commit 1 |
| **Silver — snapshot activity** | `main_silver.stg_video_stats` | After the first `dbt run` |
| **Gold — 7-day video growth** | `main_gold.fct_video_growth_7d` | After chapter 5 commit 1 |
| **Why gold is small** | `main_silver.stg_video_stats` | Heatmap of `video_id` overlap between every pair of snapshot dates — the visual answer to "why is gold sparse?" |
| **What's next** | `has_table()` checks | Cards flip from "coming soon" to ✓ shipped as the underlying tables come online |

## The auto-light-up mechanic

Every section is gated by a `has_table(schema, name)` check. When a new
dbt model lands in the warehouse (e.g. `fct_channel_velocity` lands in
chapter 6), the dashboard surfaces it with **zero code changes** — just
refresh the page.

That's why the "What's next" cards aren't decoration. They're the
project's roadmap rendered live: anything labelled *coming soon* is
genuinely missing from `dev.duckdb`; a refresh after the next dbt run
flips the relevant card to ✓.

## Three local services, distinct ports

| Service | URL | What it shows |
|---|---|---|
| Airflow | http://localhost:8080 | DAG runs, task logs, scheduler |
| dbt docs | http://localhost:8081 | Lineage graph, model SQL, sources |
| **Dashboard** | http://localhost:8501 | Live-state barometer (this) |

Bring up Airflow + dbt docs separately:

```bash
make airflow-up                                              # 8080
cd dbt_youtube && DBT_PROFILES_DIR=. dbt docs serve --port 8081
```

## Cache TTL

Queries are wrapped in `st.cache_data(ttl=10)` — Streamlit re-queries
the warehouse no more often than every 10 seconds, even if you mash the
refresh button. Plenty for a dev tool; bump or remove if you want
sub-second freshness.

## Troubleshooting

- **"No warehouse found"** → run `cd dbt_youtube && DBT_PROFILES_DIR=. dbt run` first.
- **Port 8501 already in use** → `streamlit run dashboard/app.py --server.port 8502`.
- **Empty silver / gold** → check `airflow tasks test youtube_pipeline_python collect <YYYY-MM-DD>` in the Airflow container, then re-run dbt.
