# YouTube Data Pipeline

> End-to-end analytics pipeline tracking YouTube competitor channels across two content niches (AI & Automation, Health & Longevity) — built to showcase modern data-engineering patterns: medallion architecture, Hive partitioning, quota-aware ingestion, containerised orchestration, dbt+DuckDB transformation, live Streamlit dashboard, and a curator loop that detects stale channels and queues AI-sourced replacements.

![Python](https://img.shields.io/badge/python-3.10-blue?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/docker-containerized-2496ED?logo=docker&logoColor=white)
![Airflow](https://img.shields.io/badge/orchestration-Airflow-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/transform-dbt%20%2B%20DuckDB-FF694B?logo=dbt&logoColor=white)
![Streamlit](https://img.shields.io/badge/dashboard-Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Status](https://img.shields.io/badge/status-active-brightgreen)
![Chapter](https://img.shields.io/badge/chapter-6%2F6-success)

## What it does

Daily snapshots of the latest *N* videos per channel via the **YouTube Data API v3**, partitioned Hive-style (`date=YYYY-MM-DD/channel_id=UCxxxx/`) into Parquet. From there:

1. **Airflow** schedules and orchestrates the pipeline (two parallel DAG variants — one PythonOperator, one BashOperator — to demonstrate operator trade-offs side-by-side).
2. **dbt + DuckDB** reads the parquet directly and builds silver (`stg_video_stats`, `dim_channel`) + gold (`fct_video_growth_7d`) tables.
3. A **Streamlit dashboard** reads the warehouse read-only and renders pipeline freshness, growth signals, and the channel-overlap heatmap that explains the bronze "latest-N" limitation.
4. A **curator loop** (`detect_stale.py` + `discover.py`) flags inactive channels and queues AI-sourced replacements via a `candidates.csv` review flow.

Every API run is **quota-aware** — soft-warn at 80%, hard-stop at 95% of the 10 000-unit daily budget, recorded as an append-only JSONL ledger.

## Architecture

```mermaid
flowchart LR
    Y[YouTube API v3] --> C[Collector<br/>quota-aware]
    REG[(competitors.csv<br/>registry)] --> C
    C --> B[(Bronze<br/>parquet)]
    B --> DBT[dbt + DuckDB]
    DBT --> SG[(Silver + Gold)]
    SG --> D[Streamlit<br/>dashboard]
    SG --> CUR[Curator<br/>detect_stale → discover]
    CUR -. candidates.csv<br/>review + promote .-> REG
```

**Airflow** schedules every block via three DAGs (`youtube_pipeline_python`, `youtube_pipeline_bash`, `youtube_curator`). The dotted edge is the chapter-6 feedback loop: stale channels are detected from gold, AI-sourced replacements land in `candidates.csv`, and promoted rows flow back into the registry the collector reads on its next run.

## Quickstart — from `git clone` to all three services running

> Tested on Windows 11 + Docker Desktop (WSL2 backend) + Python 3.10. macOS / Linux work the same; only the venv-activation command differs.

### 0 · Prerequisites

- **Docker Desktop** with WSL2 backend (Windows) or any Docker (macOS / Linux).
- **Python 3.10** on the host — needed *only* to run the Streamlit dashboard outside Docker. The collector, dbt, and Airflow all run inside containers.
- **A YouTube Data API v3 key** — get one at [console.cloud.google.com](https://console.cloud.google.com/apis/credentials).

### 1 · Clone + configure secrets

```bash
git clone https://github.com/lackbear/youtube-content-analysis.git
cd youtube-content-analysis/Youtube_content_analysis
cp .env.example .env
# Open .env and paste your real YOUTUBE_API_KEY.
```

### 2 · Start the Airflow stack (Postgres + scheduler + webserver)

```bash
docker compose up -d postgres airflow-init airflow-scheduler airflow-webserver
#   or, with make:
make airflow-up
```

Wait ~30 s, then open <http://localhost:8080> and log in with `admin` / `admin`. You should see three DAGs registered:

- `youtube_pipeline_python` — collect → silver → gold → notify (+ `detect_stale` in parallel)
- `youtube_pipeline_bash` — same DAG via BashOperator
- `youtube_curator` — weekly stale-detect + discover

### 3 · Run the pipeline once

In the Airflow UI, click the **▶** next to `youtube_pipeline_python`. Watch the graph go green — about 1-2 minutes for the legacy 12 channels.

This creates `data/warehouse/dev.duckdb` **owned by the container's UID 50000**. From this point on, **don't run `dbt run` or `make run` from your host shell** — that would write the warehouse with your host UID and the next DAG run would die with `Permission denied`. Use the Airflow path or `docker compose exec airflow-scheduler ...` for ad-hoc dbt commands.

### 4 · Run the dashboard

```bash
python -m venv venv
venv\Scripts\activate          # Windows PowerShell / cmd
# source venv/bin/activate     # macOS / Linux

python -m pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
#   or, with make:
make dashboard
```

Open <http://localhost:8501>. Sidebar shows three nav items: **Overview**, **Analytics** (Channels / Growth / Curator tabs), **Admin** (Ops / Diagnostics tabs). The **Admin → Ops** tab calls Airflow's REST API and can trigger DAGs from inside the dashboard.

### 5 · (Optional) dbt docs

```bash
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt_youtube && dbt docs generate && dbt docs serve --port 8081 --host 0.0.0.0"
```

Lineage graph at <http://localhost:8081>.

---

### Shutting it down

```bash
# Ctrl+C in the streamlit terminal stops the dashboard.
docker compose down              # stop Airflow; keeps DAG history in postgres_data
docker compose down --volumes    # nuclear: also wipes Airflow metadata
```

### Common gotchas (pre-paid for you)

| Symptom | Why | Fix |
|---|---|---|
| `dbt_silver` task fails with `IO Error: ... Permission denied` on `dev.duckdb` | Host process (Streamlit, manual `dbt run`) created the file as your host user; the Airflow container running as UID 50000 can't write to it | Stop Streamlit, `Remove-Item data\warehouse\dev.duckdb*`, re-trigger the DAG. dbt rebuilds it with container ownership. |
| Dashboard's **Admin → Ops** tab shows `401 Unauthorized` | Airflow 2.10 ships only `session` auth on `/api/v1/*`; we need `basic_auth` too | Already in `docker-compose.yml`; if you cloned an older snapshot run `docker compose up -d --force-recreate airflow-init airflow-webserver airflow-scheduler` |
| Sidebar nav missing in the dashboard | Browser cached the old auto-discovery layout | Hard-refresh: **Ctrl + Shift + R** |
| `Quota exceeded` from the collector | Hit 95 % of the 10 000-unit daily YouTube API budget | `make quota-today` shows usage; wait for the daily reset (midnight Pacific) |
| `no such service: airflow-init` from `docker compose` | Running from the wrong directory | `cd youtube-content-analysis/Youtube_content_analysis` first |

## Tech stack

| Layer | Tool | Chapter |
|---|---|---|
| Ingestion | Python 3.10 · `google-api-python-client` · `pandas` · `pyarrow` | 1, 2 |
| Packaging | Docker multi-stage · docker-compose | 3 |
| Orchestration | Airflow (LocalExecutor) + PostgreSQL (Airflow metadata) | 4 |
| Transformation | dbt-core + dbt-duckdb (parquet-native, swappable to dbt-databricks in phase 2) | 5 |
| Visualization | Streamlit (read-only over the local DuckDB) | 5.5 |
| Curator | DuckDB-based stale detection + AI-driven discovery + queue review | 6 |
| Future / Phase 2 | dbt-databricks adapter (one-line swap; models migrate verbatim) | — |

## Three local services

| Service | URL | What it shows |
|---|---|---|
| Airflow | http://localhost:8080 | Three DAGs: `youtube_pipeline_python`, `youtube_pipeline_bash`, `youtube_curator` |
| dbt docs | http://localhost:8081 | Lineage graph, model SQL, sources |
| Dashboard | http://localhost:8501 | Live pipeline state, channel registry, growth signals, curator queue |

![Dashboard overview](dashboard/screenshots/overview.png)

## Roadmap

> **Composable by design.** Each chapter is a self-contained block over an idempotent contract with the chapter below it. You can stop anywhere and still have a working system — just `python ingestion/Collectorv2.py` produces bronze parquet on disk; chapter 4 schedules that same script; chapter 5 reads the same parquet through dbt; phase 2 swaps DuckDB for Databricks without touching the models. **Run any subset, replay any block — every layer is idempotent over its inputs.**

| # | Chapter | Status |
|---|---|---|
| 1 | Collector v1 — working daily snapshot | ✅ shipped |
| 2 | Collector v2 — quota tracking, sub-partitioning, ingestion timestamps | ✅ shipped |
| 3 | Containerisation — Docker multi-stage + compose | ✅ shipped |
| 4 | Orchestration — Airflow + PostgreSQL | ✅ shipped |
| 5 | Transformation — dbt + DuckDB (Silver / Gold) | ✅ shipped |
| 5.5 | Live-state Streamlit dashboard | ✅ shipped |
| 6 | Dynamic competitor management — CSV registry, stale detection, AI-curated discovery, auto-promotion, bulk import, 200-row FIFO cap | ✅ shipped |

**Future / swappable blocks** *(not numbered — they replace pieces above without changing the contract)*

| Phase | Scope | Status |
|---|---|---|
| 2 | Cloud lakehouse — re-platform medallion onto Databricks Community Edition (dbt models migrate verbatim) | ⏳ stretch |

## Repo layout

Each block lives in its own folder with its own `requirements.txt` so it can be installed and run independently.

```
├── ingestion/                  # collector
│   ├── Collectorv2.py          # active collector
│   ├── Collector.py            # v1, kept for the diff narrative
│   └── requirements.txt
│
├── dags/                       # Airflow
│   ├── python_operator/        # youtube_pipeline_python
│   ├── bash_operator/          # youtube_pipeline_bash
│   └── curator/                # youtube_curator (weekly)
│
├── dbt_youtube/                # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml            # local DuckDB at ../data/warehouse/dev.duckdb
│   ├── models/
│   │   ├── bronze/sources.yml  # parquet + competitors.csv
│   │   ├── silver/             # stg_video_stats, dim_channel
│   │   └── gold/               # fct_video_growth_7d
│   └── requirements.txt
│
├── dashboard/                  # Streamlit
│   ├── app.py
│   ├── screenshots/
│   └── requirements.txt
│
├── scripts/                    # curator + one-off utilities
│   ├── detect_stale.py         # flags channels with no recent posts
│   ├── discover.py             # validates AI-sourced candidates
│   ├── API_test.py
│   └── get_channel_id.py
│
├── config.yaml                 # refresh_days, cache_max_age, quota, …
├── competitors.csv             # channel registry — handle, niche, tier, active
├── Dockerfile                  # multi-stage, non-root
├── docker-compose.yml
├── Makefile
│
├── data/                       # gitignored
│   ├── raw/video_stats/        # bronze parquet
│   ├── warehouse/dev.duckdb    # dbt output
│   └── curator/                # stale_channels.csv, candidates.csv
│
├── logs/                       # gitignored — JSONL event + quota ledger
└── docs/
    ├── ARCHITECTURE.md         # full chapter-by-chapter narrative
    ├── COLLECTORV2.md          # collector user guide
    ├── MAKE.md
    └── prompts/                # AI prompts (source-200-channels.md, …)
```

## Deep dive

- [**docs/COLLECTORV2.md**](docs/COLLECTORV2.md) — user guide for the collector: setup, all attributes, CLI recipes, output layout
- [**docs/ARCHITECTURE.md**](docs/ARCHITECTURE.md) — full design rationale, chapter-by-chapter narrative, target architecture

---

*Built as a working pipeline **and** a learning log — every chapter is a standalone file (`Collector.py`, `Collectorv2.py`, …) so the diff tells the story.*
