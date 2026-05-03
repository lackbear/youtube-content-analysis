# YouTube Data Pipeline — Architecture & Handoff Notes

*Last updated: 2026-05-03 — end of Chapter 4 (Orchestration)*

This document has two audiences. The first is **future-you starting a new chat**:
it is a context pack so the next session can pick up exactly where this one
stopped without re-explaining the last month of work. The second is the
**you who will eventually publish content about this project** — so the same
document also narrates *why* each decision was made, in plain language, with
references to the industry patterns being borrowed. Read it front-to-back once,
then keep it open as a reference.

---

## 1. Project context

The project is a portfolio-grade YouTube competitor analytics pipeline. It
tracks ~12 channels across four content niches (longevity, robotics, finance,
data engineering), snapshots the last N videos' stats daily, and — in its
target state — feeds a Bronze → Silver → Gold medallion model that powers
both dashboards and an auto-publishing content pipeline (YouTube, Instagram,
TikTok).

A secondary, deliberate goal is the project itself as content. Every evolution
of the collector is kept as a standalone file (`Collector.py`, `Collectorv2.py`,
…) so each chapter stands on its own and can be narrated one problem at a
time. The file diff between versions is the story.

Current stack on the happy path: Python 3.10, `google-api-python-client`,
`pandas` + `pyarrow`, YAML for configuration, date-partitioned Parquet on the
local filesystem, JSONL event logs. The daily YouTube Data API v3 quota is
10 000 units and every design choice around the collector respects that
constraint.

---

## 2. Chapter 2 — what we shipped in this session

Chapter 1 (`Collector.py`) was a working daily snapshot collector. Chapter 2
(`Collectorv2.py`) hardens it for real-world operation by solving four
specific problems, each of which is a textbook data-engineering concept worth
understanding in its own right.

### 2.1 Channel sub-partitioning (the Option-B change)

**Before.** A single run wrote every channel's rows to
`data/raw/video_stats/date=YYYY-MM-DD/video_stats.parquet`. Running a second
config file (different channels) on the same day would overwrite the whole
file and silently delete the first run's data.

**After.** The path evolved to
`data/raw/video_stats/date=YYYY-MM-DD/channel_id=UCxxxx/video_stats.parquet`.
Each `(date, channel_id)` pair gets its own file. Two configs running the
same day never collide because they never touch the same files. The runs are
independent the way they feel in your head.

This is exactly the Hive-style partitioning convention that Spark, Athena,
Databricks, and every modern query engine understand natively. Read the
whole `data/raw/video_stats/` directory and the engine turns the folder names
into filterable columns (`WHERE date = '2026-04-17' AND channel_id = 'UC…'`
pushes partition pruning down to the file system).

Semantics on re-run: **overwrite-per-channel**, not merge-per-row. The bronze
layer is a snapshot, not a history. If you run the collector twice on the
same channel on the same day, the second run's values (views, likes, etc.)
win. This is idiomatic for the bronze tier in a medallion architecture and
matches how Databricks Auto Loader, dbt's `full_refresh`, and similar tools
behave. The word "idempotent" comes from math — the same input applied any
number of times produces the same output. That is the property we bought.

### 2.2 `ingestion_timestamp` column on every row

A pure snapshot loses information. Looking at a parquet file, you could tell
what the stats *were*, but not *when* they were observed — the filename's
`date=` is UTC-day granularity at best, and it loses the minute you copy the
file elsewhere. So every row now carries a single `ingestion_timestamp` ISO
string (uniform within a run, distinct across runs). Downstream silver/gold
models can sort by it, window over it, or ignore it. It costs eight bytes per
row and gives you a trapdoor into the future. This is a near-universal
bronze-layer convention and often appears as `_ingest_ts`, `loaded_at`, or
`etl_inserted_ts` in real warehouses.

### 2.3 Cache loader handles both layouts

The collector's 7-day refresh window (configurable via `refresh_days`) reads
back the last week of data to decide whether each video should be re-fetched
(to track velocity) or skipped (already fresh enough). With the path layout
changing, the obvious naive implementation would break the moment you ran
v2: the cache loader would only see v2 files, forget everything v1 wrote,
and re-fetch a week of data you already had.

`load_seen_videos` now globs four patterns:
`date=*/video_stats.parquet`, `date=*/video_stats.csv`,
`date=*/channel_id=*/video_stats.parquet`,
`date=*/channel_id=*/video_stats.csv`. It unions the results and
de-duplicates by `video_id` taking the max `fetched_date`. The transition is
migration-free — no script to run, no data to move, the old files simply age
out of the 7-day window on their own. The smoke test confirms: `102 known
videos (legacy 100 + new 2)` after a mixed-layout run.

Generalising: this is the **backward-compatible read / forward-compatible
write** pattern. Readers accept both old and new formats; writers only
produce new. It is how every schema migration that does not lose data
eventually works.

### 2.4 Daily API quota tracking

Before this chapter, the collector logged an *estimate* of API units used
in the event log, but had no way to know today's cumulative usage before
starting. Two runs in the same day, a rogue `--channels` CLI flag, or a
bug in another script sharing the API key could silently push you over the
10 000-unit ceiling and then every request for the rest of the day would
return 403 with no useful context.

The solution is an append-only JSONL file per UTC day at
`logs/quota/YYYY-MM-DD.jsonl`. Each run contributes exactly one line. Reading
the file and summing `units_this_run` gives today's total — no separate
running counter to keep in sync, no possibility of the counter drifting from
the log because they are the same thing.

One line per run looks like this:

```json
{
  "timestamp":       "2026-04-17T10:30:00+00:00",
  "units_this_run":  28,
  "units_today":     312,
  "quota_limit":     10000,
  "quota_remaining": 9688,
  "channels":        3,
  "videos_fetched":  15,
  "run_outcome":     "success"
}
```

On every run, before any API call, `quota_preflight(estimated_units)` reads
the file, adds the estimate, and returns one of three decisions. **ok** under
80 %, **warn** between 80 % and 95 % (run continues, with a loud log
message), **abort** at or above 95 % (run stops, records a line with
`run_outcome: "aborted_over_quota"` so the audit trail explains why it
abstained). The 5 % buffer above the stop threshold is real quota left for
manual testing, single-channel debugging, or the `get_channel_id.py`
utility script.

The thresholds (`QUOTA_LIMIT`, `QUOTA_WARN_PCT`, `QUOTA_STOP_PCT`) are read
from `config.yaml` with hard-coded defaults so the file does not need editing
to get sensible behaviour. Drop an `api.quota_limit: 20000` line in and it
picks up automatically.

The unit estimator (`estimate_run_units`) uses the public
[YouTube Data API v3 cost model](https://developers.google.com/youtube/v3/determine_quota_cost):
each `.list` call is 1 unit regardless of parts requested. A run costs
`N_channels` for `channels.list` resolution, `N_channels` for
`playlistItems.list`, and `ceil(V_ids / 50)` for `videos.list` batches. For
12 channels × 10 videos each, that is 27 units — deeply inside the daily
budget.

---

## 3. Pain points solved — before / after at a glance

| # | Pain point | Before (v1) | After (v2) |
|---|---|---|---|
| 1 | Multiple configs overwriting each other | Second run clobbers the whole day's partition | Per-channel subfolder; disjoint writes never collide |
| 2 | No visibility into daily quota usage | Had to read the event log and eyeball `api_units` | `logs/quota/*.jsonl` + `read_units_used_today()` |
| 3 | No protection against blowing the quota | Would hit 403s mid-run with no warning | 80 % warn, 95 % abort, checked before any API call |
| 4 | Snapshots had no temporal fidelity beyond "the day" | Could not tell *when* within a day a row was captured | Every row carries `ingestion_timestamp` |
| 5 | Cache would have broken the moment the layout changed | `load_seen_videos` globbed only the old pattern | Glob both, union, de-duplicate on `video_id` |
| 6 | Same-day re-run semantics were implicit | Overwrote everything; fine but undocumented | Overwrite-per-channel; documented; intentional bronze behaviour |

The two bugs you fixed in the previous session still stand and are
preserved in v2: `VIDEO_STATS_FILE` no longer hard-coded over the config
value, and preflight reads `_cfg["api"]["key_env_var"]` instead of
assuming `YOUTUBE_API_KEY`.

---

## 4. Current architecture (post-v2)

### 4.1 File layout

*(Layout below reflects the chapter-5.5 reorganisation into composable-block
folders. Pre-reorg, `Collector.py` / `Collectorv2.py` / `requirements.txt` /
`API_test.py` / `get_channel_id.py` lived at the project root; they were
moved into block folders to make each block self-contained — own code, own
deps, runnable independently.)*

```
Youtube_content_analysis/
├── ingestion/                # Block 1
│   ├── __init__.py
│   ├── Collector.py          # Chapter 1 — kept for the story
│   ├── Collectorv2.py        # Chapter 2 — active
│   └── requirements.txt      # pandas, pyarrow, google-api-python-client, …
├── scripts/                  # one-off utilities
│   ├── API_test.py           # Sanity ping for the API key
│   └── get_channel_id.py     # One-off resolver for new handles
├── dags/                     # Block 2 — Airflow DAGs (chapter 4)
│   └── airflow_dag_v1.py
├── dbt_youtube/              # Block 3 — dbt project (chapter 5)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── requirements.txt
│   └── models/
├── dashboard/                # Block 4 — Streamlit barometer (chapter 5.5)
│   ├── app.py
│   └── requirements.txt
├── config.yaml               # All behaviour-driving knobs
├── competitors.csv           # Channel registry (chapter 6 will expand this)
├── .env                      # YOUTUBE_API_KEY (gitignored)
├── data/                     # gitignored
│   ├── raw/
│   │   └── video_stats/
│   │       ├── date=2026-04-16/
│   │       │   └── video_stats.parquet              ← legacy flat (v1)
│   │       └── date=2026-04-17/
│   │           ├── channel_id=UCAohrrjG…/video_stats.parquet   ← v2
│   │           └── …
│   └── warehouse/
│       └── dev.duckdb                               ← chapter 5 dbt output
├── logs/                     # gitignored
│   ├── 2026-04-17.jsonl                             ← event log
│   └── quota/
│       └── 2026-04-17.jsonl                         ← one line per run
└── docs/
    └── ARCHITECTURE.md                              ← this file
```

### 4.2 The run lifecycle (v2)

Trace the path of a single run in your head before the next session — it
will make every Airflow decision obvious later.

1. Import loads `config.yaml` and all the module-level constants
   (`REFRESH_DAYS`, `BATCH_SIZE`, `QUOTA_LIMIT`, …).
2. CLI argparse potentially overrides `output.format`, `channels`,
   `max_videos`.
3. `preflight()` validates inputs and environment — no network yet.
4. `log_event("run_start", …)` records the intent.
5. **`quota_preflight(estimated)`** — reads today's JSONL, decides ok / warn
   / abort. If abort, records a line with `outcome: "aborted_over_quota"`
   and returns without hitting the API.
6. `_build_client()` creates the YouTube client (first reference to the API
   key).
7. `channels.list` resolves each handle or ID to a channel object with an
   uploads-playlist ID. Each resolution is a `channel_resolved` event.
8. `load_seen_videos()` reads the entire `data/raw/video_stats/` tree in
   both layouts and returns `{video_id: last_fetched_date}`.
9. For each resolved channel, `playlistItems.list` fetches the latest N
   video IDs. `should_fetch()` decides per-video whether to enqueue or
   skip.
10. If the queue is empty, record a `no_work` quota line and exit.
11. `videos.list` is called in batches of up to 50 IDs via
    `fetch_stats_batch`. Deleted / private videos come back missing;
    those rows are marked `status: "missing"` with empty attribute values.
12. `write_output()` stamps each row with `ingestion_timestamp`, groups by
    `channel_id`, and writes one file per channel partition.
13. Final event: `run_complete` to the event log, `record_quota_usage()` to
    the quota log.

### 4.3 Configuration surface

`config.yaml` drives everything. Key groups:

- `api.key_env_var` — the name of the env variable to read (not the key).
- `api.max_retries`, `api.batch_size` — network behaviour.
- *Optional* `api.quota_limit`, `api.quota_warn_pct`, `api.quota_stop_pct` —
  defaults are sensible (10000 / 0.80 / 0.95); overriding is for when you
  pay for more quota.
- `collection.max_videos_per_channel`, `collection.refresh_days` — the
  unit of work per run and the velocity-tracking window.
- `output.video_stats_file`, `output.format` — historical; format is also
  overrideable via `--format parquet|csv`.
- `attributes` — list of attribute names to request. Fewer = smaller rows,
  same API cost.
- `channels` — list of handles with niche metadata for later grouping.

### 4.4 Observability

Two append-only JSONL streams live under `logs/`. The event log
(`YYYY-MM-DD.jsonl`) is narrative — one line per noteworthy thing that
happened during any run that day. The quota log
(`quota/YYYY-MM-DD.jsonl`) is ledger-like — one line per run, schema-stable,
summable to a daily total. JSONL is chosen over CSV for logs because each
line is independently parseable, which survives partial writes and concurrent
appends far better. Pandas can read either with `pd.read_json(path, lines=True)`.

### 4.5 Idempotency map

| Component | On second run, same day | Notes |
|---|---|---|
| Per-channel parquet | Overwritten | Last run wins; fresher stats |
| Event log | Appended | Two `run_complete` entries, intentional |
| Quota log | Appended | Cumulative total grows monotonically |
| `load_seen_videos` cache | Skips already-fetched-today videos | API cost on second run is tiny |

---

## 5. Target architecture (what the next sessions are for)

The medallion is the destination. Everything between here and there is
plumbing.

### 5.1 The medallion

**Bronze — raw snapshots.** What you have today. Append-only, partitioned,
lightly typed. Never mutated in place. Answers the question "what did we
observe and when?"

**Silver — cleaned, conformed, typed.** Derived from bronze with casts and
deduplication. One row per `(video_id, observation_date)` with `views`,
`likes`, `comments` as integers (not strings), `duration` parsed to a
`timedelta`, `published_at` as a timestamp. Built via dbt models that read
the parquet directly or via a Spark job. Answers "what is the cleaned state
of the world right now?"

**Gold — analytical rollups.** One row per question. Example gold models:
`channel_velocity_7d` (views added in the last 7 days per channel),
`video_viral_index` (like/view ratio normalised by channel average),
`niche_leaderboard` (rankings by niche, by week). Gold is where the content
pipeline draws from — short-form video analysis, weekly leaderboards,
channel comparison charts.

### 5.2 Docker — Option A POC

A `docker-compose.yml` with two services. `postgres:14` holds Airflow's
metadata database. `apache/airflow:2.x` runs the scheduler, webserver, and
executor in standalone mode. Mount the project directory into the Airflow
container at `/opt/airflow/dags` so edits to Python files are reflected
without rebuilding. Network the two via a named Docker network so the
Airflow connection string is deterministic.

Why two services and not one? Airflow needs durable state (DAG run history,
task instance state, connection secrets) that would vanish on container
restart if kept in SQLite. PostgreSQL survives restarts because its data
volume is mounted outside the container. This is the standard separation:
stateful services keep their state in named volumes, stateless services
restart freely.

Why Airflow and not Dagster or Prefect for the first orchestrator? Airflow
has the biggest ecosystem and the most Stack Overflow surface area; every
pattern you hit has been hit before. Dagster's abstractions are arguably
cleaner (software-defined assets, typed IO managers) and would be a great
follow-up chapter. Prefect 2+ is the slickest UX but has a smaller
community. For a first orchestration exposure, Airflow is the most
transferable skill.

### 5.3 The Airflow DAG

One DAG, daily schedule (e.g. `@daily` at 06:00 UTC so the previous UTC
day's videos are settled in YouTube's counting). Tasks:

1. `preflight` — check the quota file, abort the run if yesterday's
   overflow rolled over. Uses a `PythonOperator` calling
   `Collectorv2.quota_preflight`.
2. `collect` — `BashOperator` or `PythonOperator` that invokes
   `Collectorv2.run`. Idempotent via the partition layout.
3. `dbt_bronze_to_silver` — `BashOperator` running `dbt run
   --select tag:silver`. Runs inside the Airflow worker for simplicity.
4. `dbt_silver_to_gold` — `dbt run --select tag:gold`.
5. `publish_metrics` — write a daily summary to a JSON that the content
   pipeline consumes later.

Retries: two retries with exponential backoff on `collect` only; the dbt
steps are idempotent so they retry naturally. Alerting via email or Slack
on task failure — wire up via `on_failure_callback`.

### 5.4 dbt

`dbt-core` with the `dbt-duckdb` adapter is the fastest path from parquet
files to a working medallion without standing up a real warehouse. DuckDB
reads parquet directly, so the silver models are CTAS statements over the
bronze path. When you graduate to Databricks, the same dbt project runs
against the Databricks adapter with minimal changes.

Project layout to aim for:

```
dbt_youtube/
├── dbt_project.yml
├── models/
│   ├── bronze/
│   │   └── sources.yml         # declares the parquet path as a source
│   ├── silver/
│   │   ├── stg_video_stats.sql
│   │   └── dim_channel.sql
│   └── gold/
│       ├── fct_channel_velocity.sql
│       └── fct_video_leaderboard.sql
└── tests/
    └── ...
```

### 5.5 Databricks Community Edition (Phase 2)

When the medallion runs locally on DuckDB, re-platform it on Databricks
Community Edition. The dbt models migrate nearly verbatim. Bronze parquet
uploads to DBFS or an attached S3 bucket. Silver and gold become Delta
tables. Spark replaces pandas for any row counts past ~1M. Every partition
convention you already followed (`date=`, `channel_id=`) works unchanged —
this is the payoff of adopting Hive-style partitioning from day one.

### 5.6 Content pipeline (Phase 3)

Gold models feed a generation script that builds short-form video scripts
from weekly leaderboards, renders them to MP4, and publishes to YouTube /
Instagram / TikTok. This is where the "project as content" thesis becomes
monetisable. Not this chapter. Not next chapter. But the partitioning and
the medallion exist so this is possible without a rewrite.

---

## 6. Kicking off the next session — suggested prompt

Paste something like this into the new conversation. It is deliberately a
*learning* prompt, not a *do* prompt — it frames the session as Claude
teaching, not executing blind.

> I'm continuing a YouTube data pipeline project at
> `C:\Users\moham\Documents\Claude\Projects\youtube-data-pipeline\Youtube_content_analysis`.
> Read `docs/ARCHITECTURE.md` first — it has the full context of what's
> built and what the target state is. I'm ready to start Chapter 3:
> stand up Docker + Airflow + PostgreSQL locally (Option A POC) to
> schedule `Collectorv2.py` daily.
>
> Before writing any files, I want you to explain:
>
> 1. Why Airflow needs a separate PostgreSQL and what would break if I
>    used SQLite.
> 2. How the DAG's tasks should be scoped — one task per Collector phase,
>    or one task for the whole Collector run — and why.
> 3. How Airflow's scheduler knows when a run is "late" or "failed", and
>    what happens to the quota file if a run retries after exhausting
>    quota.
> 4. Which Airflow executor (SequentialExecutor, LocalExecutor,
>    CeleryExecutor) is right for this POC, and what changes when we
>    outgrow it.
>
> Only after we've discussed those four, walk me through the
> `docker-compose.yml` and the first DAG. Ask me questions before making
> assumptions. Keep the single-file-per-chapter convention — the DAG
> should land as a versioned file I can narrate later.

Save that prompt. It turns the session from "write the thing for me" into
"teach me how the thing works, then write it with me". That matches your
learning-first content thesis.

---

## 7. Concepts worth internalising before Chapter 3

None of these are blockers; all of them will make the next session click
faster. If an answer to any of them is fuzzy, ask for the explanation
before the code.

**Idempotency.** An operation is idempotent if running it N times has the
same effect as running it once. Bronze writes are idempotent because they
overwrite the partition. HTTP PUT is idempotent; POST is not. Airflow task
retries rely on this property being true for the task's side effects.

**Hive-style partitioning.** Writing `date=YYYY-MM-DD/channel_id=UCxxxx/`
in the path lets any query engine treat those as virtual columns. Partition
pruning is the biggest single performance win in a query engine and costs
nothing to adopt early.

**Medallion / Lakehouse.** Bronze is raw, silver is cleaned, gold is
modelled. The separation exists so bad data in silver can be rebuilt from
bronze without going back to the API. Popularised by Databricks but
predates it as a pattern under names like "three-tier lake".

**Orchestration vs scheduling vs execution.** cron is a *scheduler*. Bash
is an *executor*. Airflow is an *orchestrator* — it adds dependencies
between tasks, backfill, retries, alerting, lineage. The primitive is the
DAG (directed acyclic graph) of tasks.

**Backfill.** The ability to run a DAG for a historical date as if "today"
were that date. Airflow builds this in via the `execution_date` /
`logical_date` template variable. Your Collector uses UTC "today" at run
time — when you Airflow-ify it, pass the logical date in so backfills hit
the right partitions.

**Stateful vs stateless services in Docker.** Stateless services (Airflow
scheduler, webserver, workers) can be killed and restarted freely.
Stateful services (PostgreSQL) own the ground truth and must own a
persistent volume. The docker-compose file encodes exactly this separation.

**Quota as a resource.** API quota is a non-renewable daily resource like
database connections or a rate-limited API. Every serious pipeline wraps
its external calls in a quota manager. Yours does now.

---

## 8. Common pitfalls to watch for next session

Airflow out of the box writes logs to `/opt/airflow/logs` inside the
container. Mount that path to a host directory too or you will lose logs
on `docker compose down`.

The `airflow` user inside the container has a specific UID. File writes
inside mounted volumes will have that UID on the host — on Linux this
causes permission headaches. The standard fix is setting
`AIRFLOW_UID` in `.env` to match your host user (`id -u`). On Windows
with WSL2 this is usually painless.

Airflow's DAG parsing is eager: every DAG file is imported on every
scheduler tick. Do not put slow imports or I/O at module level in the DAG
file. Keep `Collectorv2.run()` out of the DAG file's top-level code; call
it only inside a task function.

The YouTube API quota resets at midnight *Pacific Time*, not UTC. If you
schedule the DAG to run at 00:00 UTC, you are running mid-afternoon in
California and the quota has not reset yet. Schedule for after ~08:00 UTC
to be safely past the PT midnight rollover on daylight-saving edge days.

DuckDB reads parquet files by glob pattern. If the silver model reads
`data/raw/video_stats/date=*/channel_id=*/video_stats.parquet`, it will
miss the legacy flat files. Either one-shot migrate the legacy files into
the sub-partitioned layout before Chapter 3, or teach the silver model to
union both globs the way `load_seen_videos` does.

---

## 9. Quick reference

**Run the current collector locally**

```bash
cd Youtube_content_analysis
source venv/bin/activate       # or the Windows equivalent
pip install -r ingestion/requirements.txt
python ingestion/Collectorv2.py                                    # uses config defaults
python ingestion/Collectorv2.py --format csv                       # flip output format
python ingestion/Collectorv2.py --channels SiimLand Physionic      # subset of channels
python ingestion/Collectorv2.py --max-videos 5                     # subset of videos
COLLECTOR_CONFIG=config_other.yaml python ingestion/Collectorv2.py # alternate config file
```

**Check today's quota usage**

```bash
python -c "
import json, sys
from pathlib import Path
from datetime import datetime, timezone
d = datetime.now(timezone.utc).date().isoformat()
p = Path(f'logs/quota/{d}.jsonl')
if not p.exists():
    print('0 units used today'); sys.exit()
total = sum(json.loads(l)['units_this_run'] for l in p.read_text().splitlines() if l.strip())
print(f'{total} units used today')
"
```

**Inspect a channel's snapshot**

```python
import pandas as pd
pd.read_parquet('data/raw/video_stats/date=2026-04-17/channel_id=UCxxxx/video_stats.parquet')
```

---

## 10. Chapter 3 — containerising the collector

Chapter 2 hardened the collector. Chapter 3 makes it **portable**. The same
Python script, the same config, the same parquet output — now runnable on
any machine that has Docker, with no "what version of Python do you have?"
conversation. This is the single biggest reason Docker exists, and it is
the pre-requisite for Chapter 4 (Airflow) because Airflow orchestrates
containers, not bare scripts.

Five files landed in this chapter:

- `Dockerfile`        — the blueprint for the container image
- `.dockerignore`     — what is NOT shipped into the image
- `docker-compose.yml`— the local stack definition (one service today, three next chapter)
- `.env.example`      — committed template for the real `.env`
- `Makefile`          — one-word shortcuts over the raw `docker compose` CLI

### 10.1 Why multi-stage — the "builder vs runtime" split

The Dockerfile has two `FROM` statements. The first is the **builder**
stage: it installs `build-essential` (gcc et al), creates a virtualenv,
and pip-installs `requirements.txt` into it. The second is the **runtime**
stage: it copies that completed virtualenv across, adds the source code,
and never touches apt-get again. Result: the final image has no
compilers, no pip cache, no apt metadata — roughly a third of the size
of a naive single-stage build and a smaller attack surface to boot.

Multi-stage builds are the default modern pattern. Before Docker 17.05
you had to build two separate images and copy between them with tar; now
it is one file, one `docker build`, and the intermediate builder layers
are garbage-collected automatically.

### 10.2 Why slim-bookworm, not alpine

If you ask Google "smallest Python Docker image" the answer is alpine.
Alpine is 5 MB. Slim is 50 MB. Seems obvious. It is not.

Alpine uses **musl libc** instead of **glibc**. Python wheels on PyPI are
built against glibc. pandas, pyarrow, numpy — every scientific Python
package you care about — ships prebuilt glibc wheels that install in
seconds. On alpine those wheels do not apply, pip falls back to building
from source, which needs gcc, a working BLAS, LLVM for some packages,
and twenty minutes of build time. Your "small image" becomes a huge one
full of compilers, or you ship broken binaries that segfault at runtime.

`python:3.10-slim-bookworm` (Debian 12, stripped) is the correct default.
It is what Airflow's own official image is built on, what most dbt
adapters target, and what production Python data platforms use. The
extra 45 MB is worth the absence of a week of debugging.

### 10.3 Why a non-root user

Containers share the host kernel. A process running as root *inside* a
container is root as far as the kernel cares. Most container escape CVEs
(the rare cases where a malicious container reaches the host) require
starting from root. Dropping privileges with `USER collector` is
defence-in-depth: if your Python process is ever exploited through, say,
a YAML parsing bug, the blast radius stops at UID 1000.

The Kubernetes community ratified this as the "restricted" PodSecurity
standard. Every production data platform already runs this way. Starting
the habit on day one costs nothing.

### 10.4 Why requirements.txt is copied before the source

Docker builds are layered, and each layer is cached by the hash of its
inputs. When you change `Collectorv2.py` but not `requirements.txt`,
Docker sees:

- Layer "copy requirements.txt" — inputs unchanged, reuse cache.
- Layer "pip install"            — inputs unchanged, reuse cache.
- Layer "copy Collectorv2.py"    — input changed, rebuild this + below.

Result: a code-only edit rebuilds in seconds, not minutes. If you copied
the code first, every code edit would invalidate the (expensive) pip
install layer. This single ordering trick is the highest-impact Docker
optimisation for Python projects.

### 10.5 Why bind mounts for data, not named volumes

The hybrid strategy from §4.1 (ARCHITECTURE pre-Docker) translates
cleanly: `./data` and `./logs` are bind-mounted into the container at
`/app/data` and `/app/logs`. New parquet files land on your host
filesystem, visible in your file explorer, `pd.read_parquet`-able from a
local notebook without any `docker cp` dance. The container is ephemeral;
the data is not.

For Chapter 4, the Postgres container will get a *named* volume —
`postgres_data` — because you never want to look at Postgres's internal
page files directly, and bind-mounting them onto a Windows filesystem is
a known source of performance pain. This is the "stateful services use
named volumes, application data uses bind mounts" pattern.

### 10.6 Why `docker-compose.yml` when there is only one service

`docker run` with all the flags we need (env file, four volumes, memory
limit, named network, resource caps) is a ninety-character one-liner. By
the time Chapter 4 adds Postgres and Airflow it would be three. Compose
moves that whole description into a version-controlled YAML file and
replaces it with `docker compose up`. The pattern scales; the shell
one-liner does not.

### 10.7 Cross-container UID matching (the WSL2 bit)

The `UID` and `GID` build args exist for one specific class of bug: when
the container runs as UID 1000 but the host user is UID 1001, every
file the container writes into a bind-mounted folder is owned by "user
1000" on the host — which on Linux looks to your host shell as "some
stranger", with files you cannot edit without `sudo chown`. Passing
`UID=${UID:-1000}` from the compose file into the Dockerfile means the
container's user and the host user are the same numeric identity. On
WSL2 most users are already UID 1000 so the defaults work. On native
Linux, run `id -u && id -g` and set them in `.env` if they differ.

### 10.8 How this sets up Chapter 4

The compose file is already shaped for the expansion. Adding Airflow
looks like this:

```yaml
services:
  collector: ...        # unchanged
  postgres: ...         # new — named volume for state
  airflow-webserver: ...# new — depends_on: postgres
  airflow-scheduler: ...# new — depends_on: postgres
volumes:
  postgres_data:        # named volume for Postgres
```

Then the Airflow DAG imports `Collectorv2.run` or calls the
`youtube-collector` image via `DockerOperator`. Either approach works —
we will pick one in Chapter 4 after discussing the trade-offs.

### 10.9 Quick reference (Chapter 3)

**First-time setup**

```bash
cp .env.example .env
# edit .env, paste your YOUTUBE_API_KEY
make build
```

**Run the collector once, inside the container**

```bash
make run              # uses config.yaml defaults
make run-debug        # tiny safe run — one channel, three videos
make run-csv          # same as `make run` but --format csv
```

**Inspect**

```bash
make logs             # last 200 lines of the most recent run
make quota-today      # "N units used today"
make shell            # interactive bash inside the container
```

**Clean up**

```bash
make clean            # stop and remove the container + dangling images
make prune            # nuclear — every unused Docker resource on the machine
```

### 10.10 Concepts worth internalising before Chapter 4

**Image vs container.** An image is a frozen blueprint. A container is a
running (or stopped) instance of that blueprint. You build images; you
run containers. Many containers can come from one image.

**Build context.** Everything in the folder with your Dockerfile, minus
what `.dockerignore` says to skip. Large contexts slow every build; this
is why we exclude `venv/`, `data/`, `logs/`.

**Layer caching.** Each instruction in the Dockerfile produces a layer.
Docker reuses a layer if its inputs haven't changed. Layer ordering is
therefore a performance question: put slow, rarely-changing things on
top, fast, frequently-changing things at the bottom.

**Bind mount vs named volume.** Bind mount = "show a host folder inside
the container". Named volume = "Docker manages a chunk of disk for this
container; I don't care where it lives." Bind for data you want to see;
named for state you only access through its owning service.

**Entrypoint vs command.** ENTRYPOINT is the fixed verb (`python
Collectorv2.py`). CMD is the default object (any default args). The user
can override CMD without overriding ENTRYPOINT. This gives you
`docker run image --format csv` ergonomics for free.

---

*End of Chapter 3 notes. Chapter 4 introduces Airflow and the
three-service compose file. Re-read §5.2 and §5.3 of this doc before
starting the next session — the target shape there is now one chapter
away.*

---

## 11. Chapter 4 — orchestrating with Airflow

Chapter 3 made the collector portable. Chapter 4 makes it **scheduled**,
**observable**, and **safe under failure**. The same Python script, the
same config, the same parquet output — now triggered nightly by a
workflow engine instead of by you typing `make run`. This is the bridge
from "I have a working data tool" to "I have a working data **pipeline**."

Three new artefacts and one substantial refactor of the collector landed:

- `dags/airflow_dag_v1.py` — the first DAG, four tasks
- `docker-compose.yml`     — Postgres + three Airflow services added; bind-mounts and env vars wire the collector in
- `Collectorv2.py` v2.1    — quota integrity refactor (try/finally, `QuotaExhaustedError`)
- `Makefile`               — `airflow-up`, `airflow-down`, `airflow-logs`, `airflow-ui` shortcuts

### 11.1 Why PostgreSQL, not SQLite

Airflow keeps its metadata — DAG run history, task instance state,
schedule decisions, connection secrets — in a relational database. The
default is SQLite, which works for tutorials and breaks for everything
else. SQLite gives you no concurrent writers (the scheduler and webserver
both want to write), and on container restart the file lives inside the
ephemeral container filesystem. One `docker compose down` and the entire
DAG run history evaporates.

PostgreSQL solves both. Concurrent writes are its job, and the data lives
in a named Docker volume (`postgres_data`) outside any container. Stop
the stack, restart it next week, and your DAG runs are still in the UI.
This is the "stateful services own their state in named volumes;
stateless services restart freely" pattern from §10.5.

Supabase and Firebase were considered and rejected. Supabase is Postgres
plus a hosted dashboard plus auth plus storage — overkill for a
metadata DB nobody but Airflow reads. Firebase is NoSQL; Airflow needs
a SQL backend.

### 11.2 Task scoping — Option C

There are three plausible ways to slice the collector into Airflow tasks:

| Option | Shape | Why it loses |
|---|---|---|
| A | One monolithic task that runs `Collectorv2.run()` | No retry granularity — a transient API hiccup forces a re-run of resolution, fetch, and write. Quota-wasteful. |
| B | One task per **collector phase** (resolve → fetch → write) | XCom serialization hell. The resolve task hands a list of channel-objects to the fetch task — those don't pickle cleanly across worker boundaries. Either you serialize manually or you bind-mount a shared filesystem and pass paths, both of which add complexity for no real benefit at this scale. |
| C | One task per **service boundary** (collect → dbt-silver → dbt-gold → notify) | Retries are scoped to the right unit. Each task is a different *type* of failure with different remediation. Picked. |

Option C is what every production pipeline I've inspected actually looks
like. Phase-per-task is a beginner trap; one-monolithic-task is a
procrastination trap. The right grain is **per service** — the collector
is one service, dbt is another, notification is another. Today only
`collect` does real work; `dbt_silver`, `dbt_gold`, and `notify` are
placeholders that will be wired up in chapters 5 and 6. Keeping them in
the DAG now means the dependency chain is locked in and the diagram is
visible end-to-end from day one.

### 11.3 Schedule — 08:00 UTC and the PT-midnight reset

Cron expression: `0 8 * * *`. Daily at 08:00 UTC. Why not `0 0 * * *`
(UTC midnight, the obvious default)?

The YouTube Data API v3 quota resets at midnight **Pacific Time**, not
UTC. PT midnight is 07:00 or 08:00 UTC depending on daylight saving. A
run at 00:00 UTC is mid-afternoon in California, after a full day's
quota burn — zero quota headroom. Running at 08:00 UTC puts the run
safely past the rollover on every DST edge case. This single line in the
DAG is worth more than every retry-policy choice put together; the
alternative is mysterious 403s for half the year.

Two more flags pin the runtime shape: `catchup=False` (never backfill
missed schedules — stale snapshots aren't useful) and `max_active_runs=1`
(one concurrent run only — avoids quota double-spend if a long-running
task overlaps with the next schedule).

### 11.4 LocalExecutor — and the upgrade path

Airflow has four executors that matter, in order of how much they
orchestrate:

- **SequentialExecutor** — runs one task at a time, single process. Toy-grade.
- **LocalExecutor** — multiple processes on one host, shared metadata DB. Picked.
- **CeleryExecutor** — workers across multiple hosts, talking via a Redis broker. Production-grade.
- **KubernetesExecutor** — each task is its own pod. Production-grade for cloud-native shops.

LocalExecutor is the right POC choice. It runs the four tasks in parallel
on one machine (which is more than this DAG ever needs) and requires zero
extra infrastructure — no Redis, no Celery workers, no ECS. The upgrade
path is config-only: change `AIRFLOW__CORE__EXECUTOR` to `CeleryExecutor`,
add a Redis service to compose, scale workers horizontally. None of the
DAG code changes.

### 11.5 The quota integrity refactor — Collectorv2 v2.1

Wiring the collector under an orchestrator surfaced a class of bugs that
didn't matter when a human invoked it manually. Three changes hardened
the run lifecycle:

**Try/finally around `record_quota_usage`.** In v2 the quota log line was
emitted on the success path. If the collector crashed after spending
units but before reaching the log line, those units were lost from the
ledger — and the next run would underestimate today's usage and could
push over quota. Wrapping the run body in `try/finally` means the ledger
fires no matter how the run exits.

**Incremental unit tracking.** v2 estimated post-hoc — "we made 12
calls, that's 12 units." v2.1 increments `units_spent` after each API
call. Same number on the happy path; meaningfully more accurate when the
run partially fails (the unit-spend at the moment of crash is preserved).

**`QuotaExhaustedError` instead of silent return.** v2's quota hard-stop
returned silently. From the orchestrator's perspective, that looked like
a successful run — green tick, nothing to alert on. v2.1 raises
`QuotaExhaustedError`; the DAG's task wrapper converts it to
`AirflowFailException`, and the task fails red. The DAG also pins
`retries=0` for this specific exception type — because the quota won't
reset within the next retry window (10 minutes), retrying just burns
more units and fails the same way.

The new `run_outcome="failed"` value rounds out the audit trail:
`success`, `no_work`, `aborted_over_quota`, `failed`. Whatever happened,
the ledger says what.

### 11.6 The collector-importable-from-Airflow dance

Three small things in `docker-compose.yml` make
`from ingestion.Collectorv2 import run` resolve from inside the Airflow
scheduler container:

- `PYTHONPATH=/opt/airflow` — Airflow only adds `dags_folder` and
  `plugins_folder` to `sys.path` by default; the project root is not on it.
- A read-only bind-mount of `./ingestion:/opt/airflow/ingestion:ro` —
  so the scheduler sees the same package the host edits. (Pre-chapter-5.5
  this was a single-file mount of `Collectorv2.py`; the chapter 5.5 reorg
  moved the collector into an `ingestion/` package, so the mount widened
  to the folder.)
- `COLLECTOR_CONFIG=/opt/airflow/config.yaml` — absolute path, so the
  collector loads the config regardless of whatever cwd the task subprocess
  inherits.

The `:ro` is intentional. The DAG should not be allowed to mutate the
collector source from a task — that would break the principle that source
code is what's committed, not what's runtime-mutated.

One more subtlety: the import of `Collectorv2` happens *inside* the task
callable, not at the top of the DAG file. Airflow re-parses every DAG
file on every scheduler tick (~30s); keeping module-level code cheap
means the scheduler stays responsive. `Collectorv2`'s import side
effects (`load_config`, `DEFAULT_CHANNELS`) only fire when the `collect`
task actually runs.

### 11.7 `_PIP_ADDITIONAL_REQUIREMENTS` — a POC pattern with a follow-up

The Airflow image doesn't ship with `pandas`, `pyarrow`,
`google-api-python-client`, etc. Two ways to fix that:

1. **Build a custom image**: `FROM apache/airflow:... + RUN pip install ...`.
   Layered, cached, fast — the right answer for production.
2. **`_PIP_ADDITIONAL_REQUIREMENTS`**: a magic env var the upstream
   Airflow entrypoint reads at container startup, runs `pip install`
   against, then proceeds. Adds 30–60s to first boot; cached afterward.

For a POC, (2) is right. It keeps the docker-compose file readable and
avoids a custom Dockerfile-per-service. The follow-up — chapter 5 or
whenever startup time starts hurting — is to bake the dependencies into
a custom image. That's documented in the docker-compose comment so
future-you doesn't forget.

### 11.8 Observability under failure — what the first runs revealed

The day this chapter shipped, the first scheduled DAG run quietly
demonstrated something useful. Of the 12 channels in `config.yaml`, **11**
resolved. The one that didn't was `ShashankKalanithi` — its handle no
longer maps to a live channel via YouTube's `forHandle` lookup.

This is exactly the kind of failure the project's "demonstration
failures" stance values: the system handled it correctly. `resolve_channel`
got an empty result, logged a `WARNING: Could not resolve:
'ShashankKalanithi'`, returned `None`, and the main loop carried on with
the 11 resolved channels. The DAG turned green; the parquet output was
complete for the 11 channels that did resolve; the quota log recorded the
24 units spent.

But the failure surfaced **only in stdout**, captured by Airflow's
per-task log file. The structured JSONL event log
(`logs/YYYY-MM-DD.jsonl`) — the source of truth for "what happened
during a run" — has no entry for the failure. `channel_resolved` events
fire on success only; there's no `channel_resolution_failed` counterpart.
A future Airflow alert keying off the structured stream wouldn't see this.

This is an observability gap, not a correctness bug. Worth a separate
`fix:` commit that emits a `channel_resolution_failed` event on the `else`
branch — the chapter ships without it, and the gap is named explicitly so
the next session can close it cleanly.

### 11.9 Drift from the §5.3 plan

§5.3 of this doc, written before chapter 4 started, sketched a 5-task DAG:
`preflight → collect → dbt_bronze_to_silver → dbt_silver_to_gold →
publish_metrics`. The shipped DAG is 4 tasks: `collect → dbt_silver →
dbt_gold → notify`. The differences:

- **No standalone `preflight` task.** The collector already does its own
  quota preflight inside `Collectorv2.run()`, with the audit trail (`event:
  "quota_preflight"`) landing in the same JSONL stream as the rest of the
  run. Splitting it into its own Airflow task would have meant duplicating
  the logic, splitting the audit trail across two task contexts, and
  passing state via XCom for no real benefit. The collector's preflight
  is the right grain.
- **Names shortened.** `dbt_bronze_to_silver` → `dbt_silver`. Shorter,
  matches dbt's own selector syntax (`tag:silver`).
- **`publish_metrics` → `notify`.** Reframed as "tell humans what
  happened" rather than "produce another data artefact." The output of
  the DAG is the bronze parquet; downstream content generation reads from
  the gold tables, not from a metrics JSON.

Worth recording so that the §5.3 sketch isn't read in isolation as the
spec — the actual implementation drifted, and for good reasons.

### 11.10 Quick reference (Chapter 4)

**Bring the stack up**

```bash
make airflow-up
make airflow-ui          # prints the URL + creds
```

**Sanity-check the DAG**

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list | grep youtube_pipeline
```

**Trigger one task in isolation (placeholder tasks burn no quota)**

```bash
docker compose exec airflow-scheduler airflow tasks test youtube_pipeline dbt_silver 2026-05-03
```

**Trigger the full DAG**

```bash
docker compose exec airflow-scheduler airflow dags trigger youtube_pipeline
```

**Tail logs**

```bash
make airflow-logs        # scheduler + webserver, interleaved
# Per-task logs live on the host:
ls logs/dag_id=youtube_pipeline/run_id=*/task_id=collect/
```

**Stop the stack (preserves DAG history)**

```bash
make airflow-down        # `down --volumes` to wipe Postgres state too
```

### 11.11 Concepts worth internalising before Chapter 5

**DAG.** A directed acyclic graph of tasks. "Directed" means edges go one
way (collect → dbt_silver, never the reverse). "Acyclic" means no loops.
The unit Airflow operates on; the file you write.

**Operator.** The "kind of work" a task does. `PythonOperator` runs a
Python callable; `BashOperator` runs a shell command; `DockerOperator`
runs a container. Pick the operator that matches the work; don't shoehorn
Bash into Python.

**Task instance.** A specific (DAG, task, execution_date) tuple.
`youtube_pipeline.collect` for `2026-05-03` is one task instance. Retries
count per-instance; the unit Airflow stores in the metadata DB.

**Execution date / logical date.** The logical timestamp the DAG run
*represents*, not the wall-clock time it ran. A scheduled run for
`2026-05-03T08:00` may execute at 09:14 if the scheduler was busy —
`execution_date` stays `2026-05-03T08:00`. Use it (not `datetime.now()`)
inside tasks to make backfills idempotent.

**Catchup.** When a DAG starts (or unpauses) after missing scheduled
runs, should Airflow run them all in order? `catchup=True` (default) yes;
`catchup=False` no. We pin `False`: stale snapshots aren't useful, and
the cost of catching up is API quota we don't want to spend on yesterday.

**XCom.** Airflow's tiny inter-task message bus, backed by the metadata
DB. Pass small values between tasks (a row count, a status flag). Don't
pass large objects through it — that's what shared filesystems / object
stores are for.

**`on_failure_callback`.** A hook that fires when a task fails. Wired up
in chapter 6 to post Slack/email alerts. Reserved in `default_args` for
now.

---

*End of Chapter 4 notes. Chapter 5 replaces the `dbt_silver` and
`dbt_gold` placeholder tasks with real `BashOperator` invocations of
`dbt run`, against a `dbt-duckdb` project that reads the bronze parquet
directly. §5.4 of this doc has the project layout to aim for.*
