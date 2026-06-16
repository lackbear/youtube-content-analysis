# Dashboard

Multi-page Streamlit app over the local DuckDB warehouse at `data/warehouse/dev.duckdb`,
with a Pipeline Ops page that talks to Airflow over its REST API.

## Layout

```
dashboard/
├── app.py                       # Overview — entrypoint, KPIs only
├── pages/                       # Filenames are ASCII-only — Windows + some
│   ├── 1_Channels.py            # Streamlit versions choke on emoji + variation
│   ├── 2_Videos_and_Growth.py   # selectors in page filenames. Each page sets
│   ├── 3_Curator.py             # its icon via st.set_page_config(page_icon=).
│   ├── 4_Pipeline_Ops.py
│   └── 5_Diagnostics.py
├── lib/
│   ├── db.py                    # Cached DuckDB connection + q()
│   ├── filters.py               # Sidebar filters with URL persistence
│   ├── theme.py                 # Altair theme + deterministic palette
│   ├── charts.py                # Reusable chart builders + KPI helpers
│   └── airflow_client.py        # Thin REST client for the ops page
└── tests/
    └── test_smoke.py            # Imports every page; catches regressions
```

The 6-pages layout is the production structure recommended in the
[Best practices check](../docs/) Notion page (chapter 6.5):

- **Overview** is the landing page — stakeholders answer "is the pipeline healthy?"
  in one glance, no tables, no filters.
- **Analytics** pages (Channels, Videos, Curator) carry their own filter sidebar,
  scoped to what makes sense on each page.
- **Operations** (Pipeline Ops) keeps writes (DAG triggers) isolated from reads.
- **Diagnostics** holds engineering-only views like the overlap heatmap.

## Run

From the project root:

```bash
venv/Scripts/python -m pip install -r dashboard/requirements.txt
venv/Scripts/streamlit run dashboard/app.py
```

Open <http://localhost:8501>.

## Filters & URL persistence

The sidebar on each analytics page sets state into URL query params
(`?start=2026-04-01&end=2026-05-03&ch=UC...|UC...`). Copy the URL bar to share
a filtered view; opening the URL in a new tab restores the same state.

Quick ranges: 7 / 30 / 90 days or all-time. Multi-select widgets carry their
selections through `|`-separated lists in the URL.

## Pipeline Ops · Airflow integration

The ops page calls the Airflow stable REST API. Configure connection via env
vars or `.streamlit/secrets.toml`:

```toml
# .streamlit/secrets.toml
[airflow]
base_url = "http://localhost:8080"
user = "admin"
password = "admin"
```

…or with environment variables:

```
AIRFLOW_BASE_URL=http://localhost:8080
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=<rotate-me>
```

Defaults match the docker-compose POC (`http://localhost:8080`, `admin`/`admin`).
**Rotate before deploying to anything that faces the internet.**

The trigger button is disabled while a run is `running`, and a confirmation
dialog appears before the POST — manual triggering is meant to be deliberate.

## Theming

Streamlit reads `.streamlit/config.toml` from the current working directory
(the project root, given how `streamlit run` is invoked above). Edit it to
re-skin the UI. A complementary **Altair theme** is applied programmatically
in `lib/theme.py` so charts share the same palette + typography.

## Caching

- `get_connection()` is `@st.cache_resource` — one DuckDB connection per
  Streamlit session, opened read-only.
- `q(sql, params)` is `@st.cache_data(ttl=300)` — 5 minute TTL.
  Keyed on the SQL string + params tuple, so flipping filters reuses prior
  results.
- After a manual Airflow trigger you can wait the TTL or click
  **Clear cache and reconnect** on the Pipeline Ops page.

## Ports

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 |
| dbt docs | http://localhost:8081 |
| Dashboard | http://localhost:8501 |

## Troubleshooting

- **No warehouse found** → `cd dbt_youtube && DBT_PROFILES_DIR=. dbt run`.
- **`IO Error: Could not set lock on file`** → `dbt run` is mid-write. Wait
  10s and reload. Production fix: have dbt write to a `.tmp` and rename atomically.
- **`Cannot reach Airflow at http://localhost:8080`** on the Ops page → check
  `docker compose ps` shows `yt-airflow-webserver` up; verify creds in env
  or `.streamlit/secrets.toml`.
- **Filters reset on reload** → expected when opening a fresh tab without
  query params; once you change a widget the URL captures the state.

## Testing

Smoke test (imports every page module against a fixture warehouse):

```
venv/Scripts/python -m pytest dashboard/tests/
```

Catches the most common regression class: a page-level `import` or
top-level lookup that crashes the whole server on first hit.
