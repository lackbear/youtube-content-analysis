"""
DuckDB connection + query helpers.

## Why this isn't a plain read-only connection to the warehouse

DuckDB is single-writer at the *file* level: a process that opens the file
read-write takes an exclusive OS lock, and a process that opens it read-only
takes a *shared* lock. Those two locks conflict. On Windows the conflict is
hard — the second open raises `IO Error: ... The process cannot access the
file because it is being used by another process`.

The old design held a long-lived `read_only=True` connection per Streamlit
session and assumed that let dbt write concurrently. It does not: as long as
the dashboard is open it holds the shared lock, so the Airflow `tag:silver`
/`tag:gold` dbt runs cannot open the warehouse and the DAG fails. That was the
project's single point of failure.

## The fix — an in-memory mirror

On first use (and whenever dbt rewrites the file) the dashboard:
  1. briefly ATTACHes the warehouse read-only,
  2. `COPY FROM DATABASE` it into an in-memory DuckDB,
  3. DETACHes — releasing the file lock entirely.

After that the dashboard queries the in-memory copy and holds *no* lock on
`dev.duckdb`, so dbt always has exclusive write access. The only contention
window is the few milliseconds of the mirror load; if dbt happens to be
mid-write right then, `_mirror()` retries with backoff.

## Freshness

`get_connection()`, `list_tables()` and `q()` are all keyed on the warehouse
file's mtime. When dbt rewrites the file the mtime bumps, the caches miss, and
the mirror is rebuilt from the fresh data on the next interaction — no restart
needed. The 5-minute `ttl` on `q()` only guards against recomputing identical
queries on every filter keystroke while the data is unchanged. The ops page
still exposes a "clear cache" button for an instant manual refresh.
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


WAREHOUSE = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "warehouse" / "dev.duckdb"
)

# How long to keep retrying the mirror load if dbt holds the write lock.
_LOAD_RETRIES = 8
_LOAD_BACKOFF_S = 0.35


def warehouse_exists() -> bool:
    return WAREHOUSE.exists()


def _warehouse_mtime() -> float:
    """Cache key for everything downstream. Changes the instant dbt rewrites
    the warehouse, which invalidates the mirror and the query caches."""
    try:
        return WAREHOUSE.stat().st_mtime
    except FileNotFoundError:
        return 0.0


@st.cache_resource(show_spinner=False)
def _mirror(_mtime: float) -> duckdb.DuckDBPyConnection:
    """Build an in-memory copy of the warehouse and return a connection to it.

    Keyed on the warehouse mtime so a dbt rewrite produces a fresh mirror.
    Holds no lock on `dev.duckdb` once it returns — the read-only ATTACH is
    released by DETACH inside the load — so dbt is never blocked by the
    dashboard.

    Retries the load while dbt holds the exclusive write lock; surfaces the
    last error if the warehouse stays locked past the retry budget.
    """
    if not WAREHOUSE.exists():
        raise FileNotFoundError(f"Warehouse not found at {WAREHOUSE}")

    last_err: Exception | None = None
    for attempt in range(_LOAD_RETRIES):
        mem = duckdb.connect(":memory:")
        try:
            mem.execute(f"ATTACH '{WAREHOUSE.as_posix()}' AS src (READ_ONLY)")
            mem.execute("COPY FROM DATABASE src TO memory")
            mem.execute("DETACH src")
            return mem
        except (duckdb.IOException, duckdb.Error) as e:
            last_err = e
            mem.close()
            # dbt is probably mid-write — back off and try again.
            time.sleep(_LOAD_BACKOFF_S * (attempt + 1))

    raise RuntimeError(
        f"Could not snapshot the warehouse after {_LOAD_RETRIES} attempts — "
        f"a dbt run may be holding the write lock. Last error: {last_err}"
    )


def get_connection() -> duckdb.DuckDBPyConnection:
    """Connection to the in-memory mirror of the warehouse (no file lock)."""
    if not WAREHOUSE.exists():
        raise FileNotFoundError(f"Warehouse not found at {WAREHOUSE}")
    return _mirror(_warehouse_mtime())


@st.cache_data(ttl=300, show_spinner=False)
def _list_tables(_mtime: float) -> list[tuple[str, str]]:
    con = get_connection()
    return con.sql(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'main', 'pg_catalog')
        ORDER BY 1, 2
        """
    ).fetchall()


def list_tables() -> list[tuple[str, str]]:
    return _list_tables(_warehouse_mtime())


def has_table(schema: str, name: str) -> bool:
    return (schema, name) in list_tables()


@st.cache_data(ttl=300, show_spinner=False)
def _q(sql: str, params: tuple | None, _mtime: float) -> pd.DataFrame:
    con = get_connection()
    if params:
        return con.execute(sql, list(params)).fetchdf()
    return con.sql(sql).df()


def q(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Run a SQL query against the warehouse mirror and return a DataFrame.

    Params are passed positionally as `?` placeholders (DuckDB convention).
    Cached on (sql, params, warehouse mtime) — switching filters reuses prior
    results, and a dbt rewrite transparently invalidates the cache.
    """
    return _q(sql, tuple(params) if params else None, _warehouse_mtime())


def clear_cache() -> None:
    """Wipe both the data cache and the mirror. Called from the ops page after
    a manual Airflow trigger to force an immediate re-snapshot."""
    st.cache_data.clear()
    st.cache_resource.clear()
