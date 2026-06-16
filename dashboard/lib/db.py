"""
DuckDB connection + query helpers.

One read-only connection is opened per Streamlit session via @st.cache_resource
(was: a fresh connection per query in the old single-page app — visible in the
profiler as a real overhead once filters fire on every keystroke).

All queries go through `q()` which is @st.cache_data with a 5-minute TTL — long
enough that the daily collector cadence doesn't thrash the cache, short enough
that a manual Airflow trigger followed by an immediate dashboard reload sees
the new data within one breath. The ops page exposes a "clear cache" button
for the impatient.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


WAREHOUSE = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "warehouse" / "dev.duckdb"
)


def warehouse_exists() -> bool:
    return WAREHOUSE.exists()


@st.cache_resource(show_spinner=False)
def get_connection() -> duckdb.DuckDBPyConnection:
    """One shared read-only connection per session.

    Read-only mode means dbt can write to the same file concurrently without
    Streamlit holding a write lock. If dbt is mid-write the open() itself can
    still race; that's caught at call sites with a friendly st.error.
    """
    if not WAREHOUSE.exists():
        raise FileNotFoundError(f"Warehouse not found at {WAREHOUSE}")
    return duckdb.connect(str(WAREHOUSE), read_only=True)


@st.cache_data(ttl=300, show_spinner=False)
def list_tables() -> list[tuple[str, str]]:
    con = get_connection()
    return con.sql(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'main', 'pg_catalog')
        ORDER BY 1, 2
        """
    ).fetchall()


def has_table(schema: str, name: str) -> bool:
    return (schema, name) in list_tables()


@st.cache_data(ttl=300, show_spinner=False)
def q(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Run a SQL query against the warehouse and return a DataFrame.

    Params are passed positionally as `?` placeholders (DuckDB convention).
    Cached on (sql, params) — switching filters reuses prior results.
    """
    con = get_connection()
    if params:
        return con.execute(sql, list(params)).fetchdf()
    return con.sql(sql).df()


def clear_cache() -> None:
    """Wipe both the data cache and the connection resource. Called from the
    ops page after a manual Airflow trigger."""
    st.cache_data.clear()
    st.cache_resource.clear()
