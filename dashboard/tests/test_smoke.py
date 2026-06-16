"""
Smoke test — every dashboard module must import cleanly.

Catches the #1 regression class for Streamlit dashboards: a top-level import
or attribute lookup that throws and 500s the whole server. We don't *render*
(that needs a Streamlit runtime) but we do compile and exec each module
against a tiny DuckDB fixture so any import-time SQL or path lookup runs.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest


DASH = Path(__file__).resolve().parents[1]
ROOT = DASH.parent
SECTIONS = sorted((DASH / "sections").glob("*.py"))
PAGES = [DASH / "_overview.py", DASH / "_analytics.py", DASH / "_admin.py"]


@pytest.fixture(scope="session", autouse=True)
def fixture_warehouse(tmp_path_factory):
    """Build a tiny dev.duckdb at the path the dashboard expects, only if
    one isn't already present. Smoke test exercises imports, not contents."""
    real = ROOT / "data" / "warehouse" / "dev.duckdb"
    if real.exists():
        return real
    real.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(real))
    con.execute("CREATE SCHEMA IF NOT EXISTS main_silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS main_gold;")
    con.execute("""
        CREATE TABLE main_silver.stg_video_stats (
            video_id VARCHAR, channel_id VARCHAR, channel_name VARCHAR,
            title VARCHAR, published_at TIMESTAMP,
            views BIGINT, likes BIGINT, comments BIGINT,
            duration_iso VARCHAR, category_id VARCHAR, thumbnail_url VARCHAR,
            fetched_date DATE, ingestion_timestamp TIMESTAMP, status VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE main_silver.dim_channel (
            handle VARCHAR, channel_id VARCHAR, name VARCHAR,
            niche VARCHAR, tier VARCHAR,
            subscribers_at_addition BIGINT, added_date DATE,
            active BOOLEAN, deactivated_date DATE, deactivated_reason VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE main_gold.fct_video_growth_7d (
            video_id VARCHAR, channel_id VARCHAR, channel_name VARCHAR,
            title VARCHAR, snapshot_date DATE, baseline_date DATE,
            days_since_baseline INTEGER,
            views BIGINT, likes BIGINT, comments BIGINT,
            views_baseline BIGINT, likes_baseline BIGINT, comments_baseline BIGINT,
            views_added_window BIGINT, likes_added_window BIGINT, comments_added_window BIGINT
        );
    """)
    con.close()
    return real


def _load(path: Path):
    sys.path.insert(0, str(DASH))
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_app_imports():
    _load(DASH / "app.py")


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_pages_import(page: Path):
    _load(page)


@pytest.mark.parametrize("section", SECTIONS, ids=lambda p: p.name)
def test_sections_import(section: Path):
    _load(section)


def test_lib_imports():
    sys.path.insert(0, str(DASH))
    import lib.db          # noqa: F401
    import lib.filters     # noqa: F401
    import lib.theme       # noqa: F401
    import lib.charts      # noqa: F401
    import lib.airflow_client  # noqa: F401
