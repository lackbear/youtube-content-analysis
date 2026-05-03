"""
detect_stale.py — flag active channels with no new posts in N days.

Joins competitors.csv (active=true rows) against the latest `published_at`
per channel in bronze parquet, then writes one row per active channel to
data/curator/stale_channels.csv with `days_since_last_post` and a boolean
`stale` flag (true when the gap exceeds curator.stale_threshold_days from
config.yaml, default 14).

Reads bronze without dbt — runs from a fresh DuckDB in-memory connection
against the parquet files directly. So it works whether dbt has built the
warehouse yet or not.

Run standalone:
    python scripts/detect_stale.py

Run from Airflow (PythonOperator):
    from scripts.detect_stale import detect_stale
    detect_stale()
"""

import os
from pathlib import Path

import duckdb
import pandas as pd
import yaml


PROJECT_ROOT     = Path(__file__).resolve().parent.parent
COMPETITORS_CSV  = PROJECT_ROOT / "competitors.csv"
BRONZE_GLOB      = PROJECT_ROOT / "data" / "raw" / "video_stats" / "**" / "*.parquet"
OUTPUT_DIR       = PROJECT_ROOT / "data" / "curator"
OUTPUT_PATH      = OUTPUT_DIR / "stale_channels.csv"
CONFIG_PATH      = Path(os.environ.get("COLLECTOR_CONFIG", str(PROJECT_ROOT / "config.yaml")))


def _load_threshold() -> int:
    """Read curator.stale_threshold_days from config; default 14 if missing."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return int(cfg.get("curator", {}).get("stale_threshold_days", 14))
    except Exception:
        return 14


def detect_stale() -> Path:
    """
    Compute staleness for every active channel and write the result to
    `data/curator/stale_channels.csv`. Returns the output path.

    Output columns:
      handle, channel_id, name, niche, tier,
      last_published_at, days_since_last_post, stale

    Channels in the CSV with active=true but no rows in bronze (never
    fetched) come through with NULL last_published_at and stale=NULL.
    They're sorted to the top via `NULLS FIRST` — those need attention.
    """
    threshold = _load_threshold()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    competitors_path = COMPETITORS_CSV.as_posix()
    bronze_glob      = BRONZE_GLOB.as_posix()

    con = duckdb.connect()
    df: pd.DataFrame = con.sql(f"""
        WITH competitors AS (
            SELECT handle, channel_id, name, niche, tier
            FROM read_csv('{competitors_path}', header=true, all_varchar=true)
            WHERE lower(active) = 'true'
        ),
        bronze AS (
            SELECT
                channel_id,
                max(cast(published_at AS timestamp)) AS last_published_at
            FROM read_parquet('{bronze_glob}', union_by_name=true)
            GROUP BY channel_id
        )
        SELECT
            c.handle,
            c.channel_id,
            c.name,
            c.niche,
            c.tier,
            b.last_published_at,
            (current_date - cast(b.last_published_at AS date))::integer AS days_since_last_post,
            (current_date - cast(b.last_published_at AS date)) > {threshold} AS stale
        FROM competitors c
        LEFT JOIN bronze b USING (channel_id)
        ORDER BY days_since_last_post DESC NULLS FIRST, c.handle
    """).df()
    con.close()

    df.to_csv(OUTPUT_PATH, index=False)
    return OUTPUT_PATH


if __name__ == "__main__":
    path = detect_stale()
    n_total = sum(1 for _ in open(path)) - 1
    print(f"Wrote {path} ({n_total} active channels evaluated)")
