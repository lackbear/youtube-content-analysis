"""
Collectorv2.py — YouTube competitor data collector
────────────────────────────────────────────────────────────────────────────────
Chapter 2 of the pipeline. What changed from v1:

  1. Idempotent multi-config safety (Option B — channel sub-partitioning)
     Path layout evolved from:
         data/raw/video_stats/date=YYYY-MM-DD/video_stats.parquet
     to:
         data/raw/video_stats/date=YYYY-MM-DD/channel_id=UCxxxx/video_stats.parquet
     Two configs running on the same day no longer clobber each other.
     Each channel's file is overwritten per-run (bronze-layer snapshot).

  2. `ingestion_timestamp` column added to every row
     Lets downstream Silver/Gold know *when* the snapshot was captured,
     even though the file itself only carries the latest snapshot per day.

  3. `load_seen_videos` reads both legacy AND new layouts
     Old flat files (pre-v2) and new sub-partitioned files coexist gracefully
     while the 7-day refresh window rolls forward.

  4. Daily API quota tracking (logs/quota/YYYY-MM-DD.jsonl)
     Read-before-write per run. Soft warn at 80 %, hard stop at 95 % of the
     10 000-unit daily quota. Schema is one JSON line per run for auditability.

Everything else is intentionally unchanged from v1 so the diff tells the story.
"""

import os
import json
import time
import math
import logging
from datetime import datetime, timezone
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import yaml
from dotenv import load_dotenv

# Load .env file if present (won't override existing shell env vars)
load_dotenv()

# ── Config loader ─────────────────────────────────────────────────────────────

CONFIG_FILE = os.environ.get("COLLECTOR_CONFIG", "config.yaml")

def load_config(path=CONFIG_FILE):
    """Load and validate config.yaml. Raises clearly if anything is missing."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Config file not found: '{path}'. "
            f"Set COLLECTOR_CONFIG env var or place config.yaml next to this script."
        )
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    required = [
        ("api.key_env_var",                str),
        ("api.max_retries",                int),
        ("api.batch_size",                 int),
        ("collection.max_videos_per_channel", int),
        ("collection.refresh_days",        int),
        ("output.video_stats_file",        str),
        ("attributes",                     list),
        ("channels",                       list),
    ]

    errors = []
    for key_path, expected_type in required:
        keys = key_path.split(".")
        val  = cfg
        try:
            for k in keys:
                val = val[k]
            if not isinstance(val, expected_type):
                errors.append(f"'{key_path}' must be {expected_type.__name__}, got {type(val).__name__}")
        except (KeyError, TypeError):
            errors.append(f"Missing required config key: '{key_path}'")

    if errors:
        for e in errors:
            logging.error(f"Config error: {e}")
        raise ValueError(f"Config validation failed with {len(errors)} error(s).")

    return cfg

# Load once at import time
_cfg             = load_config()
REFRESH_DAYS     = _cfg["collection"]["refresh_days"]
# Chapter 6 commit 1: re-fetch any cached video whose snapshot is older than
# this many days, regardless of publish age. Default 1 (daily refresh).
CACHE_MAX_AGE_DAYS = int(_cfg["collection"].get("cache_max_age_days", 1))
MAX_RETRIES      = _cfg["api"]["max_retries"]
BATCH_SIZE       = _cfg["api"]["batch_size"]
VIDEO_STATS_FILE = _cfg["output"]["video_stats_file"]
DEFAULT_ATTRS    = _cfg["attributes"]


# ── Channel registry — competitors.csv (chapter 6 commit 1) ───────────────────
# As of chapter 6, the channel list lives in competitors.csv at the project
# root. The yaml `channels:` block is kept as an empty fallback so the
# collector still works in environments without the CSV. The CSV is the
# source of truth: schema + active flag + niche/tier metadata, all in one
# place that's easy to edit by hand or programmatically.
COMPETITORS_CSV = os.environ.get("COMPETITORS_CSV", "competitors.csv")


def _load_channels_from_csv(csv_path: str) -> list:
    """
    Read competitors.csv → list of identifiers (channel_id when known, else
    handle) for rows with active=true.

    Returns [] if the file is missing, empty, or has no active rows. The
    caller falls back to the yaml `channels:` block in that case.
    """
    if not os.path.exists(csv_path):
        return []
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return []

    if df.empty or "active" not in df.columns:
        return []

    df = df[df["active"].str.strip().str.lower() == "true"]
    if df.empty:
        return []

    # Prefer pre-resolved channel_id (saves the forHandle lookup downstream);
    # fall back to handle when channel_id is empty (e.g. unresolved channels).
    out = []
    for _, row in df.iterrows():
        ident = row.get("channel_id", "").strip() or row.get("handle", "").strip()
        if ident:
            out.append(ident)
    return out


_csv_channels    = _load_channels_from_csv(COMPETITORS_CSV)
_yaml_channels   = [ch["handle"] for ch in _cfg.get("channels", [])]
DEFAULT_CHANNELS = _csv_channels or _yaml_channels

# ── V2: Quota configuration ──────────────────────────────────────────────────
# YouTube Data API v3 gives 10 000 units/day by default.
# These can be overridden via config.yaml → api.quota_*   (all optional).
QUOTA_LIMIT     = int(_cfg["api"].get("quota_limit",      10_000))
QUOTA_WARN_PCT  = float(_cfg["api"].get("quota_warn_pct", 0.80))   # 80 %
QUOTA_STOP_PCT  = float(_cfg["api"].get("quota_stop_pct", 0.95))   # 95 %


# ── V2.1: Quota-exhaustion signal ────────────────────────────────────────────
# Raised by run() when the hard-stop threshold (95 %) is crossed. Airflow's
# task wrapper catches this and converts it to an AirflowFailException with
# retries=0 — retrying 10 minutes later won't help because YouTube's quota
# resets at PT midnight, not on demand.
class QuotaExhaustedError(RuntimeError):
    """Raised when the daily YouTube API quota hard-stop is reached."""

# All attributes the script knows how to fetch.
# Each maps to: (api_part, extraction_fn(stats, contentDetails, snippet))
ATTRIBUTE_MAP = {
    "views"         : ("statistics",     lambda s, c, snip: s.get("viewCount",    "")),
    "likes"         : ("statistics",     lambda s, c, snip: s.get("likeCount",    "")),
    "comments"      : ("statistics",     lambda s, c, snip: s.get("commentCount", "")),
    "duration"      : ("contentDetails", lambda s, c, snip: c.get("duration",     "")),
    "tags"          : ("snippet",        lambda s, c, snip: "|".join(snip.get("tags", []))),
    "description"   : ("snippet",        lambda s, c, snip: snip.get("description", "")[:500]),
    "thumbnail_url" : ("snippet",        lambda s, c, snip: snip.get("thumbnails", {}).get("high", {}).get("url", "")),
    "category_id"   : ("snippet",        lambda s, c, snip: snip.get("categoryId", "")),
}

ALL_ATTRIBUTES   = list(ATTRIBUTE_MAP.keys())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Structured event logger ───────────────────────────────────────────────────

def log_event(event: str, **kwargs):
    """
    Append a structured JSON event to logs/YYYY-MM-DD.jsonl.
    Readable by Spark, pandas, or any JSON tool for post-run analysis.

    Events emitted:
        run_start        — collector kicked off
        channel_resolved — handle → channel_id resolved
        videos_queued    — how many videos will be fetched
        quota_preflight  — V2: quota usage snapshot before fetch (ok|warn|abort)
        run_complete     — final counts + estimated API units used
        run_aborted      — V2: aborted before fetch (e.g. quota hard-stop)
        run_error        — unhandled exception during run
    """
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event":     event,
        **kwargs,
    }
    log_dir  = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{datetime.now(timezone.utc).date().isoformat()}.jsonl"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

# ── V2: Quota tracking ────────────────────────────────────────────────────────
# One append-only JSONL file per UTC day at logs/quota/YYYY-MM-DD.jsonl.
# Every run contributes one line. Reading the file and summing `units_this_run`
# gives today's cumulative usage — no separate running-total to keep in sync.
#
# Schema (one JSON object per line):
#   {
#     "timestamp":       ISO-8601 UTC,
#     "units_this_run":  int,
#     "units_today":     int,           # cumulative after this run
#     "quota_limit":     int,           # mirrors config, makes the line self-describing
#     "quota_remaining": int,
#     "channels":        int,           # channels the run touched
#     "videos_fetched":  int,
#     "run_outcome":     "success" | "aborted_over_quota" | "no_work"
#   }

QUOTA_DIR = Path("logs/quota")

def _quota_file_for(day: "datetime.date") -> Path:
    return QUOTA_DIR / f"{day.isoformat()}.jsonl"


def read_units_used_today(day=None) -> int:
    """
    Sum `units_this_run` across all entries in today's quota file.
    Returns 0 if the file doesn't exist (first run of the day).
    """
    day = day or datetime.now(timezone.utc).date()
    path = _quota_file_for(day)
    if not path.exists():
        return 0
    total = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                total += int(json.loads(line).get("units_this_run", 0))
            except (json.JSONDecodeError, ValueError):
                # Malformed line — skip rather than crash. Audit log, not gospel.
                log.warning(f"Quota file has unparseable line in {path.name}, skipping.")
    return total


def estimate_run_units(num_channels: int, max_videos_per_channel: int,
                       batch_size: int = BATCH_SIZE) -> int:
    """
    Conservative pre-run estimate of YouTube API units this run will cost.
    Each .list call is 1 unit regardless of parts requested.

        channels.list        — 1 per channel (resolution)
        playlistItems.list   — 1 per channel
        videos.list          — 1 per batch-of-50 fetched videos

    Over-estimating is preferred to under-estimating; that is how we protect
    the 95 % hard-stop from undercounting edge cases.
    """
    resolve_cost  = num_channels
    playlist_cost = num_channels
    max_video_ids = num_channels * max_videos_per_channel
    videos_cost   = math.ceil(max_video_ids / batch_size) if max_video_ids else 0
    return resolve_cost + playlist_cost + videos_cost


def quota_preflight(estimated_units: int) -> str:
    """
    Decide whether to proceed, warn, or abort based on today's cumulative
    usage + the estimate for this run.

    Returns:
        "ok"    — under the warn threshold
        "warn"  — over 80 %, proceeding anyway
        "abort" — over 95 %, caller should stop before any API call
    """
    used_so_far = read_units_used_today()
    projected   = used_so_far + estimated_units
    warn_at     = QUOTA_LIMIT * QUOTA_WARN_PCT
    stop_at     = QUOTA_LIMIT * QUOTA_STOP_PCT

    log.info(
        f"Quota preflight: {used_so_far} used today + {estimated_units} estimated "
        f"= {projected}/{QUOTA_LIMIT} "
        f"(warn @ {int(warn_at)}, stop @ {int(stop_at)})"
    )
    log_event(
        "quota_preflight",
        units_today       = used_so_far,
        estimated_units   = estimated_units,
        projected_units   = projected,
        quota_limit       = QUOTA_LIMIT,
    )

    if projected >= stop_at:
        return "abort"
    if projected >= warn_at:
        return "warn"
    return "ok"


def record_quota_usage(units_this_run: int, channels: int,
                       videos_fetched: int, outcome: str) -> None:
    """Append one line describing this run to logs/quota/YYYY-MM-DD.jsonl."""
    QUOTA_DIR.mkdir(parents=True, exist_ok=True)
    day            = datetime.now(timezone.utc).date()
    used_before    = read_units_used_today(day)
    units_today    = used_before + units_this_run
    remaining      = max(QUOTA_LIMIT - units_today, 0)

    entry = {
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "units_this_run":  units_this_run,
        "units_today":     units_today,
        "quota_limit":     QUOTA_LIMIT,
        "quota_remaining": remaining,
        "channels":        channels,
        "videos_fetched":  videos_fetched,
        "run_outcome":     outcome,
    }
    with open(_quota_file_for(day), "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
    log.info(
        f"Quota recorded — run={units_this_run}u, today={units_today}u, "
        f"remaining={remaining}u, outcome={outcome}"
    )

# ── YouTube client ────────────────────────────────────────────────────────────

def _build_client():
    key_var = _cfg["api"]["key_env_var"]
    api_key = os.environ.get(key_var, "")
    if not api_key:
        raise EnvironmentError(
            f"API key not set. Run: export {key_var}='your_key'"
        )
    return build("youtube", "v3", developerKey=api_key)

# ── Preflight ─────────────────────────────────────────────────────────────────

def preflight(channels, max_videos, attributes):
    """Validate all inputs before any API call is made."""
    errors = []

    if not os.environ.get(_cfg["api"]["key_env_var"]):
        errors.append(f"{_cfg['api']['key_env_var']} environment variable is not set.")
    if not channels:
        errors.append("channels list is empty.")
    if not isinstance(max_videos, int) or not (1 <= max_videos <= 50):
        errors.append("max_videos must be an integer between 1 and 50.")

    unknown = [a for a in attributes if a not in ATTRIBUTE_MAP]
    if unknown:
        errors.append(
            f"Unknown attributes: {unknown}. Valid: {ALL_ATTRIBUTES}"
        )

    if errors:
        for e in errors:
            log.error(f"Preflight: {e}")
        raise ValueError(f"Preflight failed with {len(errors)} error(s). See logs above.")

    log.info(
        f"Preflight passed — {len(channels)} channels, "
        f"{max_videos} videos/channel, attributes: {attributes}"
    )

# ── Helpers ───────────────────────────────────────────────────────────────────

def api_call_with_retry(youtube, request_fn):
    """Execute a YouTube API request with exponential backoff retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return request_fn().execute()
        except HttpError as e:
            log.warning(f"HTTP {e.resp.status} on attempt {attempt}: {e}")
        except Exception as e:
            log.warning(f"Unexpected error on attempt {attempt}: {e}")

        if attempt < MAX_RETRIES:
            wait = 2 ** attempt
            log.info(f"Retrying in {wait}s...")
            time.sleep(wait)

    log.error("All retries exhausted.")
    return None


def resolve_channel(youtube, identifier):
    """
    Resolve a handle (e.g. 'SiimLand') or channel ID (e.g. 'UCxxxxx')
    to a channel dict. Returns None if not found.
    """
    is_id  = identifier.startswith("UC") and len(identifier) == 24
    kwargs = {"id": identifier} if is_id else {"forHandle": identifier}

    result = api_call_with_retry(
        youtube,
        lambda k=kwargs: youtube.channels().list(
            part="snippet,contentDetails", **k
        )
    )

    if not result or not result.get("items"):
        log.warning(f"Could not resolve: '{identifier}'")
        return None

    item = result["items"][0]
    log.info(f"  Resolved '{identifier}' → {item['snippet']['title']}")
    return {
        "name"        : item["snippet"]["title"],
        "channel_id"  : item["id"],
        "playlist_id" : item["contentDetails"]["relatedPlaylists"]["uploads"],
    }


def load_seen_videos():
    """
    Load cache from all date-partitioned Parquet/CSV files.
    Returns dict: { video_id -> latest fetched_date (date obj) }

    V2: reads BOTH layouts so the 7-day refresh cache keeps working across
    the transition from flat → sub-partitioned output:

        data/raw/video_stats/date=YYYY-MM-DD/video_stats.parquet            (v1)
        data/raw/video_stats/date=YYYY-MM-DD/channel_id=UCxxxx/video_stats.parquet (v2)
    """
    data_dir = Path("data/raw/video_stats")
    if not data_dir.exists():
        log.info("Cache loaded — 0 known videos (first run)")
        return {}

    # Legacy flat layout (v1) and new sub-partitioned layout (v2), parquet + csv
    parquet_legacy = list(data_dir.glob("date=*/video_stats.parquet"))
    csv_legacy     = list(data_dir.glob("date=*/video_stats.csv"))
    parquet_new    = list(data_dir.glob("date=*/channel_id=*/video_stats.parquet"))
    csv_new        = list(data_dir.glob("date=*/channel_id=*/video_stats.csv"))
    all_files      = parquet_legacy + parquet_new + csv_legacy + csv_new

    if not all_files:
        log.info("Cache loaded — 0 known videos")
        return {}

    frames = []
    for f in all_files:
        if f.suffix == ".parquet":
            frames.append(pd.read_parquet(f, columns=["video_id", "fetched_date"]))
        else:
            frames.append(pd.read_csv(f, usecols=["video_id", "fetched_date"]))

    df = pd.concat(frames, ignore_index=True)
    seen = (
        df.groupby("video_id")["fetched_date"]
        .max()
        .apply(lambda v: pd.to_datetime(v).date())
        .to_dict()
    )
    log.info(
        f"Cache loaded — {len(seen)} known videos "
        f"(legacy files: {len(parquet_legacy) + len(csv_legacy)}, "
        f"sub-partitioned: {len(parquet_new) + len(csv_new)})"
    )
    return seen


def should_fetch(video_id, published_at_str, seen, today):
    """
    Cache decision (chapter 6 commit 1 — adds cache-age clause):

    - Unknown                              → fetch
    - Cached today                         → skip
    - Cached, cache-age > CACHE_MAX_AGE    → fetch (covers stable old videos
                                              that used to freeze in cache —
                                              channels with infrequent posts
                                              never had their stats refreshed
                                              once their videos aged past
                                              REFRESH_DAYS from publish_at)
    - Cached, publish-age <= REFRESH_DAYS  → fetch (velocity tracking — daily
                                              re-checks for newly-published
                                              videos that gain views fast)
    - Else                                 → skip

    Quota cost analysis: re-fetching all 110 videos (11 channels × 10) is
    `ceil(110/50) = 3 units` for the videos.list batch — negligible at any
    config size we care about. The skip-old-videos rule was tuned for a
    1000+ channel pipeline; at our scale it was pure overhead.
    """
    if video_id not in seen:
        return True
    last_fetched = seen[video_id]
    if last_fetched >= today:
        return False

    cache_age_days = (today - last_fetched).days
    if cache_age_days > CACHE_MAX_AGE_DAYS:
        return True

    try:
        published = datetime.fromisoformat(
            published_at_str.replace("Z", "+00:00")
        ).date()
        return (today - published).days <= REFRESH_DAYS
    except (ValueError, AttributeError):
        return False


def get_latest_videos(youtube, channel, max_videos):
    """Fetch latest video IDs + metadata from a channel's uploads playlist."""
    result = api_call_with_retry(
        youtube,
        lambda: youtube.playlistItems().list(
            part="contentDetails,snippet",
            playlistId=channel["playlist_id"],
            maxResults=max_videos,
        )
    )
    if not result:
        return []

    return [
        (
            item["contentDetails"]["videoId"],
            item["snippet"].get("publishedAt", ""),
            item["snippet"].get("title", ""),
        )
        for item in result.get("items", [])
    ]


def fetch_stats_batch(youtube, video_ids, attributes):
    """
    Fetch requested attributes for a list of video IDs in batches.
    Returns dict: { video_id -> { attr: value } } or None if batch failed.
    """
    needed_parts = ",".join({ATTRIBUTE_MAP[a][0] for a in attributes})
    all_stats    = {}

    for i in range(0, len(video_ids), BATCH_SIZE):
        batch  = video_ids[i : i + BATCH_SIZE]
        result = api_call_with_retry(
            youtube,
            lambda b=batch: youtube.videos().list(
                part=needed_parts,
                id=",".join(b),
            )
        )

        if not result:
            for vid in batch:
                all_stats[vid] = None
            continue

        fetched = set()
        for item in result.get("items", []):
            vid_id = item["id"]
            s      = item.get("statistics",     {})
            c      = item.get("contentDetails", {})
            snip   = item.get("snippet",        {})
            all_stats[vid_id] = {
                attr: ATTRIBUTE_MAP[attr][1](s, c, snip)
                for attr in attributes
            }
            fetched.add(vid_id)

        for vid in batch:
            if vid not in fetched:
                all_stats[vid] = None  # deleted or private

    return all_stats


def write_output(rows, attributes, today_str):
    """
    V2: write today's snapshot with a per-channel sub-partition.

    Path layout:
        data/raw/video_stats/date=YYYY-MM-DD/channel_id=UCxxxx/video_stats.parquet

    Semantics:
      * One file per (date, channel). Overwrite-on-rerun per channel.
      * Two configs with disjoint channels can run on the same day and never
        collide with each other.
      * Every row carries an `ingestion_timestamp` so downstream can tell
        *when* the snapshot was captured.

    Format is controlled by output.format in config.yaml (default: parquet).
    """
    fmt = _cfg["output"].get("format", "parquet").lower()
    if fmt not in ("parquet", "csv"):
        raise ValueError(f"Unsupported output format '{fmt}'. Use 'parquet' or 'csv'.")

    # V2: stamp every row with when it was ingested (single timestamp per run).
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["ingestion_timestamp"] = ingestion_ts

    cols = (
        ["fetched_date", "channel_id", "channel_name",
         "video_id", "title", "published_at"]
        + attributes
        + ["status", "ingestion_timestamp"]
    )

    df = pd.DataFrame(rows).reindex(columns=cols)

    base_dir = Path(f"data/raw/video_stats/date={today_str}")
    base_dir.mkdir(parents=True, exist_ok=True)

    # V2: group by channel_id and write one file per channel partition.
    written = []
    for ch_id, group in df.groupby("channel_id", sort=False):
        partition_dir = base_dir / f"channel_id={ch_id}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        if fmt == "parquet":
            out_path = partition_dir / "video_stats.parquet"
            group.to_parquet(out_path, index=False)
        else:
            out_path = partition_dir / "video_stats.csv"
            group.to_csv(out_path, index=False, encoding="utf-8")

        written.append((ch_id, len(group), out_path))
        log.info(f"  Wrote {len(group):>3} rows → {out_path} [{fmt}]")

    log.info(f"Written {len(df)} rows across {len(written)} channel partition(s).")

# ── Main entry point ──────────────────────────────────────────────────────────

def run(channels=None, max_videos=None, attributes=None):
    """
    Daily YouTube competitor collector (v2.1).

    Lifecycle:
        1. Preflight (config + env).
        2. Quota preflight — estimate units, raise QuotaExhaustedError at 95 %.
        3. Resolve channels via channels.list.
        4. Load cache from both legacy and sub-partitioned layouts.
        5. Build fetch queue via playlistItems.list.
        6. Fetch stats via videos.list (batched).
        7. Write to per-channel sub-partitions with ingestion_timestamp.
        8. Record quota usage (append one line to logs/quota/…jsonl).

    V2.1 changes (chapter 4 — orchestration readiness):
      * The entire run is wrapped in try/finally, so `record_quota_usage` ALWAYS
        fires, even on mid-run exceptions. Closes a leak where units spent
        before a crash were never recorded (the retry would then under-estimate
        today's usage and could burn more units).
      * Units are tracked incrementally (`units_spent += 1` per API call) rather
        than estimated after the fact. More accurate when resolution partially
        fails.
      * Quota hard-stop now raises QuotaExhaustedError instead of returning
        silently. Surfaces as a visible failure in Airflow's UI; the DAG pins
        `retries=0` for this specific error because the quota won't reset
        before the next retry window anyway.
      * New `run_outcome` value: "failed" (unhandled exception during run).
        Existing values — "success", "no_work", "aborted_over_quota" — unchanged.

    Raises:
        QuotaExhaustedError — when the 95 % hard-stop is crossed at preflight.
        Any other unhandled exception from the fetch/write phases propagates;
        quota usage for whatever units were spent is still recorded.
    """
    # Fall back to config.yaml values if not explicitly passed
    if channels    is None: channels    = DEFAULT_CHANNELS
    if max_videos  is None: max_videos  = _cfg["collection"]["max_videos_per_channel"]
    if attributes  is None: attributes  = DEFAULT_ATTRS

    preflight(channels, max_videos, attributes)
    log_event("run_start", channels=channels, max_videos=max_videos, attributes=attributes)

    # State that must be recorded regardless of how run() exits.
    # Pessimistic defaults — overwritten on clean paths, preserved on crash.
    units_spent      = 0
    videos_fetched   = 0
    channels_touched = len(channels)
    outcome          = "failed"
    resolved         = []

    try:
        # ── Quota preflight ────────────────────────────────────────────────
        estimated = estimate_run_units(len(channels), max_videos, BATCH_SIZE)
        decision  = quota_preflight(estimated)
        if decision == "abort":
            log.error(
                f"Quota hard-stop reached ({int(QUOTA_STOP_PCT*100)} % of {QUOTA_LIMIT}). "
                f"Aborting before any API call."
            )
            log_event("run_aborted", reason="quota_hard_stop", estimated_units=estimated)
            outcome = "aborted_over_quota"
            raise QuotaExhaustedError(
                f"Quota hard-stop reached ({int(QUOTA_STOP_PCT*100)}% of {QUOTA_LIMIT}). "
                f"No API calls made this run."
            )
        if decision == "warn":
            log.warning(
                f"Quota soft warning — projected usage will cross "
                f"{int(QUOTA_WARN_PCT*100)} % of daily quota. Continuing."
            )

        youtube   = _build_client()
        today     = datetime.now(timezone.utc).date()
        today_str = today.isoformat()

        # ── Resolve channels ───────────────────────────────────────────────
        log.info("Resolving channels...")
        for c in channels:
            ch = resolve_channel(youtube, c)
            units_spent += 1   # channels.list is billed per attempt, success or not
            if ch:
                resolved.append(ch)
                log_event("channel_resolved", handle=ch["name"], channel_id=ch["channel_id"])
        channels_touched = len(resolved) or len(channels)
        log.info(f"Resolved {len(resolved)}/{len(channels)} channels")

        if not resolved:
            log.error("No channels resolved. Aborting.")
            outcome = "no_work"
            return

        # ── Load cache (reads both layouts) ────────────────────────────────
        seen = load_seen_videos()

        # ── Phase 1: build fetch queue ─────────────────────────────────────
        fetch_queue = []
        for channel in resolved:
            log.info(f"Scanning {channel['name']}...")
            for video_id, published_at, title in get_latest_videos(youtube, channel, max_videos):
                if should_fetch(video_id, published_at, seen, today):
                    fetch_queue.append((channel, video_id, published_at, title))
                else:
                    log.debug(f"  Skipping {video_id} (cached / too old)")
            units_spent += 1   # playlistItems.list — one per resolved channel

        log.info(f"Videos queued: {len(fetch_queue)}")
        log_event("videos_queued", count=len(fetch_queue))

        if not fetch_queue:
            log.info("Nothing new to fetch — all done.")
            log_event(
                "run_complete",
                clean=0, missing=0,
                api_units=units_spent,
                skipped_reason="nothing_new",
            )
            outcome = "no_work"
            return

        # ── Phase 2: fetch stats ───────────────────────────────────────────
        video_ids = [item[1] for item in fetch_queue]
        stats_map = fetch_stats_batch(youtube, video_ids, attributes)
        units_spent += math.ceil(len(video_ids) / BATCH_SIZE)  # videos.list batches

        # ── Phase 3: build + write rows ────────────────────────────────────
        rows          = []
        clean_count   = 0
        missing_count = 0

        for channel, video_id, published_at, title in fetch_queue:
            stats  = stats_map.get(video_id)
            status = "clean" if stats else "missing"

            row = {
                "fetched_date" : today_str,
                "channel_id"   : channel["channel_id"],
                "channel_name" : channel["name"],
                "video_id"     : video_id,
                "title"        : title,
                "published_at" : published_at,
                "status"       : status,
            }
            if stats:
                row.update(stats)
            else:
                row.update({attr: "" for attr in attributes})

            rows.append(row)
            clean_count   += (status == "clean")
            missing_count += (status == "missing")

        write_output(rows, attributes, today_str)
        videos_fetched = clean_count + missing_count

        log.info(f"Done — {clean_count} clean, {missing_count} missing, ~{units_spent} API units used.")
        log_event("run_complete", clean=clean_count, missing=missing_count, api_units=units_spent)
        outcome = "success"

    except QuotaExhaustedError:
        # outcome already set to "aborted_over_quota" above; event already emitted.
        raise
    except Exception as e:
        # Anything else — API blew up, disk full, permission denied, etc.
        # outcome stays "failed" so the finally block records it, then re-raise
        # so Airflow sees the task as failed.
        log.error(f"Unhandled error during run: {e}")
        log_event("run_error", error=str(e))
        raise
    finally:
        # ALWAYS runs — closes the "units spent before a crash went unrecorded" leak.
        record_quota_usage(
            units_this_run = units_spent,
            channels       = channels_touched,
            videos_fetched = videos_fetched,
            outcome        = outcome,
        )

# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="YouTube competitor data collector (v2).")
    parser.add_argument(
        "--format",
        choices=["parquet", "csv"],
        default=_cfg["output"].get("format", "parquet"),
        help="Output format (default: value from config.yaml)",
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        default=None,
        help="Channel handles to collect (default: all from config.yaml)",
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=None,
        help="Max videos per channel (default: value from config.yaml)",
    )
    args = parser.parse_args()

    # Override config so write_output picks it up
    _cfg["output"]["format"] = args.format

    run(channels=args.channels, max_videos=args.max_videos)
