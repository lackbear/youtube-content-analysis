import os
import json
import time
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
MAX_RETRIES      = _cfg["api"]["max_retries"]
BATCH_SIZE       = _cfg["api"]["batch_size"]
VIDEO_STATS_FILE = _cfg["output"]["video_stats_file"]
DEFAULT_CHANNELS = [ch["handle"] for ch in _cfg["channels"]]
DEFAULT_ATTRS    = _cfg["attributes"]

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
        run_start       — collector kicked off
        channel_resolved — handle → channel_id resolved
        videos_queued   — how many videos will be fetched
        run_complete    — final counts + estimated API units used
        run_error       — unhandled exception during run
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

# ── YouTube client ────────────────────────────────────────────────────────────

def _build_client():
    key_var = _cfg["api"]["key_env_var"]
    api_key = os.environ.get(key_var, "")
    if not api_key:
        raise EnvironmentError(
            f"API key not set. Run: export {key_var}=\'your_key\'"
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
    Load cache from all date-partitioned Parquet files.
    Returns dict: { video_id -> latest fetched_date (date obj) }
    """
    data_dir = Path("data/raw/video_stats")
    if not data_dir.exists():
        log.info("Cache loaded — 0 known videos (first run)")
        return {}

    parquet_files = list(data_dir.glob("date=*/video_stats.parquet"))
    csv_files     = list(data_dir.glob("date=*/video_stats.csv"))
    all_files     = parquet_files + csv_files

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
    log.info(f"Cache loaded — {len(seen)} known videos from {len(parquet_files)} partition(s)")
    return seen


def should_fetch(video_id, published_at_str, seen, today):
    """
    Cache decision:
    - Unknown              → fetch
    - Already fetched today → skip
    - Under REFRESH_DAYS   → re-fetch (velocity tracking)
    - Older                → skip
    """
    if video_id not in seen:
        return True
    if seen[video_id] >= today:
        return False
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
    Write today's snapshot to a date-partitioned file.
    Path: data/raw/video_stats/date=YYYY-MM-DD/video_stats.parquet|csv
    Format is controlled by output.format in config.yaml (default: parquet).
    Each run overwrites that day's partition (idempotent).
    """
    fmt  = _cfg["output"].get("format", "parquet").lower()
    if fmt not in ("parquet", "csv"):
        raise ValueError(f"Unsupported output format '{fmt}'. Use 'parquet' or 'csv'.")

    cols = ["fetched_date", "channel_id", "channel_name",
            "video_id", "title", "published_at"] + attributes + ["status"]

    df            = pd.DataFrame(rows).reindex(columns=cols)
    partition_dir = Path(f"data/raw/video_stats/date={today_str}")
    partition_dir.mkdir(parents=True, exist_ok=True)

    if fmt == "parquet":
        out_path = partition_dir / "video_stats.parquet"
        df.to_parquet(out_path, index=False)
    else:
        out_path = partition_dir / "video_stats.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")

    log.info(f"Written {len(rows)} rows → {out_path} [{fmt}]")

# ── Main entry point ──────────────────────────────────────────────────────────

def run(channels=None, max_videos=None, attributes=None):
    """
    Daily YouTube competitor collector.

    Args:
        channels   : list of channel handles (e.g. 'SiimLand'), channel IDs
                     (e.g. 'UCxxxxx'), or a mix of both.
        max_videos : videos to pull per channel (1–50, default 10).
        attributes : attributes to fetch, or None for all.
                     Options: views, likes, comments, duration,
                              tags, description, thumbnail_url, category_id

    Example:
        run(
            channels=["SiimLand", "UCxxxxx"],
            max_videos=10,
            attributes=["views", "likes", "duration"],
        )
    """
    # Fall back to config.yaml values if not explicitly passed
    if channels    is None: channels    = DEFAULT_CHANNELS
    if max_videos  is None: max_videos  = _cfg['collection']['max_videos_per_channel']
    if attributes  is None: attributes  = DEFAULT_ATTRS

    preflight(channels, max_videos, attributes)
    log_event("run_start", channels=channels, max_videos=max_videos, attributes=attributes)
    youtube   = _build_client()
    today     = datetime.now(timezone.utc).date()
    today_str = today.isoformat()

    # ── Resolve channels ───────────────────────────────────────────────────
    log.info("Resolving channels...")
    resolved = [ch for ch in
                (resolve_channel(youtube, c) for c in channels)
                if ch]
    log.info(f"Resolved {len(resolved)}/{len(channels)} channels")
    for ch in resolved:
        log_event("channel_resolved", handle=ch["name"], channel_id=ch["channel_id"])

    if not resolved:
        log.error("No channels resolved. Aborting.")
        return

    # ── Load cache ─────────────────────────────────────────────────────────
    seen = load_seen_videos()

    # ── Phase 1: build fetch queue ─────────────────────────────────────────
    fetch_queue = []

    for channel in resolved:
        log.info(f"Scanning {channel['name']}...")
        for video_id, published_at, title in get_latest_videos(youtube, channel, max_videos):
            if should_fetch(video_id, published_at, seen, today):
                fetch_queue.append((channel, video_id, published_at, title))
            else:
                log.debug(f"  Skipping {video_id} (cached / too old)")

    log.info(f"Videos queued: {len(fetch_queue)}")
    log_event("videos_queued", count=len(fetch_queue))

    if not fetch_queue:
        log.info("Nothing new to fetch — all done.")
        log_event("run_complete", clean=0, missing=0, api_units=0, skipped_reason="nothing_new")
        return

    # ── Phase 2: fetch stats ───────────────────────────────────────────────
    video_ids = [item[1] for item in fetch_queue]
    stats_map = fetch_stats_batch(youtube, video_ids, attributes)

    # ── Phase 3: build + write rows ────────────────────────────────────────
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
            **({} if not stats else stats),
            **({"": ""} if not stats else {}),
        }
        if not stats:
            row.update({attr: "" for attr in attributes})

        rows.append(row)
        clean_count   += (status == "clean")
        missing_count += (status == "missing")

    write_output(rows, attributes, today_str)

    units = len(resolved) + len(resolved) * 2 + (len(video_ids) // BATCH_SIZE + 1)
    log.info(f"Done — {clean_count} clean, {missing_count} missing, ~{units} API units used.")
    log_event("run_complete", clean=clean_count, missing=missing_count, api_units=units)

# ── CLI ───────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="YouTube competitor data collector.")
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
