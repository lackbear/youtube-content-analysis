"""
discover.py — validate AI-sourced candidate channels against the YouTube API.

Reads:
  - data/curator/candidates_input.csv   (handle, niche, tier — from the AI run)
  - competitors.csv                     (so we don't re-suggest tracked channels)
  - data/curator/candidates.csv         (so we preserve user's accepted/rejected status)

Writes:
  - data/curator/candidates.csv
    Schema:
      handle, channel_id, name, niche, tier, subscribers,
      last_published_at, status, discovered_date, notes

Quota cost: ~2 units per candidate (channels.list + playlistItems.list). For the
initial 200-channel bulk: 400 units = 4% of daily quota. For the weekly DAG with
≤20 new candidates: ~40 units. Both negligible.

Status flow (manual): pending -> accepted | rejected. Accepted candidates get
moved into competitors.csv by a future "promote" step (chapter 6 commit 3.5
or a cron). Rejected stays in candidates.csv so the same handle isn't suggested
again on the next run.

Run:
    python scripts/discover.py
"""

import logging
import os
from datetime import date
from pathlib import Path


PROJECT_ROOT     = Path(__file__).resolve().parent.parent
INPUT_PATH       = PROJECT_ROOT / "data" / "curator" / "candidates_input.csv"
OUTPUT_PATH      = PROJECT_ROOT / "data" / "curator" / "candidates.csv"
COMPETITORS_CSV  = PROJECT_ROOT / "competitors.csv"
CONFIG_PATH      = Path(os.environ.get("COLLECTOR_CONFIG", str(PROJECT_ROOT / "config.yaml")))


def _build_client():
    """YouTube API client. Reads the same env-var name as Collectorv2 for parity."""
    import yaml
    from dotenv import load_dotenv
    from googleapiclient.discovery import build

    load_dotenv()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    key_var = cfg.get("api", {}).get("key_env_var", "YOUTUBE_API_KEY")
    api_key = os.environ.get(key_var)
    if not api_key:
        raise RuntimeError(
            f"Missing env var {key_var} — needed to call the YouTube API. "
            f"Set it in .env or export it."
        )
    return build("youtube", "v3", developerKey=api_key)


def _tier_from_subs(subs: int) -> str:
    """Bucket by subscriber count: top ≥500k, micro 100k-500k, new <100k."""
    if subs >= 500_000:
        return "top"
    if subs >= 100_000:
        return "micro"
    return "new"


def _existing_handles_and_ids() -> set:
    """
    Build a set of {handle.lower(), channel_id} we've already tracked OR
    already discovered. Both spellings are added so dedup catches either input.
    Empty strings are stripped — they're not real identifiers.
    """
    import pandas as pd

    seen = set()
    for path in (COMPETITORS_CSV, OUTPUT_PATH):
        if not path.exists():
            continue
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        for col in ("handle", "channel_id"):
            if col in df.columns:
                seen.update(df[col].str.strip())
                seen.update(df[col].str.strip().str.lower())
    seen.discard("")
    return seen


def _resolve_channel(youtube, handle: str):
    """Resolve handle to a dict with channel info, or None if not found."""
    try:
        result = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            forHandle=handle,
        ).execute()
    except Exception as e:
        logging.warning(f"channels.list failed for '{handle}': {e}")
        return None

    items = result.get("items", [])
    if not items:
        return None

    item = items[0]
    return {
        "channel_id":      item["id"],
        "name":            item["snippet"]["title"],
        "subscribers":     int(item["statistics"].get("subscriberCount", 0)),
        "uploads_playlist": item["contentDetails"]["relatedPlaylists"]["uploads"],
    }


def _last_published_at(youtube, uploads_playlist_id: str) -> str:
    """Most recent video's publishedAt (ISO string), or '' on failure."""
    try:
        result = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=1,
        ).execute()
        items = result.get("items", [])
        if items:
            return items[0]["snippet"].get("publishedAt", "")
    except Exception as e:
        logging.warning(f"playlistItems.list failed: {e}")
    return ""


def discover() -> Path:
    """
    Read candidates_input.csv, validate each handle, dedup against existing
    tracked + discovered channels, append new validated rows to candidates.csv
    (preserving any prior status). Returns the output path.
    """
    import pandas as pd

    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"No candidates input at {INPUT_PATH}. "
            f"Save your AI-sourced CSV there first."
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    inp = pd.read_csv(INPUT_PATH, dtype=str, keep_default_na=False)

    # Preserve user-edited statuses across runs by reading + appending
    out_columns = [
        "handle", "channel_id", "name", "niche", "tier",
        "subscribers", "last_published_at", "status",
        "discovered_date", "notes",
    ]
    if OUTPUT_PATH.exists():
        existing = pd.read_csv(OUTPUT_PATH, dtype=str, keep_default_na=False)
    else:
        existing = pd.DataFrame(columns=out_columns)

    seen = _existing_handles_and_ids()
    youtube = _build_client()
    today_str = date.today().isoformat()

    new_rows = []
    n_skip_dup     = 0
    n_skip_unresolved = 0

    for _, row in inp.iterrows():
        handle     = row.get("handle", "").strip()
        niche      = row.get("niche",  "").strip()
        tier_input = row.get("tier",   "").strip().lower()

        if not handle:
            continue
        if handle.lower() in seen or handle in seen:
            n_skip_dup += 1
            continue

        resolved = _resolve_channel(youtube, handle)
        if not resolved:
            n_skip_unresolved += 1
            print(f"  not_resolved: {handle}")
            continue
        if resolved["channel_id"] in seen:
            n_skip_dup += 1
            continue

        last_pub  = _last_published_at(youtube, resolved["uploads_playlist"])
        tier_real = _tier_from_subs(resolved["subscribers"])
        notes = ""
        if tier_input and tier_input != tier_real:
            notes = f"tier_mismatch input={tier_input} validated={tier_real}"

        new_rows.append({
            "handle":            handle,
            "channel_id":        resolved["channel_id"],
            "name":              resolved["name"],
            "niche":             niche,
            "tier":              tier_real,
            "subscribers":       resolved["subscribers"],
            "last_published_at": last_pub,
            "status":            "pending",
            "discovered_date":   today_str,
            "notes":             notes,
        })
        seen.add(handle.lower())
        seen.add(resolved["channel_id"])
        print(f"  ok: {handle:<25} -> {resolved['name']:<25} "
              f"({resolved['subscribers']:>10,} subs, tier={tier_real})")

    new_df = pd.DataFrame(new_rows, columns=out_columns)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined.to_csv(OUTPUT_PATH, index=False)

    print(
        f"Wrote {OUTPUT_PATH}: "
        f"{len(existing)} existing + {len(new_rows)} new = {len(combined)} total. "
        f"({n_skip_dup} dup-skipped, {n_skip_unresolved} not_resolved.)"
    )
    return OUTPUT_PATH


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    discover()
