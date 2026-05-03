"""
promote_candidates.py — close the curator loop (chapter 6 commit 3.5).

The replacement queue mechanic: when a channel is flagged stale
(no new posts in N days) AND there's an accepted candidate in the
matching niche, swap them — deactivate the stale row, append the
candidate as active, mark the candidate as promoted.

Reads:
  - data/curator/stale_channels.csv  (stale=true rows are eligible to replace)
  - data/curator/candidates.csv      (status=accepted rows are eligible candidates)
  - competitors.csv                  (the registry to mutate)
  - config.yaml                      (curator.max_registry_size, default 200)

Writes:
  - competitors.csv                  (1 row deactivated + 1 row appended per pair;
                                     plus FIFO-eviction of the oldest inactive row
                                     when the file is at the size cap)
  - data/curator/candidates.csv      (status: accepted -> promoted)

Niche matching:
  - Strict by default: a candidate's niche must equal the stale channel's niche.
  - Exception: if the stale channel's niche is 'legacy', any niche matches.
    Legacy channels have no real niche metadata; this lets the chapter 6
    sourcing replace them during the migration period.

Hard cap on registry size (curator.max_registry_size, default 200):
  - The competitors.csv file holds at most N total rows (active + inactive).
  - Each promotion adds 1 row (candidate appended) without removing the stale row
    (we keep it inactive for historical join resolution). So promotions grow the
    file by 1 per pair. When we're at the cap, a pair would push us over.
  - To stay at-or-under N: evict the OLDEST inactive row (FIFO on
    deactivated_date, with handle as tiebreak), then proceed with the promotion.
  - If the cap is hit and there's no inactive row to evict (all 200 are active),
    the pair is skipped with a warning. The candidate stays 'accepted' for next time.

Each candidate is consumed at most once per run.

Run:
    python scripts/promote_candidates.py            # apply changes
    python scripts/promote_candidates.py --dry-run  # print what WOULD happen
"""

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

# UTF-8 stdout for Windows — channel names contain emoji.
for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


PROJECT_ROOT     = Path(__file__).resolve().parent.parent
COMPETITORS_CSV  = PROJECT_ROOT / "competitors.csv"
STALE_CSV        = PROJECT_ROOT / "data" / "curator" / "stale_channels.csv"
CANDIDATES_CSV   = PROJECT_ROOT / "data" / "curator" / "candidates.csv"
CONFIG_PATH      = Path(os.environ.get("COLLECTOR_CONFIG", str(PROJECT_ROOT / "config.yaml")))

DEACTIVATED_REASON_PROMOTE = "auto_replaced_by_curator"
DEACTIVATED_REASON_EVICT   = "evicted_by_registry_cap"
DEFAULT_MAX_REGISTRY_SIZE  = 200


def _load_max_registry_size() -> int:
    """Read curator.max_registry_size from config; default 200."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return int(cfg.get("curator", {}).get("max_registry_size", DEFAULT_MAX_REGISTRY_SIZE))
    except Exception:
        return DEFAULT_MAX_REGISTRY_SIZE


def _match_candidates(stale_df: pd.DataFrame, accepted_df: pd.DataFrame):
    """
    Pair stale rows with accepted candidates by niche.

    Returns list of (stale_row, candidate_row) tuples. Each candidate is
    consumed at most once per run; legacy stale channels fall through to
    a "match any niche" pass.
    """
    pairs = []
    accepted = accepted_df.copy()
    used_handles = set()

    # Pass 1: strict niche match
    for _, stale in stale_df.iterrows():
        if stale["niche"] == "legacy":
            continue
        pool = accepted[
            (accepted["niche"] == stale["niche"])
            & (~accepted["handle"].isin(used_handles))
        ]
        if len(pool) == 0:
            continue
        cand = pool.iloc[0]
        pairs.append((stale, cand))
        used_handles.add(cand["handle"])

    # Pass 2: fall-through for legacy stale channels (any niche)
    for _, stale in stale_df.iterrows():
        if stale["niche"] != "legacy":
            continue
        pool = accepted[~accepted["handle"].isin(used_handles)]
        if len(pool) == 0:
            continue
        cand = pool.iloc[0]
        pairs.append((stale, cand))
        used_handles.add(cand["handle"])

    return pairs


def _evict_oldest_inactive(competitors: pd.DataFrame, today_str: str):
    """
    Find the oldest inactive row eligible for eviction (deactivated BEFORE today)
    and return (df_without_it, victim_row). If no eligible row exists, return
    (df_unchanged, None).

    Excludes rows deactivated today — those are this run's freshly-flipped stale
    channels, and we want to preserve their replacement record.
    Sort key: deactivated_date ASC (oldest first), then handle ASC for tiebreak.
    """
    eligible = competitors[
        (competitors["active"] == "false")
        & (competitors["deactivated_date"] != today_str)
        & (competitors["deactivated_date"] != "")
    ]
    if eligible.empty:
        return competitors, None
    sorted_eligible = eligible.sort_values(["deactivated_date", "handle"])
    victim = sorted_eligible.iloc[0]
    return competitors.drop(index=victim.name).copy(), victim


def promote(dry_run: bool = False) -> int:
    """
    Apply (or simulate) the queue replacement. Returns number of
    promotions performed (or that would be performed in dry-run).
    """
    if not all(p.exists() for p in (COMPETITORS_CSV, STALE_CSV, CANDIDATES_CSV)):
        missing = [str(p) for p in (COMPETITORS_CSV, STALE_CSV, CANDIDATES_CSV) if not p.exists()]
        raise FileNotFoundError(f"Missing required input(s): {missing}")

    competitors = pd.read_csv(COMPETITORS_CSV, dtype=str, keep_default_na=False)
    stale_df    = pd.read_csv(STALE_CSV,       dtype=str, keep_default_na=False)
    candidates  = pd.read_csv(CANDIDATES_CSV,  dtype=str, keep_default_na=False)

    # Filter the eligible pools.
    stale_eligible = stale_df[stale_df["stale"].str.strip().str.lower() == "true"].copy()
    accepted       = candidates[candidates["status"].str.strip().str.lower() == "accepted"].copy()

    print(f"  stale (eligible to replace): {len(stale_eligible)}")
    print(f"  candidates accepted (eligible to promote): {len(accepted)}")

    if len(stale_eligible) == 0 or len(accepted) == 0:
        print("  nothing to do.")
        return 0

    pairs = _match_candidates(stale_eligible, accepted)
    if not pairs:
        print("  no niche-compatible pairs found.")
        return 0

    print(f"  pairs to promote: {len(pairs)}")
    max_size = _load_max_registry_size()
    print(f"  registry cap: {max_size} rows; current: {len(competitors)}")
    today_str = date.today().isoformat()
    new_competitor_rows = []
    promoted_handles    = []
    n_skipped_cap       = 0
    predicted_size      = len(competitors)
    prefix              = "[DRY-RUN] " if dry_run else ""

    for stale_row, cand_row in pairs:
        # Cap check BEFORE flipping anything. Each promotion adds 1 row (the
        # appended candidate); the stale flip is in-place and doesn't change
        # the row count.
        if predicted_size >= max_size:
            new_competitors, evicted = _evict_oldest_inactive(competitors, today_str)
            if evicted is None:
                print(
                    f"  {prefix}[skip-cap] cannot promote "
                    f"{stale_row['handle']} -> {cand_row['handle']}: "
                    f"registry at cap ({max_size}) and no evictable inactive row."
                )
                n_skipped_cap += 1
                continue
            competitors = new_competitors
            predicted_size -= 1
            print(
                f"  {prefix}[evict]      {evicted['handle']:<25} "
                f"(was inactive since {evicted['deactivated_date']}, "
                f"reason={evicted['deactivated_reason']})"
            )

        # Flip the stale row to inactive.
        mask = competitors["channel_id"] == stale_row["channel_id"]
        if mask.any():
            competitors.loc[mask, "active"]             = "false"
            competitors.loc[mask, "deactivated_date"]   = today_str
            competitors.loc[mask, "deactivated_reason"] = DEACTIVATED_REASON_PROMOTE

        # Stage the new active row.
        new_competitor_rows.append({
            "handle":                  cand_row["handle"],
            "channel_id":              cand_row["channel_id"],
            "name":                    cand_row["name"],
            "niche":                   cand_row["niche"],
            "tier":                    cand_row["tier"],
            "subscribers_at_addition": cand_row.get("subscribers", ""),
            "added_date":              today_str,
            "active":                  "true",
            "deactivated_date":        "",
            "deactivated_reason":      "",
        })
        promoted_handles.append(cand_row["handle"])
        predicted_size += 1
        print(
            f"  {prefix}{stale_row['handle']:<25} ({stale_row['niche']}) "
            f"-> {cand_row['handle']:<25} ({cand_row['niche']}, {cand_row['tier']}, "
            f"{cand_row.get('subscribers', '?')} subs)"
        )

    summary = (
        f"  {len(promoted_handles)} promoted, {n_skipped_cap} cap-skipped. "
        f"Final registry size: {predicted_size}/{max_size}."
    )

    if dry_run:
        print(f"  [DRY-RUN] {summary} No files modified.")
        return len(promoted_handles)

    # Concat the new active rows and write back.
    new_df = pd.DataFrame(new_competitor_rows, columns=competitors.columns)
    competitors_out = pd.concat([competitors, new_df], ignore_index=True)
    competitors_out.to_csv(COMPETITORS_CSV, index=False)

    # Flip candidate status: accepted -> promoted.
    promoted_mask = candidates["handle"].isin(promoted_handles)
    candidates.loc[promoted_mask, "status"] = "promoted"
    candidates.to_csv(CANDIDATES_CSV, index=False)

    print(summary)
    print(f"  Wrote {COMPETITORS_CSV} and {CANDIDATES_CSV}.")
    return len(promoted_handles)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen, don't modify files.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    promote(dry_run=args.dry_run)
