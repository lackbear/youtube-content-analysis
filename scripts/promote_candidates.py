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

Writes:
  - competitors.csv                  (1 row deactivated + 1 row appended per pair)
  - data/curator/candidates.csv      (status: accepted -> promoted)

Niche matching:
  - Strict by default: a candidate's niche must equal the stale channel's niche.
  - Exception: if the stale channel's niche is 'legacy', any niche matches.
    Legacy channels have no real niche metadata; this lets the chapter 6
    sourcing replace them during the migration period.

Each candidate is consumed at most once per run. If accepted candidates
exceed stale slots in a niche, leftover candidates stay accepted and
wait for the next stale event.

Run:
    python scripts/promote_candidates.py            # apply changes
    python scripts/promote_candidates.py --dry-run  # print what WOULD happen
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# UTF-8 stdout for Windows — channel names contain emoji.
for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


PROJECT_ROOT     = Path(__file__).resolve().parent.parent
COMPETITORS_CSV  = PROJECT_ROOT / "competitors.csv"
STALE_CSV        = PROJECT_ROOT / "data" / "curator" / "stale_channels.csv"
CANDIDATES_CSV   = PROJECT_ROOT / "data" / "curator" / "candidates.csv"

DEACTIVATED_REASON = "auto_replaced_by_curator"


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
    today_str = date.today().isoformat()
    new_competitor_rows = []
    promoted_handles   = []

    for stale_row, cand_row in pairs:
        msg = (
            f"  {'[DRY-RUN] ' if dry_run else ''}"
            f"{stale_row['handle']:<25} ({stale_row['niche']}) "
            f"-> {cand_row['handle']:<25} ({cand_row['niche']}, {cand_row['tier']}, "
            f"{cand_row.get('subscribers', '?')} subs)"
        )
        print(msg)

        # Mark stale row inactive in the in-memory competitors copy.
        mask = competitors["channel_id"] == stale_row["channel_id"]
        if mask.any():
            competitors.loc[mask, "active"]             = "false"
            competitors.loc[mask, "deactivated_date"]   = today_str
            competitors.loc[mask, "deactivated_reason"] = DEACTIVATED_REASON

        # Append a new active row built from the candidate.
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

    if dry_run:
        print(f"  [DRY-RUN] would promote {len(pairs)} pairs. No files modified.")
        return len(pairs)

    # Append new active rows to competitors and write back.
    new_df = pd.DataFrame(new_competitor_rows, columns=competitors.columns)
    competitors_out = pd.concat([competitors, new_df], ignore_index=True)
    competitors_out.to_csv(COMPETITORS_CSV, index=False)

    # Flip candidate status: accepted -> promoted.
    promoted_mask = candidates["handle"].isin(promoted_handles)
    candidates.loc[promoted_mask, "status"] = "promoted"
    candidates.to_csv(CANDIDATES_CSV, index=False)

    print(f"  promoted {len(pairs)} pairs. Wrote {COMPETITORS_CSV}, {CANDIDATES_CSV}.")
    return len(pairs)


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
