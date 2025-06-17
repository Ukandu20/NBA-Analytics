#!/usr/bin/env python3
"""
clean_player_clutch.py
======================
Clean the **player Clutch** raw CSVs (season-first layout) and write:

* data/processed/player_stats/clutch/<season>/<perMode>/<season_type>/<measure>.csv
  (one cleaned master table per raw file)
* data/teams/player_stats/clutch/<TEAM>/<perMode>/<season_type>/<measure>.csv
  (per-team extracts – no season level)
"""
from __future__ import annotations

import argparse, logging
from pathlib import Path
import pathlib, sys
import pandas as pd
from typing import Iterable, List
RAW_ROOT   = Path("data/raw/player_stats/clutch")
PROC_ROOT  = Path("data/processed/player_stats/clutch")
TEAM_ROOT  = Path("data/teams/player_stats/clutch")     # no season sub-dir

# ── helpers ──────────────────────────────────────────────────────────────
def _maybe_numeric(col: pd.Series) -> pd.Series:
    """
    Convert numeric-looking strings (incl. '35.7%' → 0.357) to floats.
    Leaves true text columns untouched.
    """
    if col.dtype != "object":
        return col

    pct_mask = col.str.endswith("%", na=False)
    stripped = col.str.rstrip("%")
    numeric  = pd.to_numeric(stripped, errors="coerce")

    # if everything became NaN, keep original text column
    if numeric.notna().sum() == 0:
        return col

    numeric[pct_mask] = numeric[pct_mask] / 100.0
    return numeric


def _clean_one(src: pathlib.Path, dst_master: pathlib.Path, *, force: bool) -> None:
    """Clean one raw CSV and write master + per-team splits."""
    if not force and dst_master.exists():
        logging.info("Skip existing %s", dst_master)
        return

    df = pd.read_csv(src)
    if df.empty:
        logging.warning("Empty %s", src)
        return

    df.columns = normalise_cols(df.columns)
    df = df.rename(columns={
        "player_name": "player"
        "player_last_team_id": "team_id",
        "player_last_team_abbreviation": "team",
        "team_abbreviation": "team",
    }, inplace=True)

    _ensure_team(df)  # create/standardise a `team` column
    _add_season_bounds(df)  # add `season_start` and `season_end

    

    # ── write cleaned master ───────────────────────────────────────────
    dst_master.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst_master, index=False)
    logging.info("Written %s (%d rows)", dst_master, len(df))

    # ── per-team splits ────────────────────────────────────────────────
    team_col = next((c for c in ("TEAM_ABBREVIATION", "TEAM_ID") if c in df.columns), None)
    if team_col is None:
        logging.warning("No team column in %s", src)
        return

    per_mode    = dst_master.parents[1].name     # Totals | Per36 | …
    season_type = dst_master.parent.name         # regular_season | playoffs

    for team_val, sub in df.groupby(team_col):
        team_dir = TEAM_ROOT / str(team_val).upper() / per_mode / season_type
        team_dir.mkdir(parents=True, exist_ok=True)
        sub.to_csv(team_dir / dst_master.name, index=False)

# ── main walker ──────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean player-Clutch CSVs (season-first layout)"
    )
    parser.add_argument("--season", required=True, nargs="+",
                        help="Season(s) YYYY-YY, e.g. 2024-25 2023-24")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing cleaned files")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")

    for season in args.season:
        raw_season  = RAW_ROOT  / season
        proc_season = PROC_ROOT / season
        for mode_dir in raw_season.iterdir():          # Totals / Per36 / …
            if not mode_dir.is_dir():
                continue
            for csv in mode_dir.rglob("*.csv"):
                season_type = csv.parent.name          # regular_season | playoffs
                dst = proc_season / mode_dir.name / season_type / csv.name
                _clean_one(csv, dst, force=args.force)

if __name__ == "__main__":
    main()
