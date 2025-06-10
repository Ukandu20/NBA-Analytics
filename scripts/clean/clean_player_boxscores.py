#!/usr/bin/env python3
"""
clean_player_boxscores.py
=========================

Clean the raw **player** traditional game-log CSVs created by
`scrape_player_boxscores.py` **and** place a copy of every file under a
team-keyed folder (same convention you used for the advanced logs).

Cleaned layout
--------------
data/processed/player_stats/box_scores/<season>/
├─ regular_season_traditional.csv          ← full table, cleaned
├─ playoffs_traditional.csv
└─ teams/
   ├─ LAL/
   │   ├─ regular_season_traditional.csv    ← rows where team == LAL
   │   └─ playoffs_traditional.csv
   ├─ BOS/
   │   └─ …
   ⋮

Cleaning steps
--------------
1. Normalise column names (utils.clean_helpers.normalise_cols)
2. `team_abbreviation` → `team` (upper-case, NaN → "")
3. Parse `season` (YYYY-YY) → `season_start`, `season_end`
4. Convert every remaining column to numeric (`errors="coerce"`)
   with utils.numeric_helpers.coerce_all_numeric
5. Drop exact duplicate rows
6. Write the cleaned master file **and** per-team copies

CLI
---
  -s, --season   YYYY-YY          clean one season          (default 2024-25)
  -S, --seasons  YYYY-YY …        clean multiple seasons
  -a, --all                       clean every season folder found
  -f, --force                     overwrite existing processed files
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Iterable, List

import pandas as pd

# ── project helpers ─────────────────────────────────────────────────────────
ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from utils.clean_helpers import normalise_cols          # type: ignore
from utils.numeric_helpers import coerce_all_numeric    # type: ignore

RAW_ROOT  = ROOT / "data/raw/player_stats/boxscores"
PROC_ROOT = ROOT / "data/processed/player_stats/boxscores"

# ── column helpers ──────────────────────────────────────────────────────────
def _standardise_team_abbrev(df: pd.DataFrame) -> None:
    if "team_abbreviation" in df.columns:
        df.rename(columns={"team_abbreviation": "team"}, inplace=True)
    if "team" in df.columns:
        df["team"] = df["team"].fillna("").astype(str).str.upper()

def _add_season_bounds(df: pd.DataFrame) -> None:
    if "season_year" in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)
    if "season" in df.columns:
        start = df["season"].astype(str).str.extract(r"^(\d{4})", expand=False)
        df["season_start"] = pd.to_numeric(start, errors="coerce")
        df["season_end"]   = df["season_start"] + 1

# ── small I/O util ──────────────────────────────────────────────────────────
def _write_csv(path: pathlib.Path, df: pd.DataFrame, *, force: bool) -> None:
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

# ── cleaner for a single file ───────────────────────────────────────────────
def clean_one_csv(
    src: pathlib.Path,
    dst_main: pathlib.Path,
    team_root: pathlib.Path,
    *,
    force: bool,
) -> pd.DataFrame | None:
    df = pd.read_csv(src)
    if df.empty:
        print(f"⚠️  {src.name}: empty file — skipped")
        return None

    df.columns = normalise_cols(df.columns)
    _standardise_team_abbrev(df)
    _add_season_bounds(df)

    exclude = set(df.select_dtypes(include=["object", "datetime"]).columns)
    df = coerce_all_numeric(df, list(exclude))
    df.drop_duplicates(inplace=True)

    if df.empty:
        print(f"⚠️  {src.name}: no rows after cleaning — skipped")
        return None

    # master file
    _write_csv(dst_main, df, force=force)
    print(f"✅ {dst_main.relative_to(ROOT)}  ({len(df):,} rows)")
    return df



# ── season driver ───────────────────────────────────────────────────────────
def clean_season(season: str, *, force: bool) -> None:
    raw_dir   = RAW_ROOT  / season
    proc_dir  = PROC_ROOT / season


    if not raw_dir.exists():
        print(f"⚠️  no raw data for {season}")
        return

    for csv_path in raw_dir.glob("*.csv"):
        base_name = csv_path.name
        out_main  = proc_dir / base_name
        
        df = clean_one_csv(csv_path, out_main, proc_dir / "teams", force=force)
        if df is None or df.empty:
            continue


                # team split
        if "team" in df.columns:
            for team, grp in df.groupby("team"):
                team_file = proc_dir / "teams" / str(team).upper() / base_name  # ← str()
                _write_csv(team_file, grp, force=force)

# ── CLI plumbing ────────────────────────────────────────────────────────────
def _all_seasons() -> List[str]:
    if not RAW_ROOT.exists():
        return []
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir())

def _parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Clean traditional player box-score game logs.",
    )
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("-s", "--season",  help="clean one season (e.g. 2024-25)")
    grp.add_argument("-S", "--seasons", nargs="+", help="clean multiple seasons")
    grp.add_argument("-a", "--all",     action="store_true",
                     help="clean every season folder found")
    ap.add_argument("-f", "--force",    action="store_true",
                    help="overwrite existing processed files")
    return ap.parse_args()

def _resolve_targets(args: argparse.Namespace) -> Iterable[str]:
    if args.all:
        return _all_seasons()
    if args.seasons:
        return args.seasons
    return [args.season or "2024-25"]

# ── main ────────────────────────────────────────────────────────────────────
def main() -> None:
    args = _parse_cli()
    for season in _resolve_targets(args):
        print(f"\n📂 Cleaning season {season}")
        clean_season(season, force=args.force)

if __name__ == "__main__":
    main()
