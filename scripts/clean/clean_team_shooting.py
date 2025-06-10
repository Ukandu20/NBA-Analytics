#!/usr/bin/env python3
"""
clean_team_shooting.py
======================

Clean the raw CSVs saved by `scrape_team_shooting.py`
(Teams ➤ “Shooting” tables—the shot-dashboard variant that lives under
*data/raw/team_stats/shooting/*).

Raw layout (per season)
-----------------------
data/raw/team_stats/shooting/<season>/
└─ totals/
   ├─ regular_season_general_overall.csv
   ├─ playoffs_shotclock.csv
   ⋮
└─ per_game/
   ├─ regular_season_general_overall.csv
   ⋮

Output layout (same tree, but under *processed*)
------------------------------------------------
data/processed/team_stats/shooting/<season>/
└─ totals/
   ├─ regular_season_general_overall.csv   ← cleaned
   ⋮
└─ per_game/
   └─ …

Cleaning steps
--------------
1. Normalise column names via ``utils.clean_helpers.normalise_cols``.
2. Convert ``team_abbreviation`` → ``team``  (upper-case, NaN → "").
3. Parse ``season`` (YYYY-YY) into numeric ``season_start`` / ``season_end``.
4. Convert every remaining column to numeric (``errors="coerce"``)
   using ``utils.numeric_helpers.coerce_all_numeric``.
5. Drop perfect duplicate rows.
6. Preserve the original filenames & folder hierarchy.

*No per-team sub-folders are created.*

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

# ── project root & helpers ──────────────────────────────────────────────────
ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from utils.clean_helpers import normalise_cols          # type: ignore
from utils.numeric_helpers import coerce_all_numeric    # type: ignore

RAW_ROOT  = ROOT / "data/raw/team_stats/shooting"
PROC_ROOT = ROOT / "data/processed/team_stats/shooting"

# ── column utilities ────────────────────────────────────────────────────────
def _standardise_team_abbrev(df: pd.DataFrame) -> None:
    """Replace team_abbreviation with team (upper-case)."""
    if "team_abbreviation" in df.columns:
        df.rename(columns={"team_abbreviation": "team"}, inplace=True)
    if "team" in df.columns:
        df["team"] = df["team"].fillna("").astype(str).str.upper()

def _add_season_bounds(df: pd.DataFrame) -> None:
    """Add integer season_start / season_end columns from 'season' text."""
    if "season_year" in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)
    if "season" in df.columns:
        start = df["season"].astype(str).str.extract(r"^(\d{4})", expand=False)
        df["season_start"] = pd.to_numeric(start, errors="coerce")
        df["season_end"]   = df["season_start"] + 1

# ── tiny I/O helper ─────────────────────────────────────────────────────────
def _write_csv(path: pathlib.Path, df: pd.DataFrame, *, force: bool) -> None:
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

# ── core cleaner ────────────────────────────────────────────────────────────
def clean_one_csv(src: pathlib.Path, dst: pathlib.Path, *, force: bool) -> None:
    df = pd.read_csv(src)
    if df.empty:
        print(f"⚠️  {src.name}: empty file — skipped")
        return

    # 1) normalise columns
    df.columns = normalise_cols(df.columns)

    # 2) team abbreviation → team
    _standardise_team_abbrev(df)

    # 3) season bounds
    _add_season_bounds(df)

    # 4) convert numerics
    non_numeric = set(df.select_dtypes(include=["object", "datetime"]).columns)
    df = coerce_all_numeric(df, list(non_numeric))

    # 5) drop duplicates
    df.drop_duplicates(inplace=True)

    if df.empty:
        print(f"⚠️  {src.name}: no rows after cleaning — skipped")
        return

    _write_csv(dst, df, force=force)
    print(f"✅ {dst.relative_to(ROOT)}  ({len(df):,} rows)")

def clean_season(season: str, *, force: bool) -> None:
    raw_dir  = RAW_ROOT  / season
    proc_dir = PROC_ROOT / season
    if not raw_dir.exists():
        print(f"⚠️  no raw data for {season}")
        return

    for sub in ["totals", "per_game"]:
        for csv_path in (raw_dir / sub).glob("*.csv"):
            out_path = proc_dir / sub / csv_path.name
            clean_one_csv(csv_path, out_path, force=force)

# ── CLI helpers ─────────────────────────────────────────────────────────────
def _all_seasons() -> List[str]:
    if not RAW_ROOT.exists():
        return []
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir())

def _parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Clean NBA team shooting tables.",
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
