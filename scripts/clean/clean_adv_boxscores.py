#!/usr/bin/env python3
"""
clean_adv_boxscores.py
======================

Clean the CSVs produced by `scrape_adv_boxscores.py` **and** create an
extra layer of month-level CSVs whose folders are named by 3-letter
month abbreviations (jan, feb, …, dec).  
That way every season has an identical month set, making cross-season
comparisons trivial:

data/processed/team_stats/adv_box_scores/
└─ 2024-25/
   ├─ regular_season_traditional.csv
   ├─ playoffs_misc.csv
   ⋮
   ├─ oct/               ← October games
   │   └─ oct_boxscores.csv
   ├─ nov/
   │   └─ nov_boxscores.csv
   ⋮

CLI
---
  -s, --season  YYYY-YY          clean exactly one season          (default 2024-25)
  -S, --seasons YYYY-YY …        clean *multiple* seasons
  -a, --all                      clean every season folder found
  -f, --force                    overwrite existing processed files
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from collections import defaultdict
from typing import Iterable, List

import numpy as np
import pandas as pd

# ── Project helpers ──────────────────────────────────────────────────────────
ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from utils.clean_helpers import normalise_cols          # type: ignore
from utils.numeric_helpers import coerce_all_numeric    # type: ignore

RAW_ROOT  = ROOT / "data/raw/team_stats/adv_box_scores"
PROC_ROOT = ROOT / "data/processed/team_stats/adv_box_scores"

# ─────────────────────────────────────────────────────────────────────────────
# Core column helpers
# ─────────────────────────────────────────────────────────────────────────────
def _parse_game_date(df: pd.DataFrame) -> None:
    """Convert `game_date` to pandas datetime."""
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")


def _standardise_team_abbrev(df: pd.DataFrame) -> None:
    """Rename + upper-case the team abbreviation column."""
    if "team_abbreviation" in df.columns:
        df.rename(columns={"team_abbreviation": "team"}, inplace=True)
        df["team"] = df["team"].fillna("").astype(str).str.upper()


def _add_season_bounds(df: pd.DataFrame) -> None:
    """Split season string (YYYY-YY) into numeric start / end years."""
    if "season_year" in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)

    if "season" in df.columns:
        start = df["season"].astype(str).str.extract(r"^(\d{4})", expand=False)
        df["season_start"] = pd.to_numeric(start, errors="coerce")
        df["season_end"]   = df["season_start"] + 1


def _derive_home_away(df: pd.DataFrame) -> None:
    """From `matchup` make `is_home`, `home`, `away`."""
    if "matchup" not in df.columns:
        return

    df["is_home"] = df["matchup"].str.contains(r"\s+vs\.\s+", regex=True)
    sides = df["matchup"].str.split(r"\s+vs\.\s+|\s+@\s+", n=1, expand=True)
    df["home"] = np.where(df["is_home"], sides[0], sides[1])
    df["away"] = np.where(df["is_home"], sides[1], sides[0])


# ─────────────────────────────────────────────────────────────────────────────
def clean_one_csv(src: pathlib.Path, dst: pathlib.Path) -> pd.DataFrame | None:
    """
    Load → clean → save a single CSV.

    Returns the cleaned DataFrame (or None if skipped) so the caller can
    aggregate by month afterwards.
    """
    df = pd.read_csv(src)
    if df.empty:
        print(f"⚠️  {src.name}: empty file — skipped")
        return None

    # ── Standardise columns & values ────────────────────────────────────────
    df.columns = normalise_cols(df.columns)
    _parse_game_date(df)
    _standardise_team_abbrev(df)
    _add_season_bounds(df)
    _derive_home_away(df)

    # ── Coerce numerics (skip text + datetime) ──────────────────────────────
    exclude = set(df.select_dtypes(include=["object", "datetime"]).columns)
    df = coerce_all_numeric(df, list(exclude))

    df.drop_duplicates(inplace=True)

    if df.empty:
        print(f"⚠️  {src.name}: no rows after cleaning — skipped")
        return None

    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False)
    print(f"✅ {dst.relative_to(ROOT)}  ({len(df):,} rows)")
    return df


# ─────────────────────────────────────────────────────────────────────────────
def _month_abbr_series(date_series: pd.Series) -> pd.Series:
    """
    Return 3-letter lower-case month abbreviations
    (jan, feb, …, dec) for a datetime series.
    """
    return pd.to_datetime(date_series).dt.strftime("%b").str.lower()


def _write_monthly_files(
    season_dir: pathlib.Path,
    month_dfs: dict[str, pd.DataFrame],
    *,
    force: bool
) -> None:
    """
    For each 'jan' … 'dec' key, write:
        <season_dir>/<mon>/<mon>_boxscores.csv
    """
    for mon, month_df in month_dfs.items():
        month_folder = season_dir / mon
        month_folder.mkdir(parents=True, exist_ok=True)

        out_csv = month_folder / f"{mon}_boxscores.csv"
        if out_csv.exists() and not force:
            print(f"⏩ {out_csv.relative_to(ROOT)} exists — use -f to overwrite")
            continue

        month_df.to_csv(out_csv, index=False)
        print(f"📄 {out_csv.relative_to(ROOT)}  ({len(month_df):,} rows)")


def clean_season(season: str, *, force: bool = False) -> None:
    """
    Clean **all** CSVs in one season folder, then create month-aggregate CSVs
    whose folders are named jan, feb, …, dec.
    """
    raw_dir  = RAW_ROOT  / season
    proc_dir = PROC_ROOT / season

    if not raw_dir.exists():
        print(f"⚠️  no raw data for {season}")
        return

    month_buckets: dict[str, List[pd.DataFrame]] = defaultdict(list)

    for csv_path in raw_dir.glob("*.csv"):
        out_path = proc_dir / csv_path.name

        if out_path.exists() and not force:
            print(f"⏩ {out_path.relative_to(ROOT)} exists — using cached")
            cleaned_df = pd.read_csv(out_path)
        else:
            cleaned_df = clean_one_csv(csv_path, out_path)

        if cleaned_df is None or "game_date" not in cleaned_df.columns:
            continue

        cleaned_df["mon"] = _month_abbr_series(cleaned_df["game_date"])
        for mon, grp in cleaned_df.groupby("mon"):
            month_buckets[str(mon)].append(grp.drop(columns="mon"))

    consolidated = {
        mon: pd.concat(dfs, ignore_index=True)
        for mon, dfs in month_buckets.items()
    }
    _write_monthly_files(proc_dir, consolidated, force=force)


# ─────────────────────────────────────────────────────────────────────────────
# CLI helpers
# ─────────────────────────────────────────────────────────────────────────────
def all_seasons() -> List[str]:
    """Return every season folder found under data/raw/…"""
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir())


def parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Clean advanced team box-score CSVs and split them by month."
    )
    group = ap.add_mutually_exclusive_group()
    group.add_argument("-s", "--season",  help="clean one season (e.g. 2024-25)")
    group.add_argument("-S", "--seasons", nargs="+", help="clean multiple seasons")
    group.add_argument("-a", "--all",     action="store_true",
                       help="clean every season folder found")
    ap.add_argument("-f", "--force",      action="store_true",
                    help="overwrite existing processed files")
    return ap.parse_args()


def resolve_targets(args: argparse.Namespace) -> Iterable[str]:
    if args.all:
        return all_seasons()
    if args.seasons:
        return args.seasons
    return [args.season or "2024-25"]


# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    args = parse_cli()
    for season in resolve_targets(args):
        print(f"\n📂 Cleaning season {season}")
        clean_season(season, force=args.force)


if __name__ == "__main__":
    main()
