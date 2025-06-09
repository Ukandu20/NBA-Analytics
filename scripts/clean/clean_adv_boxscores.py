#!/usr/bin/env python3
"""
clean_adv_boxscores.py
======================

• Cleans each CSV produced by `scrape_adv_boxscores.py`.
• Writes the cleaned file to data/processed/… (same name).
• Copies the cleaned file into:
      <season>/<MON>/<same-filename>.csv     (JAN, FEB, …, DEC)
      <season>/teams/<TEAM>/<same-filename>.csv
  – No concatenation: every copy keeps the rows that belong to that
    month / that team *only*.

CLI
---
  -s, --season     YYYY-YY          clean one season          (default 2024-25)
  -S, --seasons    YYYY-YY …        clean multiple seasons
  -a, --all                         clean every season folder
  -f, --force                       overwrite existing files
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Iterable, List

import numpy as np
import pandas as pd

# ── project helpers ─────────────────────────────────────────────────────────
ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from utils.clean_helpers import normalise_cols          # type: ignore
from utils.numeric_helpers import coerce_all_numeric    # type: ignore

RAW_ROOT  = ROOT / "data/raw/team_stats/adv_box_scores"
PROC_ROOT = ROOT / "data/processed/team_stats/adv_box_scores"

# ────────────────────────────────────────────────────────────────────────────
# column helpers
# ────────────────────────────────────────────────────────────────────────────
def _parse_game_date(df: pd.DataFrame) -> None:
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")


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


def _derive_home_away(df: pd.DataFrame) -> None:
    if "matchup" not in df.columns:
        return
    df["is_home"] = df["matchup"].str.contains(r"\s+vs\.\s+", regex=True)
    sides = df["matchup"].str.split(r"\s+vs\.\s+|\s+@\s+", n=1, expand=True)
    df["home"] = np.where(df["is_home"], sides[0], sides[1])
    df["away"] = np.where(df["is_home"], sides[1], sides[0])

# ────────────────────────────────────────────────────────────────────────────
def _write_csv(path: pathlib.Path, df: pd.DataFrame, *, force: bool) -> None:
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

# ────────────────────────────────────────────────────────────────────────────
def clean_one_csv(src: pathlib.Path, dst: pathlib.Path, *, force: bool) -> pd.DataFrame | None:
    """clean → write main file, return df for splitting"""
    df = pd.read_csv(src)
    if df.empty:
        print(f"⚠️  {src.name}: empty file — skipped")
        return None

    df.columns = normalise_cols(df.columns)
    _parse_game_date(df)
    _standardise_team_abbrev(df)
    _add_season_bounds(df)
    _derive_home_away(df)

    exclude = set(df.select_dtypes(include=["object", "datetime"]).columns)
    df = coerce_all_numeric(df, list(exclude))
    df.drop_duplicates(inplace=True)

    if df.empty:
        print(f"⚠️  {src.name}: no rows after cleaning — skipped")
        return None

    _write_csv(dst, df, force=force)
    print(f"✅ {dst.relative_to(ROOT)}  ({len(df):,} rows)")
    return df

# ────────────────────────────────────────────────────────────────────────────
def _month_abbr(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series).dt.strftime("%b").str.upper()

def clean_season(season: str, *, force: bool = False) -> None:
    raw_dir  = RAW_ROOT  / season
    proc_dir = PROC_ROOT / season
    if not raw_dir.exists():
        print(f"⚠️  no raw data for {season}")
        return

    for csv_path in raw_dir.glob("*.csv"):
        base_name = csv_path.name
        out_main  = proc_dir / base_name

        df = clean_one_csv(csv_path, out_main, force=force)
        if df is None or df.empty:
            continue

        # month split
        df["MON"] = _month_abbr(df["game_date"])
        for mon, grp in df.groupby("MON"):
            month_file = proc_dir / str(mon).upper() / base_name      # ← str() fixes Path /
            _write_csv(month_file, grp.drop(columns="MON"), force=force)

        # team split
        if "team" in df.columns:
            for team, grp in df.groupby("team"):
                team_file = proc_dir / "teams" / str(team).upper() / base_name  # ← str()
                _write_csv(team_file, grp, force=force)

# ────────────────────────────────────────────────────────────────────────────
def all_seasons() -> List[str]:
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir())

def parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Clean advanced team box-score CSVs; month & team splits",
    )
    g = ap.add_mutually_exclusive_group()
    g.add_argument("-s", "--season",  help="clean one season (e.g. 2024-25)")
    g.add_argument("-S", "--seasons", nargs="+", help="clean multiple seasons")
    g.add_argument("-a", "--all",     action="store_true",
                   help="clean every season folder found")
    ap.add_argument("-f", "--force",  action="store_true",
                   help="overwrite existing processed files")
    return ap.parse_args()

def resolve_targets(args: argparse.Namespace) -> Iterable[str]:
    if args.all:
        return all_seasons()
    if args.seasons:
        return args.seasons
    return [args.season or "2024-25"]

# ────────────────────────────────────────────────────────────────────────────
def main() -> None:
    args = parse_cli()
    for season in resolve_targets(args):
        print(f"\n📂 Cleaning season {season}")
        clean_season(season, force=args.force)

if __name__ == "__main__":
    main()
