#!/usr/bin/env python3
"""
clean_player_clutch.py
======================

Clean raw “player clutch” CSVs and build per-team mirrors, keeping everything
alphabetically sorted.

Raw input
---------
data/raw/player_stats/clutch/<season>/<totals|per_game>/*.csv

Output
------
data/processed/player_stats/clutch/<season>/<totals|per_game>/*.csv
└─ teams/<TEAM>/<totals|per_game>/<same-filename>.csv
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Iterable, List

import pandas as pd

# ───────────────────────── project paths ────────────────────────────────────
ROOT = pathlib.Path(__file__).resolve().parents[2]          # repo root
sys.path.append(str(ROOT))                                  # for utils/

from utils.clean_helpers  import normalise_cols             # type: ignore
from utils.numeric_helpers import coerce_all_numeric        # type: ignore

RAW_ROOT  = ROOT / "data/raw/player_stats/clutch"
PROC_ROOT = ROOT / "data/processed/player_stats/clutch"


# ───────────────────────── column helpers ──────────────────────────────────
def _ensure_team(df: pd.DataFrame) -> None:
    """
    Guarantee a `team` column (ALL-CAPS), regardless of which raw field exists.
    """
    if "team" in df.columns:
        df["team"] = df["team"].fillna("").astype(str).str.upper()
        return

    if "team_abbreviation" in df.columns:
        df["team"] = df["team_abbreviation"].fillna("").astype(str).str.upper()
    elif "team_name" in df.columns:
        df["team"] = (
            df["team_name"].fillna("").astype(str).str.upper().str.replace(" ", "_")
        )
    elif "team_id" in df.columns:
        df["team"] = df["team_id"].astype(str)


def _add_season_bounds(df: pd.DataFrame) -> None:
    """
    Split “2024-25” ➜ season_start = 2024, season_end = 2025.
    """
    if "season_year" in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)

    if "season" in df.columns:
        yr = df["season"].astype(str).str.extract(r"^(\d{4})", expand=False)
        df["season_start"] = pd.to_numeric(yr, errors="coerce")
        df["season_end"]   = df["season_start"] + 1


# ───────────────────────── I/O helper ───────────────────────────────────────
def _write_csv(path: pathlib.Path, df: pd.DataFrame, *, force: bool) -> None:
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


# ───────────────────────── per-file cleaner ────────────────────────────────
def _clean_one(
    src: pathlib.Path,
    dst_master: pathlib.Path,
    team_root: pathlib.Path,
    *,
    force: bool,
) -> None:
    df = pd.read_csv(src)
    if df.empty:
        print(f"⚠️  {src.name}: empty — skipped")
        return

    # normalise, enrich, coerce
    df.columns = normalise_cols(df.columns)
    _ensure_team(df)
    _add_season_bounds(df)

    non_num = set(df.select_dtypes(include=["object", "datetime"]).columns)
    df = coerce_all_numeric(df, list(non_num))
    df.drop_duplicates(inplace=True)

    # sort league-wide rows by team → player → season_start (if present)
    if "team" in df.columns:
        sort_cols = ["team"]
        if "player" in df.columns:
            sort_cols.append("player")
        if "season_start" in df.columns:
            sort_cols.append("season_start")
        df.sort_values(sort_cols, inplace=True, ignore_index=True)

    if df.empty:
        print(f"⚠️  {src.name}: no rows after cleaning — skipped")
        return

    # ── league-wide CSV ────────────────────────────────────────────────────
    _write_csv(dst_master, df, force=force)
    print(f"✅ {dst_master.relative_to(ROOT)}  ({len(df):,} rows)")

    # ── per-team mirrors (alphabetical) ────────────────────────────────────
    if "team" in df.columns:
        per_mode = dst_master.parent.name          # totals | per_game
        # groupby(sort=True) → alphabetical teams
        for team, grp in df.groupby("team", sort=True):
            team_path = (
                team_root
                / str(team).upper()
                / per_mode
                / dst_master.name
            )
            _write_csv(team_path, grp, force=force)


# ───────────────────────── per-season driver ───────────────────────────────
def _clean_season(season: str, *, force: bool) -> None:
    raw_season  = RAW_ROOT  / season
    proc_season = PROC_ROOT / season
    team_root   = proc_season / "teams"

    if not raw_season.exists():
        print(f"⚠️  no raw data for {season}")
        return

    for mode in ["totals", "per_game"]:
        for csv in (raw_season / mode).glob("*.csv"):
            dst_master = proc_season / mode / csv.name
            _clean_one(csv, dst_master, team_root, force=force)


# ───────────────────────── CLI helpers ─────────────────────────────────────
def _seasons_on_disk() -> List[str]:
    if not RAW_ROOT.exists():
        return []
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir())


def _parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Clean player clutch tables & build per-team splits.",
    )
    g = ap.add_mutually_exclusive_group()
    g.add_argument("-s", "--season",  help="clean one season (e.g. 2024-25)")
    g.add_argument("-S", "--seasons", nargs="+", help="clean several seasons")
    g.add_argument("-a", "--all",     action="store_true",
                   help="clean every scraped season folder")
    ap.add_argument("-f", "--force",  action="store_true",
                   help="overwrite existing processed files")
    return ap.parse_args()


def _targets(a: argparse.Namespace) -> Iterable[str]:
    if a.all:
        return _seasons_on_disk()
    if a.seasons:
        return a.seasons
    return [a.season or "2024-25"]


# ───────────────────────── entry point ─────────────────────────────────────
def main() -> None:
    args = _parse_cli()
    for season in _targets(args):
        print(f"\n📂 Cleaning season {season}")
        _clean_season(season, force=args.force)


if __name__ == "__main__":
    main()
