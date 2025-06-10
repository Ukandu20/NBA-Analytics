#!/usr/bin/env python3
"""
clean_player_playtype.py
==============================

Clean the raw ‘player shot dashboard’ tables (totals / per_game / per48) and
create per-team copies that live in:

    …/teams/<TEAM>/<totals|per_game|per48>/<filename>.csv
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Iterable, List

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from utils.clean_helpers  import normalise_cols
from utils.numeric_helpers import coerce_all_numeric

RAW_ROOT  = ROOT / "data/raw/player_stats/playtype"
PROC_ROOT = ROOT / "data/processed/player_stats/playtype"


# ── column helpers ──────────────────────────────────────────────────────────
def _ensure_team(df: pd.DataFrame) -> None:
    """Create/standardise a `team` column (ALL-CAPS) so we can write per-team files."""
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
    if "season_year" in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)

    if "season" in df.columns:
        yr = df["season"].astype(str).str.extract(r"^(\d{4})", expand=False)
        df["season_start"] = pd.to_numeric(yr, errors="coerce")
        df["season_end"]   = df["season_start"] + 1


# ── tiny I/O helper ─────────────────────────────────────────────────────────
def _write_csv(path: pathlib.Path, df: pd.DataFrame, *, force: bool) -> None:
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


# ── one-file cleaner ────────────────────────────────────────────────────────
def _clean_one(
    src: pathlib.Path,
    dst_master: pathlib.Path,
    team_root: pathlib.Path,
    *,
    force: bool,
) -> None:
    """Clean one CSV, write league-wide file + per-team copies."""
    df = pd.read_csv(src)
    if df.empty:
        print(f"⚠️  {src.name}: empty — skipped")
        return

    # normalise → then rename requested columns
    df.columns = normalise_cols(df.columns)
    df.rename(
        columns={
            "player_name": "player",
            "player_last_team_id": "team_id",
            "player_last_team_abbreviation": "team",
        },
        inplace=True,
    )

    _ensure_team(df)
    _add_season_bounds(df)

    non_num = set(df.select_dtypes(include=["object", "datetime"]).columns)
    df = coerce_all_numeric(df, list(non_num))
    df.drop_duplicates(inplace=True)

    if df.empty:
        print(f"⚠️  {src.name}: no rows after cleaning — skipped")
        return

    # league-wide file
    _write_csv(dst_master, df, force=force)
    print(f"✅ {dst_master.relative_to(ROOT)}  ({len(df):,} rows)")

    # per-team mirrors
    if "team" in df.columns:
        per_mode = dst_master.parent.name  # totals | per_game | per48
        for team, grp in df.groupby("team"):
            team_path = (
                team_root
                / str(team).upper()
                / per_mode
                / dst_master.name
            )
            _write_csv(team_path, grp, force=force)


# ── per-season driver ───────────────────────────────────────────────────────
def _clean_season(season: str, *, force: bool) -> None:
    raw_season  = RAW_ROOT  / season
    proc_season = PROC_ROOT / season
    team_root   = proc_season / "teams"

    if not raw_season.exists():
        print(f"⚠️  no raw data for {season}")
        return

    for mode in ["totals", "per_game", "per48"]:
        for csv in (raw_season / mode).glob("*.csv"):
            dst_master = proc_season / mode / csv.name
            _clean_one(csv, dst_master, team_root, force=force)


# ── CLI helpers ─────────────────────────────────────────────────────────────
def _seasons_on_disk() -> List[str]:
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir()) if RAW_ROOT.exists() else []


def _parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Clean player shot-dashboard tables & build per-team splits.",
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


# ── entry point ─────────────────────────────────────────────────────────────
def main() -> None:
    args = _parse_cli()
    for season in _targets(args):
        print(f"\n📂 Cleaning season {season}")
        _clean_season(season, force=args.force)


if __name__ == "__main__":
    main()
