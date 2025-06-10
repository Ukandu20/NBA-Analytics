#!/usr/bin/env python3
"""
clean_player_shooting.py
========================

Clean “player shooting” tables (totals / per_game) and build per-team 
mirrors, guaranteeing deterministic alphabetical ordering.

Input
-----
data/raw/player_stats/shooting/<season>/<totals|per_game>/*.csv

Output
------
data/processed/player_stats/shooting/<season>/<totals|per_game>/*.csv
└─ teams/<TEAM>/<totals|per_game>/<same-filename>.csv
"""

from __future__ import annotations
import argparse
import pathlib
import sys
from typing import Iterable, List

import pandas as pd

# add project root to path so utils/ is importable
ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from utils.clean_helpers  import normalise_cols    # type: ignore
from utils.numeric_helpers import coerce_all_numeric # type: ignore

RAW_ROOT  = ROOT / "data/raw/player_stats/shooting"
PROC_ROOT = ROOT / "data/processed/player_stats/shooting"

def _ensure_team(df: pd.DataFrame) -> None:
    """Ensure a clean ALL-CAPS `team` column, replacing spaces with underscores."""
    if "team" in df:
        df["team"] = (
            df["team"]
              .fillna("")
              .astype(str)
              .str.upper()
              .str.replace(r"\s+", "_", regex=True)
        )
    elif "team_abbreviation" in df:
        df["team"] = df["team_abbreviation"].fillna("").astype(str).str.upper()
    elif "team_name" in df:
        df["team"] = (
            df["team_name"]
              .fillna("")
              .astype(str)
              .str.upper()
              .str.replace(r"\s+", "_", regex=True)
        )
    else:
        raise KeyError("No team column found to derive `team`")

def _add_season_bounds(df: pd.DataFrame) -> None:
    """Add integer `season_start`/`season_end` based on “2024-25” style `season` strings."""
    # some files call it season_year
    if "season_year" in df.columns and "season" not in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)

    if "season" not in df.columns:
        return

    # extract first 4 digits as start, then +1 for end
    sr = df["season"].astype(str)
    start = sr.str.extract(r"^(\d{4})", expand=False)
    df["season_start"] = pd.to_numeric(start, errors="coerce")
    df["season_end"]   = df["season_start"] + 1

def _write_csv(path: pathlib.Path, df: pd.DataFrame, force: bool) -> None:
    """Write CSV only if not exists or if --force specified."""
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def _clean_file(
    src: pathlib.Path,
    dst_league: pathlib.Path,
    team_root: pathlib.Path,
    force: bool,
) -> None:
    df = pd.read_csv(src)
    if df.empty:
        print(f"⚠️  SKIP {src.name} (empty)")
        return

    # normalize column names
    df.columns = normalise_cols(df.columns)

    # ensure we have a team column
    _ensure_team(df)

    # add season bounds
    _add_season_bounds(df)

    # coerce all non-ID fields
    id_cols = {"player", "team", "season", "season_start", "season_end"}
    numeric_targets = [c for c in df.columns if c not in id_cols]
    df = coerce_all_numeric(df, numeric_targets)  # returns a new DF

    # drop duplicates
    before = len(df)
    df.drop_duplicates(inplace=True)
    dropped = before - len(df)
    if dropped:
        print(f"   • dropped {dropped} duplicate rows")

    # sort by team → player → season_start
    sort_keys = ["team"]
    if "player" in df.columns: 
        sort_keys.append("player")
    if "season_start" in df.columns:
        sort_keys.append("season_start")
    df.sort_values(sort_keys, inplace=True, ignore_index=True)

    # write league-wide
    _write_csv(dst_league, df, force)
    print(f"✅ {dst_league.relative_to(ROOT)}  ({len(df):,} rows)")

    # write per-team splits
    per_mode = dst_league.parent.name  # totals or per_game
    for team, grp in df.groupby("team", sort=True):
        out = team_root / str(team) / per_mode / dst_league.name
        _write_csv(out, grp, force)
        # you can uncomment the next line if you want a log per team-file:
        # print(f"   → {out.relative_to(ROOT)}  ({len(grp)})")

def _clean_season(season: str, force: bool) -> None:
    raw_season = RAW_ROOT / season
    proc_season = PROC_ROOT / season
    team_root = proc_season / "teams"

    if not raw_season.exists():
        print(f"⚠️  No raw data at {raw_season}")
        return

    for mode in ("totals", "per_game"):
        src_dir = raw_season / mode
        for src in sorted(src_dir.glob("*.csv")):
            dst = proc_season / mode / src.name
            _clean_file(src, dst, team_root, force)

def _seasons_available() -> List[str]:
    if not RAW_ROOT.exists():
        return []
    return sorted(d.name for d in RAW_ROOT.iterdir() if d.is_dir())

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Clean player shooting stats & build per-team mirrors"
    )
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("-s", "--season", help="one season (e.g. 2024-25)")
    grp.add_argument("-S", "--seasons", nargs="+", help="several seasons")
    grp.add_argument("-a", "--all", action="store_true", help="all seasons on disk")
    p.add_argument("-f", "--force", action="store_true", help="overwrite existing")
    return p.parse_args()

def _get_targets(args: argparse.Namespace) -> Iterable[str]:
    if args.all:
        return _seasons_available()
    if args.seasons:
        return args.seasons
    return [args.season or "2024-25"]

def main() -> None:
    args = _parse_args()
    for season in _get_targets(args):
        print(f"\n📂 Cleaning shooting stats for {season}")
        _clean_season(season, force=args.force)

if __name__ == "__main__":
    main()
