#!/usr/bin/env python3
"""
clean_player_clutch.py  ·  v6
=============================

Cleans raw **player-clutch** CSVs and writes league-wide + per-team CSVs.

Output layout
-------------
data/processed/player_stats/clutch/<season>/
    ├─ <mode-lc>/<season_type-lc?>/<file>.csv        # league-wide master
    └─ teams/<TEAM>/<mode-lc>/<season_type-lc?>/<file>.csv
"""

from __future__ import annotations

import argparse, logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

ROOT      = Path(__file__).resolve().parents[2]
RAW_ROOT  = ROOT / "data/raw/player_stats/clutch"
PROC_ROOT = ROOT / "data/processed/player_stats/clutch"

# ── project helpers ---------------------------------------------------------
import sys
sys.path.append(str(ROOT))

from utils.clean_helpers   import normalise_cols
from utils.numeric_helpers import coerce_all_numeric      # percent → 0-1

THREADS         = 4
EXCLUDE_NUMERIC = ["player", "team", "team_id", "note", "nickname"]
CORE_MODES_LC   = {"totals", "per36", "pergame", "per48", "per100possessions"}   # warn if absent

# ── column helpers ----------------------------------------------------------
def _ensure_team(df: pd.DataFrame) -> None:
    """Create/standardise a `team` column (ALL-CAPS)."""
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
        df["team"] = df["team_id"].astype("Int64").astype(str)

def _add_season_bounds(df: pd.DataFrame) -> None:
    if "season_year" in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)

    if "season" in df.columns:
        yr = df["season"].astype(str).str.extract(r"^(\d{4})", expand=False)
        df["season_start"] = pd.to_numeric(yr, errors="coerce")
        df["season_end"]   = df["season_start"] + 1

# ── I/O helper --------------------------------------------------------------
def _write_csv(path: Path, df: pd.DataFrame, *, force: bool) -> None:
    if path.exists() and not force:
        logging.info("skip %s (exists)", path.relative_to(ROOT))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logging.info("✔︎ %s  (%d rows)", path.relative_to(ROOT), len(df))

# ── core cleaner ------------------------------------------------------------
def _clean_one(
    src: Path,
    dst_master: Path,
    team_root: Path,
    *,
    per_mode: str,
    season_type: Optional[str],
    force: bool,
) -> None:
    """Read, clean, write master CSV + per-team CSVs."""
    if dst_master.exists() and not force:
        logging.info("skip %s (exists)", dst_master.relative_to(ROOT))
        return

    try:
        df = pd.read_csv(src)
    except Exception as e:
        logging.error("❌ reading %s → %s", src, e)
        return
    if df.empty:
        logging.warning("⚠️  %s empty — skipped", src.name)
        return

    # ── cleaning -----------------------------------------------------------
    df.columns = normalise_cols(df.columns)
    df.rename(
        columns={
            "player_name": "player",
            "player_last_team_id":       "team_id",
            "player_last_team_abbreviation": "team",
            "team_abbreviation":             "team",
        },
        inplace=True,
    )
    df.drop(columns=["group_set"], errors="ignore", inplace=True)
    _ensure_team(df)
    _add_season_bounds(df)

    df = coerce_all_numeric(df, exclude_cols=EXCLUDE_NUMERIC, warn_on_loss=True)
    df.drop_duplicates(inplace=True)

    if df.empty:
        logging.warning("⚠️  %s: no rows after cleaning — skipped", src.name)
        return

    # ── write master -------------------------------------------------------
    _write_csv(dst_master, df, force=force)

    # ── per-team splits ----------------------------------------------------
    if "team" not in df.columns:
        logging.warning("No `team` column in %s — team splits skipped", src.name)
        return

    for team, grp in df.groupby("team", sort=False):
        team_path = team_root / str(team).upper() / per_mode
        if season_type:
            team_path = team_path / season_type
        team_path = team_path / dst_master.name
        _write_csv(team_path, grp, force=force)

# ── per-season driver -------------------------------------------------------
def _clean_season(season: str, *, force: bool, workers: int = THREADS) -> None:
    raw_season  = RAW_ROOT / season
    proc_season = PROC_ROOT / season
    team_root   = proc_season / "teams"

    if not raw_season.exists():
        logging.warning("⚠️  no raw data for %s", season)
        return

    # warn for missing core modes
    available_modes_lc = {p.name.lower() for p in raw_season.iterdir() if p.is_dir()}
    missing = CORE_MODES_LC - available_modes_lc
    if missing:
        logging.warning("⚠️  %s missing mode folders: %s",
                        season, ", ".join(sorted(missing)))

    # process every CSV
    futures = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for mode_dir in raw_season.iterdir():
            if not mode_dir.is_dir():
                continue
            per_mode = mode_dir.name.lower()          # force folder-names to lower-case
            for csv in mode_dir.rglob("*.csv"):
                rel_parts   = csv.relative_to(mode_dir).parts
                season_type = rel_parts[0].lower() if len(rel_parts) == 2 else None

                out_dir   = proc_season / per_mode
                if season_type:
                    out_dir = out_dir / season_type
                dst_master = out_dir / csv.name    # keep original file name (.csv)

                futures.append(pool.submit(
                    _clean_one,
                    csv,
                    dst_master,
                    team_root,
                    per_mode=per_mode,
                    season_type=season_type,
                    force=force,
                ))

        for f in futures:
            f.result()   # raise exceptions early

# ── CLI helpers -------------------------------------------------------------
def _seasons_on_disk() -> List[str]:
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir()) if RAW_ROOT.exists() else []

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
    ap.add_argument("-f", "--force",   action="store_true",
                   help="overwrite existing processed files")
    ap.add_argument("-w", "--workers", type=int, default=THREADS,
                   help="parallel worker threads")
    return ap.parse_args()

def _targets(a: argparse.Namespace) -> Iterable[str]:
    if a.all:
        return _seasons_on_disk()
    if a.seasons:
        return a.seasons
    return [a.season or "2024-25"]

# ── entry-point -------------------------------------------------------------
def main() -> None:
    args = _parse_cli()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )
    for season in _targets(args):
        logging.info("📂 Cleaning season %s", season)
        _clean_season(season, force=args.force, workers=args.workers)

if __name__ == "__main__":
    main()
