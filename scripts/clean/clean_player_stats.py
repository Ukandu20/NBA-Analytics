#!/usr/bin/env python3
"""
Clean *all* player-stats CSVs under data/raw/player_stats/**  ➜  parquet.

Usage
-----
python scripts/clean/clean_player_stats.py
    –or–
python scripts/clean/clean_player_stats.py --module general clutch
"""

from __future__ import annotations
import re, sys
from pathlib import Path
import argparse
import pathlib
from typing import Iterable, Optional, Tuple
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))
# Ensure utils package is recognized
(ROOT / "utils" / "__init__.py").touch(exist_ok=True)

from utils.clean_helpers   import normalise_cols
from utils.numeric_helpers import coerce_all_numeric

RAW_ROOT   = pathlib.Path("data/raw/player_stats")
PROC_ROOT  = pathlib.Path("data/processed/player_stats")

# Regex to capture the “module” segment
#  data/raw/player_stats/<module>/<season>/<per_mode>/file.csv
MODULE_RE = re.compile(r"player_stats[\\/](?P<module>[^\\/]+)[\\/]")

# Columns that must stay *textual*
TEXT_COLS = {
    "player", "player_name", "team", "team_abbreviation", "team_name",
    "team_city", "team_id", "player_id", "player_display_first_last",
    "matchup", "game_id", "wl", "measure_type"
}

# ---------------------------------------------------------------------------

def meta_from_path(csv_path: pathlib.Path) -> Tuple[str, str, str, str]:
    """
    Derive:  module, season, season_type, per_mode  from the raw path.

    Path layout:
      data/raw/player_stats/<module>/<season>/<per_mode>/<file>.csv
    """
    rel = csv_path.relative_to(RAW_ROOT)
    module, season, per_mode = rel.parts[:3]          # safe: we control layout
    season_type = csv_path.stem.split("_", 1)[0]      # regular_season / playoffs
    return module, season, season_type, per_mode


def clean_one(csv_path: pathlib.Path) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Read a single CSV, normalise, add metadata.
    Returns (None, None) if the CSV is empty.
    """
    module, season, season_type, per_mode = meta_from_path(csv_path)
    df = pd.read_csv(csv_path)

    if df.empty:                       # <- skip zero-row files
        return None, None

    # ---- normalise + metadata ----
    df.columns = normalise_cols(df.columns)
    df["season"]      = season
    df["season_type"] = season_type
    df["per_mode"]    = per_mode

    exclude = TEXT_COLS.intersection(df.columns)
    df = coerce_all_numeric(df, exclude_cols=list(exclude))

    return module, df


# -------------------------------------------------------------------------
# 3) process_modules – type-safe checks against '--module' CLI argument
# -------------------------------------------------------------------------
def process_modules(target_modules: Optional[Iterable[str]] = None) -> None:
    buffers: dict[str, list[pd.DataFrame]] = {}

    for csv in RAW_ROOT.rglob("*.csv"):
        module, df = clean_one(csv)
        if df is None or module is None:  # empty file or module is None → skip
            continue
        if target_modules and module not in target_modules:
            continue
        buffers.setdefault(module, []).append(df)

    if not buffers:
        print("⚠️  No matching CSV files found.")
        return

    for module, frames in buffers.items():
        out_dir  = PROC_ROOT / module
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{module}_cleaned.csv"

        pd.concat(frames, ignore_index=True).to_csv(out_path, index=False)
        print(f"✅ {module:12s} → {out_path}   "
              f"({len(frames)} files, {sum(len(f) for f in frames):,} rows)")



# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean all player-stats CSVs into parquet."
    )
    parser.add_argument(
        "--module", "-m",
        action="append",
        help="Optional: restrict to one or more modules "
             "(e.g. --module general --module clutch)"
    )
    args = parser.parse_args()

    process_modules(args.module)
