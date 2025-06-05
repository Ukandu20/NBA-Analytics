#!/usr/bin/env python3
"""
Clean every raw award CSV →
  • data/processed/awards/<award>.csv
  • data/processed/awards/awards_long.csv
"""

from __future__ import annotations
import sys, glob
from pathlib import Path
import pandas as pd

# ── project paths & imports ────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))
(ROOT / "utils" / "__init__.py").touch(exist_ok=True)

from utils.clean_helpers import (
    normalise_cols,
    explode_all_team_awards,
    TEAM_AWARDS,
)

RAW_DIR  = ROOT / "data/raw/awards"
PROC_DIR = ROOT / "data/processed/awards"
PROC_DIR.mkdir(parents=True, exist_ok=True)

PLAYERS_DF = (
    pd.read_csv(ROOT / "data/processed/all_players_cleaned.csv",
                usecols=["player", "player_id"])
      .drop_duplicates("player")
)

# ── helpers to guarantee key columns ───────────────────────────────
def ensure_player_col(df: pd.DataFrame) -> pd.DataFrame:
    if "player" in df.columns:
        return df
    alt = [c for c in df.columns if "player" in c]
    if alt:
        return df.rename(columns={alt[0]: "player"})
    # fallback: first object column
    obj_cols = [c for c in df.columns if df[c].dtype == "object"]
    if obj_cols:
        return df.rename(columns={obj_cols[0]: "player"})
    raise ValueError("Could not locate a player column.")

def ensure_season_col(df: pd.DataFrame) -> pd.DataFrame:
    if "season" in df.columns:
        return df
    # Accept headers like "year", "season_year"
    alt = [c for c in df.columns if ("season" in c) or ("year" in c) or ("Season" in c)]
    if alt:
        return df.rename(columns={alt[0]: "season"})
    raise ValueError("Could not locate a season column.")

# ── clean ONE award dataframe ──────────────────────────────────────
def clean_one_award(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    if tag in TEAM_AWARDS:
        df = explode_all_team_awards(df)

    df.columns = normalise_cols(df.columns)
    df = df.loc[:, ~df.columns.duplicated()]

    df = ensure_season_col(df)
    df = ensure_player_col(df)

    # season start / end (expects 'YYYY-YY' or 'YYYY-YYYY')
    df["season_start"] = df["season"].str[:4].astype(int)
    df["season_end"]   = df["season"].str[-2:].astype(int) + 2000
    df.loc[df["season_end"] < df["season_start"], "season_end"] += 100

    # attach player_id
    df = df.merge(PLAYERS_DF, on="player", how="left")
    df["player_id"] = df["player_id"].fillna(
        df["player"].str.lower().str.replace(" ", "", regex=False) + "_na"
    )

    # numeric coercion
    skip = {"season", "player", "player_id", "award", "team_rank", "position", "lg"}
    num_cols = df.columns.difference(list(skip))
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # ensure optional cols
    for col in ("team_rank", "position"):
        if col not in df.columns:
            df[col] = pd.NA

    # column order
    front = ["season", "season_start", "season_end", "award",
             "team_rank", "player", "player_id", "position"]
    rest  = [c for c in df.columns if c not in front]
    return df[front + rest]

# ── main driver ────────────────────────────────────────────────────
def main() -> None:
    long_frames: list[pd.DataFrame] = []

    for path in sorted(glob.glob(str(RAW_DIR / "*.csv"))):
        tag = Path(path).stem.upper()
        raw = pd.read_csv(path)
        raw["award"] = tag

        tidy = clean_one_award(raw, tag)
        out_single = PROC_DIR / f"{tag.lower()}.csv"
        tidy.to_csv(out_single, index=False)
        print(f"✅ {tag:<18}→ {out_single.relative_to(ROOT)}  ({len(tidy):,} rows)")

        long_frames.append(tidy)

    awards_long = pd.concat(long_frames, ignore_index=True)
    out_long = PROC_DIR / "awards_long.csv"
    awards_long.to_csv(out_long, index=False)
    print(f"\n🎉  All done! → {out_long.relative_to(ROOT)}  ({len(awards_long):,} rows)")

# ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
