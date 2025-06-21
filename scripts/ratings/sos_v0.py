#!/usr/bin/env python3
"""
V0 SOS builder  – per-season subfolders + team abbreviation column
------------------------------------------------------------------
Input  : data/processed/schedule/<SEASON>/league_regular_season_schedule.csv
Outputs:
  • data/feature_store/sos_v0/<SEASON>/sos.csv
  • data/feature_store/sos_v0_all.csv     (concat of all seasons)

Columns in each csv:  season, team_id, team, sos_v0
"""

from pathlib import Path
import pandas as pd

ROOT = Path("data/processed/schedule")
OUT_BASE = Path("data/feature_store/sos_v0")
OUT_ALL  = Path("data/feature_store/sos_v0_all.csv")
HCA = 3.0

# ──────────────────────────────────────────────────────────────────────────
def load_one(season_dir: Path) -> pd.DataFrame:
    f  = season_dir / "league_regular_season_schedule.csv"
    df = pd.read_csv(f, parse_dates=["game_date"])
    df["season"] = season_dir.name                       # e.g. 2024-25

    req = {"season", "team_id", "team", "opponent_id",
           "home_team", "pf", "opp_pf"}
    miss = req.difference(df.columns)
    if miss:
        raise ValueError(f"{f}: missing {sorted(miss)}")

    # numeric cast + enforce int64 on IDs
    cols_num = ["team_id", "opponent_id", "pf", "opp_pf"]
    df[cols_num] = df[cols_num].apply(pd.to_numeric, downcast="integer")
    df[["team_id", "opponent_id"]] = df[["team_id", "opponent_id"]].astype("int64")

    # home flag → int8
    df["is_home"] = (df["team"] == df["home_team"]).astype("int8")


    # margin of victory adjusted for symmetric home-court
    df["mov_adj"] = df["pf"] - df["opp_pf"] - HCA * (2*df["is_home"] - 1)

    # keep abbr for later
    df = df.rename(columns={"team": "team"})
    return df[["season", "team_id", "team", "opponent_id", "mov_adj"]]

def load_all() -> pd.DataFrame:
    return pd.concat(
        [load_one(sd) for sd in ROOT.iterdir() if sd.is_dir()],
        ignore_index=True,
    )

def sos_prev_season(df: pd.DataFrame) -> pd.DataFrame:
    """
    For every season S>first:
      • look up each opponent’s mean MOV_adj from season S-1
      • average those numbers for each team
    Returns columns:  season, team_id, team, sos_v0
    """
    seasons = sorted(df["season"].unique())
    rows = []

    for prev, curr in zip(seasons[:-1], seasons[1:]):
        # strength of each team *last season*
        prev_strength = (
            df.loc[df["season"] == prev, ["team_id", "mov_adj"]]
              .groupby("team_id")["mov_adj"]
              .mean()
              .rename("mov_prev")               # Series, index = team_id
        )

        # current season rows
        curr_rows = df.loc[df["season"] == curr,
                           ["team_id", "team", "opponent_id"]]

        # map opponent_id → last-season strength
        curr_rows = curr_rows.merge(prev_strength,
                                    left_on="opponent_id",
                                    right_index=True,
                                    how="left")

        # average across all of a team’s opponents
        sos = (
            curr_rows.groupby(["team_id", "team"])["mov_prev"]
                     .mean()
                     .rename("sos_v0")
                     .reset_index()
        )
        sos["season"] = curr
        rows.append(sos)

    return pd.concat(rows, ignore_index=True)


# ──────────────────────────────────────────────────────────────────────────
def main() -> None:
    df  = load_all()
    sos = sos_prev_season(df)

    # per-season sub-folders
    for season, chunk in sos.groupby("season"):
        chunk = chunk[["season", "team_id", "team", "sos_v0"]]
        season_str = str(season).replace("-", "_")  # e.g. 2024-25 → 2024_25
        out_file = OUT_BASE / season_str / "sos_v0.csv"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        chunk.to_csv(out_file, index=False)

    # all-seasons roll-up
    OUT_ALL.parent.mkdir(parents=True, exist_ok=True)
    sos.to_csv(OUT_ALL, index=False)
    print(f"✅ wrote {len(sos)} rows → {OUT_BASE}/<season>/… and {OUT_ALL}")

if __name__ == "__main__":
    main()
