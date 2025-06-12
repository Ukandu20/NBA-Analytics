#!/usr/bin/env python3
"""
generate_team_schedules.py

For each season/team under:
  data/processed/team_stats/box_scores/<SEASON>/teams/<TEAM>/

Reads:
  - regular_season_traditional.csv
  - playoffs_traditional.csv   (if present)

Writes into:
  data/processed/schedule/<SEASON>/<TEAM>/
    • regular_season_schedule.csv
    • playoff_schedule.csv

regular_season_schedule.csv columns:
  GAME_ID, GAME_DATE, AWAY_TEAM, HOME_TEAM, GAME_WEEK

playoff_schedule.csv columns:
  GAME_ID, GAME_DATE, AWAY_TEAM, HOME_TEAM, ROUND, GAME_NO_IN_SERIES
"""
from pathlib import Path
import pandas as pd

# ─── Paths ─────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parents[2]
BOX_BASE    = ROOT / "data" / "processed" / "team_stats" / "box_scores"
SCHED_BASE  = ROOT / "data" / "processed" / "schedule"
SCHED_BASE.mkdir(parents=True, exist_ok=True)

# Map series index → round name
ROUND_LABELS = {1: "RND1", 2: "SF", 3: "CONF", 4: "FINALS"}

for season_dir in sorted(BOX_BASE.iterdir()):
    teams_dir = season_dir / "teams"
    if not teams_dir.is_dir():
        continue

    for team_dir in sorted(teams_dir.iterdir()):
        if not team_dir.is_dir():
            continue

        out_dir = SCHED_BASE / season_dir.name / team_dir.name
        out_dir.mkdir(parents=True, exist_ok=True)

        # ─── Regular Season ────────────────────────────────────────
        reg = team_dir / "regular_season_traditional.csv"
        if reg.exists():
            df = pd.read_csv(reg, parse_dates=["game_date"])
            df = df.sort_values("game_date").reset_index(drop=True)
            df["GAME_WEEK"] = df.index + 1

            sched = (
                df[["game_id", "game_date", "away", "home", "GAME_WEEK"]]
                  .rename(columns={
                      "game_id":   "GAME_ID",
                      "game_date": "GAME_DATE",
                      "away":      "AWAY_TEAM",
                      "home":      "HOME_TEAM",
                  })
            )
            sched.to_csv(out_dir / "regular_season_schedule.csv", index=False)
            print(f"✅ {season_dir.name}/{team_dir.name} → regular_season_schedule.csv")

        # ─── Playoffs ──────────────────────────────────────────────
        ply = team_dir / "playoffs_traditional.csv"
        if ply.exists():
            df = pd.read_csv(ply, parse_dates=["game_date"])
            df = df.sort_values("game_date").reset_index(drop=True)

            # determine opponent
            team_code = team_dir.name
            df["OPPONENT"] = df.apply(
                lambda r: r["home"] if r["away"] == team_code else r["away"], axis=1
            )

            # series groups: increment when opponent changes
            df["SERIES_ID"] = (df["OPPONENT"] != df["OPPONENT"].shift()).cumsum()

            # game number within that series
            df["GAME_NO_IN_SERIES"] = df.groupby("SERIES_ID").cumcount() + 1

            # map to round label (1→RND1, 2→SF, etc.)
            df["ROUND"] = df["SERIES_ID"].map(ROUND_LABELS).fillna("UNKNOWN")

            sched = (
                df[["game_id", "game_date", "away", "home", "ROUND", "GAME_NO_IN_SERIES"]]
                  .rename(columns={
                      "game_id":           "GAME_ID",
                      "game_date":         "GAME_DATE",
                      "away":              "AWAY_TEAM",
                      "home":              "HOME_TEAM",
                      "GAME_NO_IN_SERIES": "GAME_NO_IN_SERIES",
                  })
            )
            sched.to_csv(out_dir / "playoff_schedule.csv", index=False)
            print(f"✅ {season_dir.name}/{team_dir.name} → playoff_schedule.csv")
