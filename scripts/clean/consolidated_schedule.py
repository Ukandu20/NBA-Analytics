#!/usr/bin/env python3
"""
generate_consolidated_schedules.py

Creates per-season, team-level consolidated schedules with GAME_WEEK reset per team.

Outputs to:
  data/processed/schedule/<SEASON>/
    - regular_season_schedule.csv
    - playoff_schedule.csv
"""

from pathlib import Path
import pandas as pd

# ─── Paths ────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parents[2]
BOX_BASE   = ROOT / "data" / "processed" / "team_stats" / "boxscores"
SCHED_BASE = ROOT / "data" / "processed" / "schedule"
SCHED_BASE.mkdir(parents=True, exist_ok=True)

# Map series index → round name
ROUND_LABELS = {1: "RND1", 2: "SF", 3: "CONF", 4: "FINALS"}

for season_dir in sorted(BOX_BASE.iterdir()):
    if not season_dir.is_dir():
        continue

    out_dir = SCHED_BASE / season_dir.name
    out_dir.mkdir(parents=True, exist_ok=True)

    # ─── Regular Season ───────────────────────────────────────────────
    reg_fp = season_dir / "regular_season_traditional.csv"
    if reg_fp.exists():
        df = pd.read_csv(reg_fp, parse_dates=["game_date"])

        # Explode into one row per TEAM
        home = df.rename(columns={"home": "TEAM", "away": "OPPONENT"}).copy()
        home["HOME/AWAY"] = "HOME"
        away = df.rename(columns={"away": "TEAM", "home": "OPPONENT"}).copy()
        away["HOME/AWAY"] = "AWAY"
        teamsched = pd.concat([home, away], ignore_index=True)

        # Assign GAME_WEEK per TEAM (1…n)
        teamsched = teamsched.sort_values(["TEAM", "game_date"]).reset_index(drop=True)
        teamsched["GAME_WEEK"] = teamsched.groupby("TEAM").cumcount() + 1

        # Rename and select columns
        teamsched.rename(columns={
            "game_id":   "GAME_ID",
            "game_date": "GAME_DATE"
        }, inplace=True)
        cols = ["GAME_ID", "GAME_DATE", "TEAM", "OPPONENT", "HOME/AWAY", "GAME_WEEK"]
        teamsched.to_csv(out_dir / "regular_season_schedule.csv", columns=cols, index=False)
        print(f"✅ {season_dir.name} → regular_season_schedule.csv")

    # ─── Playoffs ──────────────────────────────────────────────────────
    ply_fp = season_dir / "playoffs_traditional.csv"
    if ply_fp.exists():
        df = pd.read_csv(ply_fp, parse_dates=["game_date"])

        # Explode into one row per TEAM
        home = df.rename(columns={"home": "TEAM", "away": "OPPONENT"}).copy()
        home["HOME/AWAY"] = "HOME"
        away = df.rename(columns={"away": "TEAM", "home": "OPPONENT"}).copy()
        away["HOME/AWAY"] = "AWAY"
        teamsched = pd.concat([home, away], ignore_index=True)

        # Series grouping per TEAM (consecutive vs same opponent)
        teamsched = teamsched.sort_values(["TEAM", "game_date"]).reset_index(drop=True)
        teamsched["SERIES_ID"] = teamsched.groupby("TEAM")["OPPONENT"] \
                                    .transform(lambda s: (s != s.shift()).cumsum())

        # Game number within each series (1–7)
        teamsched["GAME_NO_IN_SERIES"] = (
            teamsched
            .groupby(["TEAM", "SERIES_ID"])
            .cumcount()
            + 1
        )

        # Map series index to round label
        teamsched["ROUND"] = teamsched["SERIES_ID"] \
                                    .map(ROUND_LABELS) \
                                    .fillna("UNKNOWN")

        # Rename and select columns
        teamsched.rename(columns={
            "game_id":   "GAME_ID",
            "game_date": "GAME_DATE"
        }, inplace=True)
        cols = [
            "GAME_ID", "GAME_DATE", "TEAM", "OPPONENT", "HOME/AWAY",
            "ROUND", "GAME_NO_IN_SERIES"
        ]
        teamsched.to_csv(out_dir / "playoff_schedule.csv", columns=cols, index=False)
        print(f"✅ {season_dir.name} → playoff_schedule.csv")
