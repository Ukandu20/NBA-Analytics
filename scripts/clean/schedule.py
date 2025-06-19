#!/usr/bin/env python3
"""
generate_team_schedules.py · v5-lc
----------------------------------
Produces per-team **and** league-wide schedules with *all* headers lower-case.

Output tree
-----------
data/processed/schedule/<SEASON>/
    │
    ├─ <TEAM>/regular_season_schedule.csv      (game_no)
    ├─ <TEAM>/playoff_schedule.csv             (round, game_no_in_series)
    │
    ├─ league_regular_season_schedule.csv      (team, game_week)
    └─ league_playoff_schedule.csv             (team, round, game_no_in_series)
"""

from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd

# ── paths & constants ───────────────────────────────────────────────────────
ROOT      = Path(__file__).resolve().parents[2]
BOX_ROOT  = ROOT / "data/processed/team_stats/boxscores"
OUT_ROOT  = ROOT / "data/processed/schedule"
OUT_ROOT.mkdir(parents=True, exist_ok=True)

TRAD      = "traditional.csv"
ROUND_LBL = ["rnd1", "sf", "conf", "finals"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s")

# ── helpers ─────────────────────────────────────────────────────────────────
def parse_matchup(m: str) -> tuple[str, str]:
    """Return (away, home) team codes from 'GSW @ LAL' or 'LAL vs GSW'."""
    m = (m.replace("vs.", "vs").replace(" @ ", "@")
          .replace(" vs ", "vs").strip())
    if "@" in m:
        away, home = m.split("@")
    else:
        home, away = m.split("vs")
    return away.strip(), home.strip()

def add_home_away(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure lower-case away_team / home_team columns."""
    if {"away", "home"}.issubset(df.columns):
        df["away_team"] = df["away"]; df["home_team"] = df["home"]
    else:
        df[["away_team", "home_team"]] = df["matchup"].apply(parse_matchup).tolist()
    return df

def add_points(df: pd.DataFrame) -> pd.DataFrame:
    df["pf"] = df["points"] if "points" in df.columns else df["pts"]
    df["opp_pf"] = (df["opponent_points"]
                    if "opponent_points" in df.columns
                    else df["pf"] - df["plus_minus"])
    return df

def out_path(season: str, *parts) -> Path:
    p = OUT_ROOT / season
    for part in parts[:-1]:
        p = p / part; p.mkdir(exist_ok=True, parents=True)
    return p / parts[-1]

# ── main walker ────────────────────────────────────────────────────────────
for season_dir in sorted(BOX_ROOT.iterdir()):
    if not season_dir.is_dir():
        continue
    season = season_dir.name
    reg_csv = season_dir / "totals/regular_season" / TRAD
    if not reg_csv.exists():
        logging.info("⏭  %s  (no regular totals)", season)
        continue

    # ------------- REGULAR SEASON -----------------------------------------
    reg = (pd.read_csv(reg_csv, parse_dates=["game_date"])
             .pipe(add_home_away)
             .pipe(add_points))
    tcol = "team" if "team" in reg.columns else "team_abbreviation"

    # per-team files
    for team, sub in reg.groupby(tcol):
        sub = sub.sort_values("game_date").reset_index(drop=True)
        sub["game_no"] = sub.index + 1
        sub[["game_id","game_date","away_team","home_team",
             "game_no","pf","opp_pf"]] \
          .to_csv(out_path(season, team, "regular_season_schedule.csv"), index=False)

    # league file (team + game_week)
    league_rows = []
    for team, sub in reg.groupby(tcol):
        sub = sub.sort_values("game_date").reset_index(drop=True)
        sub["team"] = team
        sub["game_week"] = sub.index + 1
        league_rows.append(sub[["team","game_id","game_date",
                                "away_team","home_team",
                                "game_week","pf","opp_pf"]])
    pd.concat(league_rows, ignore_index=True)\
      .to_csv(out_path(season, "league_regular_season_schedule.csv"), index=False)

    logging.info("✔︎ %s league regular (%d rows)", season, sum(len(r) for r in league_rows))

    # ------------- PLAYOFFS ----------------------------------------------
    ply_csv = season_dir / "totals/playoffs" / TRAD
    if not ply_csv.exists():
        continue

    ply = (pd.read_csv(ply_csv, parse_dates=["game_date"])
             .pipe(add_home_away)
             .pipe(add_points))

    league_ply_rows = []

    for team, sub in ply.groupby(tcol):
        sub = sub.sort_values("game_date").reset_index(drop=True)
        if sub.empty: continue

        sub["opponent"] = sub.apply(
            lambda r: r["home_team"] if r["away_team"] == team else r["away_team"], axis=1)
        sub["series_id"] = (sub["opponent"] != sub["opponent"].shift()).cumsum()
        sub["game_no_in_series"] = sub.groupby("series_id").cumcount() + 1
        sub["round"] = (sub["series_id"].rank(method="dense").astype(int)
                        .map(lambda i: ROUND_LBL[i-1] if i <= len(ROUND_LBL) else f"rnd{i}"))

        # per-team playoff schedule
        sub[["game_id","game_date","away_team","home_team",
             "pf","opp_pf","round","game_no_in_series"]] \
          .to_csv(out_path(season, team, "playoff_schedule.csv"), index=False)

        # add TEAM col and collect for league file
        sub["team"] = team
        league_ply_rows.append(
            sub[["team","game_id","game_date","away_team","home_team",
                 "pf","opp_pf","round","game_no_in_series"]])

    if league_ply_rows:
        pd.concat(league_ply_rows, ignore_index=True) \
          .to_csv(out_path(season, "league_playoff_schedule.csv"), index=False)
        logging.info("✔︎ %s league playoffs (%d rows)",
                     season, sum(len(r) for r in league_ply_rows))
