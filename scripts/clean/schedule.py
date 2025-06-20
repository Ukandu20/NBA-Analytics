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
    if {"away", "home"}.issubset(df.columns):
        df["away_team"], df["home_team"] = df["away"], df["home"]
    else:
        df[["away_team", "home_team"]] = df["matchup"].apply(parse_matchup).tolist()
    return df

def add_points(df: pd.DataFrame) -> pd.DataFrame:
    df["pf"] = df["points"] if "points" in df.columns else df["pts"]
    df["opp_pf"] = (df["opponent_points"]
                    if "opponent_points" in df.columns
                    else df["pf"] - df["plus_minus"])
    df["differential"] = df["plus_minus"] if "plus_minus" in df.columns else df["pf"] - df["opp_pf"]
    df["pf"] = df["pf"].astype("int64", errors="ignore")
    df["opp_pf"] = df["opp_pf"].astype("int64", errors="ignore")
    df["differential"] = df["differential"].astype("int64", errors="ignore")
    return df



def add_opponent_info(df: pd.DataFrame) -> pd.DataFrame:
    """Add numeric opponent_id using the other team_id in same game."""
    game_to_ids = df.groupby("game_id")["team_id"].apply(list).to_dict()
    df["opponent_id"] = df.apply(
        lambda r: next(t for t in game_to_ids[r.game_id] if t != r.team_id),
        axis=1,
    ).astype("int64")

    """Add opponent name as team_abbreviation."""
    df["opponent"] = df.apply(
        lambda r: r.away_team if r.home_team == r.team else r.home_team, axis=1
    )
    return df

def out_path(season: str, *parts) -> Path:
    p = OUT_ROOT / season
    for part in parts[:-1]:
        p = p / part; p.mkdir(parents=True, exist_ok=True)
    return p / parts[-1]

# ── main walker ────────────────────────────────────────────────────────────
for season_dir in sorted(BOX_ROOT.iterdir()):
    if not season_dir.is_dir():
        continue
    season = season_dir.name

    # ---------- REGULAR SEASON -------------------------------------------
    reg_csv = season_dir / "totals/regular_season" / TRAD
    if not reg_csv.exists():
        logging.info("⏭  %s  (no regular totals)", season); continue

    reg = (pd.read_csv(reg_csv, parse_dates=["game_date"])
             .pipe(add_home_away)
             .pipe(add_points)
             .pipe(add_opponent_info))
    tcode = "team" if "team" in reg.columns else "team_abbreviation"

    # per-team files
    for team, sub in reg.groupby(tcode):
        sub = sub.sort_values("game_date").reset_index(drop=True)
        sub["season"]  = season
        sub["game_no"] = sub.index + 1
        sub[["season","team_id","game_id","game_date","away_team","home_team","opponent",
             "opponent_id","game_no","pf","opp_pf","wl", "differential"]] \
          .to_csv(out_path(season, team, "regular_season_schedule.csv"), index=False)
        sub.drop_duplicates()

    # league regular (team + game_week)
    league_rows = []
    for team, sub in reg.groupby(tcode):
        sub = sub.sort_values("game_date").reset_index(drop=True)
        sub["season"] = season
        sub["team"]   = team
        sub["game_week"] = sub.index + 1
        league_rows.append(
            sub[["season","team_id","team","game_id","game_date","away_team","home_team",
                 "opponent","opponent_id","game_week","pf","opp_pf","wl", "differential"]])
        sub.drop_duplicates()
    pd.concat(league_rows, ignore_index=True) \
      .to_csv(out_path(season, "league_regular_season_schedule.csv"), index=False)

    # ---------- PLAYOFFS --------------------------------------------------
    ply_csv = season_dir / "totals/playoffs" / TRAD
    if not ply_csv.exists(): continue

    ply = (pd.read_csv(ply_csv, parse_dates=["game_date"])
             .pipe(add_home_away)
             .pipe(add_points)
             .pipe(add_opponent_info))

    league_ply_rows = []
    for team, sub in ply.groupby(tcode):
        sub = sub.sort_values("game_date").reset_index(drop=True)
        if sub.empty: continue

        sub["series_id"] = (sub["opponent_id"] != sub["opponent_id"].shift()).cumsum()
        sub["game_no_in_series"] = sub.groupby("series_id").cumcount() + 1
        sub["round"] = (sub["series_id"].rank(method="dense").astype(int)
                        .map(lambda i: ROUND_LBL[i-1] if i<=len(ROUND_LBL) else f"rnd{i}"))
        sub["season"] = season
        sub.drop_duplicates()

        # per-team playoff
        sub[["season","team_id","game_id","game_date","away_team","home_team",
             "opponent","opponent_id","pf","opp_pf","wl","differential","round","game_no_in_series"]] \
          .to_csv(out_path(season, team, "playoff_schedule.csv"), index=False)

        sub["team"] = team
        league_ply_rows.append(
            sub[["season","team_id","team","game_id","game_date","away_team","home_team",
                 "opponent","opponent_id","pf","opp_pf","wl","differential","round","game_no_in_series"]])
        sub.drop_duplicates()

    if league_ply_rows:
        pd.concat(league_ply_rows, ignore_index=True) \
          .to_csv(out_path(season, "league_playoff_schedule.csv"), index=False)