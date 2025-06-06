#!/usr/bin/env python3
"""
scrape_adv_boxscores.py
=======================
Fetch every team game-log (Traditional, Advanced, Four Factors, Scoring, Misc)
for **both** the 2024-25 Regular Season and Playoffs.

Outputs
-------
data/raw/boxscores/2024-25/
    regular_season_traditional.csv
    regular_season_advanced.csv
    ...
    playoffs_misc.csv
"""

import os, time, argparse, requests, pathlib
import pandas as pd

# ── YOU CAN EDIT THESE TWO BLOCKS ───────────────────────────────────────────
# 1. Minimal headers required by stats.nba.com (feel free to tweak UA)
API_HEADERS = {
    "User-Agent":
        ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
         "AppleWebKit/537.36 (KHTML, like Gecko) "
         "Chrome/125.0.0.0 Safari/537.36"),
    "Referer":    "https://www.nba.com/",
    "Origin":     "https://www.nba.com",
    "Accept":     "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}

# 2. Where to store CSVs
DATA_ROOT = pathlib.Path("data/raw/player_stats/adv_box_scores")
# ────────────────────────────────────────────────────────────────────────────

TEAM_GAMELOGS = "https://stats.nba.com/stats/teamgamelogs"

MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "fourfactors": "Four Factors",
    "scoring":     "Scoring",
    "misc":        "Misc",
}


def call_api(params: dict) -> pd.DataFrame:
    """Low-level wrapper around teamgamelogs → DataFrame."""
    resp = requests.get(TEAM_GAMELOGS, headers=API_HEADERS,
                        params=params, timeout=15)
    resp.raise_for_status()
    js = resp.json()
    return pd.DataFrame(js["resultSets"][0]["rowSet"],
                        columns=js["resultSets"][0]["headers"])


def fetch_one_combo(season: str, season_type: str,
                    measure_key: str, out_dir: pathlib.Path) -> None:
    """Download one (Season, SeasonType, MeasureType) combo and save CSV."""
    measure_type = MEASURE_MAP[measure_key]
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{season_type.lower().replace(' ', '_')}_{measure_key}.csv"
    fpath = out_dir / fname

    params = {
        "Season":       season,
        "SeasonType":   season_type,          # "Regular Season" | "Playoffs"
        "MeasureType":  measure_type,         # "Base", "Advanced", …
        "PerMode":      "Totals",
        "LeagueID":     "00",
        "TeamID":       "0",                  # all teams
        "PlayerOrTeam": "Team",
    }

    df = call_api(params)
    df.to_csv(fpath, index=False)
    print(f"✅ {fname}  ({len(df):,} rows)")

    time.sleep(0.6)   # be polite to the API


def main(season: str):
    out_dir = DATA_ROOT / season
    for season_type in ("Regular Season", "Playoffs"):
        for key in MEASURE_MAP:
            fetch_one_combo(season, season_type, key, out_dir)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="2024-25",
                    help="NBA season (format YYYY-YY, default 2024-25)")
    args = ap.parse_args()
    main(args.season)
