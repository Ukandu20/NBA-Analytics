#!/usr/bin/env python3
"""
scrape_boxscores_regular.py
===========================
Download *team* game-level **traditional box scores** (MeasureType=Base)
for the requested NBA season.

Outputs
-------
data/raw/boxscores_regular/2024-25/
    regular_season_traditional.csv
    playoffs_traditional.csv
"""

import time, argparse, requests, pathlib
import pandas as pd

# ── Minimal headers that keep the Stats API happy ───────────────────────────
API_HEADERS = {
    "User-Agent":
        ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
         "AppleWebKit/537.36 (KHTML, like Gecko) "
         "Chrome/125.0.0.0 Safari/537.36"),
    "Referer": "https://www.nba.com/",
    "Origin":  "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}

# ── Where to store the CSV files ────────────────────────────────────────────
DATA_ROOT = pathlib.Path("data/raw/player_stats/box_scores")

TEAM_GAMELOGS = "https://stats.nba.com/stats/teamgamelogs"

# Only one measure family for “regular” box scores
MEASURE_MAP = {"traditional": "Base"}        # UI label → API value


def call_api(params: dict) -> pd.DataFrame:
    """Wrap a call to teamgamelogs and return a DataFrame."""
    r = requests.get(TEAM_GAMELOGS, headers=API_HEADERS,
                     params=params, timeout=15)
    r.raise_for_status()
    js = r.json()
    return pd.DataFrame(js["resultSets"][0]["rowSet"],
                        columns=js["resultSets"][0]["headers"])


def fetch(season: str, season_type: str, label: str,
          out_dir: pathlib.Path) -> None:
    """Fetch one SeasonType (Regular / Playoffs) and save CSV."""
    measure_type = MEASURE_MAP[label]
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{season_type.lower().replace(' ', '_')}_{label}.csv"
    fpath = out_dir / fname

    params = {
        "Season":       season,
        "SeasonType":   season_type,        # "Regular Season" | "Playoffs"
        "MeasureType":  measure_type,       # "Base"
        "PerMode":      "Totals",
        "LeagueID":     "00",
        "TeamID":       "0",                # all teams
        "PlayerOrTeam": "Team",
    }

    df = call_api(params)
    df.to_csv(fpath, index=False)
    print(f"✅ {fname}  ({len(df):,} rows)")
    time.sleep(0.6)        # gentle pause


def main(season: str):
    out_dir = DATA_ROOT / season
    for season_type in ("Regular Season", "Playoffs"):
        fetch(season, season_type, "traditional", out_dir)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="2024-25",
                    help="NBA season (format YYYY-YY, default 2024-25)")
    args = ap.parse_args()
    main(args.season)
