#!/usr/bin/env python3
"""
scrape_player_boxscores.py
===========================
Download *player* game-level traditional box scores (MeasureType=Base)
for Regular Season & Playoffs, for one or more seasons.

Outputs
-------
data/raw/player_stats/box_scores/<season>/
    regular_season_traditional.csv
    playoffs_traditional.csv
"""

import time
import argparse
import pathlib

import requests
import pandas as pd

# ─── NBA Stats API headers ────────────────────────────────────────────────────
API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer":            "https://www.nba.com/",
    "Origin":             "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}

ENDPOINT   = "https://stats.nba.com/stats/playergamelogs"
DATA_ROOT  = pathlib.Path("data/raw/player_stats/boxscores")

SEASON_TYPES = ["Regular Season", "Playoffs"]


def call_api(params: dict) -> pd.DataFrame:
    """Hit the playergamelogs endpoint and return the first resultSet."""
    resp = requests.get(ENDPOINT, headers=API_HEADERS,
                        params=params, timeout=20)
    resp.raise_for_status()
    js = resp.json()
    return pd.DataFrame(js["resultSets"][0]["rowSet"],
                        columns=js["resultSets"][0]["headers"])


def fetch(season: str, season_type: str, out_dir: pathlib.Path) -> None:
    """
    Fetch one CSV for the given season & season_type,
    writing to out_dir/<season_type>_traditional.csv.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{season_type.lower().replace(' ', '_')}_traditional.csv"
    fpath = out_dir / fname

    params = {
        "Season":           season,
        "SeasonType":       season_type,     # "Regular Season" | "Playoffs"
        "MeasureType":      "Base",          # traditional box score
        "PerMode":          "Totals",
        "LeagueID":         "00",
        "PlayerOrTeam":     "Player",        # ensure player‐level logs
        "PlayerID":         "0",             # all players
        "TeamID":           "0",             # all teams
        # all the hidden blanks the NBA site actually sends:
        "College":          "",
        "Country":          "",
        "DraftYear":        "",
        "DraftPick":        "",
        "Height":           "",
        "PlayerExperience": "",
        "PlayerPosition":   "",
        "Weight":           "",
        "StarterBench":     "",
        "TwoWay":           "0",
        "Conference":       "", "Division":       "",
        "GameScope":        "", "GameSegment":    "",
        "DateFrom":         "", "DateTo":         "",
        "LastNGames":       "0", "Location":      "",
        "Month":            "0", "Outcome":       "",
        "PORound":          "0", "PaceAdjust":    "N",
        "PlusMinus":        "N", "Rank":          "N",
        "Period":           "0", "SeasonSegment":  "",
        "ShotClockRange":   "", "VsConference":   "",
        "VsDivision":       "", "OpponentTeamID":  "0",
    }

    try:
        df = call_api(params)
    except requests.HTTPError as e:
        code = e.response.status_code
        print(f"🚫 {fname} → HTTP {code} {e.response.reason}")
        return
    except Exception as e:
        print(f"⚠️  {fname} → {type(e).__name__}: {e}")
        return

    df.to_csv(fpath, index=False)
    rel = fpath.relative_to(DATA_ROOT)
    print(f"✅ {rel}  ({len(df):,} rows)")
    time.sleep(0.6)  # gentle pause to avoid hammering the API


def main(seasons: list[str]):
    for season in seasons:
        season_root = DATA_ROOT / season
        for st in SEASON_TYPES:
            fetch(season, st, season_root)


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Scrape player traditional boxscores"
    )
    p.add_argument(
        "--season", "-s",
        action="append",
        help="NBA season (format YYYY-YY); can be passed multiple times"
    )
    args = p.parse_args()
    seasons = args.season or ["2024-25"]
    main(seasons)
