#!/usr/bin/env python3
"""
scrape_player_adv_boxscores.py
==============================
Download *player* game‐level box scores (Traditional, Advanced,
Four Factors, Scoring, Misc) for one or more NBA seasons:

  • Regular Season
  • Playoffs

Outputs
-------
data/raw/player_stats/adv_box_scores/<season>/
    regular_season_traditional.csv
    regular_season_advanced.csv
    ...
    playoffs_misc.csv
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
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}

ENDPOINT   = "https://stats.nba.com/stats/playergamelogs"
DATA_ROOT  = pathlib.Path("data/raw/player_stats/adv_boxscores")

# ─── Five measure families under the player game‐logs UI ────────────────────
MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "fourfactors": "Four Factors",
    "scoring":     "Scoring",
    "misc":        "Misc",
}

SEASON_TYPES = ["Regular Season", "Playoffs"]


def call_api(params: dict) -> pd.DataFrame:
    """Hit playergamelogs and return the first resultSet as a DataFrame."""
    resp = requests.get(ENDPOINT, headers=API_HEADERS,
                        params=params, timeout=20)
    resp.raise_for_status()
    js = resp.json()
    data = js["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


def fetch_combo(season: str,
                season_type: str,
                measure_key: str,
                out_dir: pathlib.Path) -> None:
    """
    Download one Season × SeasonType × MeasureType combo
    and save it as CSV.
    """
    measure_type = MEASURE_MAP[measure_key]
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"{season_type.lower().replace(' ', '_')}_{measure_key}.csv"
    fpath = out_dir / fname

    params = {
        "Season":           season,
        "SeasonType":       season_type,
        "MeasureType":      measure_type,
        "PerMode":          "Totals",
        "LeagueID":         "00",
        "PlayerOrTeam":     "Player",
        "PlayerID":         "0",    # all players
        "TeamID":           "0",    # all teams
        # hidden blanks the UI actually sends:
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
    time.sleep(0.6)  # throttle to avoid rate‐limit


def main(seasons: list[str]):
    for season in seasons:
        season_dir = DATA_ROOT / season
        for st in SEASON_TYPES:
            for mk in MEASURE_MAP:
                fetch_combo(season, st, mk, season_dir)


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Scrape player advanced box scores (game logs)"
    )
    p.add_argument(
        "--season", "-s",
        action="append",
        help="NBA season (YYYY-YY). Can be passed multiple times."
    )
    args = p.parse_args()
    seasons = args.season or ["2024-25"]
    main(seasons)
