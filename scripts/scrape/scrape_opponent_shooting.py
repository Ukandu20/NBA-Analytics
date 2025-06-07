#!/usr/bin/env python3
"""
scrape_team_opponent_shooting.py
================================
Download Teams ▸ Opponent Shooting tables.

• Categories    ─ General, ShotClock, Dribbles, Touch Time,
                  Closest Defender, Closest Defender +10  
• General ranges ─ Overall, Catch and Shoot, Pullups, Less Than 10 ft  
• Per-Mode      ─ Totals | PerGame  
• SeasonType    ─ Regular Season | Playoffs  

Folder layout
-------------
data/raw/team_stats/opponent_shooting/<season>/totals/    *.csv
data/raw/team_stats/opponent_shooting/<season>/per_game/  *.csv
"""

import time
import argparse
import pathlib

import requests
import pandas as pd

# ── NBA Stats API constants ─────────────────────────────────────────────────
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36"),
    "Referer":            "https://www.nba.com/",
    "Origin":             "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}
ENDPOINT  = "https://stats.nba.com/stats/leaguedashoppptshot"
DATA_ROOT = pathlib.Path("data/raw/team_stats/opponent_shooting")

# map our keys to the API's PtMeasureType values
CAT_MAP = {
    "general":            "General",
    "shotclock":          "ShotClock",
    "dribbles":           "Dribble",
    "touch_time":         "TouchTime",
    "closest_defender":   "ClosestDefender",
    "closest_defender10": "ClosestDefender10ftPlus",
}

# only the “general” category uses these sub-ranges
GENERAL_RANGE = {
    "overall":     "Overall",
    "catch_shoot": "Catch and Shoot",
    "pullups":     "Pullups",
    "lt10ft":      "Less Than 10 ft",
}

SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = {"Totals": "totals", "PerGame": "per_game"}


def call_api(params: dict) -> pd.DataFrame:
    """Hit the Opponent-Shooting endpoint and return a tidy DataFrame."""
    r = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    js = r.json()["resultSets"][0]
    return pd.DataFrame(js["rowSet"], columns=js["headers"])


def fetch_combo(season: str,
                season_type: str,
                cat_key: str,
                per_mode: str,
                range_key: str | None) -> None:
    """
    Download one CSV for:
      SeasonType × Category × PerMode [× GeneralRange]
    """
    # only two sub-folders ever: totals/ or per_game/
    subdir = DATA_ROOT / season / PER_MODES[per_mode]
    subdir.mkdir(parents=True, exist_ok=True)

    # build filename, add suffix for general
    suffix = f"_{range_key}" if (cat_key == "general" and range_key) else ""
    fname  = (f"{season_type.lower().replace(' ', '_')}_"
              f"{cat_key}{suffix}.csv")
    fpath  = subdir / fname

    # full set of params (blanks everywhere else)
    params = {
        "Season":            season,
        "SeasonType":        season_type,
        "PerMode":           per_mode,
        "PtMeasureType":     CAT_MAP[cat_key],
        "LeagueID":          "00",
        "TeamID":            "0",
        "LastNGames":        "0",
        "Location":          "",
        "Outcome":           "",
        "Month":             "0",
        "OpponentTeamID":    "0",
        "Period":            "0",
        "Conference":        "",
        "Division":          "",
        "VsConference":      "",
        "VsDivision":        "",
        "GameSegment":       "",
        "DateFrom":          "",
        "DateTo":            "",
        "PORound":           "0",
        "PaceAdjust":        "N",
        "PlusMinus":         "N",
        "Rank":              "N",
        "SeasonSegment":     "",
        # provide all possible range filters, blank by default
        "GeneralRange":      "",
        "ShotClockRange":    "",
        "DribbleRange":      "",
        "TouchTimeRange":    "",
        "ClosestDefDistRange":"",
    }

    # only “general” uses its sub-range
    if cat_key == "general" and range_key:
        params["GeneralRange"] = GENERAL_RANGE[range_key]

    try:
        df = call_api(params)
    except requests.HTTPError as e:
        print(f"🚫 {fname} → HTTP {e.response.status_code}")
        return

    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(DATA_ROOT)}  ({len(df)} rows)")
    time.sleep(0.6)  # gentle on the Stats API


def main(season: str):
    for season_type in SEASON_TYPES:
        for per_mode in PER_MODES:
            # 1) “general” category with its 4 sub-ranges
            for rng in GENERAL_RANGE:
                fetch_combo(season, season_type, "general", per_mode, rng)
            # 2) all the other categories (no sub-range)
            for cat in ("shotclock", "dribbles", "touch_time",
                        "closest_defender", "closest_defender10"):
                fetch_combo(season, season_type, cat, per_mode, None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape Teams ➤ Opponent Shooting"
    )
    parser.add_argument("--season", default="2024-25",
                        help="NBA season (YYYY-YY), e.g. 2024-25")
    args = parser.parse_args()
    main(args.season)
