#!/usr/bin/env python3
"""
scrape_team_shot_dashboard.py
=============================
Teams ▸ Shot Dashboard:

• Categories    ─ General, ShotClock, Dribbles, TouchTime,
                  ClosestDefender, ClosestDefender10ftPlus
• General ranges ─ Overall, Catch and Shoot, Pullups, Less Than 10 ft
• Per-Mode      ─ Totals | PerGame
• SeasonType    ─ Regular Season | Playoffs

Output layout
-------------
data/raw/team_stats/shot_dashboard/<season>/totals/    *.csv
data/raw/team_stats/shot_dashboard/<season>/per_game/  *.csv
"""

import time
import argparse
import pathlib

import requests
import pandas as pd

# ── API constants ───────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36"),
    "Referer":            "https://www.nba.com/",
    "Origin":             "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}
ENDPOINT  = "https://stats.nba.com/stats/leaguedashteamptshot"
DATA_ROOT = pathlib.Path("data/raw/team_stats/shot_dashboard")

# Map your category keys → API PtMeasureType values
CAT_MAP = {
    "general":            "General",
    "shotclock":          "ShotClock",
    "dribbles":           "Dribble",
    "touch_time":         "TouchTime",
    "closest_defender":   "ClosestDefender",
    "closest_defender10": "ClosestDefender10ftPlus",
}

# Only the “general” category uses these sub-ranges
GENERAL_RANGE = {
    "overall":     "Overall",
    "catch_shoot": "Catch and Shoot",
    "pullups":     "Pullups",
    "lt10ft":      "Less Than 10 ft",
}

SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = {"Totals": "totals", "PerGame": "per_game"}


def call_api(params: dict) -> pd.DataFrame:
    """Hit the Shot Dashboard endpoint and return a DataFrame."""
    resp = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


def fetch_combo(season: str,
                season_type: str,
                cat_key: str,
                per_mode: str,
                range_key: str | None) -> None:
    """
    Download one CSV for:
      SeasonType × Category × PerMode [× GeneralRange]
    and save it under DATA_ROOT/season/{totals,per_game}/
    """
    # pick the correct sub-folder (totals/ or per_game/)
    subdir = DATA_ROOT / season / PER_MODES[per_mode]
    subdir.mkdir(parents=True, exist_ok=True)

    # build the filename, including range if this is "general"
    suffix = f"_{range_key}" if (cat_key == "general" and range_key) else ""
    fname  = f"{season_type.lower().replace(' ', '_')}_{cat_key}{suffix}.csv"
    fpath  = subdir / fname

    # core parameters
    params = {
        "Season":        season,
        "SeasonType":    season_type,
        "PerMode":       per_mode,
        "PtMeasureType": CAT_MAP[cat_key],
        "LeagueID":      "00",
        "TeamID":        "0",
        # blank out all optional filters
        "LastNGames":      "0",
        "Location":        "",
        "Outcome":         "",
        "Month":           "0",
        "OpponentTeamID":  "0",
        "Period":          "0",
        "Conference":      "",
        "Division":        "",
        "VsConference":    "",
        "VsDivision":      "",
        "GameSegment":     "",
        "DateFrom":        "",
        "DateTo":          "",
        "PORound":         "0",
        # include *all* range keys, blank by default
        "GeneralRange":       "",
        "ShotClockRange":     "",
        "DribbleRange":       "",
        "TouchTimeRange":     "",
        "ClosestDefDistRange": "",
    }

    # if we're in the "general" category, set its sub-range
    if cat_key == "general" and range_key:
        params["GeneralRange"] = GENERAL_RANGE[range_key]

    try:
        df = call_api(params)
    except requests.HTTPError as e:
        print(f"🚫 {fname} → HTTP {e.response.status_code}")
        return

    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(DATA_ROOT)} ({len(df)} rows)")
    time.sleep(0.6)  # polite pause


def main(season: str):
    # loop seasons, per-modes, categories, and general-ranges
    for season_type in SEASON_TYPES:
        for per_mode in PER_MODES:
            # 1) "general" with its four sub-ranges
            for rng in GENERAL_RANGE:
                fetch_combo(season, season_type, "general", per_mode, rng)
            # 2) every other category, no range
            for cat in ("shotclock", "dribbles", "touch_time",
                        "closest_defender", "closest_defender10"):
                fetch_combo(season, season_type, cat, per_mode, None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2024-25",
                        help="NBA season (YYYY-YY), e.g. 2024-25")
    args = parser.parse_args()
    main(args.season)
