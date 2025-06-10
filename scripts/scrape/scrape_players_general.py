#!/usr/bin/env python3
"""
scrape_player_general.py
=========================
Download Players ▸ General tables (all measure families) for both
Regular Season and Playoffs, saving:

    • Totals  → data/raw/player_stats/player_general/<season>/totals/
    • Per-Game → data/raw/player_stats/player_general/<season>/per_game/

Usage
-----
python scrape_player_general.py --season 2024-25
"""

import time
import argparse
import pathlib
import requests
import pandas as pd

# ─── API constants ────────────────────────────────────────────────────────────
HEADERS = {
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

ENDPOINT  = "https://stats.nba.com/stats/leaguedashplayerstats"
DATA_ROOT = pathlib.Path("data/raw/player_stats/general")

# Only the measure families you actually want to fetch
MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "misc":        "Misc",
    "scoring":     "Scoring",
    "usage":       "Usage",
    "opponent":    "Opponent",   # this one still 500s sometimes
    "defense":     "Defense",
    "violations":  "Violations",
}

SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = {
    "Totals": "totals", "PerGame": "per_game"
}


def call_api(params: dict) -> pd.DataFrame:
    """Hit the NBA API and return a DataFrame of the first resultSet."""
    resp = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
    resp.raise_for_status()
    js = resp.json()
    return pd.DataFrame(
        js["resultSets"][0]["rowSet"],
        columns=js["resultSets"][0]["headers"]
    )


def fetch_combo(season: str,
                season_type: str,
                measure_key: str,
                per_mode: str,
                out_root: pathlib.Path) -> None:
    """Download one SeasonType × MeasureType × PerMode combo and save as CSV."""
    measure_type = MEASURE_MAP[measure_key]
    subdir = out_root / PER_MODES[per_mode]     # "per_game" or "totals"
    subdir.mkdir(parents=True, exist_ok=True)

    fname = f"{season_type.lower().replace(' ', '_')}_{measure_key}.csv"
    fpath = subdir / fname

    params = {
        "Season":           season,
        "SeasonType":       season_type,
        "PerMode":          per_mode,
        "MeasureType":      measure_type,
        "LeagueID":         "00",
        "PlayerID":         "0",
        "TeamID":           "0",
        # all the “invisible” blank filters the NBA UI actually sends
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
        "Conference":       "",  "Division":       "",
        "GameScope":        "",  "GameSegment":    "",
        "DateFrom":         "",  "DateTo":         "",
        "LastNGames":       "0", "Location":       "",
        "Month":            "0", "Outcome":        "",
        "PORound":          "0", "PaceAdjust":     "N",
        "PlusMinus":        "N", "Rank":           "N",
        "Period":           "0", "SeasonSegment":  "",
        "ShotClockRange":   "",  "VsConference":   "",
        "VsDivision":       "",  "OpponentTeamID":  "0",
    }

    try:
        df = call_api(params)
    except requests.HTTPError as err:
        code = err.response.status_code
        # skip any 400 or 500 errors
        print(f"🚫 {fname} → HTTP {code} {err.response.reason}")
        return
    except Exception as err:
        print(f"⚠️  {fname} → {type(err).__name__}: {err}")
        return

    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(out_root.parent)}  ({len(df):>3} rows)")
    time.sleep(0.6)  # be kind to the API


def main(seasons: list[str]):
    for season in seasons:
        out_root = DATA_ROOT / season
        for season_type in SEASON_TYPES:
            for per_mode in PER_MODES:
                for measure_key in MEASURE_MAP:
                    fetch_combo(season, season_type, measure_key,
                                per_mode, out_root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # allow you to pass --season many times, or omit to default
    parser.add_argument(
        "--season", "-s",
        action="append",
        help="One NBA season (format YYYY-YY).  Can be passed multiple times."
    )
    args = parser.parse_args()

    # if no --season was given, default to 2024-25
    seasons = args.season or ["2024-25"]
    main(seasons)
