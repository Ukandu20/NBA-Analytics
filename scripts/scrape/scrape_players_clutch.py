#!/usr/bin/env python3
"""
scrape_player_clutch.py
======================
Players ▸ Clutch dashboard

• MeasureType    ─ Traditional, Advanced, Misc, Scoring, Usage  
                  (“Four Factors” / “Opponent” are **team-only**)  
• PerMode        ─ Totals | PerGame  
• SeasonType     ─ Regular Season | Playoffs  
• AheadBehind    ─ “Ahead or Behind”  
• ClutchTime     ─ “Last 5 Minutes” with PointDiff = 5  
  (same defaults the site uses)

CSV layout
----------
data/raw/player_stats/clutch/<season>/totals/*.csv  
data/raw/player_stats/clutch/<season>/per_game/*.csv
"""

import time
import argparse
import pathlib
import requests
import pandas as pd

# ── request headers ────────────────────────────────────────────────────────
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

ENDPOINT  = "https://stats.nba.com/stats/leaguedashplayerclutch"
DATA_ROOT = pathlib.Path("data/raw/player_stats/clutch")

MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "misc":        "Misc",
    "scoring":     "Scoring",
    "usage":       "Usage",
}

SEASON_TYPES      = ["Regular Season", "Playoffs"]
PER_MODE_FOLDERS  = {"Totals": "totals", "PerGame": "per_game"}


# ── helpers ────────────────────────────────────────────────────────────────
def call_api(params: dict) -> pd.DataFrame:
    """Robust fetch with retry on 5xx or connection errors (3 attempts)."""
    delay = 1.0
    for attempt in range(1, 4):
        try:
            resp = requests.get(ENDPOINT, headers=HEADERS,
                                params=params, timeout=20)
            if 500 <= resp.status_code < 600:
                raise requests.HTTPError(response=resp)
            resp.raise_for_status()
            data = resp.json()["resultSets"][0]
            return pd.DataFrame(data["rowSet"], columns=data["headers"])
        except (requests.ConnectionError, requests.HTTPError) as err:
            if attempt == 3:
                raise
            time.sleep(delay)
            delay *= 2.0
    # should never reach
    raise RuntimeError("call_api exhausted retries")


def fetch_combo(season: str,
                season_type: str,
                measure_key: str,
                per_mode: str) -> None:
    """Save one SeasonType × MeasureType × PerMode table to disk."""
    out_dir = DATA_ROOT / season / PER_MODE_FOLDERS[per_mode]
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"{season_type.lower().replace(' ', '_')}_{measure_key}.csv"
    fpath = out_dir / fname

    params = {
        "Season":        season,
        "SeasonType":    season_type,
        "SeasonSegment": "",                 # all
        "PerMode":       per_mode,
        "MeasureType":   MEASURE_MAP[measure_key],
        "LeagueID":      "00",
        # clutch filters (site defaults)
        "AheadBehind":   "Ahead or Behind",
        "ClutchTime":    "Last 5 Minutes",
        "PointDiff":     "5",
        # blank the rest
        "TeamID":         "0",
        "Conference":     "",
        "Division":       "",
        "GameScope":      "",
        "GameSegment":    "",
        "LastNGames":     "0",
        "DateFrom":       "",
        "DateTo":         "",
        "Location":       "",
        "Outcome":        "",
        "Month":          "0",
        "OpponentTeamID": "0",
        "PORound":        "",
        "PaceAdjust":     "N",
        "PlusMinus":      "N",
        "Rank":           "N",
        "Period":         "0",
        "ShotClockRange": "",
        "StarterBench":   "",
        "VsConference":   "",
        "VsDivision":     "",
    }

    try:
        df = call_api(params)
    except Exception as exc:
        print(f"🚫 {fname} → {type(exc).__name__}: {exc}")
        return

    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(DATA_ROOT)}  ({len(df):,} rows)")
    time.sleep(0.6)


def scrape_season(season: str) -> None:
    for stype in SEASON_TYPES:
        for pmod in PER_MODE_FOLDERS:
            for mkey in MEASURE_MAP:
                fetch_combo(season, stype, mkey, pmod)


# ── entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Scrape Players ➤ Clutch dashboard"
    )
    ap.add_argument(
        "--season", "-s",
        action="append",
        help="NBA season (YYYY-YY); can be supplied multiple times"
    )
    args = ap.parse_args()
    seasons = args.season or ["2024-25"]
    for yr in seasons:
        scrape_season(yr)
