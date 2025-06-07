#!/usr/bin/env python3
"""
scrape_team_clutch.py
=====================
Download Teams ▸ Clutch tables:

• MeasureType    ─ Traditional, Advanced, Four Factors, Misc, Scoring, Opponent  
• PerMode        ─ Totals | PerGame  
• SeasonType     ─ Regular Season | Playoffs  
• SeasonSegment  ─ All Season Segments  

Writes to:
data/raw/team_stats/clutch/<season>/totals/*.csv
data/raw/team_stats/clutch/<season>/per_game/*.csv
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

ENDPOINT  = "https://stats.nba.com/stats/leaguedashteamclutch"
DATA_ROOT = pathlib.Path("data/raw/team_stats/clutch")

# the six measure families exposed in the Clutch UI
MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "fourfactors": "Four Factors",
    "misc":        "Misc",
    "scoring":     "Scoring",
    "opponent":    "Opponent",
}

SEASON_TYPES    = ["Regular Season", "Playoffs"]
PER_MODE_FOLDERS = {"Totals": "totals", "PerGame": "per_game"}


def call_api(params: dict) -> pd.DataFrame:
    """
    Call the clutch endpoint with up to 3 retries on network / 5xx errors.
    Returns a pandas DataFrame.
    """
    backoff = 1.0
    for attempt in range(1, 4):
        try:
            resp = requests.get(ENDPOINT, headers=HEADERS,
                                params=params, timeout=20)
            # retry on 5xx
            if 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            data = resp.json()["resultSets"][0]
            return pd.DataFrame(data["rowSet"], columns=data["headers"])

        except (requests.ConnectionError, requests.HTTPError) as err:
            if attempt == 3:
                # after 3rd failure, propagate to fetch_combo
                raise
            print(f"⚠️  attempt {attempt} failed: {err!r}, retrying in {backoff}s")
            time.sleep(backoff)
            backoff *= 2.0

    # should never hit this
    raise RuntimeError("Unreachable: call_api exhausted all retries")


def fetch_combo(season: str,
                season_type: str,
                measure_key: str,
                per_mode: str) -> None:
    """
    Fetch one (SeasonType × MeasureType × PerMode) and write CSV.
    """
    # Build exactly: data/raw/team_stats/clutch/<season>/<totals|per_game>/
    out_dir = DATA_ROOT / season / PER_MODE_FOLDERS[per_mode]
    out_dir.mkdir(parents=True, exist_ok=True)

    # Filename carries only season_type & measure_key
    fname = f"{season_type.lower().replace(' ', '_')}_{measure_key}.csv"
    fpath = out_dir / fname

    # The full set of parameters the endpoint expects:
    params = {
        "Season":         season,
        "SeasonType":     season_type,
        "SeasonSegment":  "",                 # all segments
        "PerMode":        per_mode,
        "MeasureType":    MEASURE_MAP[measure_key],
        "LeagueID":       "00",
        "TeamID":         "0",
        # *** required clutch filters ***
        "AheadBehind":    "Ahead or Behind",
        "ClutchTime":     "Last 5 Minutes",
        "PointDiff":      "5",
        # blank out every other filter to avoid 400s
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
    except Exception as e:
        print(f"🚫 {fname} → {type(e).__name__}: {e}")
        return

    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(DATA_ROOT)}  ({len(df)} rows)")
    time.sleep(0.6)


def main(season: str):
    for stype in SEASON_TYPES:
        for pmod in PER_MODE_FOLDERS:
            for mkey in MEASURE_MAP:
                fetch_combo(season, stype, mkey, pmod)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape Teams ➤ Clutch dashboard"
    )
    parser.add_argument("--season", default="2024-25",
                        help="NBA season (YYYY-YY), e.g. 2024-25")
    args = parser.parse_args()
    main(args.season)
