#!/usr/bin/env python3
"""
scrape_team_playtype.py
=======================
Teams ➤ Playtype (Synergy)

• PlayType       ─ isolation, transition,
                   pr_ball_handler, pr_roll_man,
                   post_up, spot_up,
                   handoff, cut,
                   off_screen, putbacks  
• TypeGrouping   ─ offensive | defensive  
• PerMode        ─ Totals | PerGame  
• SeasonType     ─ Regular Season | Playoffs  

Writes CSVs to:
  data/raw/team_stats/playtype/<season>/totals/*.csv
  data/raw/team_stats/playtype/<season>/per_game/*.csv
"""

import time
import argparse
import pathlib

import requests
import pandas as pd

# ─── NBA Stats API constants ─────────────────────────────────────────────────
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36"),
    "Referer":            "https://www.nba.com/",
    "Origin":             "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}

ENDPOINT         = "https://stats.nba.com/stats/synergyplaytypes"
DATA_ROOT        = pathlib.Path("data/raw/team_stats/playtype")

# ─── Correct PlayType → API value map ────────────────────────────────────────
PLAYTYPE_MAP = {
    "isolation":        "Isolation",
    "transition":       "Transition",
    "pr_ball_handler":  "PRBallHandler",
    "pr_roll_man":      "PRRollman",
    "post_up":          "Postup",
    "spot_up":          "Spotup",
    "handoff":          "Handoff",
    "cut":              "Cut",
    "off_screen":       "OffScreen",
    "putbacks":         "OffRebound",
}

TYPE_GROUP_KEYS  = ["offensive", "defensive"]
SEASON_TYPES     = ["Regular Season", "Playoffs"]
PER_MODE_FOLDERS = {"Totals": "totals", "PerGame": "per_game"}

# will hold the canonical header list
SCHEMA: list[str] = []


def init_schema(season: str):
    """
    Fire one known request (Isolation/offensive/totals) to learn
    the full CSV schema up front.
    """
    global SCHEMA
    params = {
        "SeasonYear":     season,
        "SeasonType":     "Regular Season",
        "PerMode":        "Totals",
        "LeagueID":       "00",
        "PlayType":       PLAYTYPE_MAP["isolation"],
        "PlayerOrTeam":   "T",
        "TypeGrouping":   "offensive",
        # blank everything else
        **{key: "" for key in (
            "Conference", "Division", "GameScope", "GameSegment",
            "DateFrom", "DateTo", "Location", "Outcome", "SeasonSegment",
            "StarterBench", "ShotClockRange", "VsConference", "VsDivision"
        )},
        "LastNGames":     "0",
        "Month":          "0",
        "OpponentTeamID": "0",
        "PORound":        "",
        "Period":         "0",
    }
    r = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    data = r.json().get("resultSets", [])[0]
    SCHEMA = data["headers"]


def call_api(params: dict) -> pd.DataFrame:
    """
    Fetch, retry on network/5xx up to 3×, immediate empty on 4xx.
    """
    backoff = 1.0
    for attempt in range(1, 4):
        try:
            r = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
            if 500 <= r.status_code < 600:
                raise requests.HTTPError(response=r)
            r.raise_for_status()
            data = r.json().get("resultSets", [])[0]
            return pd.DataFrame(data["rowSet"], columns=data["headers"])
        except requests.HTTPError as err:
            code = err.response.status_code if err.response else None
            if code and 400 <= code < 500:
                # if the combo truly doesn't exist, return an empty DF
                return pd.DataFrame(columns=SCHEMA)
            if attempt == 3:
                raise
            time.sleep(backoff)
            backoff *= 2.0
        except requests.ConnectionError:
            if attempt == 3:
                raise
            time.sleep(backoff)
            backoff *= 2.0
    raise RuntimeError("call_api exhausted retries")


def fetch_combo(season: str,
                season_type: str,
                per_mode: str,
                pt_key: str,
                grp_key: str) -> None:
    """
    Download one CSV for season_type × per_mode × playtype × grouping.
    """
    # Only these two directories ever get created:
    out_dir = DATA_ROOT / season / PER_MODE_FOLDERS[per_mode]
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"{season_type.lower().replace(' ', '_')}_{pt_key}_{grp_key}.csv"
    fpath = out_dir / fname

    params = {
        "SeasonYear":     season,
        "SeasonType":     season_type,
        "PerMode":        per_mode,
        "LeagueID":       "00",
        "PlayType":       PLAYTYPE_MAP[pt_key],
        "PlayerOrTeam":   "T",
        "TypeGrouping":   grp_key,
        **{key: "" for key in (
            "Conference", "Division", "GameScope", "GameSegment",
            "DateFrom", "DateTo", "Location", "Outcome", "SeasonSegment",
            "StarterBench", "ShotClockRange", "VsConference", "VsDivision"
        )},
        "LastNGames":     "0",
        "Month":          "0",
        "OpponentTeamID": "0",
        "PORound":        "",
        "Period":         "0",
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
    init_schema(season)
    for season_type in SEASON_TYPES:
        for per_mode in PER_MODE_FOLDERS:
            for pt_key in PLAYTYPE_MAP:
                for grp_key in TYPE_GROUP_KEYS:
                    fetch_combo(season, season_type, per_mode, pt_key, grp_key)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Scrape Teams ➤ Playtype")
    p.add_argument("--season", default="2024-25",
                   help="NBA season (YYYY-YY), e.g. 2024-25")
    args = p.parse_args()
    main(args.season)
