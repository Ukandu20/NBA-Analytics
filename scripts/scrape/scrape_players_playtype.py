#!/usr/bin/env python3
"""
scrape_player_playtype.py
=========================
Players ➤ PlayType (Synergy)

• PlayType       ─ isolation, transition,
                   pr_ball_handler, pr_roll_man,
                   post_up, spot_up,
                   handoff, cut,
                   off_screen, putbacks
• TypeGrouping   ─ offensive | defensive
• PerMode        ─ Totals | PerGame
• SeasonType     ─ Regular Season | Playoffs

Writes CSVs to:

    data/raw/player_stats/playtype/<season>/totals/*.csv
    data/raw/player_stats/playtype/<season>/per_game/*.csv
"""

import time
import argparse
import pathlib
import requests
import pandas as pd

# ─── NBA-stats request headers ──────────────────────────────────────────────
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

ENDPOINT  = "https://stats.nba.com/stats/synergyplaytypes"
DATA_ROOT = pathlib.Path("data/raw/player_stats/playtype")

# UI-key  →  Synergy API value
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

SCHEMA: list[str] = []      # populated once per run


def init_schema(season: str) -> None:
    """Hit one known combo to capture the full header list."""
    global SCHEMA
    params = {
        "SeasonYear":   season,
        "SeasonType":   "Regular Season",
        "PerMode":      "Totals",
        "LeagueID":     "00",
        "PlayType":     PLAYTYPE_MAP["isolation"],
        "PlayerOrTeam": "P",      # ← players
        "TypeGrouping": "offensive",
        # all other filters blank / default
        **{k: "" for k in (
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
    SCHEMA = r.json()["resultSets"][0]["headers"]


def call_api(params: dict) -> pd.DataFrame:
    """Robust fetch with retry on 5xx or connection errors."""
    pause = 1.0
    for attempt in range(1, 4):
        try:
            r = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
            if 500 <= r.status_code < 600:
                raise requests.HTTPError(response=r)
            r.raise_for_status()
            data = r.json()["resultSets"][0]
            return pd.DataFrame(data["rowSet"], columns=data["headers"])
        except requests.HTTPError as err:
            code = err.response.status_code if err.response else None
            if code and 400 <= code < 500:
                # Bad combo → empty table with correct columns
                return pd.DataFrame(columns=SCHEMA)
            if attempt == 3:
                raise
        except requests.ConnectionError:
            if attempt == 3:
                raise
        time.sleep(pause)
        pause *= 2.0
    raise RuntimeError("call_api exhausted retries")


def fetch_combo(
        season: str,
        season_type: str,
        per_mode: str,
        pt_key: str,
        grp_key: str
) -> None:
    """Download one CSV for season_type × per_mode × playtype × grouping."""
    out_dir = DATA_ROOT / season / PER_MODE_FOLDERS[per_mode]
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"{season_type.lower().replace(' ', '_')}_{pt_key}_{grp_key}.csv"
    fpath = out_dir / fname

    params = {
        "SeasonYear":   season,
        "SeasonType":   season_type,
        "PerMode":      per_mode,
        "LeagueID":     "00",
        "PlayType":     PLAYTYPE_MAP[pt_key],
        "PlayerOrTeam": "P",                # ← players
        "TypeGrouping": grp_key,
        **{k: "" for k in (
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
    except Exception as exc:
        print(f"🚫 {fname} → {type(exc).__name__}: {exc}")
        return

    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(DATA_ROOT)}  ({len(df):,} rows)")
    time.sleep(0.6)


def scrape_season(season: str) -> None:
    init_schema(season)
    for season_type in SEASON_TYPES:
        for per_mode in PER_MODE_FOLDERS:
            for pt_key in PLAYTYPE_MAP:
                for grp_key in TYPE_GROUP_KEYS:
                    fetch_combo(season, season_type, per_mode, pt_key, grp_key)


# ── entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Scrape Players ➤ Synergy PlayType"
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
