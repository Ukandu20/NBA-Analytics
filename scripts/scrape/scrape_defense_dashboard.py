#!/usr/bin/env python3
"""
scrape_team_defense.py
======================
Download Teams ▸ Defense Dashboard tables.

• Categories ─ Overall, 3-Pointers, 2-Pointers,
               Less-Than-6 Ft, Less-Than-10 Ft, Greater-Than-15 Ft
• Per-Mode   ─ Totals | PerGame
• SeasonType ─ Regular Season | Playoffs

Folder layout
-------------
data/raw/team_defense/<season>/totals/    *.csv
data/raw/team_defense/<season>/per_game/  *.csv
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
    "Referer": "https://www.nba.com/",
    "Origin":  "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}
ENDPOINT   = "https://stats.nba.com/stats/leaguedashptteamdefend"
DATA_ROOT  = pathlib.Path("data/raw/player_stats/defense_dashboard")

DEF_CAT = {
    "overall":        "Overall",
    "three_pointers": "3 Pointers",
    "two_pointers":   "2 Pointers",
    "lt6ft":          "Less Than 6Ft",
    "lt10ft":         "Less Than 10Ft",
    "gt15ft":         "Greater Than 15Ft",
}

SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = {"Totals": "totals", "PerGame": "per_game"}


# ── helpers ────────────────────────────────────────────────────────────────
def call_api(params: dict) -> pd.DataFrame:
    """Call the stats endpoint and return a DataFrame."""
    resp = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
    resp.raise_for_status()
    js = resp.json()
    return pd.DataFrame(js["resultSets"][0]["rowSet"],
                        columns=js["resultSets"][0]["headers"])


def fetch_combo(season: str,
                season_type: str,
                cat_key: str,
                per_mode: str,
                out_root: pathlib.Path) -> None:
    """Download one SeasonType × Category × PerMode combo and save CSV."""
    subdir = out_root / PER_MODES[per_mode]        # only totals / per_game
    subdir.mkdir(parents=True, exist_ok=True)

    fname = f"{season_type.lower().replace(' ', '_')}_{cat_key}.csv"
    fpath = subdir / fname

    params = {
        "Season":          season,
        "SeasonType":      season_type,
        "PerMode":         per_mode,
        "DefenseCategory": DEF_CAT[cat_key],
        "LeagueID":        "00",
        "TeamID":          "0",
        "PlayerOrTeam":    "Team",
        # filters left blank / default
        "Conference":      "", "Division":   "", "GameSegment": "",
        "Location":        "", "Month":      "0", "Outcome":    "",
        "PORound":         "0", "Period":    "0", "LastNGames": "0",
        "OpponentTeamID":  "0", "VsConference": "", "VsDivision": "",
        "DateFrom":        "", "DateTo":     "",
    }

    try:
        df = call_api(params)
    except requests.HTTPError as e:
        print(f"🚫 {fname} → {e.response.status_code} ({e.response.reason})")
        return

    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(out_root.parent)}  ({len(df):3} rows)")
    time.sleep(0.6)            # gentle on the API


# ── entry point ────────────────────────────────────────────────────────────
def main(season: str):
    out_root = DATA_ROOT / season
    for season_type in SEASON_TYPES:
        for per_mode in PER_MODES:
            for cat_key in DEF_CAT:
                fetch_combo(season, season_type,
                            cat_key, per_mode, out_root)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="2024-25",
                    help="NBA season (YYYY-YY, default 2024-25)")
    args = ap.parse_args()
    main(args.season)
