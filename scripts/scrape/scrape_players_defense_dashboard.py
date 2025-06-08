#!/usr/bin/env python3
"""
scrape_player_defense_dashboard.py
Players ▸ Defense Dashboard  (multi-season)

• Categories ─ Overall, 3-Pointers, 2-Pointers, <6 Ft, <10 Ft, >15 Ft
• Per-Mode   ─ Totals | PerGame
• SeasonType ─ Regular Season | Playoffs

CSV layout:
data/raw/player_stats/defense_dashboard/<season>/totals/*.csv
data/raw/player_stats/defense_dashboard/<season>/per_game/*.csv
"""

import time
import argparse
import pathlib
import requests
import pandas as pd

# ── API constants ───────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Origin":  "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}

# *** single, correct endpoint (works for both players & teams) ***
ENDPOINT  = "https://stats.nba.com/stats/leaguedashptdefend"
DATA_ROOT = pathlib.Path("data/raw/player_stats/defense_dashboard")

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
    resp = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


def fetch_combo(season: str,
                season_type: str,
                cat_key: str,
                per_mode: str,
                out_root: pathlib.Path) -> None:
    """One SeasonType × Category × PerMode → CSV"""
    subdir = out_root / PER_MODES[per_mode]
    subdir.mkdir(parents=True, exist_ok=True)

    fname = f"{season_type.lower().replace(' ', '_')}_{cat_key}.csv"
    fpath = subdir / fname

    params = {
        "Season":          season,
        "SeasonType":      season_type,
        "PerMode":         per_mode,
        "DefenseCategory": DEF_CAT[cat_key],
        "LeagueID":        "00",
        "PlayerOrTeam":    "Player",   # ← **must be Player**
        "PlayerID":        "0",
        "TeamID":          "0",
        # keep every other filter blank/default
        "Conference": "", "Division": "", "GameSegment": "",
        "Location":   "", "Month":    "0", "Outcome":     "",
        "PORound":    "0", "Period":  "0", "LastNGames":  "0",
        "OpponentTeamID": "0", "VsConference": "", "VsDivision": "",
        "DateFrom": "", "DateTo": "",
    }

    try:
        df = call_api(params)
    except requests.HTTPError as e:
        print(f"🚫 {fname} → HTTP {e.response.status_code} {e.response.reason}")
        return
    except Exception as e:
        print(f"⚠️  {fname} → {type(e).__name__}: {e}")
        return

    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(DATA_ROOT)}  ({len(df):,} rows)")
    time.sleep(0.6)


# ── main ────────────────────────────────────────────────────────────────────
def main(seasons: list[str]):
    for season in seasons:
        root = DATA_ROOT / season
        for stype in SEASON_TYPES:
            for pmode in PER_MODES:
                for cat in DEF_CAT:
                    fetch_combo(season, stype, cat, pmode, root)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Scrape Players ▸ Defense Dashboard"
    )
    ap.add_argument(
        "--season", "-s",
        action="append",
        help="NBA season YYYY-YY (can repeat)",
    )
    args = ap.parse_args()
    seasons = args.season or ["2024-25"]
    main(seasons)
