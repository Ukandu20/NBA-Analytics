#!/usr/bin/env python3
"""
scrape_player_boxscores.py
===========================
Fetch *player* game-level traditional box scores (MeasureType=Base)
for one or more seasons, season-types (Regular Season, Playoffs),
and per-modes (Totals, PerGame, Per36, Per48, Per100Possessions),
organizing output as:

    <output-root>/<season>/<perMode>/<season_type>/traditional.csv

Usage:
    python scrape_player_boxscores.py \
      --season 2022-23 2023-24 \
      [--season-type "Regular Season" Playoffs] \
      [--per-mode Totals PerGame Per36 Per48 Per100Possessions] \
      [--output-root data/raw/player_stats/boxscores] \
      [--skip-existing] \
      [--delay 0.6]
"""
import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

# ── CONFIGURATION ─────────────────────────────────────────────────────────
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES = ["Totals", "PerGame", "Per36", "Per48", "Per100Possessions"]
API_URL = "https://stats.nba.com/stats/playergamelogs"
# ────────────────────────────────────────────────────────────────────────────

def create_session(headers: dict) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(headers)
    return session


def fetch_boxscores(
    session: requests.Session,
    season: str,
    season_type: str,
    per_mode: str,
) -> pd.DataFrame:
    params = {
        "Season":       season,
        "SeasonType":   season_type,
        "MeasureType":  "Base",
        "PerMode":      per_mode,
        "LeagueID":     "00",
        "PlayerID":     "0",
        "TeamID":       "0",
        "PlayerOrTeam": "Player",
        # blank out UI filters
        "College": "", "Country": "", "DraftYear": "", "DraftPick": "",
        "Height": "", "PlayerExperience": "", "PlayerPosition": "", "Weight": "",
        "StarterBench": "", "TwoWay": "0", "Conference": "", "Division": "",
        "GameScope": "", "GameSegment": "", "DateFrom": "", "DateTo": "",
        "LastNGames": "0", "Location": "", "Month": "0", "Outcome": "",
        "PORound": "0", "PaceAdjust": "N", "PlusMinus": "N", "Rank": "N",
        "Period": "0", "SeasonSegment": "", "ShotClockRange": "",
        "VsConference": "", "VsDivision": "", "OpponentTeamID": "0",
    }
    r = session.get(API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


def main():
    parser = argparse.ArgumentParser(
        description="Scrape player traditional box scores in season-first layout."
    )
    parser.add_argument(
        "--season", nargs="+", required=True,
        help="One or more NBA seasons (YYYY-YY), e.g. 2023-24"
    )
    parser.add_argument(
        "--season-type", nargs="+", choices=SEASON_TYPES,
        default=SEASON_TYPES,
        help="Season types to fetch"
    )
    parser.add_argument(
        "--per-mode", nargs="+", choices=PER_MODES,
        default=PER_MODES,
        help="Per-mode options: Totals, PerGame, Per36, Per48, Per100Possessions"
    )
    parser.add_argument(
        "--output-root", type=Path,
        default=Path("data/raw/player_stats/boxscores"),
        help="Root directory for output CSVs"
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip download if file exists"
    )
    parser.add_argument(
        "--delay", type=float, default=0.6,
        help="Seconds to sleep between requests"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    session = create_session(DEFAULT_HEADERS)

    for season in args.season:
        for per_mode in args.per_mode:
            for stype in args.season_type:
                out_dir = (
                    args.output_root
                    / season
                    / per_mode
                    / stype.lower().replace(' ', '_')
                )
                out_dir.mkdir(parents=True, exist_ok=True)

                fpath = out_dir / "traditional.csv"
                if args.skip_existing and fpath.exists():
                    logging.info("Skipping existing %s", fpath)
                    continue

                try:
                    df = fetch_boxscores(session, season, stype, per_mode)
                    if df.empty:
                        logging.warning("No data for %s | %s | %s", season, per_mode, stype)
                        continue
                    df.to_csv(fpath, index=False)
                    logging.info("Saved %s (%d rows)", fpath, len(df))
                except Exception as e:
                    logging.error("Error %s | %s | %s: %s", season, per_mode, stype, e)
                time.sleep(args.delay)

if __name__ == "__main__":
    main()
