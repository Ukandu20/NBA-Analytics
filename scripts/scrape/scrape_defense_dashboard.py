#!/usr/bin/env python3
"""
scrape_team_defense.py
======================
Fetch team defense dashboard stats (Overall, 3 Pointers, 2 Pointers,
Less Than 6Ft, Less Than 10Ft, Greater Than 15Ft) for one or more NBA
seasons, season-types, categories, and per-modes, organizing output as:

    <output-root>/<season>/<perMode>/<season_type>/<category>.csv

By default, it fetches all PER_MODES (Totals, PerGame) and all categories.

Usage:
    python scrape_team_defense.py \
        --season 2022-23 2023-24 \
        [--season-type "Regular Season" Playoffs] \
        [--category overall three_pointers two_pointers lt6ft lt10ft gt15ft] \
        [--per-mode Totals PerGame] \
        [--output-root data/raw/team_stats/defense_dashboard] \
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
DEF_CAT = {
    "overall":        "Overall",
    "three_pointers": "3 Pointers",
    "two_pointers":   "2 Pointers",
    "lt6ft":          "Less Than 6Ft",
    "lt10ft":         "Less Than 10Ft",
    "gt15ft":         "Greater Than 15Ft",
}
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = ["Totals", "PerGame"]
ENDPOINT     = "https://stats.nba.com/stats/leaguedashptteamdefend"
# ────────────────────────────────────────────────────────────────────────────

def create_session(headers: dict) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(headers)
    return session


def fetch_defense(
    session: requests.Session,
    season: str,
    season_type: str,
    cat_key: str,
    per_mode: str,
) -> pd.DataFrame:
    params = {
        "Season":          season,
        "SeasonType":      season_type,
        "PerMode":         per_mode,
        "DefenseCategory": DEF_CAT[cat_key],
        "LeagueID":        "00",
        "TeamID":          "0",
        "PlayerOrTeam":    "Team",
        # leave other filters blank
        "Conference":      "", "Division": "", "GameSegment": "",
        "Location":        "", "Month":    "0", "Outcome": "",
        "PORound":         "0", "Period":   "0", "LastNGames": "0",
        "OpponentTeamID":  "0", "VsConference": "", "VsDivision": "",
        "DateFrom":        "", "DateTo":   "",
    }
    resp = session.get(ENDPOINT, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


def main():
    parser = argparse.ArgumentParser(
        description="Scrape team defense dashboard with season-first layout."
    )
    parser.add_argument(
        "--season", nargs="+", required=True,
        help="One or more NBA seasons (format YYYY-YY), e.g. 2023-24"
    )
    parser.add_argument(
        "--season-type", nargs="+", choices=SEASON_TYPES,
        default=SEASON_TYPES,
        help="Season types to fetch"
    )
    parser.add_argument(
        "--category", nargs="+", choices=DEF_CAT.keys(),
        default=list(DEF_CAT.keys()),
        help="Defense categories to fetch"
    )
    parser.add_argument(
        "--per-mode", nargs="+", choices=PER_MODES,
        default=PER_MODES,
        help="Per-mode options: fetch all modes by default"
    )
    parser.add_argument(
        "--output-root", type=Path,
        default=Path("data/raw/team_stats/defense_dashboard"),
        help="Root directory for output CSVs"
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip downloading if target CSV already exists"
    )
    parser.add_argument(
        "--delay", type=float, default=0.6,
        help="Seconds to sleep between requests"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s"
    )
    session = create_session(DEFAULT_HEADERS)

    for season in args.season:
        for per_mode in args.per_mode:
            for season_type in args.season_type:
                out_dir = (
                    args.output_root
                    / season
                    / per_mode
                    / season_type.lower().replace(' ', '_')
                )
                out_dir.mkdir(parents=True, exist_ok=True)

                for cat_key in args.category:
                    fpath = out_dir / f"{cat_key}.csv"
                    if args.skip_existing and fpath.exists():
                        logging.info("Skipping %s", fpath)
                        continue
                    try:
                        df = fetch_defense(
                            session, season, season_type, cat_key, per_mode
                        )
                        if df.empty:
                            logging.warning(
                                "No data: %s | %s | %s | %s",
                                season, per_mode, season_type, cat_key
                            )
                            continue
                        df.to_csv(fpath, index=False)
                        logging.info("Saved %s (%d rows)", fpath, len(df))
                    except Exception as e:
                        logging.error(
                            "Error %s | %s | %s | %s: %s",
                            season, per_mode, season_type, cat_key, e
                        )
                    time.sleep(args.delay)

if __name__ == "__main__":
    main()
