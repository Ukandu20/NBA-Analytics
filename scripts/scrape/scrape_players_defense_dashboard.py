#!/usr/bin/env python3
"""
scrape_players_defense_dashboard.py
===================================
Fetch **player** Defense Dashboard tables for one or more seasons, season‑types,
per‑modes (supported: Totals, PerGame), and distance categories, saving as:

    <output-root>/<season>/<perMode>/<season_type>/<category>.csv

**Categories**
    overall, three_pointers, two_pointers, lt6ft, lt10ft, gt15ft

Usage
-----
python scrape_players_defense_dashboard.py \
    --season 2022-23 2023-24 \
    [--season-type "Regular Season" Playoffs] \
    [--per-mode Totals PerGame] \
    [--category overall three_pointers two_pointers lt6ft lt10ft gt15ft] \
    [--output-root data/raw/player_stats/defense_dashboard] \
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

# ── CONSTANTS ────────────────────────────────────────────────────────────
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}
API_URL = "https://stats.nba.com/stats/leaguedashptdefend"
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES = ["Totals", "PerGame"]  # player endpoint supports only these
DEF_CAT = {
    "overall":        "Overall",
    "three_pointers": "3 Pointers",
    "two_pointers":   "2 Pointers",
    "lt6ft":          "Less Than 6Ft",
    "lt10ft":         "Less Than 10Ft",
    "gt15ft":         "Greater Than 15Ft",
}
# ─────────────────────────────────────────────────────────────────────────

def create_session(headers: dict) -> requests.Session:
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(headers)
    return session


def fetch_defense(session: requests.Session, season: str, season_type: str,
                  category: str, per_mode: str) -> pd.DataFrame:
    params = {
        "Season":          season,
        "SeasonType":      season_type,
        "PerMode":         per_mode,
        "DefenseCategory": DEF_CAT[category],
        "LeagueID":        "00",
        "PlayerOrTeam":    "Player",
        # blank filters
        **{k: "" for k in (
            "Conference","Division","GameSegment","Location","Month",
            "Outcome","PORound","Period","LastNGames","OpponentTeamID",
            "VsConference","VsDivision","DateFrom","DateTo"
        )},
    }
    resp = session.get(API_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


# ── MAIN ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrape player Defense Dashboard stats (season‑first layout)."
    )
    parser.add_argument("--season", nargs="+", required=True,
                        help="Seasons YYYY-YY, e.g. 2024-25")
    parser.add_argument("--season-type", nargs="+", choices=SEASON_TYPES,
                        default=SEASON_TYPES)
    parser.add_argument("--per-mode", nargs="+", choices=PER_MODES,
                        default=PER_MODES)
    parser.add_argument("--category", nargs="+", choices=DEF_CAT.keys(),
                        default=list(DEF_CAT.keys()))
    parser.add_argument("--output-root", type=Path,
                        default=Path("data/raw/player_stats/defense_dashboard"))
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--delay", type=float, default=0.6)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    session = create_session(DEFAULT_HEADERS)

    for season in args.season:
        for per_mode in args.per_mode:
            for stype in args.season_type:
                base_dir = args.output_root / season / per_mode / stype.lower().replace(' ', '_')
                base_dir.mkdir(parents=True, exist_ok=True)
                for cat in args.category:
                    fpath = base_dir / f"{cat}.csv"
                    if args.skip_existing and fpath.exists():
                        logging.info("Skipping %s", fpath)
                        continue
                    try:
                        df = fetch_defense(session, season, stype, cat, per_mode)
                        if df.empty:
                            logging.warning("No data %s | %s | %s | %s", season, per_mode, stype, cat)
                            continue
                        df.to_csv(fpath, index=False)
                        logging.info("Saved %s (%d rows)", fpath, len(df))
                    except Exception as e:
                        logging.error("Error %s | %s | %s | %s: %s", season, per_mode, stype, cat, e)
                    time.sleep(args.delay)

if __name__ == "__main__":
    main()
