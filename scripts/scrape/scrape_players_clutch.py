#!/usr/bin/env python3
"""
scrape_players_clutch.py
========================
Fetch *player* Clutch dashboard stats (Traditional, Advanced, Four Factors, Misc, Scoring, Opponent)
for one or more seasons, season-types (Regular Season, Playoffs),
measures, and per-modes (Totals, PerGame), organizing output as:

    <output-root>/<season>/<perMode>/<season_type>/<measure>.csv

Usage:
    python scrape_players_clutch.py \
      --season 2022-23 2023-24 \
      [--season-type "Regular Season" Playoffs] \
      [--measure traditional advanced fourfactors misc scoring opponent] \
      [--per-mode Totals PerGame] \
      [--output-root data/raw/player_stats/clutch] \
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

# Configuration
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
MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "fourfactors": "Four Factors",
    "misc":        "Misc",
    "scoring":     "Scoring",
    "opponent":    "Opponent",
}
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES = ["Totals", "PerGame", "Per36", "Per48", "Per100Possessions"]
API_URL = "https://stats.nba.com/stats/leaguedashplayerclutch"

# Session creation with retries
def create_session(headers: dict) -> requests.Session:
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=2,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(headers)
    return session

# Fetch one combo of season, season_type, measure, per_mode
def fetch_clutch(session: requests.Session, season: str, season_type: str,
                  measure: str, per_mode: str) -> pd.DataFrame:
    params = {
        "Season":        season,
        "SeasonType":    season_type,
        "PerMode":       per_mode,
        "MeasureType":   MEASURE_MAP[measure],
        "LeagueID":      "00",
        "PlayerID":      "0",
        # required clutch filters
        "AheadBehind":   "Ahead or Behind",
        "ClutchTime":    "Last 5 Minutes",
        "PointDiff":     "5",
        # blank others
        **{k: "" for k in (
            "Conference","Division","GameScope","GameSegment",
            "DateFrom","DateTo","Location","Outcome",
            "Month","OpponentTeamID","PORound","PaceAdjust",
            "PlusMinus","Rank","Period","ShotClockRange","StarterBench",
            "VsConference","VsDivision","LastNGames"
        )}
    }
    r = session.get(API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])

# Main CLI

def main():
    parser = argparse.ArgumentParser(
        description="Scrape player Clutch stats season-first layout."
    )
    parser.add_argument("--season", nargs="+", required=True,
                        help="NBA seasons YYYY-YY")
    parser.add_argument("--season-type", nargs="+", choices=SEASON_TYPES,
                        default=SEASON_TYPES)
    parser.add_argument("--measure", nargs="+", choices=MEASURE_MAP.keys(),
                        default=list(MEASURE_MAP.keys()))
    parser.add_argument("--per-mode", nargs="+", choices=PER_MODES,
                        default=PER_MODES)
    parser.add_argument("--output-root", type=Path,
                        default=Path("data/raw/player_stats/clutch"))
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--delay", type=float, default=2)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    session = create_session(DEFAULT_HEADERS)

    for season in args.season:
        for per_mode in args.per_mode:
            for stype in args.season_type:
                out_dir_base = args.output_root / season / per_mode / stype.lower().replace(' ','_')
                out_dir_base.mkdir(parents=True, exist_ok=True)
                for measure in args.measure:
                    out_file = out_dir_base / f"{measure}.csv"
                    if args.skip_existing and out_file.exists():
                        logging.info("Skipping %s", out_file)
                        continue
                    try:
                        df = fetch_clutch(session, season, stype, measure, per_mode)
                        if df.empty:
                            logging.warning(
                                "No data %s | %s | %s | %s",
                                season, per_mode, stype, measure)
                            continue
                        df.to_csv(out_file, index=False)
                        logging.info("Saved %s (%d rows)", out_file, len(df))
                    except Exception as e:
                        logging.error("Error %s | %s | %s | %s: %s",
                                      season, per_mode, stype, measure, e)
                    time.sleep(args.delay)

if __name__ == "__main__":
    main()
