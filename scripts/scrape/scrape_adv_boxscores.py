#!/usr/bin/env python3
"""
scrape_adv_boxscores.py
=======================
Fetch team game-logs (Traditional, Advanced, Four Factors, Scoring, Misc)
for one or more NBA seasons, season-types, measures, and per-modes
(incl. PerGame, Per36, Per48, Per100Possessions), organizing output as:

    <output-root>/<season>/<perMode>/<season_type>/<measure>.csv

By default, all per-modes in PER_MODES are fetched when --per-mode is omitted.

Usage:
    python scrape_adv_boxscores.py \
        --season 2022-23 2023-24 2024-25 \
        [--season-type "Regular Season" Playoffs] \
        [--measure traditional advanced misc] \
        [--output-root data/raw/team_stats/adv_boxscores] \
        [--skip-existing]
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

MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "fourfactors": "Four Factors",
    "scoring":     "Scoring",
    "misc":        "Misc",
}

SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = ["Totals", "PerGame", "Per36", "Per48", "Per100Possessions"]
API_URL      = "https://stats.nba.com/stats/teamgamelogs"
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


def fetch_gamelogs(
    session: requests.Session,
    season: str,
    season_type: str,
    measure_key: str,
    per_mode: str,
) -> pd.DataFrame:
    params = {
        "Season": season,
        "SeasonType": season_type,
        "MeasureType": MEASURE_MAP[measure_key],
        "PerMode": per_mode,
        "LeagueID": "00",
        "TeamID": "0",
        "PlayerOrTeam": "Team",
    }
    resp = session.get(API_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()["resultSets"][0]
    df = pd.DataFrame(data["rowSet"], columns=data["headers"])
    return df


def main():
    ap = argparse.ArgumentParser(
        description="Scrape NBA advanced boxscores with season-first layout."
    )
    ap.add_argument(
        "--season", nargs="+", required=True,
        help="One or more NBA seasons (format YYYY-YY), e.g. 2023-24"
    )
    ap.add_argument(
        "--season-type", nargs="+", choices=SEASON_TYPES,
        default=SEASON_TYPES,
        help="Season types to fetch"
    )
    ap.add_argument(
        "--measure", nargs="+", choices=MEASURE_MAP.keys(),
        default=list(MEASURE_MAP.keys()),
        help="Measure types to fetch (traditional, advanced, etc.)"
    )
    ap.add_argument(
        "--per-mode", nargs="+", choices=PER_MODES,
        default=PER_MODES,
        help="Per-mode options: fetch all modes by default"
    )
    ap.add_argument(
        "--output-root", type=Path,
        default=Path("data/raw/team_stats/adv_boxscores"),
        help="Root directory for output CSVs"
    )
    ap.add_argument(
        "--skip-existing", action="store_true",
        help="Skip downloading if target CSV already exists"
    )
    ap.add_argument(
        "--delay", type=float, default=0.6,
        help="Seconds to sleep between requests"
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s"
    )

    session = create_session(DEFAULT_HEADERS)

    for season in args.season:
        for per_mode in args.per_mode:
            for stype in args.season_type:
                out_dir = (
                    args.output_root
                    / season
                    / per_mode
                    / stype.lower().replace(" ", "_")
                )
                out_dir.mkdir(parents=True, exist_ok=True)

                for measure_key in args.measure:
                    fpath = out_dir / f"{measure_key}.csv"

                    if args.skip_existing and fpath.exists():
                        logging.info("Skipping existing %s", fpath)
                        continue

                    try:
                        df = fetch_gamelogs(
                            session, season, stype, measure_key, per_mode
                        )
                        if df.empty:
                            logging.warning(
                                "No data for %s | %s | %s | %s",
                                season, per_mode, stype, measure_key
                            )
                            continue

                        df.to_csv(fpath, index=False)
                        logging.info("Saved %s (%d rows)", fpath, len(df))
                    except Exception as e:
                        logging.error(
                            "Failed %s | %s | %s | %s: %s",
                            season, per_mode, stype, measure_key, e
                        )

                    time.sleep(args.delay)

if __name__ == "__main__":
    main()
