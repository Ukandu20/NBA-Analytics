#!/usr/bin/env python3
"""
scrape_team_shooting.py
=======================
Fetch Teams ➤ Shot Dashboard stats (General, ShotClock, Dribbles,
TouchTime, ClosestDefender, ClosestDefender10ftPlus) for one or more
NBA seasons, season-types, per-modes (Totals, PerGame), and sub-ranges
(for General), organizing output as:

    <output-root>/<season>/<perMode>/<season_type>/<category>[_<range>].csv

By default, fetches only supported PER_MODES (Totals, PerGame) and all categories.

Usage:
    python scrape_team_shooting.py \
        --season 2022-23 2023-24 \
        [--season-type "Regular Season" Playoffs] \
        [--per-mode Totals PerGame] \
        [--category general shotclock dribbles touch_time closest_defender closest_defender10] \
        [--output-root data/raw/team_stats/shooting] \
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
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}
CAT_MAP = {
    "general":            "General",
    "shotclock":          "ShotClock",
    "dribbles":           "Dribble",
    "touch_time":         "TouchTime",
    "closest_defender":   "ClosestDefender",
    "closest_defender10": "ClosestDefender10ftPlus",
}
GENERAL_RANGE = {
    "overall":     "Overall",
    "catch_shoot": "Catch and Shoot",
    "pullups":     "Pullups",
    "lt10ft":      "Less Than 10 ft",
}
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = ["Totals", "PerGame"]
API_URL      = "https://stats.nba.com/stats/leaguedashteamptshot"
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


def fetch_shooting(
    session: requests.Session,
    season: str,
    season_type: str,
    category: str,
    per_mode: str,
    range_key: str | None,
) -> pd.DataFrame:
    params = {
        "Season":           season,
        "SeasonType":       season_type,
        "PerMode":          per_mode,
        "PtMeasureType":    CAT_MAP[category],
        "LeagueID":         "00",
        "TeamID":           "0",
        # blank defaults for all optional filters
        **{k: "" for k in (
            "LastNGames","Location","Outcome","Month",
            "OpponentTeamID","Period","Conference","Division",
            "VsConference","VsDivision","GameSegment",
            "DateFrom","DateTo","PORound"
        )},
        # include all range slots blank by default
        "GeneralRange":       "",
        "ShotClockRange":     "",
        "DribbleRange":       "",
        "TouchTimeRange":     "",
        "ClosestDefDistRange":"",
    }
    # set general sub-range if applicable
    if category == "general" and range_key:
        params["GeneralRange"] = GENERAL_RANGE[range_key]

    resp = session.get(API_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


def main():
    parser = argparse.ArgumentParser(description="Scrape Teams ➤ Shot Dashboard season-first layout.")
    parser.add_argument("--season", nargs="+", required=True, help="One or more NBA seasons (YYYY-YY)")
    parser.add_argument("--season-type", nargs="+", choices=SEASON_TYPES, default=SEASON_TYPES)
    parser.add_argument("--per-mode", nargs="+", choices=PER_MODES, default=PER_MODES)
    parser.add_argument("--category", nargs="+", choices=CAT_MAP.keys(), default=list(CAT_MAP.keys()))
    parser.add_argument("--output-root", type=Path, default=Path("data/raw/team_stats/shooting"))
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--delay", type=float, default=0.6)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    session = create_session(DEFAULT_HEADERS)

    for season in args.season:
        for per_mode in args.per_mode:
            for season_type in args.season_type:
                for cat in args.category:
                    ranges = GENERAL_RANGE.keys() if cat == "general" else [None]
                    for rng in ranges:
                        out_dir = args.output_root / season / per_mode / season_type.lower().replace(' ', '_')
                        out_dir.mkdir(parents=True, exist_ok=True)
                        suffix = f"_{rng}" if rng else ""
                        fpath = out_dir / f"{season_type.lower().replace(' ', '_')}_{cat}{suffix}.csv"
                        if args.skip_existing and fpath.exists():
                            logging.info("Skipping %s", fpath)
                            continue
                        try:
                            df = fetch_shooting(session, season, season_type, cat, per_mode, rng)
                            if df.empty:
                                logging.warning("No data for %s | %s | %s | %s", season, per_mode, season_type, cat)
                                continue
                            df.to_csv(fpath, index=False)
                            logging.info("Saved %s (%d rows)", fpath, len(df))
                        except Exception as e:
                            logging.error("Error %s | %s | %s | %s: %s", season, per_mode, season_type, cat, e)
                        time.sleep(args.delay)

if __name__ == "__main__":
    main()
