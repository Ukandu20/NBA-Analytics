#!/usr/bin/env python3
"""
scrape_team_playtype.py
=======================
Fetch Teams ➤ Playtype (Synergy)

• PlayType       ─ isolation, transition,
                   pr_ball_handler, pr_roll_man,
                   post_up, spot_up,
                   handoff, cut,
                   off_screen, putbacks
• TypeGrouping   ─ offensive | defensive
• PerMode        ─ Totals | PerGame
• SeasonType     ─ Regular Season | Playoffs

Organizes output as:
    <output-root>/<season>/<perMode>/<season_type>/<playtype>_<group>.csv

Usage:
    python scrape_team_playtype.py \
        --season 2022-23 2023-24 \
        [--season-type "Regular Season" Playoffs] \
        [--per-mode Totals PerGame] \
        [--playtype isolation transition pr_ball_handler pr_roll_man post_up spot_up handoff cut off_screen putbacks] \
        [--group offensive defensive] \
        [--output-root data/raw/team_stats/playtype] \
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
    "Referer":            "https://www.nba.com/",
    "Origin":             "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}
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
TYPE_GROUP_KEYS = ["offensive", "defensive"]
SEASON_TYPES    = ["Regular Season", "Playoffs"]
PER_MODES       = ["Totals", "PerGame"]
API_URL         = "https://stats.nba.com/stats/synergyplaytypes"
DATA_ROOT       = Path("data/raw/team_stats/playtype")
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


def init_schema(session: requests.Session, season: str) -> None:
    """
    Pre-fetch one call to capture CSV schema headers.
    """
    params = {
        "SeasonYear":   season,
        "SeasonType":   "Regular Season",
        "PerMode":      "Totals",
        "LeagueID":     "00",
        "PlayType":     PLAYTYPE_MAP["isolation"],
        "PlayerOrTeam": "T",
        "TypeGrouping": "offensive",
        **{k: "" for k in (
            "Conference", "Division", "GameScope", "GameSegment",
            "DateFrom", "DateTo", "Location", "Outcome", "SeasonSegment",
            "StarterBench", "ShotClockRange", "VsConference", "VsDivision"
        )},
        "LastNGames":    "0",
        "Month":         "0",
        "OpponentTeamID":"0",
        "PORound":       "",
        "Period":        "0",
    }
    r = session.get(API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json().get("resultSets", [])[0]
    global SCHEMA
    SCHEMA = data["headers"]


def fetch_playtype(
    session: requests.Session,
    season: str,
    season_type: str,
    per_mode: str,
    playtype: str,
    group: str,
) -> pd.DataFrame:
    params = {
        "SeasonYear":   season,
        "SeasonType":   season_type,
        "PerMode":      per_mode,
        "LeagueID":     "00",
        "PlayType":     PLAYTYPE_MAP[playtype],
        "PlayerOrTeam": "T",
        "TypeGrouping": group,
        **{k: "" for k in (
            "Conference", "Division", "GameScope", "GameSegment",
            "DateFrom", "DateTo", "Location", "Outcome", "SeasonSegment",
            "StarterBench", "ShotClockRange", "VsConference", "VsDivision"
        )},
        "LastNGames":    "0",
        "Month":         "0",
        "OpponentTeamID":"0",
        "PORound":       "",
        "Period":        "0",
    }
    r = session.get(API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json().get("resultSets", [])[0]
    return pd.DataFrame(data.get("rowSet", []), columns=SCHEMA)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Teams ➤ Playtype dashboard with season-first layout."
    )
    parser.add_argument(
        "--season", nargs="+", required=True,
        help="One or more NBA seasons (format YYYY-YY)"
    )
    parser.add_argument(
        "--season-type", nargs="+", choices=SEASON_TYPES,
        default=SEASON_TYPES,
        help="Season types to fetch"
    )
    parser.add_argument(
        "--per-mode", nargs="+", choices=PER_MODES,
        default=PER_MODES,
        help="Per-mode options: fetch all modes by default"
    )
    parser.add_argument(
        "--playtype", nargs="+", choices=PLAYTYPE_MAP.keys(),
        default=list(PLAYTYPE_MAP.keys()),
        help="PlayType categories to fetch"
    )
    parser.add_argument(
        "--group", nargs="+", choices=TYPE_GROUP_KEYS,
        default=TYPE_GROUP_KEYS,
        help="Type grouping: offensive or defensive"
    )
    parser.add_argument(
        "--output-root", type=Path,
        default=DATA_ROOT,
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
        init_schema(session, season)
        for per_mode in args.per_mode:
            for season_type in args.season_type:
                for playtype in args.playtype:
                    for group in args.group:
                        out_dir = (
                            args.output_root
                            / season
                            / per_mode
                            / season_type.lower().replace(" ", "_")
                        )
                        out_dir.mkdir(parents=True, exist_ok=True)
                        fname = f"{playtype}_{group}.csv"
                        fpath = out_dir / fname

                        if args.skip_existing and fpath.exists():
                            logging.info("Skipping existing %s", fpath)
                            continue

                        try:
                            df = fetch_playtype(
                                session, season, season_type,
                                per_mode, playtype, group
                            )
                            if df.empty:
                                logging.warning(
                                    "No data for %s | %s | %s | %s | %s",
                                    season, per_mode, season_type,
                                    playtype, group
                                )
                                continue

                            df.to_csv(fpath, index=False)
                            logging.info("Saved %s (%d rows)", fpath, len(df))
                        except Exception as e:
                            logging.error(
                                "Error %s | %s | %s | %s | %s: %s",
                                season, per_mode, season_type,
                                playtype, group, e
                            )
                        time.sleep(args.delay)

if __name__ == "__main__":
    main()
