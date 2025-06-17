#!/usr/bin/env python3
"""
scrape_players_playtype.py
==========================
Fetch Synergy *Player* play‑type statistics for one or more seasons.
All per‑modes (Totals, PerGame, Per36, Per48, Per100Possessions) and every
Synergy play‑type are fetched by default and stored in a *season‑first* folder
layout:

    <output_root>/<season>/<perMode>/<season_type>/<play_type>.csv

Run Examples
------------
    python scrape_players_playtype.py --season 2023-24 2024-25
    python scrape_players_playtype.py --season 2024-25 --play-type Isolation Postup --per-mode Totals PerGame
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

# ── CONSTANTS ──────────────────────────────────────────────────────────────
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

# Play‑type labels exactly as shown in the stats UI → API parameter value
PLAY_TYPES = [
    "Transition",
    "Isolation",
    "PRBallHandler",   # Pick‑and‑Roll & Ball‑Handler
    "PRRollman",       # Pick‑and‑Roll & Roll‑Man
    "Postup",
    "Spotup",
    "Handoff",
    "OffScreen",
    "Cut",
    "Putbacks",
    "Misc",
]

# The Synergy endpoint used for both teams & players
API_URL = "https://stats.nba.com/stats/synergyplaytypes"
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES = ["Totals", "PerGame", "Per36", "Per48", "Per100Possessions"]

# ── HELPERS ────────────────────────────────────────────────────────────────

def create_session(headers: dict) -> requests.Session:
    """Session with retry on 5xx/429."""
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(headers)
    return s


def fetch_playtype(
    session: requests.Session,
    season: str,
    season_type: str,
    play_type: str,
    per_mode: str,
) -> pd.DataFrame:
    """Call Synergy endpoint and return a DataFrame."""
    params = {
        "LeagueID": "00",
        "Season": season,
        "SeasonType": season_type,
        "PlayerOrTeam": "Player",
        "TypeGrouping": "offensive",           # consistent with UI
        "PlayType": play_type,
        "PerMode": per_mode,
        "Rank": "N",
        "Category": "Efficiency",             # required—does not affect cols
    }
    resp = session.get(API_URL, params=params, timeout=20)
    resp.raise_for_status()
    js = resp.json()
    data = js["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


# ── ENTRY‑POINT ────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Players → Synergy Play‑Type dashboard (season‑first layout)."
    )
    parser.add_argument(
        "--season", nargs="+", required=True,
        help="One or more NBA seasons (format YYYY-YY)",
    )
    parser.add_argument(
        "--season-type", nargs="+", choices=SEASON_TYPES,
        default=SEASON_TYPES,
        help="Season types to fetch",
    )
    parser.add_argument(
        "--play-type", nargs="+", choices=PLAY_TYPES,
        default=PLAY_TYPES,
        help="Synergy play‑types (fetch all by default)",
    )
    parser.add_argument(
        "--per-mode", nargs="+", choices=PER_MODES,
        default=PER_MODES,
        help="Per‑mode options (fetch all by default)",
    )
    parser.add_argument(
        "--output-root", type=Path,
        default=Path("data/raw/player_stats/playtype"),
        help="Root directory where CSVs are written",
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip if target CSV already exists",
    )
    parser.add_argument(
        "--delay", type=float, default=0.6,
        help="Seconds to sleep between successive API calls",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    session = create_session(DEFAULT_HEADERS)

    for season in args.season:
        for per_mode in args.per_mode:
            for season_type in args.season_type:
                # directory → <root>/<season>/<perMode>/<season_type>/
                base_dir = (
                    args.output_root
                    / season
                    / per_mode
                    / season_type.lower().replace(" ", "_")
                )
                base_dir.mkdir(parents=True, exist_ok=True)

                for play_type in args.play_type:
                    fpath = base_dir / f"{play_type.lower()}.csv"
                    if args.skip_existing and fpath.exists():
                        logging.info("Skipping %s", fpath)
                        continue

                    try:
                        df = fetch_playtype(
                            session, season, season_type, play_type, per_mode
                        )
                        if df.empty:
                            logging.warning(
                                "No data: %s | %s | %s | %s",
                                season, per_mode, season_type, play_type,
                            )
                            continue
                        df.to_csv(fpath, index=False)
                        logging.info("Saved %s (%d rows)", fpath, len(df))
                    except Exception as e:
                        logging.error(
                            "Error %s | %s | %s | %s: %s",
                            season, per_mode, season_type, play_type, e,
                        )
                    time.sleep(args.delay)


if __name__ == "__main__":
    main()
