#!/usr/bin/env python3
"""
scrape_players_general.py
=========================
Fetch **player‑level** General tables for any NBA season in a
**season‑first** folder layout:

    <output-root>/<season>/<perMode>/<season_type>/<measure>.csv

*Measures*   traditional, advanced, fourfactors, misc, scoring, opponent, defense, violations  
*Per‑modes*  Totals, PerGame, Per36, Per48, Per100Possessions  
*Season types* Regular Season, Playoffs

Example call
------------
```bash
python scrape_players_general.py \
  --season 2024-25 \
  --measure traditional advanced \
  --per-mode Totals PerGame \
  --delay 0.5
```
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

# ── CONSTANTS ─────────────────────────────────────────────────────────────
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
API_URL = "https://stats.nba.com/stats/leaguedashplayerstats"
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES = ["Totals", "PerGame", "Per36", "Per48", "Per100Possessions"]
MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "misc":        "Misc",
    "scoring":     "Scoring",
    "defense":     "Defense",
    "violations":  "Violations",
}

# ── HELPERS ───────────────────────────────────────────────────────────────

def create_session(headers: dict) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.headers.update(headers)
    return sess


def fetch_player_general(
    session: requests.Session,
    season: str,
    season_type: str,
    measure_key: str,
    per_mode: str,
) -> pd.DataFrame:
    """Call leaguedashplayerstats and return a DataFrame."""
    params = {
        "Season":        season,
        "SeasonType":    season_type,
        "PerMode":       per_mode,
        "MeasureType":   MEASURE_MAP[measure_key],
        "LeagueID":      "00",
        "PlayerID":      "0",   # all players
        # Numeric filters must be explicit "0" not ""
        "LastNGames":    "0",
        "Month":         "0",
        "PORound":       "0",
        "Period":        "0",
        # Blank / flag filters
        "College": "", "Country": "", "DraftYear": "", "DraftPick": "",
        "Height": "", "PlayerExperience": "", "PlayerPosition": "", "Weight": "",
        "StarterBench": "", "TwoWay": "0",
        "Conference": "", "Division": "",
        "GameScope": "", "GameSegment": "",
        "DateFrom": "", "DateTo": "",
        "Location": "", "Outcome": "",
        "PaceAdjust": "N", "PlusMinus": "N", "Rank": "N",
        "SeasonSegment": "", "ShotClockRange": "",
        "VsConference": "", "VsDivision": "",
        "OpponentTeamID": "0",
    }
    response = session.get(API_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])


# ── MAIN ──────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape player general stats (season‑first layout)."
    )
    parser.add_argument("--season", nargs="+", required=True,
                        help="Season(s) in YYYY-YY format")
    parser.add_argument("--season-type", nargs="+", choices=SEASON_TYPES,
                        default=SEASON_TYPES)
    parser.add_argument("--measure", nargs="+", choices=MEASURE_MAP.keys(),
                        default=list(MEASURE_MAP.keys()))
    parser.add_argument("--per-mode", nargs="+", choices=PER_MODES,
                        default=PER_MODES)
    parser.add_argument("--output-root", type=Path,
                        default=Path("data/raw/player_stats/general"))
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--delay", type=float, default=0.6)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    session = create_session(DEFAULT_HEADERS)

    for season in args.season:
        for per_mode in args.per_mode:
            for season_type in args.season_type:
                out_dir = (
                    args.output_root / season / per_mode / season_type.lower().replace(' ', '_')
                )
                out_dir.mkdir(parents=True, exist_ok=True)
                for mkey in args.measure:
                    fpath = out_dir / f"{mkey}.csv"
                    if args.skip_existing and fpath.exists():
                        logging.info("Skipping %s", fpath)
                        continue
                    try:
                        df = fetch_player_general(session, season, season_type, mkey, per_mode)
                        if df.empty:
                            logging.warning("No data %s | %s | %s | %s", season, per_mode, season_type, mkey)
                            continue
                        df.to_csv(fpath, index=False)
                        logging.info("Saved %s (%d rows)", fpath, len(df))
                    except Exception as exc:
                        logging.error("Error %s | %s | %s | %s: %s", season, per_mode, season_type, mkey, exc)
                    time.sleep(args.delay)

if __name__ == "__main__":
    main()
