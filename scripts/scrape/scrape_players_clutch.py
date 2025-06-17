#!/usr/bin/env python3
"""
scrape_players_clutch.py
========================
Scrape **player Clutch** dashboard stats in a season‑first folder layout:

    <output-root>/<season>/<perMode>/<season_type>/<measure>.csv

Supported parameters
--------------------
* **MeasureTypes** – traditional, advanced, misc, scoring, usage  
  (Four Factors & Opponent are team‑only)
* **Per‑modes**    – Totals | PerGame | Per36 | Per48 | Per100Possessions
* **SeasonTypes**  – Regular Season | Playoffs

Example
~~~~~~~
```bash
python scrape_players_clutch.py \
  --season 2024-25 2023-24 \
  --per-mode Totals PerGame \
  --measure traditional scoring \
  --delay 0.6
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
HEADERS = {
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
API_URL = "https://stats.nba.com/stats/leaguedashplayerclutch"
OUTPUT_ROOT = Path("data/raw/player_stats/clutch")
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES = ["Totals", "PerGame", "Per36", "Per48", "Per100Possessions"]
MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "misc":        "Misc",
    "scoring":     "Scoring",
    "usage":       "Usage",
}

# ── SESSION WITH RETRIES ─────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.mount("https://", HTTPAdapter(max_retries=Retry(
        total=5, backoff_factor=1,
        allowed_methods=["GET"], status_forcelist=[429,500,502,503,504])))
    return s


# ── API CALL ─────────────────────────────────────────────────────────────

def call_endpoint(sess: requests.Session, params: dict) -> pd.DataFrame:
    for attempt in range(1, 3):
        try:
            r = sess.get(API_URL, params=params, timeout=20)
            r.raise_for_status()
            js = r.json()["resultSets"][0]
            return pd.DataFrame(js["rowSet"], columns=js["headers"])
        except requests.HTTPError as e:
            if e.response is not None and 500 <= e.response.status_code < 600 and attempt == 1:
                time.sleep(2)
                continue
            raise
    raise RuntimeError("Unreachable loop")


def fetch_combo(sess: requests.Session, season: str, season_type: str,
                measure_key: str, per_mode: str) -> pd.DataFrame:
    params = {
        "Season":        season,
        "SeasonType":    season_type,
        "SeasonSegment": "",             # all
        "PerMode":       per_mode,
        "MeasureType":   MEASURE_MAP[measure_key],
        "LeagueID":      "00",
        # clutch‑specific filters (site defaults)
        "AheadBehind":   "Ahead or Behind",
        "ClutchTime":    "Last 5 Minutes",
        "PointDiff":     "5",
        # blank optional filters / numeric zeros where required
        "Conference": "", "Division": "", "GameScope": "", "GameSegment": "",
        "DateFrom": "", "DateTo": "", "Location": "", "Outcome": "",
        "Month": "0", "OpponentTeamID": "0", "Period": "0",
        "VsConference": "", "VsDivision": "",
        "TeamID": "0", "LastNGames": "0", "PORound": "",
        "PaceAdjust": "N", "PlusMinus": "N", "Rank": "N",
        "StarterBench": "", "ShotClockRange": "",
    }
    return call_endpoint(sess, params)


# ── MAIN CLI ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape player Clutch dashboard (season‑first layout).")
    parser.add_argument("--season", required=True, nargs="+",
                        help="Seasons YYYY-YY, e.g. 2024-25")
    parser.add_argument("--season-type", nargs="+", choices=SEASON_TYPES,
                        default=SEASON_TYPES)
    parser.add_argument("--per-mode", nargs="+", choices=PER_MODES,
                        default=PER_MODES)
    parser.add_argument("--measure", nargs="+", choices=MEASURE_MAP.keys(),
                        default=list(MEASURE_MAP.keys()))
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--delay", type=float, default=0.6)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    sess = make_session()

    for season in args.season:
        for per_mode in args.per_mode:
            for stype in args.season_type:
                out_dir = args.output_root / season / per_mode / stype.lower().replace(" ", "_")
                out_dir.mkdir(parents=True, exist_ok=True)
                for mkey in args.measure:
                    fpath = out_dir / f"{mkey}.csv"
                    if args.skip_existing and fpath.exists():
                        logging.info("Skipping %s", fpath)
                        continue
                    try:
                        df = fetch_combo(sess, season, stype, mkey, per_mode)
                        if df.empty:
                            logging.warning("Empty %s | %s | %s | %s", season, per_mode, stype, mkey)
                            continue
                        df.to_csv(fpath, index=False)
                        logging.info("Saved %s (%d rows)", fpath, len(df))
                    except Exception as exc:
                        logging.error("Error %s | %s | %s | %s: %s", season, per_mode, stype, mkey, exc)
                    time.sleep(args.delay)

if __name__ == "__main__":
    main()
