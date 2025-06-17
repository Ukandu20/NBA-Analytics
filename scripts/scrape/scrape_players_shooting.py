#!/usr/bin/env python3
"""
scrape_players_shooting.py
==========================
NBA‑Stats **Player → Shooting** dashboard (player‑tracking) in a season‑first
folder structure:

    <output-root>/<season>/<perMode>/<season_type>/<category>.csv

* Categories
    • general_overall, general_catch_shoot, general_pullups, general_lt10ft
    • shotclock, dribbles, touch_time
    • closest_defender, closest_defender10

* Per‑modes   – Totals | PerGame  (API only supports these two for players)
* SeasonTypes – Regular Season | Playoffs

Example
-------
```bash
python scrape_players_shooting.py \
  --season 2024-25 \
  --category general_overall shotclock \
  --per-mode Totals \
  --season-type "Regular Season" \
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
API_URL = "https://stats.nba.com/stats/leaguedashplayerptshot"  # player shooting
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES = ["Totals", "PerGame"]

# UI key → (PtMeasureType, GeneralRange) tuple as expected by the endpoint
CATEGORY_MAP = {
    # General ranges
    "general_overall":        ("General", "Overall"),
    "general_catch_shoot":    ("General", "Catch and Shoot"),
    "general_pullups":        ("General", "Pullups"),
    "general_lt10ft":         ("General", "Less Than 10 ft"),
    # Timing / movement dimensions
    "shotclock":              ("ShotClock", ""),
    "dribbles":               ("Dribble",   ""),
    "touch_time":             ("TouchTime", ""),
    # Defender distance
    "closest_defender":       ("ClosestDefender", ""),
    "closest_defender10":     ("ClosestDefender10ftPlus", ""),
}

# ── SESSION WITH RETRIES ─────────────────────────────────────────────────

def create_session() -> requests.Session:
    sess = requests.Session()
    retry_cfg = Retry(total=5, backoff_factor=1,
                      status_forcelist=[429, 500, 502, 503, 504],
                      allowed_methods=["GET"])
    sess.mount("https://", HTTPAdapter(max_retries=retry_cfg))
    sess.headers.update(HEADERS)
    return sess


# ── API CALL ─────────────────────────────────────────────────────────────

def call_endpoint(sess: requests.Session, params: dict) -> pd.DataFrame:
    for attempt in range(1, 3):
        try:
            r = sess.get(API_URL, params=params, timeout=20)
            r.raise_for_status()
            js = r.json()["resultSets"][0]
            return pd.DataFrame(js["rowSet"], columns=js["headers"])
        except requests.HTTPError as err:
            if err.response is not None and 500 <= err.response.status_code < 600 and attempt == 1:
                time.sleep(2)
                continue
            raise
    raise RuntimeError("Unreachable call loop")


def fetch_combo(sess: requests.Session, season: str, season_type: str,
                per_mode: str, cat_key: str) -> pd.DataFrame:
    pt_measure, general_range = CATEGORY_MAP[cat_key]
    params = {
        "Season":         season,
        "SeasonType":     season_type,
        "PerMode":        per_mode,
        "PtMeasureType":  pt_measure,
        "GeneralRange":   general_range,
        "LeagueID":       "00",
        "PlayerID":       "0",
        # blank filters
        "Conference": "", "Division": "", "GameSegment": "", "Location": "",
        "Month": "0", "Outcome": "", "OpponentTeamID": "0", "Period": "0",
        "VsConference": "", "VsDivision": "", "DateFrom": "", "DateTo": "",
        "LastNGames": "0",
    }
    return call_endpoint(sess, params)


# ── MAIN ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape player shooting dashboard (season‑first layout)."
    )
    parser.add_argument("--season", nargs="+", required=True)
    parser.add_argument("--season-type", nargs="+", choices=SEASON_TYPES,
                        default=SEASON_TYPES)
    parser.add_argument("--category", nargs="+", choices=CATEGORY_MAP.keys(),
                        default=list(CATEGORY_MAP.keys()))
    parser.add_argument("--per-mode", nargs="+", choices=PER_MODES, default=PER_MODES)
    parser.add_argument("--output-root", type=Path,
                        default=Path("data/raw/player_stats/shooting"))
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--delay", type=float, default=0.6)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    sess = create_session()

    for season in args.season:
        for per_mode in args.per_mode:
            for stype in args.season_type:
                base_dir = args.output_root / season / per_mode / stype.lower().replace(" ", "_")
                base_dir.mkdir(parents=True, exist_ok=True)
                for cat in args.category:
                    fpath = base_dir / f"{cat}.csv"
                    if args.skip_existing and fpath.exists():
                        logging.info("Skipping %s", fpath)
                        continue
                    try:
                        df = fetch_combo(sess, season, stype, per_mode, cat)
                        if df.empty:
                            logging.warning("Empty %s | %s | %s | %s", season, per_mode, stype, cat)
                            continue
                        df.to_csv(fpath, index=False)
                        logging.info("Saved %s (%d rows)", fpath, len(df))
                    except Exception as exc:
                        logging.error("Error %s | %s | %s | %s: %s", season, per_mode, stype, cat, exc)
                    time.sleep(args.delay)

if __name__ == "__main__":
    main()
