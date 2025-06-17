#!/usr/bin/env python3
"""
scrape_players_shot_dashboard.py
================================
Fetch *player* shot-dashboard splits (`leaguedashplayerptshot`) and write them to

    <output-root>/<season>/<perMode>/<season_type>/<split>.csv

Valid per-modes for this endpoint are **Totals** and **PerGame** (anything
else returns 400s – that’s why you saw those errors).

Splits
------
general_overall │ general_catch_shoot │ general_pullups │ general_lt10ft
shotclock │ dribbles │ touch_time │ closest_defender │ closest_defender10

Example
~~~~~~~
python scrape_players_shot_dashboard.py \
       --season 2023-24 2024-25 \
       --split general_overall shotclock \
       --per-mode Totals \
       --delay 0.4
"""
from __future__ import annotations

import argparse, logging, time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

# ── CONSTANTS ──────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36"),
    "Referer":            "https://www.nba.com/",
    "Origin":             "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}
API_URL      = "https://stats.nba.com/stats/leaguedashplayerptshot"
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = ["Totals", "PerGame"]                # <- ONLY ones that work

# CLI key  →  (PtMeasureType , Range parameter-name , Range value)
SPLIT_MAP = {
    "general_overall":       ("General",     "GeneralRange", "Overall"),
    "general_catch_shoot":   ("General",     "GeneralRange", "Catch and Shoot"),
    "general_pullups":       ("General",     "GeneralRange", "Pullups"),
    "general_lt10ft":        ("General",     "GeneralRange", "Less Than 10 ft"),
    "shotclock":             ("ShotClock",   "",             ""),
    "dribbles":              ("Dribble",     "",             ""),
    "touch_time":            ("TouchTime",   "",             ""),
    "closest_defender":      ("ClosestDefender",    "",      ""),
    "closest_defender10":    ("ClosestDefender10ftPlus", "", ""),
}

# ── SESSION WITH RETRIES ───────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.mount("https://", HTTPAdapter(max_retries=Retry(
        total=4, backoff_factor=1,
        allowed_methods=["GET"], status_forcelist=[429,500,502,503,504])))
    return s

# ── CORE FETCH ─────────────────────────────────────────────────────────────
def fetch_split(sess: requests.Session, season:str, season_type:str,
                split_key:str, per_mode:str) -> pd.DataFrame:
    mtype, rng_param, rng_val = SPLIT_MAP[split_key]
    params = {
        "Season":        season,
        "SeasonType":    season_type,
        "PerMode":       per_mode,
        "PtMeasureType": mtype,
        "LeagueID":      "00",
        "PlayerID":      "0",
        # split-specific range
        rng_param:       rng_val,
        # blank optional filters
        "Conference": "", "Division": "", "GameSegment": "",
        "Location": "",   "Month": "0",   "Outcome": "",
        "OpponentTeamID": "0", "Period": "0",
        "VsConference": "", "VsDivision": "",
        "DateFrom": "", "DateTo": "", "LastNGames": "0",
    }
    r = sess.get(API_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()["resultSets"][0]
    return pd.DataFrame(data["rowSet"], columns=data["headers"])

# ── MAIN CLI ───────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Scrape Players ➤ Shot Dashboard (season-first layout).")
    ap.add_argument("--season", "-s", required=True, nargs="+",
                    help="Season(s) YYYY-YY, e.g. 2024-25")
    ap.add_argument("--season-type", nargs="+", choices=SEASON_TYPES,
                    default=SEASON_TYPES)
    ap.add_argument("--split", nargs="+", choices=SPLIT_MAP.keys(),
                    default=list(SPLIT_MAP.keys()))
    ap.add_argument("--per-mode", nargs="+", choices=PER_MODES,
                    default=PER_MODES)
    ap.add_argument("--output-root", type=Path,
                    default=Path("data/raw/player_stats/shot_dashboard"))
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--delay", type=float, default=0.6)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    sess = make_session()

    for season in args.season:
        for per_mode in args.per_mode:
            for stype in args.season_type:
                base = args.output_root / season / per_mode / stype.lower().replace(" ", "_")
                base.mkdir(parents=True, exist_ok=True)
                for split in args.split:
                    fpath = base / f"{split}.csv"
                    if args.skip_existing and fpath.exists():
                        logging.info("Skipping %s", fpath)
                        continue
                    try:
                        df = fetch_split(sess, season, stype, split, per_mode)
                        if df.empty:
                            logging.warning("Empty %s | %s | %s | %s",
                                            season, per_mode, stype, split)
                            continue
                        df.to_csv(fpath, index=False)
                        logging.info("Saved %s (%d rows)", fpath, len(df))
                    except Exception as e:
                        logging.error("Error %s | %s | %s | %s: %s",
                                      season, per_mode, stype, split, e)
                    time.sleep(args.delay)

if __name__ == "__main__":
    main()
