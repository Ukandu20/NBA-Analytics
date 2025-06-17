#!/usr/bin/env python3
"""
scrape_players_playtype.py
==========================
Scrape **Synergy** play‑type stats for NBA **players**
into a season‑first folder layout:

    <output-root>/<season>/<perMode>/<season_type>/<play_type>_<group>.csv

Parameters
----------
* **Play‑types**   – isolation, transition, pr_ball_handler, pr_roll_man,
                     post_up, spot_up, handoff, cut, off_screen, putbacks, misc
* **Groupings**    – offensive (default) | defensive
* **Per‑modes**    – Totals | PerGame  (player endpoint supports only these)
* **SeasonTypes**  – Regular Season | Playoffs

Example
~~~~~~~
```bash
python scrape_players_playtype.py \
  --season 2024-25 \
  --play-type isolation transition \
  --grouping offensive \
  --per-mode Totals \
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
API_URL = "https://stats.nba.com/stats/synergyplaytypes"
SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES = ["Totals", "PerGame"]  # only valid choices for player endpoint
TYPE_GROUPS = ["offensive", "defensive"]

# CLI key → Synergy PlayType string
PLAYTYPE_MAP = {
    "isolation":       "Isolation",
    "transition":      "Transition",
    "pr_ball_handler": "PRBallHandler",
    "pr_roll_man":     "PRRollman",
    "post_up":         "Postup",
    "spot_up":         "Spotup",
    "handoff":         "Handoff",
    "cut":             "Cut",
    "off_screen":      "OffScreen",
    "putbacks":        "OffRebound",
    "misc":            "Misc",
}

# ── SESSION WITH RETRIES ─────────────────────────────────────────────────

def create_session() -> requests.Session:
    session = requests.Session()
    retry_cfg = Retry(total=5, backoff_factor=1,
                      status_forcelist=[429, 500, 502, 503, 504],
                      allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry_cfg))
    session.headers.update(HEADERS)
    return session


# ── SYNERGY CALL ─────────────────────────────────────────────────────────

def synergy_request(session: requests.Session, params: dict) -> pd.DataFrame:
    for attempt in range(1, 3):  # one manual retry on 5xx
        try:
            resp = session.get(API_URL, params=params, timeout=20)
            resp.raise_for_status()
            js = resp.json()["resultSets"][0]
            return pd.DataFrame(js["rowSet"], columns=js["headers"])
        except requests.HTTPError as err:
            if err.response is not None and 500 <= err.response.status_code < 600 and attempt == 1:
                time.sleep(2)
                continue
            raise
    raise RuntimeError("Unreachable: synergy_request loop")


def fetch_combo(session: requests.Session, season: str, season_type: str,
                play_key: str, grouping: str, per_mode: str) -> pd.DataFrame:
    params = {
        "LeagueID":      "00",
        "SeasonYear":    season,          # full season string works for Synergy
        "SeasonType":    season_type,
        "PlayerOrTeam":  "P",            # player rows
        "TypeGrouping":  grouping,
        "PlayType":      PLAYTYPE_MAP[play_key],
        "PerMode":       per_mode,
        "Rank":          "N",
        # Blank optional filters
        "ContextMeasure": "", "ContextFilter": "",
        "Conference": "", "Division": "", "GameScope": "", "GameSegment": "",
        "DateFrom": "", "DateTo": "", "Location": "", "Outcome": "",
        "SeasonSegment": "", "StarterBench": "", "ShotClockRange": "",
        "VsConference": "", "VsDivision": "",
        "LastNGames": "0", "Month": "0", "OpponentTeamID": "0", "PORound": "",
        "Period": "0",
    }
    return synergy_request(session, params)


# ── MAIN ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Synergy player play‑type stats (season‑first layout)."
    )
    parser.add_argument("--season", required=True, nargs="+",
                        help="Season(s) in YYYY-YY format, e.g. 2024-25")
    parser.add_argument("--season-type", nargs="+", choices=SEASON_TYPES,
                        default=SEASON_TYPES)
    parser.add_argument("--play-type", nargs="+", choices=PLAYTYPE_MAP.keys(),
                        default=list(PLAYTYPE_MAP.keys()))
    parser.add_argument("--grouping", nargs="+", choices=TYPE_GROUPS,
                        default=["offensive"])
    parser.add_argument("--per-mode", nargs="+", choices=PER_MODES, default=PER_MODES)
    parser.add_argument("--output-root", type=Path,
                        default=Path("data/raw/player_stats/playtype"))
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
                for play in args.play_type:
                    for grp in args.grouping:
                        fpath = base_dir / f"{play}_{grp}.csv"
                        if args.skip_existing and fpath.exists():
                            logging.info("Skipping %s", fpath)
                            continue
                        try:
                            df = fetch_combo(sess, season, stype, play, grp, per_mode)
                            if df.empty:
                                logging.warning("Empty %s | %s | %s | %s | %s", season, per_mode, stype, play, grp)
                                continue
                            df.to_csv(fpath, index=False)
                            logging.info("Saved %s (%d rows)", fpath, len(df))
                        except Exception as exc:
                            logging.error("Error %s | %s | %s | %s | %s: %s", season, per_mode, stype, play, grp, exc)
                        time.sleep(args.delay)

if __name__ == "__main__":
    main()
