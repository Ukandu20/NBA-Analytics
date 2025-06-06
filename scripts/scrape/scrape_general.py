#!/usr/bin/env python3
"""
scrape_team_general.py
======================
Download Teams ▸ General tables (all measure families) for both
Regular Season and Playoffs, saving

    • Totals  →  …/team_general/<season>/totals/
    • Per-Game → …/team_general/<season>/per_game/

Usage
-----
python scrape_team_general.py --season 2024-25
"""

import time, argparse, requests, pathlib
import pandas as pd

# ── API constants ──────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36"),
    "Referer": "https://www.nba.com/",
    "Origin":  "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}
ENDPOINT   = "https://stats.nba.com/stats/leaguedashteamstats"
DATA_ROOT  = pathlib.Path("data/raw/player_stats/general")

MEASURE_MAP = {
    "traditional": "Base",
    "advanced":    "Advanced",
    "fourfactors": "Four Factors",
    "misc":        "Misc",
    "scoring":     "Scoring",
    "opponent":    "Opponent",
    "defense":     "Defense",
    "violations":  "Violations",
    #  "Estimated Advanced" tends to 400; omit or special-case if needed
}

SEASON_TYPES = ["Regular Season", "Playoffs"]
PER_MODES    = {
    "Totals":   "totals",      # per-mode → sub-folder name
    "PerGame":  "per_game",
}


# ── helpers ────────────────────────────────────────────────────────────────
def call_api(params: dict) -> pd.DataFrame:
    r = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    js = r.json()
    return pd.DataFrame(js["resultSets"][0]["rowSet"],
                        columns=js["resultSets"][0]["headers"])


def fetch_combo(season: str,
                season_type: str,
                measure_key: str,
                per_mode: str,
                out_root: pathlib.Path) -> None:
    """Download one SeasonType × MeasureType × PerMode combo and save CSV."""
    measure_type = MEASURE_MAP[measure_key]

    subdir  = out_root / PER_MODES[per_mode]
    subdir.mkdir(parents=True, exist_ok=True)

    fname = (f"{season_type.lower().replace(' ', '_')}_"
             f"{measure_key}.csv")
    fpath = subdir / fname

    params = {
        "Season":          season,
        "SeasonType":      season_type,
        "PerMode":         per_mode,
        "MeasureType":     measure_type,
        "LeagueID":        "00",
        "TeamID":          "0",
        "Conference":      "", "Division": "", "GameScope": "",
        "GameSegment":     "", "DateFrom": "", "DateTo": "",
        "LastNGames":      "0", "Location": "", "Month": "0",
        "Outcome":         "", "PORound": "0",
        "PaceAdjust":      "N", "PlusMinus": "N", "Rank": "N",
        "Period":          "0", "SeasonSegment": "",
        "ShotClockRange":  "", "TwoWay": "0",
        "VsConference":    "", "VsDivision":   "",
        "OpponentTeamID":  "0",
    }

    df = call_api(params)
    df.to_csv(fpath, index=False)
    print(f"✅ {fpath.relative_to(out_root.parent)}  ({len(df):>3} rows)")
    time.sleep(0.6)


# ── entry point ────────────────────────────────────────────────────────────
def main(season: str):
    out_root = DATA_ROOT / season
    for season_type in SEASON_TYPES:
        for per_mode in PER_MODES:
            for measure_key in MEASURE_MAP:
                fetch_combo(season, season_type, measure_key,
                            per_mode, out_root)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="2024-25",
                    help="NBA season (YYYY-YY, default 2024-25)")
    args = ap.parse_args()
    main(args.season)
