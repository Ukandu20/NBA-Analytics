

import os
import yaml

# 1) Locate config.yaml (adjust if your script runs from a different CWD)
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")

# 2) Safely load it
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    _cfg = yaml.safe_load(f)

# 3) Expose top-level directories
RAW_DIR       = _cfg["raw_dir"]
PROCESSED_DIR = _cfg["processed_dir"]
EXTERNAL_DIR  = _cfg["external_dir"]

# 4) Expose sub-paths. Example for players_basic.csv, players_detailed.csv, etc.
RAW_PLAYERS_BASIC     = _cfg["raw_subdirs"]["players_basic"].replace("${raw_dir}", RAW_DIR)
RAW_PLAYERS_DETAILED  = _cfg["raw_subdirs"]["players_detailed"].replace("${raw_dir}", RAW_DIR)
RAW_TEAMS_BASIC       = _cfg["raw_subdirs"]["teams_basic"].replace("${raw_dir}", RAW_DIR)
RAW_TEAMS_DETAILED    = _cfg["raw_subdirs"]["teams_detailed"].replace("${raw_dir}", RAW_DIR)
RAW_MVP               = _cfg["raw_subdirs"]["mvp_raw"].replace("${raw_dir}", RAW_DIR)
RAW_ALL_PLAYERS       = _cfg["raw_subdirs"]["all_players_raw"].replace("${raw_dir}", RAW_DIR)

# 5) Expose the player_stats folders in raw:
RAW_PLAYER_STATS = {
    key: path.replace("${raw_dir}", RAW_DIR)
    for key, path in _cfg["raw_subdirs"]["player_stats"].items()
}

# 6) Expose the team_stats folders in raw:
RAW_TEAM_STATS = {}
for category, subdict in _cfg["raw_subdirs"]["team_stats"].items():
    if isinstance(subdict, str):
        RAW_TEAM_STATS[category] = subdict.replace("${raw_dir}", RAW_DIR)
    else:
        # nested structure
        RAW_TEAM_STATS[category] = {
            subkey: subpath.replace("${raw_dir}", RAW_DIR)
            for subkey, subpath in subdict.items()
        }

# 7) Expose schedule seasons:
SCHEDULE_FOLDERS = [ d.replace("${raw_dir}", RAW_DIR) for d in [
    f"{RAW_DIR}/schedule/{year}" for year in _cfg["seasons"]["years"]
] ]

# 8) Expose processed subfolders:
PER_TEAM_CODES = _cfg["processed_subdirs"]["per_team"]
PER_TEAM_STATS_SEASONS = _cfg["processed_subdirs"]["per_team_stats"]["season_dirs"]

# Build full paths under data/processed
PROCESSED_PER_TEAM = [
    os.path.join(PROCESSED_DIR, "per_team", code) for code in PER_TEAM_CODES
]
PROCESSED_PER_TEAM_STATS = []
for code in PER_TEAM_CODES:
    for season in PER_TEAM_STATS_SEASONS:
        PROCESSED_PER_TEAM_STATS.append(
            os.path.join(PROCESSED_DIR, "per_team_stats", code, season)
        )

# 9) API endpoints and headers
API_ENDPOINTS = _cfg["api"]
API_HEADERS   = _cfg["api"]["headers"]

# 10) Defaults (e.g. silhouette URL, missing birthdate)
DEFAULTS = _cfg["defaults"]
