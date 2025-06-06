# settings.py

import yaml
from pathlib import Path

# 1) Locate config.yaml (assumes this file sits in the same folder)
CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

# 2) Load and validate config
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    _cfg = yaml.safe_load(f)

required_keys = [
    "raw_dir", "processed_dir", "external_dir",
    "raw_subdirs", "processed_subdirs",
    "api", "defaults"
]
missing = [k for k in required_keys if k not in _cfg]
if missing:
    raise RuntimeError(f"Missing required config keys in config.yaml: {missing}")

# 3) Expose top-level directories as Path objects
RAW_DIR       = Path(_cfg["raw_dir"])
PROCESSED_DIR = Path(_cfg["processed_dir"])
EXTERNAL_DIR  = Path(_cfg["external_dir"])

# 4) Expose raw file paths for players & teams
RAW_PLAYERS_BASIC    = RAW_DIR / _cfg["raw_subdirs"]["players_basic"]
RAW_PLAYERS_DETAILED = RAW_DIR / _cfg["raw_subdirs"]["players_detailed"]
RAW_TEAMS_BASIC      = RAW_DIR / _cfg["raw_subdirs"]["teams_basic"]
RAW_TEAMS_DETAILED   = RAW_DIR / _cfg["raw_subdirs"]["teams_detailed"]

# 5) Expose raw award folder
RAW_AWARDS_DIR       = RAW_DIR / "awards"
PROCESSED_AWARDS_DIR = PROCESSED_DIR / _cfg["processed_subdirs"]["awards"]


# 6) Expose raw player_stats folders
RAW_PLAYER_STATS = {
    key: (RAW_DIR / subpath)
    for key, subpath in _cfg["raw_subdirs"]["player_stats"].items()
}

# 7) Expose raw team_stats folders
_raw_team = _cfg["raw_subdirs"]["team_stats"]
RAW_TEAM_STATS = {key: (RAW_DIR / subpath) for key, subpath in _raw_team.items()}

# 8) Expose schedule folders
SEASONS = _cfg["seasons"]["years"]
SCHEDULE_FOLDERS = [RAW_DIR / f"schedule/{year}" for year in SEASONS]

# 9) Expose processed per_team & per_team_stats paths
PER_TEAM_CODES = _cfg["processed_subdirs"]["per_team"]
PER_TEAM_STATS_SEASONS = _cfg["processed_subdirs"]["per_team_stats"]["season_dirs"]

PROCESSED_PER_TEAM = [PROCESSED_DIR / "per_team" / code for code in PER_TEAM_CODES]
PROCESSED_PER_TEAM_STATS = [
    PROCESSED_DIR / "per_team_stats" / code / season
    for code in PER_TEAM_CODES
    for season in PER_TEAM_STATS_SEASONS
]

# 10) API endpoints and headers
API_ENDPOINTS = _cfg["api"]
API_HEADERS   = _cfg["api"]["headers"]

# 11) Defaults (e.g. silhouette URL, missing birthdate, retiree threshold)
DEFAULTS = _cfg["defaults"]
