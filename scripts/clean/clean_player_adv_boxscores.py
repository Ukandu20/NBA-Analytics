#!/usr/bin/env python3
"""
clean_player_adv_boxscores.py · v7
==================================

Handles **both** raw season layouts:

1) data/raw/player_stats/adv_boxscores/<SEASON>/<MODE>/<season_type?>/*.csv
2) data/raw/player_stats/adv_boxscores/<SEASON>/*.csv   (flat)

Writes league-wide + per-team CSVs into:

data/processed/player_stats/adv_boxscores/<season>/
    ├─ <mode>/<season_type?>/*.csv
    └─ teams/<TEAM>/<mode>/<season_type?>/*.csv
"""
from __future__ import annotations
import argparse
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional
import pandas as pd
import sys

# ── CONFIG (edit if you rename folders) ────────────────────────────────────
TABLE        = "adv_boxscores"
CORE_MODES   = {"totals", "pergame", "per36", "per48", "per100possessions"}
FLAT_SYNONYM = {"per100poss": "per100possessions"}  # filename shorthands
EXTRA_DROP   = {"group_set"}                        # always-delete cols
EXCLUDE_NUM  = {
    "player", "team", "team_id", "team_name", "play_type",
    "type_grouping", "nickname", "pos", "season", "matchup",
    "wl", "game_date", "min_sec"
}

ROOT      = Path(__file__).resolve().parents[2]
RAW_ROOT  = ROOT / f"data/raw/player_stats/{TABLE}"
PROC_ROOT = ROOT / f"data/processed/player_stats/{TABLE}"
THREADS   = 4

sys.path.append(str(ROOT))
from utils.clean_helpers   import normalise_cols
from utils.numeric_helpers import coerce_all_numeric

# ── regex for season_type detection ────────────────────────────────────────
_SEASON_TYPE_RE = re.compile(r"(regular[_-]?season|playoffs?)", re.I)

# ── helper to strip season-type prefix from filenames ─────────────────────
def _measure_filename(fname: str) -> str:
    """
    Remove any leading 'regular_season_' or 'playoffs_' prefix,
    returning just '<measure>.csv'. If no prefix, returns fname unchanged.
    """
    base = Path(fname).name
    # strip prefix matching season_type + optional separator
    name = re.sub(r'^(regular[_-]?season|playoffs?)_?', '', base, flags=re.I)
    return name

# ── ensure there’s a 'team' column ────────────────────────────────────────
def _ensure_team(df: pd.DataFrame) -> None:
    if "team" in df.columns:
        df["team"] = df["team"].fillna("").astype(str).str.upper()
        return
    for c in ("team_abbreviation", "team_name", "team_id"):
        if c in df.columns:
            df["team"] = df[c].fillna("").astype(str).str.upper()
            break

# ── add/normalize season columns ──────────────────────────────────────────
def _add_bounds(df: pd.DataFrame) -> None:
    """
    Adds/standardises season columns:
      • If `season` exists → derive start/end.
      • Else if `season_year` exists → rename to `season`.
      • Else if `season_id` exists → convert 22024 → "2024-25".
    """
    if "season_year" in df.columns and "season" not in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)

    if "season" not in df.columns and "season_id" in df.columns:
        start = df["season_id"].astype(str).str[-4:].astype(int, errors="ignore")
        df["season"] = start.astype(str) + "-" + ((start + 1) % 100).astype(str).str.zfill(2)

    if "season" in df.columns:
        start = df["season"].astype(str).str.extract(r"^(\d{4})", expand=False)
        df["season_start"] = pd.to_numeric(start, errors="coerce")
        df["season_end"]   = df["season_start"] + 1

# ── write CSV if needed ──────────────────────────────────────────────────
def _write(path: Path, df: pd.DataFrame, *, force: bool) -> None:
    if path.exists() and not force:
        logging.info("skip %s (exists)", path.relative_to(ROOT))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logging.info("✔︎ %s (%d rows)", path.relative_to(ROOT), len(df))

# ── process one file ─────────────────────────────────────────────────────
def _clean_one(
    src: Path,
    dst: Path,
    team_root: Path,
    *,
    per_mode: str,
    season_type: Optional[str],
    force: bool
) -> None:
    # rename dst so filename is just <measure>.csv
    dst = dst.with_name(_measure_filename(dst.name))
    if dst.exists() and not force:
        logging.info("skip %s (exists)", dst.relative_to(ROOT))
        return

    df = pd.read_csv(src)
    if df.empty:
        logging.warning("⚠️  %s empty — skipped", src.name)
        return

    # standardize column names
    df.columns = normalise_cols(df.columns)
    df.rename(columns={
        "player_name": "player",
        "player_last_team_id": "team_id",
        "player_last_team_abbreviation": "team",
        "team_abbreviation": "team",
        "player_position": "pos"
    }, inplace=True)

    # drop unwanted cols
    df.drop(columns=list(EXTRA_DROP & set(df.columns)), errors="ignore", inplace=True)

    # fixes
    _ensure_team(df)
    _add_bounds(df)
    df = coerce_all_numeric(df, exclude_cols=list(EXCLUDE_NUM), warn_on_loss=True)
    df.drop_duplicates(inplace=True)
    if df.empty:
        return

    # write league-wide
    _write(dst, df, force=force)

    # write per-team splits
    if "team" not in df.columns:
        return
    for team, grp in df.groupby("team", sort=False):
        tdir = team_root / team.upper() / per_mode / (season_type or "")
        team_dst = tdir / _measure_filename(src.name)
        _write(team_dst, grp, force=force)

# ── parse flat‐layout filenames ───────────────────────────────────────────
def _parse_flat_filename(fname: str) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (per_mode, season_type) or (None, None) if not recognised.
    """
    base = fname.lower().replace(".csv", "")
    for short, long in FLAT_SYNONYM.items():
        base = base.replace(short, long)
    stype = None
    m = _SEASON_TYPE_RE.search(base)
    if m:
        stype = "regular_season" if "regular" in m.group(0).lower() else "playoffs"
        base = base.replace(m.group(0), "").strip("_-")
    parts = base.split("_")
    for token in reversed(parts):
        if token in CORE_MODES:
            return token, stype
    return None, None

# ── clean one season ───────────────────────────────────────────────────────
def _clean_season(season: str, *, force: bool, workers: int) -> None:
    raw = RAW_ROOT / season
    proc = PROC_ROOT / season
    team_root = proc / "teams"
    if not raw.exists():
        logging.warning("no raw data for %s", season)
        return

    # check mode dirs
    miss = CORE_MODES - {d.name.lower() for d in raw.iterdir() if d.is_dir()}
    if miss and any(d.is_dir() for d in raw.iterdir()):
        logging.warning("%s missing mode folders: %s", season, ", ".join(sorted(miss)))

    pool = ThreadPoolExecutor(max_workers=workers)
    tasks: List = []

    # 1) directory-based layout
    for mode_dir in [d for d in raw.iterdir() if d.is_dir()]:
        per_mode = mode_dir.name.lower()
        for csv in mode_dir.rglob("*.csv"):
            rel = csv.relative_to(mode_dir).parts
            stype = rel[0].lower() if len(rel) == 2 else None
            dst = (proc / per_mode) / (stype or "") / csv.name
            tasks.append(
                pool.submit(
                    _clean_one, csv, dst, team_root,
                    per_mode=per_mode, season_type=stype, force=force
                )
            )

    # 2) flat layout
    for csv in raw.glob("*.csv"):
        per_mode, stype = _parse_flat_filename(csv.name)
        if per_mode is None:
            logging.warning("⚠️  unable to parse mode from %s – skipped", csv.name)
            continue
        dst = (proc / per_mode) / (stype or "") / csv.name
        tasks.append(
            pool.submit(
                _clean_one, csv, dst, team_root,
                per_mode=per_mode, season_type=stype, force=force
            )
        )

    # wait for all
    for f in tasks:
        f.result()

# ── CLI scaffolding ────────────────────────────────────────────────────────
def _seasons_on_disk() -> List[str]:
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir()) if RAW_ROOT.exists() else []

def _parse() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Clean advanced box-score tables & build per-team splits."
    )
    g = ap.add_mutually_exclusive_group()
    g.add_argument("-s", "--season", help="Single season (e.g. 2024-25)")
    g.add_argument("-S", "--seasons", nargs="+", help="Multiple seasons")
    g.add_argument("-a", "--all", action="store_true", help="All seasons on disk")
    ap.add_argument("-f", "--force", action="store_true", help="Overwrite existing")
    ap.add_argument("-w", "--workers", type=int, default=THREADS, help="Parallel threads")
    return ap.parse_args()

def _targets(a: argparse.Namespace) -> List[str]:
    if a.all:
        return _seasons_on_disk()
    return a.seasons or ([a.season] if a.season else [])

def main() -> None:
    args = _parse()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    targets = _targets(args) or _seasons_on_disk()
    for s in targets:
        logging.info("📂 Cleaning season %s", s)
        _clean_season(s, force=args.force, workers=args.workers)

if __name__ == "__main__":
    main()
