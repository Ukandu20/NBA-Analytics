#!/usr/bin/env python3
"""
clean_team_adv_boxscores.py · v1
================================

Cleans team-level *advanced box-score* CSVs with optional
season-type and month splits, and writes league-wide masters plus per-team mirrors.
"""

from __future__ import annotations
import argparse, logging, re, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional

import pandas as pd

# ── CONFIG ──────────────────────────────────────────────────────────────────
TABLE        = "clutch"     # folder under team_stats
CORE_MODES   = {"totals", "pergame", "per48" , "per36", "per100possessions"}
FLAT_SYNONYM = {"pergame": "per_game", "per100poss": "per100possessions"}
EXTRA_DROP   = {"group_set"}
EXCLUDE_NUM  = {
    "team", "team_id", "team_name", "nickname",
    "conference", "division", "season", "matchup", "wl", "game_date", "min_sec"
}

ROOT      = Path(__file__).resolve().parents[2]
RAW_ROOT  = ROOT / f"data/raw/team_stats/{TABLE}"
PROC_ROOT = ROOT / f"data/processed/team_stats/{TABLE}"
THREADS   = 4

sys.path.append(str(ROOT))
from utils.clean_helpers   import normalise_cols
from utils.numeric_helpers import coerce_all_numeric

_SEASON_TYPE_RE = re.compile(r"(regular[_-]?season|playoffs?)", re.I)

# ── helpers ----------------------------------------------------------------
def _ensure_team(df: pd.DataFrame) -> None:
    if "team" not in df.columns:
        for c in ("team_abbreviation", "team_name", "team_id"):
            if c in df.columns:
                df["team"] = df[c]
                break
    df["team"] = df["team"].fillna("").astype(str).str.upper()

def _add_bounds(df: pd.DataFrame) -> None:
    if "season_year" in df.columns and "season" not in df.columns:
        df.rename(columns={"season_year": "season"}, inplace=True)
    if "season" not in df.columns and "season_id" in df.columns:
        base = df["season_id"].astype(str).str[-4:].astype(int, errors="ignore")
        df["season"] = base.astype(str) + "-" + ((base + 1) % 100).astype(str).str.zfill(2)
    if "season" in df.columns:
        s = df["season"].str.extract(r"^(\d{4})", expand=False)
        df["season_start"] = pd.to_numeric(s, errors="coerce")
        df["season_end"]   = df["season_start"] + 1

def _write(path: Path, df: pd.DataFrame, *, force: bool) -> None:
    if path.exists() and not force:
        logging.info("skip %s (exists)", path.relative_to(ROOT)); return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logging.info("✔︎ %s  (%d rows)", path.relative_to(ROOT), len(df))

# ── core clean -------------------------------------------------------------
def _clean_one(src: Path, dst: Path, team_root: Path,
               *, mode: str, stype: Optional[str], month: Optional[str],
               force: bool) -> None:
    if dst.exists() and not force: return
    df = pd.read_csv(src)
    if df.empty:
        logging.warning("⚠️  %s empty — skipped", src.name); return

    df.columns = normalise_cols(df.columns)
    df.rename(columns={
        "player_name":"player","player_last_team_id":"team_id",
        "player_last_team_abbreviation":"team","team_abbreviation":"team",
        "player_position": "pos"}, inplace=True)
    _ensure_team(df); _add_bounds(df)
    df.drop(columns=list(EXTRA_DROP & set(df.columns)), errors="ignore", inplace=True)

    df = coerce_all_numeric(df, exclude_cols=list(EXCLUDE_NUM), warn_on_loss=True)
    df.drop_duplicates(inplace=True)
    if df.empty: return

    _write(dst, df, force=force)


# ── file-name helper for flat layout --------------------------------------
def _parse_flat(fname: str) -> tuple[Optional[str], Optional[str]]:
    base = fname.lower().removesuffix(".csv")
    for s, l in FLAT_SYNONYM.items(): base = base.replace(s, l)
    stype = None
    m = _SEASON_TYPE_RE.search(base)
    if m:
        stype = "regular_season" if "regular" in m.group(0) else "playoffs"
        base = base.replace(m.group(0), "").strip("_-")
    for token in reversed(base.split("_")):
        if token in CORE_MODES:
            return token, stype
    return None, None

# ── season driver ----------------------------------------------------------
def _clean_season(season: str, *, force: bool, workers: int) -> None:
    raw  = RAW_ROOT / season
    proc = PROC_ROOT / season
    team_root = proc / "teams"
    if not raw.exists():
        logging.warning("no raw data for %s", season); return

    miss = CORE_MODES - {d.name.lower() for d in raw.iterdir() if d.is_dir()}
    if miss: logging.warning("%s missing mode dirs: %s", season, ", ".join(sorted(miss)))

    pool = ThreadPoolExecutor(max_workers=workers)
    tasks = []

    # Directory layout with optional season_type / month
    for mode_dir in [d for d in raw.iterdir() if d.is_dir()]:
        mode = mode_dir.name.lower()
        for csv in mode_dir.rglob("*.csv"):
            parts = csv.relative_to(mode_dir).parts
            stype = month = None
            if len(parts) == 3: stype, month, _ = map(str.lower, parts)
            elif len(parts) == 2:
                a, _ = map(str.lower, parts)
                if a in ("regular_season", "playoffs"): stype = a
                else: month = a
            out = proc / mode / (stype or "") / (month or "") / csv.name
            tasks.append(pool.submit(
                _clean_one, csv, out, team_root,
                mode=mode, stype=stype, month=month, force=force))

    # Flat layout
    for csv in raw.glob("*.csv"):
        mode, stype = _parse_flat(csv.name)
        if mode is None:
            logging.warning("⚠️  can't parse %s – skipped", csv.name); continue
        out = proc / mode / (stype or "") / csv.name
        tasks.append(pool.submit(
            _clean_one, csv, out, team_root,
            mode=mode, stype=stype, month=None, force=force))

    for t in tasks: t.result()

# ── CLI --------------------------------------------------------------------
def _seasons_on_disk() -> List[str]:
    return [p.name for p in RAW_ROOT.iterdir() if p.is_dir()] if RAW_ROOT.exists() else []

def _parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Clean team advanced box-score tables (mode + month splits)."
    )
    g = ap.add_mutually_exclusive_group()
    g.add_argument("-s", "--season"); g.add_argument("-S", "--seasons", nargs="+")
    g.add_argument("-a", "--all", action="store_true")
    ap.add_argument("-f", "--force", action="store_true")
    ap.add_argument("-w", "--workers", type=int, default=THREADS)
    return ap.parse_args()

def _targets(a) -> List[str]:
    return _seasons_on_disk() if a.all else (a.seasons or [a.season or "2024-25"])

def main() -> None:
    args = _parse_cli()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    for s in _targets(args):
        logging.info("📂 Cleaning season %s", s)
        _clean_season(s, force=args.force, workers=args.workers)

if __name__ == "__main__":
    main()
