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
import argparse, logging, re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, List, Optional
import pandas as pd, sys

# ── CONFIG (edit if you rename folders) ────────────────────────────────────
TABLE        = "general"                  # table name
CORE_MODES   = {"totals", "pergame", "per36", "per48", "per100possessions"}
FLAT_SYNONYM = {"per100poss": "per100possessions"}         # filename shorthands
EXTRA_DROP   = {"group_set"}                               # always-delete cols
EXCLUDE_NUM  = {"player", "team", "team_id", "note", "nickname", "pos",}

ROOT      = Path(__file__).resolve().parents[2]
RAW_ROOT  = ROOT / f"data/raw/player_stats/{TABLE}"
PROC_ROOT = ROOT / f"data/processed/player_stats/{TABLE}"
THREADS   = 4

sys.path.append(str(ROOT))
from utils.clean_helpers   import normalise_cols
from utils.numeric_helpers import coerce_all_numeric

# ── helpers ----------------------------------------------------------------
_SEASON_TYPE_RE = re.compile(r"(regular[_-]?season|playoffs?)", re.I)

def _ensure_team(df: pd.DataFrame) -> None:
    if "team" in df.columns:
        df["team"] = df["team"].fillna("").astype(str).str.upper(); return
    for c in ("team_abbreviation", "team_name", "team_id"):
        if c in df.columns:
            df["team"] = df[c].fillna("").astype(str).str.upper(); break

def _add_bounds(df: pd.DataFrame) -> None:
    if "season_year" in df.columns:
        df.rename(columns={"season_year":"season"}, inplace=True)
    if "season" in df.columns:
        yr=df["season"].astype(str).str.extract(r"^(\d{4})",expand=False)
        df["season_start"]=pd.to_numeric(yr,errors="coerce")
        df["season_end"]=df["season_start"]+1

def _write(path:Path, df:pd.DataFrame, *, force:bool)->None:
    if path.exists() and not force:
        logging.info("skip %s (exists)", path.relative_to(ROOT)); return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logging.info("✔︎ %s (%d rows)", path.relative_to(ROOT), len(df))

def _clean_one(src:Path, dst:Path, team_root:Path,
               *, per_mode:str, season_type:Optional[str], force:bool)->None:
    if dst.exists() and not force: return
    df=pd.read_csv(src); 
    if df.empty: logging.warning("⚠️  %s empty — skipped", src.name); return
    df.columns=normalise_cols(df.columns)
    df.rename(columns={
        "player_name":"player","player_last_team_id":"team_id",
        "player_last_team_abbreviation":"team","team_abbreviation":"team",
        "player_position": "pos"}, inplace=True)
    df.drop(columns=list(EXTRA_DROP & set(df.columns)), errors="ignore", inplace=True)
    _ensure_team(df); _add_bounds(df)
    df=coerce_all_numeric(df, exclude_cols=list(EXCLUDE_NUM), warn_on_loss=True)
    df.drop_duplicates(inplace=True)
    if df.empty: return
    _write(dst, df, force=force)
    if "team" not in df.columns: return
    for team, grp in df.groupby("team", sort=False):
        tdir=team_root/str(team).upper()/per_mode/(season_type or "")
        _write(tdir/src.name, grp, force=force)

# ── filename parsing for FLAT layout ---------------------------------------
def _parse_flat_filename(fname:str)->tuple[Optional[str], Optional[str]]:
    """
    Returns (per_mode, season_type) or (None, None) if not recognised.
    """
    base = fname.lower().replace(".csv", "")
    # handle synonyms
    for short,long in FLAT_SYNONYM.items():
        base = base.replace(short, long)
    # extract season_type
    stype = None
    m=_SEASON_TYPE_RE.search(base)
    if m:
        stype = "regular_season" if "regular" in m.group(0) else "playoffs"
        base = base.replace(m.group(0), "")
        base = base.strip("_-")
    parts = base.split("_")
    # mode is the last token that matches CORE_MODES
    for token in reversed(parts):
        if token in CORE_MODES:
            return token, stype
    return None, None

# ── season driver -----------------------------------------------------------
def _clean_season(season:str,*,force:bool,workers:int)->None:
    raw=RAW_ROOT/season; proc=PROC_ROOT/season; team_root=proc/"teams"
    if not raw.exists(): logging.warning("no raw data for %s", season); return

    # warn core-modes (directory layout only)
    miss = CORE_MODES - {p.name.lower() for p in raw.iterdir() if p.is_dir()}
    if miss and any(p.is_dir() for p in raw.iterdir()):
        logging.warning("%s missing mode folders: %s", season, ", ".join(sorted(miss)))

    pool=ThreadPoolExecutor(max_workers=workers); tasks=[]
    # 1) directory-based files
    for mode_dir in [d for d in raw.iterdir() if d.is_dir()]:
        per_mode=mode_dir.name.lower()
        for csv in mode_dir.rglob("*.csv"):
            rel=csv.relative_to(mode_dir).parts
            stype=rel[0].lower() if len(rel)==2 else None
            out=(proc/per_mode)/(stype or "")/csv.name
            tasks.append(pool.submit(_clean_one, csv, out, team_root,
                                     per_mode=per_mode, season_type=stype, force=force))
    # 2) flat files in season root
    for csv in raw.glob("*.csv"):
        per_mode, stype=_parse_flat_filename(csv.name)
        if per_mode is None:
            logging.warning("⚠️  unable to parse mode from %s – skipped", csv.name); continue
        out=(proc/per_mode)/(stype or "")/csv.name
        tasks.append(pool.submit(_clean_one, csv, out, team_root,
                                 per_mode=per_mode, season_type=stype, force=force))
    for f in tasks: f.result()

# ── CLI / entrypoint --------------------------------------------------------
def _seasons_on_disk()->List[str]:
    return sorted(p.name for p in RAW_ROOT.iterdir() if p.is_dir()) if RAW_ROOT.exists() else []

def _parse()->argparse.Namespace:
    ap=argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
         description="Clean advanced box-score tables & build per-team splits.")
    g=ap.add_mutually_exclusive_group()
    g.add_argument("-s","--season"); g.add_argument("-S","--seasons",nargs="+"); g.add_argument("-a","--all",action="store_true")
    ap.add_argument("-f","--force",action="store_true"); ap.add_argument("-w","--workers",type=int,default=THREADS)
    return ap.parse_args()

def _targets(a): return _seasons_on_disk() if a.all else (a.seasons or [a.season or "2024-25"])

def main()->None:
    args=_parse(); logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)-8s %(message)s")
    for s in _targets(args):
        logging.info("📂 Cleaning season %s", s)
        _clean_season(s, force=args.force, workers=args.workers)

if __name__=="__main__": main()
