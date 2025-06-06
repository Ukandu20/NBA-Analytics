#!/usr/bin/env python3
"""
all_players_cleaned.py  –  NO settings.py / config.yaml required
===============================================================

• Reads `data/raw/players_basic.csv`
        (and optionally `players_detailed.csv` → --merge)
• Cleans, normalises, fixes headshots, splits draft info, etc.
• Optional --api-fill grabs missing height/weight/experience.
• Writes `data/processed/all_players_cleaned.csv`.

Utility imports (normalise_cols & coerce_all_numeric) come from utils/.
Everything else is hard-coded below – tweak the constants block if
your folder names or defaults differ.
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

# ───────────────────────────────
# 0)  Make sure we can import utils
#     (add project_root to sys.path)
# ───────────────────────────────
import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))  # ../../

from utils.clean_helpers   import normalise_cols
from utils.numeric_helpers import coerce_all_numeric

# ───────────────────────────────
# 1)  CONSTANTS – tweak freely
# ───────────────────────────────
RAW_DIR        = Path("data/raw")
PROCESSED_DIR  = Path("data/processed")

RAW_PLAYERS_BASIC    = RAW_DIR / "players_basic.csv"
RAW_PLAYERS_DETAILED = RAW_DIR / "players_detailed.csv"   # optional
OUT_PATH             = PROCESSED_DIR / "all_players_cleaned.csv"

SILHOUETTE_URL = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
HEADSHOT_CDN   = "https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"

MISSING_BIRTHDATE              = "1980-01-01"
UNDRAFTED_CODE                 = "UDF"
RETIREE_MISSING_CORE_THRESHOLD = 4
FREE_AGENT_LABEL               = "FA"

API_URL = "https://stats.nba.com/stats/commonplayerinfo?PlayerID={pid}"
API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Origin":  "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Accept":  "application/json, text/plain, */*",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}

# ───────────────────────────────
# 2)  Regex helpers
# ───────────────────────────────
_draft_year  = re.compile(r"(\d{4})")
_draft_round = re.compile(r"(?:round|rnd)\s*(\d+)|\bR(\d+)\b", re.I)
_draft_pick  = re.compile(r"(?:pick|#)\s*(\d+)", re.I)

_pid_url   = re.compile(r"/player/(\d+)/")
_pid_query = re.compile(r"PlayerID=(\d+)", re.I)

# ───────────────────────────────
# 3)  Tiny converters
# ───────────────────────────────
def _height_to_in(v: Any) -> float:
    try:
        ft, inch = map(int, str(v).split("-"))
        return ft * 12 + inch
    except Exception:
        return np.nan


def _weight_to_lbs(v: Any) -> float:
    try:
        m = re.search(r"\d+", str(v))
        return float(m.group()) if m else np.nan
    except Exception:
        return np.nan


def _years(txt: Any) -> float:
    try:
        return int(str(txt).split()[0])
    except Exception:
        return np.nan


def _parse_draft(raw: Any) -> tuple[float, float, float]:
    if not isinstance(raw, str) or not raw or "undrafted" in raw.lower():
        return np.nan, np.nan, np.nan
    y  = _draft_year .search(raw)
    rd = _draft_round.search(raw)
    pk = _draft_pick .search(raw)
    year  = int(y.group(1)) if y else np.nan
    if rd:
        grp = rd.groups()
        rnd = int(grp[0] or grp[1])
    else:
        rnd = np.nan
    pick = int(pk.group(1)) if pk else np.nan
    return year, rnd, pick


def _extract_pid(url: str | float) -> str | None:
    m = _pid_url.search(str(url)) or _pid_query.search(str(url))
    return m.group(1) if m else None


def _fix_headshot(row: pd.Series) -> str:
    url = str(row.get("headshot_url", "")).strip()
    pid = row.get("pid")
    if url and url.lower() not in ("nan", SILHOUETTE_URL):
        return url
    return HEADSHOT_CDN.format(pid=pid) if pid else SILHOUETTE_URL


def _api_backfill(df: pd.DataFrame) -> pd.DataFrame:
    need = df["height"].isna() | df["weight"].isna() | df["experience"].isna()
    todo = df[need].copy()
    if todo.empty:
        print("API fill: nothing to fetch.")
        return df

    sess = requests.Session();  sess.headers.update(API_HEADERS)
    print(f"API fill: fetching {len(todo)} players …")

    for idx, row in todo.iterrows():
        pid = row["pid"]
        if not pid:
            continue
        try:
            js  = sess.get(API_URL.format(pid=pid), timeout=8).json()
            rs  = js["resultSets"][0];  hdr = rs["headers"];  dat = rs["rowSet"][0]
            info = dict(zip(hdr, dat))
            if pd.isna(df.at[idx, "height"]):
                df.at[idx, "height"] = _height_to_in(info.get("HEIGHT"))
            if pd.isna(df.at[idx, "weight"]):
                df.at[idx, "weight"] = _weight_to_lbs(info.get("WEIGHT"))
            if pd.isna(df.at[idx, "experience"]):
                df.at[idx, "experience"] = _years(info.get("SEASON_EXP"))
        except Exception:
            pass
        time.sleep(0.2)
    return df


def _build_player_ids(df: pd.DataFrame) -> pd.Series:
    parts = (
        df["player"].str.lower()
                     .str.replace(r"[^a-z ]", "", regex=True)
                     .str.strip()
                     .str.split(" ", expand=True)
    )
    last  = parts[1].fillna(parts[0]).str[:5]
    first = parts[0].str[:2]
    base  = last + first
    rank  = base.groupby(base).cumcount().add(1).astype(str).str.zfill(2)
    return (base + rank).str.ljust(9, "0")

# ───────────────────────────────
# 4)  main()
# ───────────────────────────────
def main(merge: bool = False, api_fill: bool = False) -> None:
    if not RAW_PLAYERS_BASIC.exists():
        raise FileNotFoundError(f"Missing {RAW_PLAYERS_BASIC}")

    df_basic = pd.read_csv(RAW_PLAYERS_BASIC, dtype=str)
    df_basic.columns = normalise_cols(df_basic.columns)

    if merge and RAW_PLAYERS_DETAILED.exists():
        df_det = pd.read_csv(RAW_PLAYERS_DETAILED, dtype=str)
        df_det.columns = normalise_cols(df_det.columns)
        extras = [c for c in df_det.columns if c not in df_basic.columns]
        df = df_basic.merge(df_det[["profile_url", *extras]], on="profile_url", how="left")
    else:
        df = df_basic.copy()

    # ----- core defaults ------------------------------------------------
    for c in ("headshot_url", "birthdate", "experience", "draft"):
        if c not in df.columns:
            df[c] = pd.NA if c == "birthdate" else np.nan
    if "is_retired" not in df.columns:
        df["is_retired"] = False

    # ----- pid + headshots ---------------------------------------------
    df["pid"]          = df["profile_url"].apply(_extract_pid)
    df["headshot_url"] = df.apply(_fix_headshot, axis=1)

    # ----- tidy player / team ------------------------------------------
    df["player"] = df["player"].astype(str).str.strip()
    df["team"]   = df["team"].fillna("").astype(str).str.strip()
    df.loc[df["is_retired"] == True, "team"] = "RET"
    df.loc[(df["is_retired"] == False) & (df["team"] == ""), "team"] = FREE_AGENT_LABEL

    # ----- position splits ---------------------------------------------
    if "position" in df.columns:
        df["position"] = df["position"].astype(str).str.upper().str.strip()
        df["position_list"]    = df["position"].str.split(r"[-/,]", regex=True)
        df["position_primary"] = df["position_list"].str[0]
        df["position_alt"]     = df["position_list"].apply(
            lambda lst: "|".join(lst[1:]) if len(lst) > 1 else ""
        )
    else:
        df["position_primary"] = np.nan
        df["position_alt"]     = ""

    # ----- height / weight / experience --------------------------------
    df["height"]     = df["height"].apply(_height_to_in) if "height" in df else np.nan
    df["weight"]     = df["weight"].apply(_weight_to_lbs) if "weight" in df else np.nan
    df["experience"] = df["experience"].apply(_years)    if "experience" in df else np.nan

    # ----- birthdate ----------------------------------------------------
    df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")
    df["birthdate"] = df["birthdate"].fillna(pd.to_datetime(MISSING_BIRTHDATE))

    # ----- draft split --------------------------------------------------
    if "draft" in df.columns:
        splits = df["draft"].apply(_parse_draft).tolist()
        df[["draft_year", "draft_round", "draft_pick"]] = pd.DataFrame(splits, index=df.index)
        df["draft_status"] = np.where(df["draft_year"].isna(), UNDRAFTED_CODE, "Drafted")
        mask_udf = df["draft_status"] == UNDRAFTED_CODE
        df.loc[mask_udf, ["draft_year", "draft_round", "draft_pick"]] = 0
        df.drop(columns="draft", inplace=True, errors="ignore")

    for c in ("draft_year", "draft_round", "draft_pick"):
        if c in df.columns:
            df[c] = df[c].astype("Int64")

    # ----- retiree prune -----------------------------------------------
    core = ["height", "weight", "position", "country", "birthdate",
            "draft_year", "draft_pick", "experience"]
    present = [c for c in core if c in df.columns]
    df["missing_core"] = (
        df[present].isna() | df[present].astype(str).eq("")
    ).sum(axis=1)
    df = df[~((df["is_retired"] == True) & (df["missing_core"] >= RETIREE_MISSING_CORE_THRESHOLD))]
    df.drop(columns="missing_core", inplace=True)

    # ----- player_id ----------------------------------------------------
    df["player_id"] = _build_player_ids(df)

    # ----- optional API fill -------------------------------------------
    if api_fill:
        df = _api_backfill(df)

    # ----- coerce NUMERIC (everything except these) --------------------
    exclude = [
        "player", "team", "position", "position_list",
        "position_primary", "position_alt", "profile_url",
        "headshot_url", "pid", "birthdate", "draft_status"
    ]
    df = coerce_all_numeric(df, exclude)

    # ----- missing-value report & save ---------------------------------
    print("\n─ Missing values per column ─")
    print(df.replace("", np.nan).isna().sum().sort_values(ascending=False).to_string())
    print("─────────────────────────────\n")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"✅ Saved → {OUT_PATH}  ({len(df):,} rows)")


# ───────────────────────────────
# 5) CLI entrypoint
# ───────────────────────────────
if __name__ == "__main__":
    argp = argparse.ArgumentParser(description="Clean all_players CSV.")
    argp.add_argument("--merge",    action="store_true", help="merge players_detailed.csv if present")
    argp.add_argument("--api-fill", action="store_true", help="API back-fill height/weight/experience")
    opts = argp.parse_args()
    main(merge=opts.merge, api_fill=opts.api_fill)
