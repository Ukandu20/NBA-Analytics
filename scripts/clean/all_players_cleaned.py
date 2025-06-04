#!/usr/bin/env python
"""
Clean NBA player roster → data/processed/all_players_cleaned.csv
================================================================
2025-06-02
  • NEW  : `pid` column (numeric Player-ID)
  • Rule : if headshot_url missing or the silhouette fallback,
           rebuild → https://cdn.nba.com/headshots/nba/latest/1040x760/<pid>.png
           (silhouette kept as last-resort fallback)
  • Undrafted (UDF) → draft_* == 0
  • Missing birthdate → 1980-01-01 (from players_basic)
  • Optional --api-fill back-fills height / weight / experience
"""
import os
import re
import time
import argparse
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests


RAW_DIR   = "data/raw"
OUT_FPATH = "data/processed/all_players_cleaned.csv"

SILHOUETTE_URL = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
HEADSHOT_CDN   = "https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
CHECK_CDN      = False          # set True to HEAD-check each CDN url (slower)

API_URL = "https://stats.nba.com/stats/commonplayerinfo?PlayerID={pid}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Origin":  "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Accept":  "application/json, text/plain, */*",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
}


# ───────────────────────────────────────────────────────── helpers ─────────
def _load(fname: str) -> pd.DataFrame:
    path = os.path.join(RAW_DIR, fname)
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()


def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        pd.Index(map(str, df.columns))
        .str.strip().str.lower().str.replace(" ", "_")
    )
    return df


# ---------- basic parsers ----------
def _height_to_in(v):
    try:
        ft, inch = map(int, str(v).split("-"))
        return ft * 12 + inch
    except:
        return np.nan


def _weight_to_lbs(v):
    try:
        m = re.search(r"\d+", str(v))
        return float(m.group()) if m else np.nan
    except:
        return np.nan


def _years(txt):
    try:
        return int(str(txt).split()[0])
    except:
        return np.nan


# ---------- draft string splitter ----------
_draft_year  = re.compile(r"(\d{4})")
_draft_round = re.compile(r"(?:round|rnd)\s*(\d+)|\bR(\d+)\b", re.I)
_draft_pick  = re.compile(r"(?:pick|#)\s*(\d+)", re.I)


def _parse_draft(raw):
    if not isinstance(raw, str) or not raw or "undrafted" in raw.lower():
        return np.nan, np.nan, np.nan
    year = _draft_year.search(raw)
    rnd  = _draft_round.search(raw)
    pick = _draft_pick.search(raw)
    return (
        int(year.group(1)) if year else np.nan,
        int(next(g for g in rnd.groups() if g)) if rnd else np.nan,
        int(pick.group(1)) if pick else np.nan,
    )


# ---------- pid extraction ----------
_pid_url   = re.compile(r"/player/(\d+)/")
_pid_query = re.compile(r"PlayerID=(\d+)", re.I)


def _extract_pid(url: str | float) -> str | None:
    """Return numeric Player-ID from profile_url, else None."""
    s = str(url)
    m = _pid_url.search(s) or _pid_query.search(s)
    return m.group(1) if m else None


def _fix_headshot(row) -> str:
    """
    If headshot_url is NaN / empty / silhouette → try CDN/<pid>.png
    Otherwise leave as-is.
    """
    url = str(row.get("headshot_url", "")).strip()
    pid = row.get("pid")

    if url and url != "nan" and url != SILHOUETTE_URL:
        return url  # already good

    if not pid:
        return SILHOUETTE_URL

    cdn = HEADSHOT_CDN.format(pid=pid)
    if CHECK_CDN:
        try:
            ok = requests.head(cdn, headers=HEADERS, timeout=4).status_code == 200
            return cdn if ok else SILHOUETTE_URL
        except:
            return SILHOUETTE_URL
    else:
        return cdn


# ---------- optional API back-fill ----------
def api_backfill(df: pd.DataFrame) -> pd.DataFrame:
    miss = df["height"].isna() | df["weight"].isna() | df["experience"].isna()
    todo = df[miss].copy()
    if todo.empty:
        print("API fill: nothing to fetch.")
        return df

    sess = requests.Session()
    sess.headers.update(HEADERS)
    print(f"API fill: fetching {len(todo)} players …")

    for idx, row in todo.iterrows():
        pid = row["pid"]
        if not pid:
            continue
        try:
            js = sess.get(API_URL.format(pid=pid), timeout=8).json()
            data = dict(zip(
                js["resultSets"][0]["headers"],
                js["resultSets"][0]["rowSet"][0]
            ))
            if pd.isna(df.at[idx, "height"]):
                df.at[idx, "height"] = _height_to_in(data.get("HEIGHT"))
            if pd.isna(df.at[idx, "weight"]):
                df.at[idx, "weight"] = _weight_to_lbs(data.get("WEIGHT"))
            if pd.isna(df.at[idx, "experience"]):
                df.at[idx, "experience"] = _years(data.get("SEASON_EXP"))
        except:
            pass
        time.sleep(0.2)
    return df


# ---------- Basketball-Reference-style short ID ----------
def build_ids(df: pd.DataFrame) -> pd.Series:
    prefixes = (
        df["player"].str.lower()
                     .str.replace(r"[^a-z ]", "", regex=True)
                     .str.strip()
                     .str.split(expand=True)
    )
    base = prefixes[1].fillna(prefixes[0]).str[:5] + prefixes[0].str[:2]
    rank = base.groupby(base).cumcount().add(1).astype(str).str.zfill(2)
    return (base + rank).str.ljust(9, "0")


# ────────────────────────────────────────────────────────── main ──────────
def main(merge=False, api_fill=False):
    basic    = _load("players_basic.csv")
    detailed = _load("players_detailed.csv") if merge else pd.DataFrame()
    if basic.empty:
        raise FileNotFoundError("players_basic.csv not found in data/raw")

    basic = _clean_cols(basic)
    if merge and not detailed.empty:
        detailed = _clean_cols(detailed)

        override_cols = ["headshot_url", "experience", "draft", "legacy"]
        basic = basic.drop(columns=[c for c in override_cols if c in basic.columns])

        extras = [c for c in detailed.columns if c not in basic.columns and c != "birthdate"]
        basic = basic.merge(
            detailed[["profile_url", *extras]],
            on="profile_url", how="left"
        )

    df = basic.copy()

    # ---------- ensure core columns exist ----------
    for col in ("headshot_url", "birthdate", "experience", "draft"):
        if col not in df.columns:
            df[col] = pd.NaT if col == "birthdate" else np.nan
    if "is_retired" not in df:
        df["is_retired"] = False

    # ---------- pid + headshot fix ----------
    df["pid"] = df["profile_url"].apply(_extract_pid)
    df["headshot_url"] = df.apply(_fix_headshot, axis=1)

    # ---------- tidy strings ----------
    df["player"] = df["player"].astype(str).str.strip()
    df["team"]   = df["team"].fillna("").astype(str).str.strip()
    df.loc[df["is_retired"], "team"] = "RET"
    df.loc[(~df["is_retired"]) & (df["team"] == ""), "team"] = "FA"

    # ---------- position parsing ----------
    if "position" in df:
        df["position"] = (
            df["position"].astype(str).str.upper().str.strip().replace("", np.nan)
        )
        df["position_list"]    = df["position"].str.split(r"[-/,]", regex=True)
        df["position_primary"] = df["position_list"].str[0]
        df["position_alt"]     = df["position_list"].apply(
            lambda lst: "|".join(lst[1:]) if len(lst) > 1 else ""
        )
    else:
        df["position_primary"] = np.nan
        df["position_alt"]     = ""
        df["position_list"]    = [[]] * len(df)

    # ---------- height / weight / experience ----------
    if "height" in df:
        df["height"] = df["height"].apply(_height_to_in)
    else:
        df["height"] = pd.Series([np.nan] * len(df)).apply(_height_to_in)
    if "weight" in df:
        df["weight"] = df["weight"].apply(_weight_to_lbs)
    else:
        df["weight"] = pd.Series([np.nan] * len(df)).apply(_weight_to_lbs)
    if "experience" in df:
        df["experience"] = df["experience"].apply(_years)
    else:
        df["experience"] = pd.Series([np.nan] * len(df)).apply(_years)
    df["birthdate"]  = pd.to_datetime(df["birthdate"], errors="coerce")

    # ---------- draft split ----------
    if "draft" in df.columns:
        dv = df["draft"].apply(_parse_draft).tolist()
        df[["draft_year", "draft_round", "draft_pick"]] = pd.DataFrame(dv, index=df.index)
        df["draft_status"] = np.where(df["draft_year"].isna(), "UDF", "Drafted")
        m1 = df["draft_status"].eq("Drafted") & df["draft_round"].isna()
        df.loc[m1, "draft_round"] = 1
        udf = df["draft_status"].eq("UDF")
        df.loc[udf, ["draft_year", "draft_round", "draft_pick"]] = 0
        df.drop(columns="draft", inplace=True, errors="ignore")

    for c in ("draft_year", "draft_round", "draft_pick"):
        if c in df:
            df[c] = df[c].astype("Int64")

    # ---------- defaults ----------
    df["birthdate"] = df["birthdate"].fillna(pd.Timestamp("1980-01-01"))

    # ---------- retiree prune ----------
    CORE = [
        "height", "weight", "position", "country", "birthdate",
        "draft_year", "draft_pick", "experience"
    ]
    present = [c for c in CORE if c in df.columns]
    df["missing_core"] = (
        df[present].isna() | df[present].astype(str).eq("")
    ).sum(axis=1)
    df = df[~(df["is_retired"] & (df["missing_core"] >= 4))].drop(columns="missing_core")

    # ---------- unique player_id ----------
    df["player_id"] = build_ids(df)

    # ---------- optional API back-fill ----------
    if api_fill:
        df = api_backfill(df)

    # ---------- report & save ----------
    print("\n─ Missing values per column ─")
    print(df.replace("", np.nan).isna().sum().sort_values(ascending=False).to_string())
    print("─────────────────────────────\n")

    os.makedirs(os.path.dirname(OUT_FPATH), exist_ok=True)
    try:
        df.to_csv(OUT_FPATH, index=False)
    except PermissionError:
        raise SystemExit(f"⚠️  Close {OUT_FPATH} in other apps and run again.")
    print(f"✅ saved → {OUT_FPATH}  ({len(df):,} rows)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--merge",    action="store_true", help="merge players_detailed.csv if present")
    ap.add_argument("--api-fill", action="store_true", help="back-fill gaps via Stats API")
    main(**vars(ap.parse_args()))
