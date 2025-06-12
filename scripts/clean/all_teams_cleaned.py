"""
Clean NBA franchise list (basic + detailed) → data/processed/teams_cleaned.csv
----------------------------------------------------------------------------
Usage
  python scripts/clean_teams.py            # basic only
  python scripts/clean_teams.py --merge    # merge teams_detailed.csv if present
"""
import os
import argparse
import numpy as np
import pandas as pd

RAW_DIR   = "data/raw"
OUT_FPATH = "data/processed/teams_cleaned.csv"


# ────────────────────────────── helpers ────────────────────────────────
def _load(name: str) -> pd.DataFrame:
    p = os.path.join(RAW_DIR, name)
    return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()


def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        pd.Index(map(str, df.columns))
          .str.strip()
          .str.lower()
    )
    return df


# ─────────────────────────────── main ──────────────────────────────────
def main(merge: bool):
    basic    = _load("teams_basic.csv")
    detailed = _load("teams_detailed.csv") if merge else pd.DataFrame()

    if basic.empty:
        raise FileNotFoundError("teams_basic.csv not found in data/raw")

    basic    = _clean_cols(basic)
    detailed = _clean_cols(detailed) if not detailed.empty else detailed

    # ── merge extras ────────────────────────────────────────────────────
    if not detailed.empty:
        keep = [
            c for c in detailed.columns
            if c not in basic.columns
            or c in ("city", "state", "arena", "capacity", "owner",
                     "head_coach", "conferencename", "divisionname")
        ]
        basic = basic.merge(
            detailed[["team_id", *keep]],
            on="team_id",
            how="left",
        )

    df = basic.copy()

    # ── drop 'state' (rarely populated) ────────────────────────────────
    df = df.drop(columns="state", errors="ignore")

    # ── tidy text fields ────────────────────────────────────────────────
    df["team_name"]  = df["team_name"].astype(str).str.strip()
    df["nickname"]   = df["nickname"].fillna("").astype(str).str.strip()
    df["short_code"] = df["short_code"].fillna("").astype(str).str.upper()

    if "short_code" in df.columns:
        df.rename(columns={"short_code": "team"}, inplace=True)
    
    # ── numeric coercions ───────────────────────────────────────────────
    for col in ("team_id", "first_season", "last_season", "capacity"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "is_active" in df.columns:
        df["is_active"] = df["is_active"].astype(bool)

    # ── default missing capacity → 0  (Int64) ───────────────────────────
    if "capacity" in df.columns:
        df["capacity"] = df["capacity"].fillna(0).astype("Int64")

    # ── *** DROP inactive franchises *** ────────────────────────────────
    if "is_active" in df.columns:
        active_before = len(df)
        df = df[df["is_active"]]
        print(f"Dropped {active_before - len(df)} inactive franchises.\n")

    df = df.drop_duplicates()

    # ── missing-values report ───────────────────────────────────────────
    miss = (
        df.replace("", np.nan)
          .isna()
          .sum()
          .sort_values(ascending=False)
    )
    print("─ Missing values per column ─")
    print(miss.to_string())
    print("─────────────────────────────\n")

    # ── save ────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUT_FPATH), exist_ok=True)
    df.to_csv(OUT_FPATH, index=False)
    print(f"✅ saved → {OUT_FPATH}  ({len(df):,} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--merge", action="store_true",
        help="merge teams_detailed.csv if present",
    )
    main(parser.parse_args().merge)
