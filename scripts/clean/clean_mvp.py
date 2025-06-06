#!/usr/bin/env python
"""
Clean raw MVP CSV exported from Basketball-Reference and write
data/processed/mvp_cleaned.csv
"""
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from utils.clean_helpers import normalise_cols   # noqa: E402


RAW_CSV  = ROOT / "data" / "raw"       / "mvp_raw.csv"
CLEAN_CSV = ROOT / "data" / "processed" / "mvp_cleaned.csv"


def clean_mvp_csv(input_path: Path = RAW_CSV, output_path: Path = CLEAN_CSV) -> None:
    # ── load ───────────────────────────────────────────────────────────────
    df = pd.read_csv(input_path)

    # ── normalise / rename columns ─────────────────────────────────────────
    df.columns = normalise_cols(df.columns)        # snake_case
    df.columns = list(df.columns[:-1]) + ["player_id"]   # last column → player_id

    if "tm" in df.columns:
        df.rename(columns={"tm": "team"}, inplace=True)

    # ── numeric conversion (everything except identifiers / labels) ───────
    text_cols = ["season", "lg", "player", "team", "player_id"]
    numeric_cols = df.columns.difference(text_cols)
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # ── season_start / season_end ──────────────────────────────────────────
    df["season_start"] = df["season"].str[:4].astype(int)
    df["season_end"]   = df["season_start"] + 1      # ← simple, bullet-proof

    # ── tidy player / team fields ──────────────────────────────────────────
    df["team"]   = df["team"].fillna("").str.strip().replace("", "FA")
    df["player"] = df["player"].str.strip()

    # ── drop exact duplicate rows (multi-team rows differ, so stay) ───────
    df = df.drop_duplicates()

    # ── save ───────────────────────────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Cleaned MVP data saved to: {output_path}")


if __name__ == "__main__":
    clean_mvp_csv()
