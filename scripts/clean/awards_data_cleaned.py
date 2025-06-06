#!/usr/bin/env python
"""
Clean every raw award CSV in data/raw/awards/ and write
a cleaned, long‐format version to data/processed/awards/<stem>_cleaned.csv.

Special handling:
  • If a CSV has columns like "Unnamed: 4", "Unnamed: 5", …, these
    are treated as “extra player” columns. We melt them into a single
    'player' column so each player occupies its own row.

Output for each file:
  data/processed/awards/<stem>_cleaned.csv
"""
from pathlib import Path
import sys
import re

import pandas as pd

# ── PROJECT ROOT & IMPORT HELPERS ────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))
# Ensure utils package is recognized
(ROOT / "utils" / "__init__.py").touch(exist_ok=True)

from utils.clean_helpers import normalise_cols

# ── RAW / PROCESSED DIRECTORIES ─────────────────────────────────────────────
RAW_DIR  = ROOT / "data" / "raw" / "awards"
PROC_DIR = ROOT / "data" / "processed" / "awards"

# Ensure the processed/awards directory exists
PROC_DIR.mkdir(parents=True, exist_ok=True)


def clean_award_csv(input_path: Path, output_path: Path, award_name: str) -> None:
    """
    1. Read a single raw award CSV (input_path)
    2. Normalize & rename columns
    3. Detect any “Unnamed: …” columns and melt them into a single 'player' column
    4. Add an "award" column (award_name)
    5. Convert numeric columns (excluding award, player, etc.)
    6. Split 'season' into season_start/season_end
    7. Tidy text fields (team, player)
    8. Drop exact duplicates
    9. Write cleaned CSV to output_path
    """

    # ── 1) CHECK INPUT ────────────────────────────────────────────────────────
    if not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    # ── 2) LOAD ───────────────────────────────────────────────────────────────
    df = pd.read_csv(input_path)
    print(f"\n📥 Loaded {len(df)} rows from '{input_path.name}'.")

    # ── 3) NORMALIZE & RENAME EXISTING COLUMNS ─────────────────────────────────
    df.columns = normalise_cols(df.columns)

    # Rename 'tm' → 'team' if present
    if "tm" in df.columns:
        df.rename(columns={"tm": "team"}, inplace=True)

    # If a “9999” column exists (as in some previous MVP code), rename it to 'player_id'
    if "9999" in df.columns:
        df.rename(columns={"9999": "player_id"}, inplace=True)

    # Drop any unwanted columns like 'voting' if present
    df = df.drop(columns={"voting"}, errors="ignore")

    # ── 4) HANDLE MULTIPLE “Unnamed: …” COLUMNS ───────────────────────────────
    # Find all columns whose name matches r"^unnamed:\s*\d+$" (case‐insensitive).
    unnamed_pattern = re.compile(r"^unnamed:\s*\d+$", flags=re.IGNORECASE)
    unnamed_cols = [c for c in df.columns if unnamed_pattern.match(c)]

    if unnamed_cols:
        print(f"🔍 Found {len(unnamed_cols)} Unnamed player‐columns: {unnamed_cols}")

        # We assume the other columns (besides unnamed_cols) should be kept as-is.
        other_cols = [c for c in df.columns if c not in unnamed_cols]

        # Melt: each “Unnamed: X” becomes its own row under a new 'player' column.
        df = (
            df
            .melt(
                id_vars=other_cols,
                value_vars=unnamed_cols,
                var_name="member_rank",
                value_name="player"
            )
            .drop(columns=["member_rank"])  # we don’t need the original “Unnamed” label
            .loc[lambda d: d["player"].notna()]  # drop any rows where player was NaN
            .reset_index(drop=True)
        )
        print(f"🔄 After melting, {len(df)} total rows (one per player).")
    else:
        # If there are no “Unnamed: X” columns, ensure at least one 'player' column exists
        if "player" not in df.columns:
            # It's possible the CSV is already single-player-per-row. We trust that.
            print("ℹ️ No 'player' column found and no Unnamed columns. Leaving as-is.")

    # ── 5) ADD AWARD NAME COLUMN ───────────────────────────────────────────────
    df["award"] = award_name

    # ── 6) NUMERIC CONVERSION ─────────────────────────────────────────────────
    # Treat the following as text, never coerce to numeric:
    text_cols = ["season", "lg", "player", "team", "player_id", "award"]
    existing_text_cols = [c for c in text_cols if c in df.columns]
    numeric_cols = df.columns.difference(existing_text_cols)

    # Convert all other columns to numeric (NaN if not parseable)
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    print(f"🔢 Converted to numeric (excluding text cols): {list(numeric_cols)}")

    # ── 7) SPLIT 'season' ─────────────────────────────────────────────────────
    if "season" in df.columns:
        # Handle "YYYY-YY" or "YYYY–YY" (en dash) by extracting the first 4 digits
        df["season_start"] = df["season"].str.extract(r"^(\d{4})", expand=False).astype(int)
        df["season_end"] = df["season_start"] + 1
    else:
        print("⚠️ 'season' column missing; skipping season_start/season_end.")

    # ── 8) TIDY TEXT FIELDS ────────────────────────────────────────────────────
    if "team" in df.columns:
        df["team"] = df["team"].fillna("").str.strip().replace("", "FA")
    if "player" in df.columns:
        df["player"] = df["player"].str.strip()

    # ── 9) DROP EXACT DUPLICATES ───────────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    print(f"🗑️ Dropped {dropped} exact duplicate rows (if any). Total now: {len(df)}.")

    # ── 10) SAVE CLEANED CSV ───────────────────────────────────────────────────
    try:
        df.to_csv(output_path, index=False)
        print(f"✅ Saved cleaned data to: {output_path.name}")
    except PermissionError as e:
        raise PermissionError(
            f"Cannot write to '{output_path}'. Is it open or read-only?"
        ) from e


if __name__ == "__main__":
    # ── LOOP OVER EVERY CSV IN RAW_DIR ────────────────────────────────────────
    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ No CSV files found in {RAW_DIR}. Nothing to clean.")
        sys.exit(0)

    for input_path in csv_files:
        stem = input_path.stem.lower()

        # ── SKIP any “all_*_teams” files ───────────────────────────────────
        # If the filename stem starts with "all_" and ends with "_teams",
        # we skip it entirely:
        if stem.startswith("all_") and stem.endswith("_teams"):
            print(f"⚠️ Skipping all_*teams file: '{input_path.name}'")
            continue

        # Derive award_name from the filename stem (e.g. "roty", "mvp", etc.)
        award_name = stem

        # Output filename: "<stem>_cleaned.csv"
        output_name = f"{stem}_cleaned.csv"
        output_path = PROC_DIR / output_name

        print(f"\n🔄 Cleaning '{input_path.name}' → '{output_name}' (award='{award_name}')")
        clean_award_csv(
            input_path=input_path,
            output_path=output_path,
            award_name=award_name
        )
