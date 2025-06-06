#!/usr/bin/env python3
"""
Clean “all_*_teams.csv” files in data/raw/awards/ so that each player
occupies its own row (instead of having 5 “Unnamed” columns per row).

For each file matching all_*_teams.csv, this script will:
  1. Read the raw CSV.
  2. Normalize column names to snake_case (using pandas).
  3. Rename 'tm' → 'team_rank' and drop 'voting'.
  4. Identify any column whose name starts with "unnamed" and melt
     them into a single 'player' column.
  5. Split 'player' into:
       • player_name  (e.g. "Nikola Jokić")
       • position     (e.g. "C")
  6. Split 'season' (e.g. "2024-25") into two ints:
       • season_start = 2024
       • season_end   = 2025
  7. Drop duplicates and save to data/processed/awards/<stem>_cleaned.csv.
"""

from pathlib import Path
import pandas as pd

RAW_DIR  = Path("data/raw/awards")
PROC_DIR = Path("data/processed/awards")
PROC_DIR.mkdir(parents=True, exist_ok=True)

def clean_one_team_file(input_path: Path) -> None:
    # 1) Load
    df = pd.read_csv(input_path)
    print(f"\n📥 Loaded {len(df):,} rows from {input_path.name}")

    # 2) Normalize column names to snake_case
    #    (strip, lowercase, replace spaces/punctuation with underscores)
    df.columns = (
        df.columns
          .astype(str)
          .str.strip()
          .str.lower()
          # replace any sequence of non-alphanumeric with underscore
          .str.replace(r"[^\w]+", "_", regex=True)
          # remove leading/trailing underscores
          .str.strip("_")
    )

    # 3) Rename 'tm' → 'team_rank'; drop 'voting'
    if "tm" in df.columns:
        df = df.rename(columns={"tm": "team_rank"})
    if "voting" in df.columns:
        df = df.drop(columns=["voting"])

    # 4) Find all columns whose name starts with "unnamed"
    unnamed_cols = [c for c in df.columns if c.startswith("unnamed")]
    if not unnamed_cols:
        print("   • No 'unnamed' columns found—nothing to melt.")
        return

    print(f"   • Found {len(unnamed_cols)} unnamed columns: {unnamed_cols}")

    # 5) Melt them into one 'player' column
    #    Keep id_vars = all other columns except unnamed_cols
    id_vars = [c for c in df.columns if c not in unnamed_cols]
    df_long = (
        df
        .melt(
            id_vars=id_vars,
            value_vars=unnamed_cols,
            var_name="member_rank",
            value_name="player"
        )
        # drop rows where player is NaN or blank
        .loc[lambda d: d["player"].notna() & (d["player"].astype(str).str.strip() != "")]
        .drop(columns=["member_rank"])
        .reset_index(drop=True)
    )
    print(f"   • After melting: {len(df_long):,} rows (one per player)")

    # 6) Parse 'player' into 'player_name' and 'position'
    #    We assume the last token is the position (e.g. "C", "F", "G")
    def split_name_pos(x):
        parts = x.rsplit(" ", 1)
        if len(parts) == 2 and len(parts[1]) <= 2:
            return parts[0], parts[1]
        else:
            return x, None  # fallback: put whole string in name, no position
    df_long[["player_name", "position"]] = df_long["player"].apply(
        lambda x: pd.Series(split_name_pos(x.strip()))
    )

    # 7) Add 'award' column (override or supplement existing)
    award_name = input_path.stem.lower()  # e.g. 'all_league_teams'
    df_long["award"] = award_name

    # 8) Convert 'season' → season_start & season_end
    if "season" in df_long.columns:
        # season is a string like "2024-25"
        df_long["season_start"] = (
            df_long["season"]
            .astype(str)
            .str.slice(0, 4)
            .astype(int, errors="ignore")
        )
        # If season_end is literally "25", convert to 2025
        def compute_end(x):
            txt = str(x).strip()
            if "-" in txt:
                # e.g. "2024-25" → tail is "25"
                end_part = txt.split("-")[-1]
                try:
                    end_year = int(end_part)
                    # If end_part is two digits, prepend first two digits of start
                    if len(end_part) == 2:
                        prefix = txt[:2]  # "20"
                        return int(prefix + end_part)
                    return end_year
                except:
                    return None
            return None
        df_long["season_end"] = df_long["season"].apply(compute_end)
    else:
        print("   • No 'season' column to split.")

    # 9) Trim whitespace on text fields
    for col in ["lg", "team_rank", "player", "player_name", "position", "award"]:
        if col in df_long.columns:
            df_long[col] = df_long[col].astype(str).str.strip().str.lower()

    # 10) Drop exact duplicates
    before = len(df_long)
    df_long = df_long.drop_duplicates().reset_index(drop=True)
    dropped = before - len(df_long)
    print(f"   • Dropped {dropped:,} duplicates → {len(df_long):,} rows remain")

    # 11) Save cleaned CSV
    output_path = PROC_DIR / f"{input_path.stem}_cleaned.csv"
    df_long.to_csv(output_path, index=False)
    print(f"✅ Wrote cleaned file to {output_path.name}")

if __name__ == "__main__":
    # Find every all_*_teams.csv in data/raw/awards
    pattern = "all_*_teams.csv"
    csv_files = sorted(RAW_DIR.glob(pattern))

    if not csv_files:
        print(f"⚠️  No files matching {pattern} in {RAW_DIR}")
        exit(0)

    for fpath in csv_files:
        clean_one_team_file(fpath)
