# scripts/clean_player_stats_all.py

import pandas as pd
import os

file_paths = [
    "data/raw/player_stats/adj_shooting/2025.csv",
    "data/raw/player_stats/shooting/2025.csv",
    "data/raw/player_stats/advanced/2025.csv",
    "data/raw/player_stats/per_36/2025.csv",
    "data/raw/player_stats/per_100/2025.csv",
    "data/raw/player_stats/per_game/2025.csv",
    "data/raw/player_stats/totals/2025.csv",
    "data/raw/player_stats/play_by_play/2025.csv",
]

def clean_player_stats_csv(input_path, output_path):
    try:
        df = pd.read_csv(input_path)

        # Normalize column names
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("%", "pct")
            .str.replace("/", "_")
            .str.replace("tm", "team")
        )

        # Rename last column to player_id
        df.columns = list(df.columns[:-1]) + ['player_id']

        # Clean text fields
        df['player'] = df['player'].astype(str).str.strip()
        if 'team' in df.columns:
            df['team'] = df['team'].fillna('').astype(str).str.strip().replace('', 'FA')

        # Convert numeric columns (excluding identifier columns)
        exclude_cols = ['player', 'team', 'pos', 'season', 'player_id']
        for col in df.columns:
            if col not in exclude_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop exact duplicates only (not multi-team appearances)
        df = df.drop_duplicates()

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save cleaned data
        df.to_csv(output_path, index=False)
        print(f"✅ Saved {output_path}")

    except Exception as e:
        print(f"❌ Failed to process {input_path}: {e}")

def run_cleaning():
    for input_path in file_paths:
        stat_type = input_path.split("/")[3]
        year = os.path.splitext(os.path.basename(input_path))[0]
        output_path = f"data/processed/player_stats/{stat_type}/{year}.csv"
        clean_player_stats_csv(input_path, output_path)

if __name__ == "__main__":
    run_cleaning()
