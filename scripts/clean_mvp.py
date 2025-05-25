import pandas as pd
import os

def clean_mvp_csv(input_path="data/raw/mvp_raw.csv", output_path="data/processed/mvp_cleaned.csv"):
    # Load raw data
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

    # Rename the last column to player_id
    df.columns = list(df.columns[:-1]) + ['player_id']

    # Convert numeric columns
    numeric_cols = df.columns.difference(['season', 'lg', 'player', 'team', 'player_id'])
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # Parse season into start and end year
    df['season_start'] = df['season'].str[:4].astype(int)
    df['season_end'] = df['season'].str[-2:].astype(int) + 2000
    df['season_end'] = df.apply(
        lambda x: x['season_end'] if x['season_end'] >= x['season_start'] else x['season_end'] + 100,
        axis=1
    )

    # Clean team and player fields
    df['team'] = df['team'].fillna("").str.strip().replace('', 'FA')
    df['player'] = df['player'].str.strip()

            # Drop exact duplicates only (not multi-team appearances)
    df = df.drop_duplicates()

    # Save cleaned CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Cleaned MVP data saved to: {output_path}")

if __name__ == "__main__":
    clean_mvp_csv()