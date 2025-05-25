# scripts/clean_player_profiles.py

import pandas as pd
import os

def clean_player_profiles_csv(
    input_path="data/raw/player_profiles.csv",
    output_path="data/processed/player_profiles_cleaned.csv",
    default_headshot="utils/images/john_doe_headshot.png"
):
    """
    Cleans the scraped player profile data:
      - Fills missing headshot_url with a default image
      - Parses experience into numeric years
      - Converts birthdate into datetime
      - Saves cleaned CSV
    """
    # Load raw profiles
    df = pd.read_csv(input_path)

    # Normalize column names
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(" ", "_")
    )

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Fill missing or blank headshots
    df['headshot_url'] = df.get('headshot_url', "") \
                           .fillna("") \
                           .astype(str) \
                           .str.strip()
    df.loc[df['headshot_url'] == "", 'headshot_url'] = default_headshot

    # Parse experience (e.g., "4 Years" → 4.0)
    if 'experience' in df.columns:
        df['experience_years'] = (
            df['experience']
              .astype(str)
              .str.extract(r'(\d+)')[0]
              .astype(float)
        )

    # Parse birthdate to datetime
    if 'birthdate' in df.columns:
        df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')

    # Save cleaned profiles
    df.to_csv(output_path, index=False)
    print(f"✅ Cleaned player profiles saved to: {output_path}")

if __name__ == "__main__":
    clean_player_profiles_csv()
