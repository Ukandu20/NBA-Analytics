import pandas as pd
import os

def clean_players_csv(input_path="data/raw/players_raw.csv", output_path="data/processed/players_cleaned.csv"):
    df = pd.read_csv(input_path)

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # Convert height (e.g., "6-10" → 82 inches)
    if 'height' in df.columns:
        def height_to_inches(h):
            try:
                feet, inches = map(int, h.split('-'))
                return feet * 12 + inches
            except:
                return None
        df['height_in'] = df['height'].apply(height_to_inches)

    # Convert weight (e.g., "240 lbs" → 240)
    if 'weight' in df.columns:
        df['weight_lbs'] = df['weight'].str.extract(r'(\d+)').astype(float)

    # Normalize position (e.g., "G-F" → "G")
    if 'position' in df.columns:
        df['position'] = df['position'].str.strip().str.upper().str.split('-').str[0]

    # Clean and standardize number field
    if 'number' in df.columns:
        df['number'] = df['number'].fillna("").astype(str).str.strip().replace('', 'FA')

    # Clean and standardize last_attended field
    if 'last_attended' in df.columns:
        df['last_attended'] = df['last_attended'].fillna("").astype(str).str.strip()

    # Clean other string fields
    df['player'] = df['player'].str.strip()
    df['team'] = df['team'].fillna("").str.strip().replace('', 'FA')

    # Generate player_id from name
    def generate_player_id(name):
        try:
            parts = name.lower().split()
            last = parts[-1]
            first = parts[0]
            return (last[:5] + first[:2] + "01").ljust(9, '0')
        except:
            return None

    df['player_id'] = df['player'].apply(generate_player_id)

    # Convert numeric columns
    exclude_cols = ['player', 'team', 'position', 'last_attended', 'country', 'player_id', 'number']
    for col in df.columns:
        if col not in exclude_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop exact duplicates
    df = df.drop_duplicates()

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Cleaned player data saved to: {output_path}")

if __name__ == "__main__":
    clean_players_csv()
