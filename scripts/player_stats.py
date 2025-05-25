# scripts/save_player_stats_csv.py

import pandas as pd
from io import StringIO
import os

file_paths = [
    "data/raw/player_stats/adj_shooting/2025.txt",
    "data/raw/player_stats/shooting/2025.txt",
    "data/raw/player_stats/advanced/2025.txt",
    "data/raw/player_stats/per_36/2025.txt",
    "data/raw/player_stats/per_100/2025.txt",
    "data/raw/player_stats/per_game/2025.txt",
    "data/raw/player_stats/totals/2025.txt",
    "data/raw/player_stats/play_by_play/2025.txt",
]

def save_player_stats_csv():
    for input_txt_path in file_paths:
        try:
            with open(input_txt_path, 'r', encoding='utf-8') as file:
                raw_data = file.read()

            # Find where the actual CSV content starts (ignore leading junk)
            start_index = raw_data.find("Player,")
            if start_index == -1:
                print(f"❌ 'Player,' header not found in: {input_txt_path}")
                continue

            csv_content = raw_data[start_index:].strip()
            df = pd.read_csv(StringIO(csv_content))

            # Extract stat type and year from path
            parts = input_txt_path.split('/')
            stat_type = parts[3]
            year = os.path.splitext(parts[4])[0]

            # Save as .csv in same directory
            output_dir = f"data/raw/player_stats/{stat_type}"
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{year}.csv")

            df.to_csv(output_path, index=False)
            print(f"✅ Saved {stat_type}/{year}.csv")

        except Exception as e:
            print(f"❌ Failed to process {input_txt_path}: {e}")

if __name__ == "__main__":
    save_player_stats_csv()
