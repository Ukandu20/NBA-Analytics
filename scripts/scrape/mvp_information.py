# scripts/save_mvp_data.py

import pandas as pd
from io import StringIO
import os

def save_mvp_csv():
    input_txt_path = "data/raw/awards/mvp.txt"  # Adjust path if file is elsewhere

    # Read text content from file
    with open(input_txt_path, 'r', encoding='utf-8') as file:
        raw_data = file.read()

    # Find where the actual CSV content starts (ignore leading info)
    start_index = raw_data.find("Season,")
    if start_index == -1:
        print("❌ 'Season,' header not found in the text file.")
        return

    csv_content = raw_data[start_index:].strip()

    # Convert text to DataFrame
    df = pd.read_csv(StringIO(csv_content))

    # Ensure output directory exists
    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)

    # Save as CSV
    output_path = os.path.join(output_dir, "mvp_raw.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ MVP data saved to {output_path}")

if __name__ == "__main__":
    save_mvp_csv()
