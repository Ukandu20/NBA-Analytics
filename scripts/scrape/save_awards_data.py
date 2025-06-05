#!/usr/bin/env python3
"""
Parse every awards .txt in data/external/awards/ → data/raw/awards/<AWARD>.csv
and also concat into data/raw/awards/awards_raw.csv (long format).
"""
import os, glob
import pandas as pd
from io import StringIO

IN_DIR  = "data/external/awards"
OUT_DIR = "data/raw/awards"
OUT_LONG = os.path.join(OUT_DIR, "awards_raw.csv")

def parse_one(txt_path: str) -> pd.DataFrame:
    award = os.path.splitext(os.path.basename(txt_path))[0]  # “mvp”, “roty” …
    with open(txt_path, encoding="utf-8") as f:
        txt = f.read()
    start = txt.find("Season,")
    if start == -1:
        raise ValueError(f"Header not found in {txt_path}")
    df = pd.read_csv(StringIO(txt[start:].strip()))
    df["award"] = award.lower()
    return df

def main():
    os.makedirs(OUT_DIR, exist_ok=True)             # ← NEW

    all_frames = []
    for txt in glob.glob(os.path.join(IN_DIR, "*.txt")):
        df = parse_one(txt)

        # correct per-award filename
        fname    = os.path.basename(txt).replace(".txt", ".csv")
        out_csv  = os.path.join(OUT_DIR, fname)

        df.to_csv(out_csv, index=False)
        print("✅ saved", out_csv)
        all_frames.append(df)

    if not all_frames:                              # safety guard
        print("⚠️  No .txt files found in", IN_DIR)
        return

    # union → long raw table
    long = pd.concat(all_frames, ignore_index=True)
    long.to_csv(OUT_LONG, index=False)
    print("✅ long table →", OUT_LONG)

if __name__ == "__main__":
    main()
