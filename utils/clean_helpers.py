# utils/clean_helpers.py
import re
import pandas as pd

def normalise_cols(cols: pd.Index) -> pd.Index:
    """
    • Strip leading/trailing whitespace
    • Lowercase
    • Replace any sequence of non‐alphanumeric characters with "_"
    • Replace "%" → "_pct" before or after as needed
    • Collapse multiple underscores into one
    • Strip leading/trailing underscores
    """
    # 1) Trim & lowercase
    cols = cols.str.strip().str.lower()
    # 2) Replace "%" with "_pct"
    cols = cols.str.replace("%", "_pct", regex=False)
    # 3) Replace "/" with "_"
    cols = cols.str.replace("/", "_", regex=False)
    # 4) Replace any remaining non‐alphanumeric with "_"
    cols = cols.str.replace(r"[^\w]+", "_", regex=True)
    # 5) Collapse multiple underscores → single "_"
    cols = cols.str.replace(r"_+", "_", regex=True)
    # 6) Strip leading/trailing underscores
    cols = cols.str.replace(r"^_|_$", "", regex=True)
    return cols



