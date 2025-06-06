# utils/numeric_helpers.py

import pandas as pd

def coerce_all_numeric(df: pd.DataFrame, exclude_cols: list[str]) -> pd.DataFrame:
    """
    Convert every column in df to numeric, except those listed in exclude_cols.
    Non-parseable values become NaN.
    Returns the same DataFrame with conversions applied in-place.
    """
    to_numeric = [c for c in df.columns if c not in exclude_cols]
    df[to_numeric] = df[to_numeric].apply(pd.to_numeric, errors="coerce")
    return df
