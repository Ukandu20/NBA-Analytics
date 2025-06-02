# utils/clean_helpers.py
import re
import pandas as pd

def normalise_cols(cols: pd.Index) -> pd.Index:
    """
    • trims leading/trailing spaces
    • lower-cases
    • replaces internal spaces with _
    • converts '%' → 'pct'
    • converts '/' → '_'
    • collapses consecutive underscores
    """
    cols = (
        cols.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("%", "_pct", regex=False)
            .str.replace("/", "_",  regex=False)
            .str.replace("__+", "_", regex=True)
    )
    return cols
