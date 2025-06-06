# utils/award_helpers.py

import pandas as pd

def find_unnamed_columns(cols: list[str]) -> list[str]:
    """
    Return a list of column names that start with "unnamed" and contain at least one digit.
    E.g. "unnamed_4", "unnamed: 5", "unnamed4", etc.
    """
    return [c for c in cols if c.lower().startswith("unnamed") and any(ch.isdigit() for ch in c)]

def melt_unnamed_columns(df: pd.DataFrame, unnamed_cols: list[str], id_vars: list[str], value_name="player") -> pd.DataFrame:
    """
    Melt all columns in unnamed_cols into a single column named value_name.
    Keeps columns in id_vars intact. Drops rows where the melted value is null or blank.
    Returns a new, re-indexed DataFrame.
    """
    if not unnamed_cols:
        return df.copy()

    melted = (
        df.melt(
            id_vars=id_vars,
            value_vars=unnamed_cols,
            var_name="_member_rank",
            value_name=value_name
        )
        .drop(columns=["_member_rank"])
        .loc[lambda d: d[value_name].notna() & (d[value_name].astype(str).str.strip() != "")]
        .reset_index(drop=True)
    )
    return melted
