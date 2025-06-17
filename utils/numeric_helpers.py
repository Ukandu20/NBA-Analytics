from __future__ import annotations

import re, warnings
import pandas as pd
from typing import Sequence

# ── constants ───────────────────────────────────────────────────────────────
_PCT_STR_RE  = re.compile(r"^-?\d+(?:\.\d+)?%$")          # strings like “47.8%”
PCT_NAME_RE  = re.compile(r"(pct|percentage)$", re.I)     # columns ending with _pct
WARN_THRESH  = 0.25                                       # >25 % values lost → warn

# ── helpers ─────────────────────────────────────────────────────────────────
def _convert_pct_strings(s: pd.Series) -> pd.Series:
    """'47.8%' → 0.478 ; leaves other values untouched."""
    mask = s.astype(str).str.contains(_PCT_STR_RE, na=False, regex=True)
    if mask.any():
        s = s.copy()
        s[mask] = s[mask].str.rstrip("%").astype(float) / 100.0
    return s

def _scale_pct_numeric(s: pd.Series, col_name: str) -> pd.Series:
    """
    If a numeric column looks like a percentage (name ends with _pct etc.)
    **and** any value is >1, assume it’s 0-100 scale and divide by 100.
    """
    if PCT_NAME_RE.search(col_name) and pd.api.types.is_numeric_dtype(s):
        if s.max(skipna=True) > 1.0:
            return s / 100.0
    return s

# ── public API ──────────────────────────────────────────────────────────────
def coerce_all_numeric(
    df: pd.DataFrame,
    exclude_cols: Sequence[str] = (),
    *,
    warn_on_loss: bool = True,
) -> pd.DataFrame:
    """
    • Convert everything *not* in `exclude_cols` to numeric  
    • Parse '%' strings **and** rescale numeric % columns to 0-1  
    • Warn if > 25 % of values turn into NaN after coercion
    """
    for col in df.columns:
        if col in exclude_cols:
            continue

        # handle % strings first
        series = _convert_pct_strings(df[col])

        # attempt numeric coercion
        before_na = series.isna().sum()
        series = pd.to_numeric(series, errors="coerce")

        # scale numeric percentages if needed
        series = _scale_pct_numeric(series, col)

        # warn on massive data loss
        if warn_on_loss:
            new_na = series.isna().sum() - before_na
            if len(series) and new_na / len(series) > WARN_THRESH:
                warnings.warn(
                    f"{col}: {new_na}/{len(series)} values became NaN during coercion."
                )

        df[col] = series  # in-place assignment

    return df
