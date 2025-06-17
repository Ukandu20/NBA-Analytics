# utils/clean_helpers_v2.py
from __future__ import annotations
import re
import pandas as pd
from typing import Iterable

_UNDERSCORE = re.compile(r"_+")

def _dedup(names: Iterable[str]) -> list[str]:
    """Ensure column-name uniqueness by suffixing _1, _2 … when needed."""
    seen: dict[str, int] = {}
    out  = []
    for n in names:
        base = n
        idx  = seen.get(base, 0)
        if idx:
            n = f"{base}_{idx}"
        out.append(n)
        seen[base] = idx + 1
    return out

def normalise_cols(cols: pd.Index, *, dedup: bool = True) -> pd.Index:
    """
    Canonicalise column names.

    Parameters
    ----------
    cols : pd.Index
    dedup : bool
        Ensure uniqueness by suffixing "_1", "_2", …

    Returns
    -------
    pd.Index
    """
    cols = (
        pd.Index(cols.astype(str))
            .str.strip()
            .str.lower()
            .str.replace("%", "_pct", regex=False)
            .str.replace("/", "_",  regex=False)
            .str.replace(r"[^\w]+", "_", regex=True)      # keep Unicode letters
            .str.replace(r"_+", "_",          regex=True)        # ← collapse runs
            .str.replace(r"^_|_$", "",        regex=True)  
    )
    cols = pd.Index(cols)
    if dedup:
        cols = pd.Index(_dedup(cols))
    return cols
