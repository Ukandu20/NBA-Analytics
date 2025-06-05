"""
utils.clean_helpers
───────────────────
Shared helpers for award cleaning.
"""

from __future__ import annotations
import pandas as pd
import re

# ──────────────────────────────────────────────────────────────────
# 1. Column-name normaliser
# ──────────────────────────────────────────────────────────────────
def normalise_cols(cols: pd.Index | list[str]) -> pd.Index:
    return (
        pd.Index(cols)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("%", "pct")
        .str.replace("/", "_")
        .str.replace(r"[^\w]", "", regex=True)   # keep a-z,0-9,_
    )

# ──────────────────────────────────────────────────────────────────
# 2. Explode 5-man team awards
# ──────────────────────────────────────────────────────────────────
TEAM_AWARDS = {
    "ALL_LEAGUE_TEAMS",
    "ALL_DEFENSE_TEAMS",
    "ALL_ROOKIE_TEAMS",
}

_POS_CODES = {"F", "G", "C", "PF", "PG", "SF", "SG"}

def _explode_team_award(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    id_cols = [
        c for c in df.columns
        if re.fullmatch(r"(Season|Lg|Voting|Tm|award)", c, flags=re.I)
    ]
    player_cols = [
        c for c in df.columns
        if re.match(r"(unnamed|player)", c, flags=re.I)
    ]
    if not player_cols:
        raise ValueError(f"No player columns found for team award {tag}")

    records: list[dict] = []
    for _, row in df.iterrows():
        base = row[id_cols].to_dict()
        if "Tm" in base:
            base["team_rank"] = base.pop("Tm")
        for col in player_cols:
            raw = row[col]
            if pd.isna(raw) or str(raw).strip() == "":
                continue
            rec = base.copy()
            txt = str(raw)
            if tag == "ALL_LEAGUE_TEAMS":
                name, _, maybe_pos = txt.rpartition(" ")
                if maybe_pos in _POS_CODES:
                    rec["player"], rec["position"] = name, maybe_pos
                else:
                    rec["player"], rec["position"] = txt, pd.NA
            else:
                rec["player"], rec["position"] = txt, pd.NA
            records.append(rec)

    return pd.DataFrame.from_records(records)

def explode_all_team_awards(df: pd.DataFrame) -> pd.DataFrame:
    chunks: list[pd.DataFrame] = []
    for tag in TEAM_AWARDS:
        mask = df["award"].str.upper() == tag
        if mask.any():
            chunks.append(_explode_team_award(df.loc[mask].copy(), tag))
            df = df.loc[~mask]
    chunks.append(df)
    return pd.concat(chunks, ignore_index=True).dropna(axis=1, how="all")
