# utils/data_loader.py

import os
import glob
import pandas as pd
import streamlit as st
from pathlib import Path

# ── Base paths ───────────────────────────────────────────────────────────────
BASE_PLAYER = Path("data/processed/player_stats")
BASE_TEAM   = Path("data/processed/team_stats")
BASE_MVP    = Path("data/processed/mvp")
BASE_FS     = Path("data/processed/feature_store")

# ── Player & Team metrics ────────────────────────────────────────────────────
@st.cache_data
def list_metrics(is_player: bool = True) -> list[str]:
    base = BASE_PLAYER if is_player else BASE_TEAM
    return sorted(d.name for d in base.iterdir() if d.is_dir())

@st.cache_data
def list_seasons(metric: str, is_player: bool = True) -> list[str]:
    base = (BASE_PLAYER if is_player else BASE_TEAM) / metric
    return sorted((d.name for d in base.iterdir() if d.is_dir()), reverse=True)

@st.cache_data
def list_measures(metric: str, season: str, is_player: bool = True) -> list[str]:
    base = (BASE_PLAYER if is_player else BASE_TEAM) / metric / season
    return sorted(d.name for d in base.iterdir() if d.is_dir())

@st.cache_data
def load_metric_df(
    metric: str,
    season: str,
    measure: str,
    is_player: bool = True
) -> pd.DataFrame:
    """
    Recursively loads all CSVs under:
      player_stats/<metric>/<season>/<measure>/**.csv
    or
      team_stats/<metric>/<season>/<measure>/**.csv
    """
    base = (BASE_PLAYER if is_player else BASE_TEAM) / metric / season / measure
    pattern = str(base / "**" / "*.csv")
    files   = glob.glob(pattern, recursive=True)
    if not files:
        st.warning(f"No CSVs found for {metric}/{season}/{measure}")
        return pd.DataFrame()

    df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    df["season"] = season
    for col in ("player","team","team_id","team_name"):
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df

# ── MVP ─────────────────────────────────────────────────────────────────────
@st.cache_data
def load_mvp() -> pd.DataFrame:
    return pd.read_csv(BASE_MVP / "mvp_cleaned.csv")

# ── Feature Store ───────────────────────────────────────────────────────────
@st.cache_data
def list_features() -> list[str]:
    return sorted(d.name for d in BASE_FS.iterdir() if d.is_dir())

@st.cache_data
def list_feature_seasons(feature: str) -> list[str]:
    path = BASE_FS / feature
    return sorted((d.name for d in path.iterdir() if d.is_dir()), reverse=True)

@st.cache_data
def load_feature_df(feature: str, season: str) -> pd.DataFrame:
    pattern = str((BASE_FS / feature / season / "**" / "*.csv"))
    files   = glob.glob(pattern, recursive=True)
    if not files:
        st.warning(f"No feature_store CSVs for {feature}/{season}")
        return pd.DataFrame()
    df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    df["season"] = season
    if "team" in df.columns:
        df["team"] = df["team"].astype(str)
    return df
