import streamlit as st
from pathlib import Path
import pandas as pd

from team_data import load_team_data
from team_kpi import compute_kpis
from team_charts import render_charts

# ----------------------------------------------------------------------------
# Page configuration
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Team Dashboard", layout="wide", initial_sidebar_state="collapsed")
st.sidebar.title("Filters")

# ----------------------------------------------------------------------------
# Season and team selectors
# ----------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"
TEAM_DATA = DATA_DIR / "teams_cleaned.csv"

seasons = [
    "2024-25","2023-24","2022-23","2021-22",
    "2020-21","2019-20","2018-19","2017-18",
    "2016-17","2015-16",
]
season = st.sidebar.selectbox("Season", seasons, index=0)

try:
    prev_season = seasons[seasons.index(season) + 1]
except (ValueError, IndexError):
    prev_season = None

team_bios = pd.read_csv(TEAM_DATA).rename(columns=str.lower)
team_codes = sorted(team_bios["team"].unique())
team_abbr = st.sidebar.selectbox("Team", team_codes, index=0)
team_info = team_bios.loc[team_bios["team"] == team_abbr].iloc[0]
team_id = int(team_info["team_id"])
team_name = team_info["team_name"]

def render_tab(season_type: str) -> None:
    dfs = load_team_data(season, season_type)
    kpis = compute_kpis(
        dfs,
        season=season,
        season_type=season_type,
        team_id=team_id,
        team_abbr=team_abbr,
        team_name=team_name,
        prev_season=prev_season,
        team_bios=team_bios,
    )
    render_charts(dfs, kpis, season, season_type, team_abbr, team_name, prev_season)

# ----------------------------------------------------------------------------
# Render tabs
# ----------------------------------------------------------------------------

tab_reg, tab_play = st.tabs(["Regular Season", "Playoffs"])
with tab_reg:
    render_tab("regular_season")
with tab_play:
    render_tab("playoffs")