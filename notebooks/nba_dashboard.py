# app.py

import streamlit as st
import sys
from pathlib import Path

# ── PROJECT ROOT & IMPORT HELPERS ────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
# Ensure utils package is recognized
(ROOT / "utils" / "__init__.py").touch(exist_ok=True)


from utils.data_loader import (
    list_metrics, list_seasons, list_measures, load_metric_df,
    load_mvp, list_features, list_feature_seasons, load_feature_df
)

from tabs.mvp_dashboard      import render as render_mvp
from tabs.team_dashboard import render as render_team


# ── 1. Page config ───────────────────────────────────────────────────────────
st.set_page_config(page_title="NBA Analytics", layout="wide")
st.sidebar.title("Filters")

# 2.1 Season
default_player_metric = list_metrics(is_player=True)[0]
all_seasons = list_seasons(default_player_metric, is_player=True)
season      = st.sidebar.selectbox("Season", all_seasons)

# 2.2 Team
# We'll load a quick players_df with defaults so we can populate the team list
_default_measure = (
    "totals" 
    if "totals" in list_measures(default_player_metric, season, True) 
    else list_measures(default_player_metric, season, True)[0]
)
_tmp_df = load_metric_df(default_player_metric, season, _default_measure, is_player=True)
team_options = sorted(_tmp_df["team"].unique()) if not _tmp_df.empty else []
team = st.sidebar.selectbox("Team", team_options)

# 2.3 Player search (selectbox is searchable)
player_list = (
    sorted(_tmp_df[_tmp_df["team"] == team]["player"].unique())
    if not _tmp_df.empty else []
)
player = st.sidebar.selectbox("Player", player_list)

# ── 3. Advanced Filters ─────────────────────────────────────────────────────
with st.sidebar.expander("Advanced Filters", expanded=False):
    st.markdown("#### Player Metrics")
    player_metric = st.selectbox("Metric", list_metrics(is_player=True),
                                 index=0)
    measures = list_measures(player_metric, season, is_player=True)
    default_idx = measures.index("totals") if "totals" in measures else 0
    player_measure = st.selectbox("Measure", measures, index=default_idx)

    st.markdown("#### Team Metrics")
    team_metric = st.selectbox("Metric", list_metrics(is_player=False),
                               index=0)
    team_measures = list_measures(team_metric, season, is_player=False)
    default_idx = team_measures.index("totals") if "totals" in team_measures else 0
    team_measure = st.selectbox("Measure", team_measures, index=default_idx)

    st.markdown("#### Feature Store")
    feature = st.selectbox("Feature", list_features(), index=0)
    fs_seasons = list_feature_seasons(feature)
    feature_season = (
        season if season in fs_seasons
        else st.selectbox("Feature Season", fs_seasons)
    )

# ── 4. Load Data ─────────────────────────────────────────────────────────────
players_df = load_metric_df(player_metric, season, player_measure, is_player=True)
teams_df   = load_metric_df(team_metric,   season, team_measure,   is_player=False)
fs_df      = load_feature_df(feature, feature_season)
mvp_df     = load_mvp()

# ── 5. Tabs ─────────────────────────────────────────────────────────────────
# Create the tabs
tab1, tab2, tab3, tab4 = st.tabs(
    ["🏆 MVP","📊 Team","📈 Seasonal","👤 Player"]
)

with tab1:
    render_mvp()

with tab2:
    render_team(teams_df, fs_df, season, team, team_metric)

"""with tab_season:
    st.header(f"League-Wide – {player_metric.title()} ({season})")
    df = players_df[players_df["season"] == season]
    # default metric column—replace 'pts' with your preferred stat
    summary = df.groupby("team")["pts"].mean().sort_values()
    st.bar_chart(summary)

with tab_player:
    st.header(f"{player} – {player_metric.title()} in {season}")
    df = players_df[
        (players_df["team"]   == team) &
        (players_df["player"] == player) &
        (players_df["season"] == season)
    ]
    # replace 'game_date'/'pts' with your actual columns
    st.line_chart(df.set_index("game_date")["pts"])
"""