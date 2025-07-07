import streamlit as st
import pandas as pd
import altair as alt
import math

from team_chart_utils import add_home_away, with_month
# Backwards compatibility for older imports
from team_kpi import get_team_leaders  # re-exported

def render_charts(
    dfs: dict[str, pd.DataFrame],
    kpis: dict[str, any],
    season: str,
    season_type: str,
    team_abbr: str,
    team_name: str,
    prev_season: str | None
):
    # unpack needed dfs
    team_bios      = dfs["team_bios"]
    df_gen_trad    = dfs["df_gen_trad"]
    df_gen_adv     = dfs["df_gen_adv"]
    df_gen_scoring = dfs["df_gen_scoring"]
    df_box_adv     = dfs["df_boxscore_adv"]
    df_player_all  = dfs["df_player_all"]

    # prepare team boxscore
    df_box_adv = add_home_away(df_box_adv)
    team_id    = int(team_bios.query("team == @team_abbr")["team_id"].iloc[0])
    df_team_bs = df_box_adv[df_box_adv.team_id == team_id].copy()
    df_team_bs = with_month(df_team_bs)

    # header
    logo_url = team_bios.query("team == @team_abbr")["logo_url"].iloc[0]
    st.markdown(
        f"<div style='display:flex;justify-content:space-between;align-items:center'>"
        f"<h1>How good were the {team_name} in {season}?</h1>"
        f"<img src='{logo_url}' height='50'/></div>",
        unsafe_allow_html=True
    )

    # KPI cards
    cards = st.columns(5)
    metrics = [
        ("Win %",        kpis["win_pct"],    kpis["win_arrow"],    kpis["win_delta"],    kpis["win_color"],    kpis["win_rank"]),
        ("Off Rating",   kpis["off_rating"], kpis["off_arrow"],  kpis["off_delta"], kpis["off_color"], kpis["off_rank"]),
        ("Def Rating",   kpis["def_rating"], kpis["def_arrow"],  kpis["def_delta"], kpis["def_color"], kpis["def_rank"]),
        ("Net Rating",   kpis["net_rating"], kpis["net_arrow"],  kpis["net_delta"], kpis["net_color"], kpis["net_rank"]),
        ("True Shooting %", kpis["ts_pct"],    "",                 "",              "",               kpis["ts_rank"])  
    ]