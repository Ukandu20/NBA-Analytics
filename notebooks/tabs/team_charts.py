import streamlit as st
import pandas as pd
import altair as alt
import math

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
_SUP_MAP = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")
def to_superscript(n: int) -> str:
    return str(n).translate(_SUP_MAP)

def rank_color(rank: int, total_teams: int = 30) -> str:
    third = total_teams // 3
    if rank <= third:
        return "green"
    elif rank <= 2*third:
        return "orange"
    else:
        return "red"

@st.cache_data
def parse_matchup(m: str) -> tuple[str, str]:
    m = m.replace("vs.", "vs").replace(" @ ", "@").replace(" vs ", "vs").strip()
    if "@" in m:
        away, home = m.split("@")
    else:
        home, away = m.split("vs")
    return away.strip(), home.strip()

@st.cache_data
def add_home_away(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[["away_team","home_team"]] = df["matchup"].apply(parse_matchup).tolist()
    df["opp_team"] = df.apply(lambda r: r.away_team if r.home_team == r.team else r.home_team, axis=1)
    return df

@st.cache_data
def with_month(df: pd.DataFrame, date_col: str = "game_date") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["month"] = df[date_col].dt.month
    months = {1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MAY",6:"JUN",7:"JUL",8:"AUG",9:"SEP",10:"OCT",11:"NOV",12:"DEC"}
    df["month_name"] = df["month"].map(months)
    df["month"] = pd.Categorical(df["month"], categories=list(months.keys()))
    return df


def get_team_leaders(
    df_player_all: pd.DataFrame,
    team_id: int,
    *,
    min_games: int | None = None,
    season_type: str = "regular",
) -> dict[str, pd.DataFrame]:
    if min_games is None:
        min_games = 4 if season_type == "playoffs" else 50
    # league-level df
    league_games = (
        df_player_all
        .groupby(["player_id","player","headshot_url"])["game_id"]
        .nunique().reset_index(name="games")
    )
    league_avgs = (
        df_player_all
        .groupby(["player_id","player","headshot_url"])[["pts","reb","ast"]]
        .mean().reset_index()
    )
    league_df = league_avgs.merge(league_games, on=["player_id","player","headshot_url"]) \
        .query("games >= @min_games")
    for stat in ["pts","reb","ast"]:
        league_df[f"{stat}_rank"] = league_df[stat].rank(method="min", ascending=False).astype(int)
    eligible = len(league_df)

    # team-level df
    team_df = df_player_all[df_player_all.team_id == team_id]
    games   = team_df.groupby(["player_id","player","headshot_url"])["game_id"].nunique().reset_index(name="games")
    avgs    = team_df.groupby(["player_id","player","headshot_url"])[["pts","reb","ast"]].mean().reset_index()
    team_stats = avgs.merge(games, on=["player_id","player","headshot_url"]) \
        .query("games >= @min_games")
    team_stats = team_stats.merge(
        league_df[["player_id","pts_rank","reb_rank","ast_rank"]],
        on="player_id", how="left"
    )

    leaders: dict[str, pd.DataFrame] = {}
    for stat in ["pts","reb","ast"]:
        cols = ["player","headshot_url",stat,f"{stat}_rank"]
        df3 = (
            team_stats.nlargest(3, stat)[cols]
                       .rename(columns={f"{stat}_rank":"rank"})
                       .reset_index(drop=True)
        )
        df3["total_players"] = eligible
        leaders[stat] = df3
    return leaders


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
    for col, (title, value, arr, delta, color, rank) in zip(cards, metrics):
        delta_text = f" {arr}{delta}" if delta else ""
        col.markdown(
            f"<div style='padding:1rem;border:1px solid #555;border-radius:8px'>"
            f"<h4 style='margin:0;color:#ddd'>{title}</h4>"
            f"<div style='font-size:2rem;color:#0a7dfa;font-weight:bold'>{round(value,1)}{delta_text}</div>"
            f"<div style='color:{color}'>Rank: {rank}</div></div>",
            unsafe_allow_html=True
        )

    # Player leaders panels
    leaders = kpis["leaders"]
    leader_info = [("Scoring Leaders","PPG","pts"),("Rebound Leaders","RPG","reb"),("Assist Leaders","APG","ast")]
    lead_cols = st.columns(3)
    for col, (title, suffix, key) in zip(lead_cols, leader_info):
        df3 = leaders[key]
        html = f"<div style='padding:1rem;border:1px solid #555;border-radius:8px'><h4 style='color:#fff'>{title}</h4>"
        for i,row in df3.iterrows():
            html += (
                f"<div style='display:flex;align-items:center;margin-bottom:.5rem'>"
                f"<img src='{row['headshot_url']}' width='32' height='32' style='border-radius:50%;margin-right:.5rem'/>"
                f"{i+1}. {row['player']} — {round(row[key],1)} {suffix} "
                f"<span style='font-size:.8rem;color:#aaa'>#{row['rank']} of {row['total_players']}</span>"
                f"</div>"
            )
        html += "</div>"
        col.markdown(html, unsafe_allow_html=True)

    # Monthly P/R/A chart
    if {'game_date','pts','reb','ast'}.issubset(df_team_bs.columns):
        dfm = df_team_bs.copy()
        dfm['month_num'] = dfm['game_date'].dt.month
        bym = dfm.groupby('month_num').agg(PPG=('pts','mean'), RPG=('reb','mean'), APG=('ast','mean')).reset_index()
        months_map = {1:'JAN',2:'FEB',3:'MAR',4:'APR',5:'MAY',6:'JUN',7:'JUL',8:'AUG',9:'SEP',10:'OCT',11:'NOV',12:'DEC'}
        bym['month'] = bym['month_num'].map(months_map)
        df_long = bym.melt(id_vars=['month'], value_vars=['PPG','RPG','APG'], var_name='Metric', value_name='Value')
        chart = (
            alt.Chart(df_long)
            .mark_bar()
            .encode(
                x=alt.X('month:N', sort=list(months_map.values())),
                y='Value:Q',
                color='Metric:N',
                xOffset='Metric:N'
            )
        )
        st.altair_chart(chart, use_container_width=True)
