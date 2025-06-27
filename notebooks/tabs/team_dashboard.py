from turtle import title
import streamlit as st
from pathlib import Path
import pandas as pd
from io import BytesIO
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import plotly.graph_objects as go
import plotly.express as px

# ─────────────────────────────────────────────────────────────────────────────
# Data Paths and Directories
# ─────────────────────────────────────────────────────────────────────────────
DATA_DIR   = Path(__file__).resolve().parents[2] / "data" / "processed"
TEAM_DIR   = DATA_DIR / "team_stats"
TEAM_DATA  = DATA_DIR / "teams_cleaned.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Page configuration
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Team Dashboard", layout="wide",initial_sidebar_state="collapsed")
st.sidebar.title("Filters")

# ─────────────────────────────────────────────────────────────────────────────
# Seasons picker
# ─────────────────────────────────────────────────────────────────────────────
seasons = [
    "2024-25","2023-24","2022-23","2021-22",
    "2020-21","2019-20","2018-19","2017-18",
    "2016-17","2015-16",
]
season = st.sidebar.selectbox("Season", seasons, index=0)

# ─────────────────────────────────────────────────────────────────────────────
# Team picker
# ─────────────────────────────────────────────────────────────────────────────
team_bios = pd.read_csv(TEAM_DATA).rename(columns=str.lower)
team_codes = sorted(team_bios["team"].unique())  # three-letter codes
team_abbr = st.sidebar.selectbox("Team", team_codes, index=0)
team_info = team_bios.loc[team_bios["team"] == team_abbr].iloc[0]
team_id   = int(team_info["team_id"])
team_name = team_info["team_name"]

# ─────────────────────────────────────────────────────────────────────────────
# Constants for folder structure
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_METRIC      = "adv_boxscores"
DEFAULT_MEASURE     = "pergame"
DEFAULT_SEASON_TYPE = "regular_season"
DEFAULT_MODE = "advanced"

#Base directory for team stats
base_dir = (TEAM_DIR    / DEFAULT_METRIC    / season    / DEFAULT_MEASURE    / DEFAULT_SEASON_TYPE)
df_all = pd.concat([
    pd.read_csv(f).assign(season=season)
    for f in base_dir.glob("*.csv")
], ignore_index=True)

# drop any stray 'team' col, then merge in canonical team info
df_all = df_all.drop(columns=["team", "team_name"], errors="ignore")
df_all = df_all.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

#st.write("📂 CSVs loaded for df_all:", list(base_dir.glob("*.csv")))
#st.write("🧮 rows per file:",
#         {f.name: len(pd.read_csv(f)) for f in base_dir.glob("*.csv")})

#st.write("⚙️ teams_cleaned.csv has these team_id counts:",
#         team_bios["team_id"].value_counts().head())

# ─────────────────────────────────────────────────────────────────────────────
# Load advanced boxscores for this season
# ─────────────────────────────────────────────────────────────────────────────

boxscore_dir = (TEAM_DIR / "adv_boxscores" / season / DEFAULT_MEASURE / DEFAULT_SEASON_TYPE)
box_adv = (boxscore_dir / "advanced.csv")
box_trad = (boxscore_dir / "traditional.csv")
df_boxscore_adv = pd.read_csv(box_adv)
df_boxscore_trad = pd.read_csv(box_trad)
df_boxscore = pd.concat([
    pd.read_csv(f).assign(season=season)
    for f in boxscore_dir.glob("*.csv")
], ignore_index=True)

df_boxscore = df_boxscore.rename(columns=str.lower)
df_boxscore = df_boxscore.drop(columns=["team", "team_name"], errors="ignore")
df_boxscore = df_boxscore.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

df_boxscore_adv = df_boxscore_adv.rename(columns=str.lower)
df_boxscore_adv = df_boxscore_adv.drop(columns=["team", "team_name"], errors="ignore")
df_boxscore_adv = df_boxscore_adv.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)




df_boxscore_trad = df_boxscore_trad.rename(columns=str.lower)
df_boxscore_trad = df_boxscore_trad.drop(columns=["team", "team_name"], errors="ignore")
df_boxscore_trad = df_boxscore_trad.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

pm = (
    df_boxscore_trad
    .loc[:, ["game_id","matchup","plus_minus","pts"]]     
)
df_boxscore_adv = df_boxscore_adv.merge(
    pm,
    on=["game_id", "matchup"],
    how="left"    
)
df_boxscore_adv = df_boxscore_adv.drop_duplicates(subset=["game_id", "matchup"])



# ─────────────────────────────────────────────────────────────────────────────
# Load per-team summary ("general") data for this season
# ─────────────────────────────────────────────────────────────────────────────
general_dir = (    TEAM_DIR    / "general"    / season    / DEFAULT_MEASURE    / DEFAULT_SEASON_TYPE)
gen_trad = (general_dir / "traditional.csv")
gen_adv = (general_dir / "advanced.csv")
df_gen = pd.concat([
    pd.read_csv(f).assign(season=season)
    for f in general_dir.glob("*.csv")
], ignore_index=True)

df_gen_trad = pd.read_csv(gen_trad)
df_gen_adv = pd.read_csv(gen_adv)

df_gen_trad = df_gen_trad.rename(columns=str.lower)
df_gen_trad = df_gen_trad.drop(columns=["team"], errors="ignore")
df_gen_trad = df_gen_trad.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

df_gen_adv = df_gen_adv.rename(columns=str.lower)
df_gen_adv = df_gen_adv.drop(columns=["team"], errors="ignore")
df_gen_adv = df_gen_adv.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)
# bring only the plus_minus column into your advanced‐summary table
df_gen_adv = df_gen_adv.merge(
    df_gen_trad[["team_id", "plus_minus"]],
    on="team_id",
    how="left"
)


df_gen = df_gen.drop(columns=["team", "team_name"], errors="ignore")
df_gen = df_gen.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

# Load clutch stats for this season
clutch_dir = (TEAM_DIR / "clutch" / season / DEFAULT_MEASURE / DEFAULT_SEASON_TYPE)
clutch_adv = (clutch_dir / "advanced.csv")
clutch_trad = (clutch_dir / "traditional.csv")
df_clutch_adv = pd.read_csv(clutch_adv)
df_clutch_trad = pd.read_csv(clutch_trad)
df_clutch = pd.concat([
    pd.read_csv(f).assign(season=season)
    for f in clutch_dir.glob("*.csv")
], ignore_index=True)

df_clutch = df_clutch.rename(columns=str.lower)
df_clutch = df_clutch.drop(columns=["team"], errors="ignore")
df_clutch = df_clutch.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

df_clutch_adv = df_clutch_adv.rename(columns=str.lower)
df_clutch_adv = df_clutch_adv.drop(columns=["team"], errors="ignore")
df_clutch_adv = df_clutch_adv.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

df_clutch_trad = df_clutch_trad.rename(columns=str.lower)
df_clutch_trad = df_clutch_trad.drop(columns=["team"], errors="ignore")
df_clutch_trad = df_clutch_trad.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)



#Efficiency Benchmark metrics
eff_benchmark_metrics = {
    "Net Rating":         "net_rating",
    "TS%":               "ts_pct",    # adjust to your actual column name
    "AST/TO Ratio":       "ast_to",    # if you computed this in a column
    "Drb %":          "dreb_pct",
    "eFG%":               "efg_pct",
    "pace":               "pace",
}

# compute a DataFrame of league‐wide stats

league = (
    df_clutch_adv
    .set_index("team_id")[list(eff_benchmark_metrics.values())]
)
league_median    = league.median()
league_percentile = league.rank(pct=True).round(3)



# ─────────────────────────────────────────────────────────────────────────────
# Filter both tables to the selected team
# ─────────────────────────────────────────────────────────────────────────────
df_team_metric  = df_boxscore_adv[df_boxscore_adv["team_id"] == team_id]

df_team_general = df_gen[df_gen["team_id"] == team_id]
df_team_gen_trad = df_gen_trad[df_gen_trad["team_id"] == team_id]
df_team_gen_adv = df_gen_adv[df_gen_adv["team_id"] == team_id]
df_team_adv_box = df_boxscore_adv[df_boxscore_adv["team_id"] == team_id]

df_clutch = df_clutch[df_clutch["team_id"] == team_id]
df_clutch_adv = df_clutch_adv[df_clutch_adv["team_id"] == team_id]
df_clutch_trad = df_clutch_trad[df_clutch_trad["team_id"] == team_id]




team_row = league.loc[team_id]

# ─────────────────────────────────────────────────────────────────────────────
# Add a 'month' column to the game-by-game table
# ─────────────────────────────────────────────────────────────────────────────
season_month_order = [10, 11, 12, 1, 2, 3, 4]
months = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR",
    5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG",
    9: "SEP",10: "OCT",11: "NOV",12: "DEC"
}

@st.cache_data
def with_month(df: pd.DataFrame, date_col="game_date") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["month"]      = df[date_col].dt.month
    df["month_name"] = df["month"].map(months)
    df["month"]      = pd.Categorical(df["month"], categories=months)
    return df

df_team_metric = with_month(df_team_metric, "game_date")

# ─────────────────────────────────────────────────────────────────────────────
# Header and basic KPIs
# ─────────────────────────────────────────────────────────────────────────────
logo_url = team_info["logo_url"]

# Helpers (as before)
_SUP_MAP = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")
def to_superscript(n: int) -> str:
    return str(n).translate(_SUP_MAP)

def rank_color(rank: int, total_teams: int = 30) -> str:
    third = total_teams // 3
    if   rank <= third:     return "green"
    elif rank <= 2*third:   return "orange"
    else:                   return "red"

# 1) First, make sure every row has explicit home/away teams
def parse_matchup(m: str) -> tuple[str,str]:
    m = m.replace("vs.", "vs").replace(" @ ", "@").replace(" vs ", "vs").strip()
    if "@" in m:
        away, home = m.split("@")
    else:
        home, away = m.split("vs")
    return away.strip(), home.strip()

def add_home_away(df: pd.DataFrame) -> pd.DataFrame:
    
    df_boxscore_adv[["away_team","home_team"]] = df_boxscore_adv["matchup"].apply(parse_matchup).tolist()

    # add opp_team which is the opponent the team faced
    # Opponent is the other side of the venue
    df_boxscore_adv["opp_team"] = df_boxscore_adv.apply(
        lambda r: r.away_team if r.home_team == r.team else r.home_team, axis=1
    )
    return df


df_boxscore_adv = add_home_away(df_boxscore_adv)
df_boxscore_adv

df_boxscore_adv["wl"] = df_boxscore_adv["wl"].str.upper()  # normalize wins/losses

# 2) Compute every team’s home W/L totals
home = df_team_metric[df_team_metric["matchup"].str.contains("vs", case=False)]
home_summary = (
    home
    .groupby("team_id")["wl"]
    .value_counts()
    .unstack(fill_value=0)
    .rename(columns={"W":"home_w","L":"home_l"})
)
h_wins = home_summary["home_w"].sum()
h_losses = home_summary["home_l"].sum()
# 3) Compute every team’s away W/L totals
away = df_team_metric[df_team_metric["matchup"].str.contains("@")]
away_summary = (
    away
    .groupby("team_id")["wl"]
    .value_counts()
    .unstack(fill_value=0)
    .rename(columns={"W":"away_w","L":"away_l"})
)
a_wins = away_summary["away_w"].sum()
a_losses = away_summary["away_l"].sum()

st.write(df_boxscore_adv.shape)

# 4) Stitch together, compute percentages and ranks
summary = (
    home_summary
    .join(away_summary, how="outer")
    .fillna(0)
    .astype(int)
    .assign(
        h_pct=lambda d: d.home_w.div(d.home_w + d.home_l).fillna(0),
        a_pct=lambda d: d.away_w.div(d.away_w + d.away_l).fillna(0)
    )
)
summary["h_rank"] = summary["h_pct"].rank(ascending=False, method="min").astype(int)
summary["a_rank"] = summary["a_pct"].rank(ascending=False, method="min").astype(int)

# 5) Merge those four new cols into your ADVANCED summary BEFORE filtering
df_gen_adv = df_gen_adv.merge(
    summary[["h_pct","h_rank","a_pct","a_rank"]],
    left_on="team_id", right_index=True, how="left"
)

# 6) Now when you slice to your team, you still carry its league rank
df_team_gen_adv = df_gen_adv[df_gen_adv["team_id"] == team_id]

# and you can simply read off:
home_pct      = df_team_gen_adv["h_pct"].iloc[0]
home_pct_rank = df_team_gen_adv["h_rank"].iloc[0]
away_pct      = df_team_gen_adv["a_pct"].iloc[0]
away_pct_rank = df_team_gen_adv["a_rank"].iloc[0]

    # Games played
games_played = df_team_metric["game_id"].nunique()

# Win-loss %
win_loss_pct = (
        df_team_general["w_pct"].iloc[0]
        if "w_pct" in df_team_general.columns and len(df_team_general)>0
        else 0.0
    )

win_loss_pct_rank = (
        df_team_gen_trad["w_rank"].iloc[0]
        if "w_rank" in df_team_gen_trad.columns and len(df_team_gen_trad)>0
        else None
    )
    
    # PPG & rank
points_per_game = (
        df_team_general["pts"].mean()
        if "pts" in df_team_general.columns and len(df_team_general)>0
        else 0.0
    )
pts_rank = (
        df_team_gen_trad["pts_rank"].iloc[0]
        if "pts_rank" in df_team_gen_trad.columns and len(df_team_gen_trad)>0
        else None
    )
    # RPG & APG (from game-by-game)
rebounds_per_game = (
        (df_team_general["reb"]).mean()
        if "reb" in df_team_general.columns and len(df_team_general)>0
        else 0.0
    )
trb_rank = (
        df_team_gen_trad["reb_rank"].iloc[0]
        if "reb_rank" in df_team_gen_trad.columns and len(df_team_gen_trad)>0
        else None
    )
assists_per_game = (
        df_team_general["ast"].mean()
        if "ast" in df_team_general.columns and len(df_team_general)>0
        else 0.0
    )

ast_rank = (
        df_team_gen_trad["ast_rank"].iloc[0]
        if "ast_rank" in df_team_gen_trad.columns and len(df_team_gen_trad)>0
        else None
    )

net_rating = (
        df_clutch_adv["net_rating"].iloc[0]
        if "net_rating" in df_clutch_adv.columns and len(df_clutch_adv)>0
        else None
)

net_rating_rank = (
        df_clutch_adv["net_rating_rank"].iloc[0]
        if "net_rating_rank" in df_clutch_adv.columns and len(df_clutch_adv)>0
        else None
)

off_rating = (
        df_team_gen_adv["off_rating"].iloc[0]
        if "off_rating" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
        else None
)

def_rating = (
        df_team_gen_adv["def_rating"].iloc[0]
        if "def_rating" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
        else None
    )

off_rating_rank = (
        df_team_gen_adv["off_rating_rank"].iloc[0]
        if "off_rating_rank" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
        else None
    )

def_rating_rank = (
        df_team_gen_adv["def_rating_rank"].iloc[0]
        if "def_rating_rank" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
        else None
    )

#Team Pace
pace = (
        df_team_gen_adv["pace"].iloc[0]
        if "pace" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
        else None
    )

pace_rank = (
        df_team_gen_adv["pace_rank"].iloc[0]
        if "pace_rank" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
        else None
    )

ts_pct = (
        df_clutch_adv["ts_pct"].mean()
        if "ts_pct" in df_clutch_adv.columns and len(df_clutch_adv)>0
        else 0.0
    )

ts_pct_rank = (
        df_clutch_adv["ts_pct_rank"].iloc[0]
        if "ts_pct_rank" in df_clutch_adv.columns and len(df_clutch_adv)>0
        else None
    )

    # Format ranks with superscript and color
win_loss_pct_sup = to_superscript(win_loss_pct_rank) if win_loss_pct_rank is not None else ""
pts_sup  = to_superscript(pts_rank) if pts_rank is not None else ""
trb_sup  = to_superscript(trb_rank) if trb_rank is not None else "" 
ast_sup  = to_superscript(ast_rank) if ast_rank is not None else ""
net_rating_sup = to_superscript(net_rating_rank) if net_rating_rank is not None else ""
off_rating_sup = to_superscript(off_rating_rank) if off_rating_rank is not None else ""
def_rating_sup = to_superscript(def_rating_rank) if def_rating_rank is not None else ""
pace_sup = to_superscript(pace_rank) if pace_rank is not None else ""
ts_pct_sup = to_superscript(ts_pct_rank) if ts_pct_rank is not None else ""
win_loss_pct_color = rank_color(win_loss_pct_rank if win_loss_pct_rank is not None else 30, total_teams=30)
pts_color = rank_color(pts_rank if pts_rank is not None else 30, total_teams=30)
trb_color = rank_color(trb_rank if trb_rank is not None else 30, total_teams=30)
ast_color = rank_color(ast_rank if ast_rank is not None else 30, total_teams=30)
net_rating_color = rank_color(net_rating_rank if net_rating_rank is not None else 30, total_teams=30)
off_rating_color = rank_color(off_rating_rank if off_rating_rank is not None else 30, total_teams=30)
def_rating_color = rank_color(def_rating_rank if def_rating_rank is not None else 30, total_teams=30)
pace_color = rank_color(pace_rank if pace_rank is not None else 30, total_teams=30)
ts_pct_color = rank_color(ts_pct_rank if ts_pct_rank is not None else 30, total_teams=30)

# earlier in your pipeline
df_boxscore_adv = add_home_away(df_boxscore_adv)         # gives you away_team & home_team
df_team_metric = df_boxscore_adv[df_boxscore_adv.team_id==team_id].copy()


    # Wins & losses (one row per team in df_team_general)
wins   = int(df_team_general["w"].iloc[0])
losses = int(df_team_general["l"].iloc[0])



# Largest victory and biggest defeat
if "plus_minus" in df_team_metric.columns and not df_team_metric.empty:
    # Make sure we have away_team/home_team
    # (skip re-parsing matchup if you've already called add_home_away)
    df = df_team_metric.copy()

    idx_max = df["plus_minus"].idxmax()
    largest_victory         = int(df.at[idx_max, "plus_minus"])
    largest_victory_opponent = df.at[idx_max, "opp_team"]
    largest_victory_pts     = int(df.at[idx_max, "pts"])
    victory_opp_points      = largest_victory_pts - largest_victory

    # Biggest defeat
    idx_min = df["plus_minus"].idxmin()
    biggest_defeat          = int(df.at[idx_min, "plus_minus"])
    biggest_defeat_opponent = df.at[idx_min, "opp_team"]
    biggest_defeat_pts      = int(df.at[idx_min, "pts"])
    defeat_opp_points       = biggest_defeat_pts - biggest_defeat

else:
    # Fallback if no plus_minus column or empty
    largest_victory, largest_victory_opponent = None, ""
    biggest_defeat, biggest_defeat_opponent = None, ""
    largest_victory_pts, victory_opp_points = 0, 0
    biggest_defeat_pts, defeat_opp_points = 0, 0






w_rank = (
    df_team_general["w_rank"].iloc[0]
    if "w_rank" in df_team_general.columns and len(df_team_general)>0
        else None
    )
l_rank = (
        df_team_general["l_rank"].iloc[0]
        if "l_rank" in df_team_general.columns and len(df_team_general)>0
        else None
)



w_sup   = to_superscript(w_rank) if w_rank is not None else ""
l_sup   = to_superscript(l_rank) if l_rank is not None else ""
h_sup  = to_superscript(home_pct_rank) if home_pct_rank is not None else ""
a_sup  = to_superscript(away_pct_rank) if away_pct_rank is not None else ""
w_color = rank_color(w_rank if w_rank is not None else 30, total_teams=30)
l_color = rank_color(l_rank if l_rank is not None else 30, total_teams=30)
a_color = rank_color(away_pct_rank if away_pct_rank is not None else 30, total_teams=30)
h_color = rank_color(home_pct_rank if home_pct_rank is not None else 30, total_teams=30)

container = st.container(border=True)
with container:
    container.markdown(
        f"""
        <div style="display: flex; align-items: center; justify-content: space-between;">
        <h1 style="margin: 0;">{team_name} — {season}</h1>
        <img src="{logo_url}" alt="{team_abbr} logo" style="height:50px">
        </div>
        """,
        unsafe_allow_html=True
    )
    col1, col2, col3, col4, col5, col6 = container.columns(6)
    col1.markdown(
        f"""
        <div>Win %</div>
        <span style="font-size:2rem;">{win_loss_pct:.3}</span>
        <sup style="color:{win_loss_pct_color}; font-size:1rem;">{win_loss_pct_sup}</sup>        
        </div>
        """,
        unsafe_allow_html=True,
    )

    #Net Rating
    col2.markdown(
        f"""
        <div>Net Rtg</div>
        <span style="font-size:2rem;">{net_rating}</span>
        <sup style="color:{net_rating_color}; font-size:1rem;">{net_rating_sup}</sup>        
        </div>
        """,
        unsafe_allow_html=True,
    )

    #offensive Rating
    col3.markdown(
        f"""
        <div>Off Rtg</div>
        <span style="font-size:2rem;">{off_rating}</span>
        <sup style="color:{off_rating_color}; font-size:1rem;">{off_rating_sup}</sup>
        </div>
        """,
        unsafe_allow_html=True,
    )

    #Defensive Rating
    col4.markdown(
        f"""
        <div>Def Rtg</div>
        <span style="font-size:2rem;">{def_rating}</span>
        <sup style="color:{def_rating_color}; font-size:1rem;">{def_rating_sup}</sup>
        </div>
        """,
        unsafe_allow_html=True,
    )

    #Pace
    col5.markdown(
        f"""
        <div>Pace</div>
        <span style="font-size:2rem;">{pace}</span>
        <sup style="color:{pace_color}; font-size:1rem;">{pace_sup}</sup>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    

    
    # True Shooting Percentage (TS%)
    col6.markdown(
        f"""
        <div style="font-size:0.8rem; color:gray;">TS%</div>
        <span style="font-size:2rem;">{ts_pct}</span>
        <sup style="color:{ts_pct_color}; font-size:1rem;">{ts_pct_sup}</sup>
        </div>
        """,
        unsafe_allow_html=True,
    )


    # ─────────────────────────────────────────────────────────────────────────────
    # Additional team-level KPIs
    # ─────────────────────────────────────────────────────────────────────────────
    

    col1, col2, col3, col4 = st.columns([2,2,1,1])
    col1.markdown(
        f"""
        <div>Largest Victory</div>
        <span style="font-size:2rem;">        
        <span style="color:green">{largest_victory_pts} - {victory_opp_points}</span> vs. {largest_victory_opponent}</span>       
        </div>
        """,
        unsafe_allow_html=True,
    )
    col2.markdown(
        f"""
        <div>Biggest Defeat</div>
        <span style="font-size:2rem;">
        <span style="color:red">{biggest_defeat_pts} - {defeat_opp_points}</span> vs. {biggest_defeat_opponent}</span>                
        </div>
        """,
        unsafe_allow_html=True,
    )
    col3.markdown(
        f"""
        <div>Home Record</div>
        <span style="font-size:2rem;">{h_wins}-{h_losses}</span>
        <sup style="color:{h_color}; font-size:1rem;">{h_sup}</sup>
        
        </div>
        """,
        unsafe_allow_html=True,
    )
    col4.markdown(
        f"""
        <div>Away Record</div>
        <span style="font-size:2rem;">{a_wins}-{a_losses}</span>
        <sup style="color:{a_color}; font-size:1rem;">{a_sup}</sup>
        
        </div>
        """,
        unsafe_allow_html=True,
    )


    col1, col2 = st.columns([1,1])
    # 3) Efficiency benchmarks → Radar (Spider) chart
    with col1:
        # 1) Prepare labels & keys
        labels = list(eff_benchmark_metrics.keys())
        cols   = list(eff_benchmark_metrics.values())

        # 2) Pull percentiles for this team and the median (0.5)
        team_pct = [ league_percentile.loc[team_id, c] for c in cols ]
        med_pct  = [ 0.5 for _ in cols ]

        import plotly.graph_objects as go

        fig = go.Figure()

        # 3) League‐median trace
        fig.add_trace(go.Scatterpolar(
            r=med_pct,
            theta=labels,
            fill="toself",
            name="League Median",
            marker=dict(symbol="circle", size=6),
            line=dict(color="gray", dash="dash")
        ))

        # 4) Team trace
        fig.add_trace(go.Scatterpolar(
            r=team_pct,
            theta=labels,
            fill="toself",
            name=team_name,
            marker=dict(symbol="circle", size=6),
            line=dict(color="green")
        ))

        # 5) Layout & styling
        fig.update_layout(
            polar=dict(
                bgcolor= "rgba(0,0,0,0)",
                radialaxis=dict(
                    gridcolor="gray",
                    visible=True,
                    range=[0, 1],
                    tick0=0,
                    dtick=0.25
                )
            ),
            showlegend=True,
            legend=dict(
                orientation="h",
                x=0.5,
                xanchor="center",
                y=1.1
            ),
            title=f"Efficiency Radar",
            margin=dict(t=40, b=20, l=20, r=20)
        )

        st.plotly_chart(fig, use_container_width=True)
    # ─────────────────────────────────────────────────────────────────────────────
    # PPG per Month chart
    # ─────────────────────────────────────────────────────────────────────────────
    chart_col1, chart_col2= st.columns([1,1])

    # 1) PPG per Month
    with chart_col1:
        if "pts" in df_team_metric.columns:
            # 1) Prepare the monthly PPG series
            df = df_team_metric.copy()
            df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
            pts_by_month = (
                df
                .groupby(df["game_date"].dt.month)["pts"]
                .mean()
                .round(2)
            )
            # preserve season order (Oct→Apr)
            order = (
                df
                .groupby(df["game_date"].dt.month)["game_date"]
                .min()
                .sort_values()
                .index
            )
            month_names = [months[m] for m in order]
            pts_by_month = pts_by_month.reindex(order).rename(index=months).dropna()

            # 2) Build a Plotly bar chart
            import plotly.express as px

            fig = px.bar(
                x=month_names,
                y=pts_by_month.values,
                labels={"x": "Month", "y": "PPG"},
                title="Monthly Points Per Game (PPG)",
            )

            # 3) (Optional) add a horizontal line at the league median PPG
            median_ppg = df_gen["pts"].median()
            fig.add_hline(
                y=median_ppg,
                line_dash="dash",
                line_color="gray",
                annotation_text=f"League Median: {median_ppg:.1f}",
                annotation_position="bottom right"
            )

            # 4) Final layout tweaks
            fig.update_layout(
                xaxis=dict(tickmode="array", tickvals=month_names, ticktext=month_names),
                margin=dict(t=40, b=30, l=40, r=20)
            )

            st.plotly_chart(fig, use_container_width=True)


    # 2) Net Rating per Month
    with chart_col2:
        if "net_rating" in df_team_metric.columns:
            # 1) Prepare the monthly average series
            df = df_team_metric.copy()
            df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
            monthly = (
                df
                .groupby(df["game_date"].dt.month)["net_rating"]
                .mean()
                .round(2)
            )
            # order by first appearance in the season
            order = (
                df
                .groupby(df["game_date"].dt.month)["game_date"]
                .min()
                .sort_values()
                .index
            )
            month_names = [months[m] for m in order]
            monthly = monthly.reindex(order).rename(index=months).dropna()
            # extract numeric x positions
            x_vals = list(range(len(monthly)))
            y_vals = monthly.values.tolist()

            league_med = league_median["net_rating"]


            fig = go.Figure()

            # 2) add below‐median segments
            for i in range(len(x_vals) - 1):
                seg_x = x_vals[i : i + 2]
                seg_y = y_vals[i : i + 2]
                color = "green" if sum(seg_y)/2 >= league_med else "red"
                fig.add_trace(go.Scatter(
                    x=seg_x, y=seg_y,
                    mode="lines",
                    line=dict(color=color, width=3),
                    showlegend=False
                ))

            # 3) add markers for each month
            marker_colors = [
                "green" if y >= league_med else "red"
                for y in y_vals
            ]
            fig.add_trace(go.Scatter(
                x=x_vals, y=y_vals,
                mode="markers",
                marker=dict(color=marker_colors, size=3),
                name=team_name
            ))

            # 4) add horizontal median line
            fig.add_hline(
                y=league_med,
                line=dict(color="gray", dash="dash"),
                annotation_text=f"Med: {league_med:.2f}",
                annotation_position="bottom right"
            )

            # 5) layout tweaks
            fig.update_layout(
                xaxis=dict(
                    tickmode="array",
                    tickvals=x_vals,
                    ticktext=month_names,
                    title="Month"
                ),
                yaxis_title="Net Rating",
                title=f"Monthly Net Rating",
                margin=dict(t=40, b=30, l=40, r=20)
            )

            st.plotly_chart(fig, use_container_width=True)


    

    # ────────────────────────────────────────────────────────────────────────────
    # Advanced Stats KPIs
    # ────────────────────────────────────────────────────────────────────────────
    c9, c10, c11= st.columns([1,1,2])

    #Shooting Efficiency Group bar Chart containing eFG%, TS%, FT% and 3PT%
    with c9:

        df_trad_all = (
        pd.read_csv(gen_trad)
          .rename(columns=str.lower)
          .drop(columns=["team"], errors="ignore")
        )
        # all-team advanced (for efg_pct, ts_pct)
        df_adv_all = (
            pd.read_csv(clutch_adv)
            .rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
        )

        # compute league medians
        median_vals = {
            "efg_pct": df_adv_all["efg_pct"].mean(),
            "ts_pct":  df_adv_all["ts_pct"].mean(),
            "ft_pct":  df_trad_all["ft_pct"].mean(),
            "fg3_pct": df_trad_all["fg3_pct"].mean(),
        }
        shoot_eff = (
            df_team_gen_trad[["team", "team_id", "ft_pct", "fg3_pct"]]
            .merge(
                df_clutch_adv[["team_id", "efg_pct", "ts_pct"]],
                on="team_id",
                how="left"
            )
        )

        # 2) Melt to long format for grouped bars
        metrics = ["efg_pct", "ts_pct", "ft_pct", "fg3_pct"]
        df_long = shoot_eff.melt(
            id_vars="team",
            value_vars=metrics,
            var_name="Shooting Metric",
            value_name="Percentage"
        )

        # 3) Create grouped horizontal bar chart
        fig = px.bar(
            df_long,
            y="Percentage",
            x="team",
            color="Shooting Metric",
            orientation="v",
            barmode="group",
            labels={"team": "Team", "Percentage": "Efficiency"},
            title="Shooting Efficiency"
        )

        # constants that Plotly uses under the hood for grouped bars
        n = len(metrics)          # number of bars in the group
        group_width = 0.8         # default fraction of the category “slot” occupied by the entire group
        bar_width   = group_width / n

        for idx, m in enumerate(metrics):
            # 1) compute the league median for this metric
            med = median_vals[m]

            # 2) compute the center of this bar in paper-coords
            offset_frac = ((idx - (n-1)/2) / n) * group_width
            center_frac = 0.5 + offset_frac

            # 3) compute the left/right edges of this bar in paper-coords
            x0 = center_frac - bar_width/2
            x1 = center_frac + bar_width/2

            # 4) draw a horizontal dashed line at y=med, only from x0→x1
            fig.add_shape(
                type="line",
                xref="paper", x0=x0, x1=x1,
                yref="y",     y0=med, y1=med,
                line=dict(color="gray", dash="dot")
            )
        st.plotly_chart(fig, use_container_width=True)

    # ────────────────────────────────────────────────────────────────────────────
    # Rebounding and Hustle Stats KPIs
    with c10:
        df_team_gen_adv = df_team_gen_adv.merge(
            df_gen[["team_id","blka", "blk", "stl"]],
            on="team_id", how="left"
        )
        df_team_gen_adv["blk_pct"] = (df_team_gen_adv["blk"] / df_team_gen_adv["blka"]).fillna(0)
        df_hustle = df_team_gen_adv[["team", "team_id", "dreb_pct", "oreb_pct", "stl", "blk_pct"]]
        df_hustle = df_hustle.melt(
            id_vars=["team", "team_id"],
            value_vars=["dreb_pct", "oreb_pct", "stl", "blk_pct"],
            var_name="Hustle Metric",
            value_name="Percentage"
        )

        fig = px.bar(
            df_hustle,
            y="Percentage",
            x="team",
            color="Hustle Metric",
            orientation="v",
            barmode="group",
            labels={"team": "Team", "Percentage": "Percentage"},
            title="Rebounding and Hustle"
        )
        st.plotly_chart(fig, use_container_width=True)
    
        #Win loss timeline line chart
    with c11:
        df = df_team_metric.copy()
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
        df = df.sort_values("game_date")

        median_mov = df["plus_minus"].median()
        fig = go.Figure()

        # split above/below so you can color both line segments and markers
        df["above"] = df["plus_minus"].where(df["plus_minus"] >= median_mov)
        df["below"] = df["plus_minus"].where(df["plus_minus"] <  median_mov)
        x = df["game_date"]

        # below‐median (red)
        fig.add_trace(go.Scatter(
            x=x, y=df["below"],
            mode="lines+markers",
            line=dict(color="red", width=2),
            marker=dict(size=6),
            name="Below Median"
        ))
        # above‐median (green)
        fig.add_trace(go.Scatter(
            x=x, y=df["above"],
            mode="lines+markers",
            line=dict(color="green", width=2),
            marker=dict(size=6),
            name="Above Median"
        ))

        # horizontal median line
        fig.add_hline(
            y=median_mov,
            line=dict(color="gray", dash="dash"),
            annotation_text=f"med: {median_mov:.1f}",
            annotation_position="bottom right"
        )

        fig.update_layout(
            xaxis_title="Game Date",
            yaxis_title="Margin of Victory",
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
            title= "Margin of Victory timeline"
        )

        st.plotly_chart(fig, use_container_width=True)



