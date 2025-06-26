import streamlit as st
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from io import BytesIO
from PIL import Image
import glob

#Data Paths and Directories
DATA_DIR   = Path(__file__).resolve().parents[2] / "data" / "processed"
TEAM_DIR   = DATA_DIR / "team_stats"
TEAM_DATA = DATA_DIR / "teams_cleaned.csv"

#Page configuration
# ────────────────────────────────────
st.set_page_config(page_title="Team Dashboard", layout="wide")
st.sidebar.title("Filters")

# ────────────────────────────────────
# Seasons, metrics, teams, measures, season_types, modes

seasons = [
    "2024-25", "2023-24", "2022-23", "2021-22",
    "2020-21", "2019-20", "2018-19", "2017-18",
    "2016-17", "2015-16",
]
metrics = [
    "adv_boxscores", "boxscores", "clutch", "defense_dashboard",
    "general", "opponent_shooting", "playtype", "shooting", "shot_dashboard",
]
#teams list
teams = ["ATL", "BKN", "BOS", "CHA", "CHI", "CLE", "DAL", "DEN", "GSW", "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", 'NOP',"NYK", "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS", "TOR", "UTA", "WAS" ]

measures = ["totals", "pergame","per48", "per100possessions", "per36"]
season_types = ["regular_season", "playoffs"]
modes = ["advanced", "fourfactors", "defense", "misc", "opponent", "scoring", "traditional", "violations"]
months = {1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"}

season = st.sidebar.selectbox("Season", seasons, index=0)

# Default values for metric, measure, season_type, and mode
DEFAULT_METRIC      = "adv_boxscores"
DEFAULT_MEASURE     = "totals"
DEFAULT_SEASON_TYPE = "regular_season"
DEFAULT_MODE        = "advanced"


team_bios = pd.read_csv(TEAM_DATA).rename(columns = str.lower)
team_codes = sorted(team_bios["team"].unique())
team_abbr = st.sidebar.selectbox("Team", team_codes, index=0)
team_id = int(team_bios[team_bios["team"] == team_abbr]["team_id"].iloc[0])
team_name = team_bios[team_bios["team"] == team_abbr]["team_name"].iloc[0]

# Build the folder you actually want
base_dir = TEAM_DIR / DEFAULT_METRIC / season / DEFAULT_MEASURE / DEFAULT_SEASON_TYPE
df_all = pd.concat([
    pd.read_csv(f).assign(season=season) 
    for f in base_dir.glob("*.csv")
], ignore_index=True)

df_all = df_all.drop(columns = ["team"], errors="ignore")
df_all = (
    df_all
    .merge(
       team_bios[["team","team_id", "logo_url"]],
       on="team_id",
       how="left"
    )
)

# now load your “general” files for the same season
general_dir = TEAM_DIR / "general" / season / DEFAULT_MEASURE / DEFAULT_SEASON_TYPE
df_gen = pd.concat([
    pd.read_csv(f).assign(season=season) 
    for f in general_dir.glob("*.csv")
], ignore_index=True)

df_gen = df_gen.drop(columns = ["team"], errors="ignore")
df_gen = (
    df_gen
    .merge(
       team_bios[["team","team_id", "logo_url"]],
       on="team_id",
       how="left"
    )
)

# now load your “clutch” files for the same season
clutch_dir = TEAM_DIR / "clutch" / season / DEFAULT_MEASURE / DEFAULT_SEASON_TYPE
df_clutch = pd.concat([
    pd.read_csv(f).assign(season=season) 
    for f in clutch_dir.glob("*.csv")
], ignore_index=True)

df_clutch = df_clutch.drop(columns = ["team"], errors="ignore")
df_clutch = (
    df_clutch
    .merge(
       team_bios[["team","team_id", "logo_url"]],
       on="team_id",
       how="left"
    )
)






# — filter both on the exact same key — 
#df_team_metric = df_all[df_all["team_id"] == team_id]
df_team_general = df_gen   [df_gen   ["team_id"] == team_id]



@st.cache_data
def with_month(df: pd.DataFrame, date_col="game_date") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["month"]  = df[date_col].dt.month

    df["month_name"] = df["month"].apply(lambda m: months.get(m))

    # Make month an ordered categorical for proper sorting
    df["month"] = pd.Categorical(df["month"], categories=months, ordered=True)
    return df

df_all = with_month(df_all, "game_date")







# ────────────────────────────────────
# Team selector (now based on your data)
# ────────────────────────────────────
teams = sorted(df_all["team_name"].unique())

df = df_all[df_all["team_name"] == team_name]

df = df.merge(
    df_gen[["team_id", "w", "l", "team"]],
    on="team_id",)

df = (
    df
    .rename(columns={"team_x": "team"})
    .drop(columns=["team_y"])
)


# Get the first logo URL for this team
logo_url = df["logo_url"].dropna().unique()
logo_url = logo_url[0] if len(logo_url) > 0 else None


body_container = st.container()
# ────────────────────────────────────
with body_container:
    # Header with team logo
    body_container.markdown(
        f"""
        <div style="display: flex; align-items: center; justify-content: space-between;">
            <h1 style="margin: 0;">{team_name} — {season}</h1>
            <img src="{logo_url}" alt="{team_name} logo" style="height: 50px; width: auto;">
        </div>
        """,
        unsafe_allow_html=True
    )
    # Add 2 rows with 3 columns each for the KPIs
    col1, col2, col3 = st.columns(3)
    col1.metric("Games Played", df["game_id"].nunique())
    # if there’s exactly one row, grab it; otherwise sum as a fallback
    wins   = df["w"].iloc[0] 
    losses = df["l"].iloc[0]
    col2.metric("Wins",   wins)
    col3.metric("Losses", losses)

    # Add a row for the win-loss percentage
    win_loss_pct = wins / (wins + losses) if (wins + losses) > 0 else 0.0
    points_per_game = df["pts"].mean() if "pts" in df.columns else 0.0
    if "oreb" in df.columns and "dreb" in df.columns:
        rebounds_per_game = (df["oreb"] + df["dreb"]).mean()
    else:
        rebounds_per_game = 0.0
    assists_per_game = df["ast"].mean() if "ast" in df.columns else 0.0
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Win-Loss %", f"{win_loss_pct:.2%}")
    col2.metric("PPG", f"{points_per_game:.1f}")
    col3.metric("RPG", f"{rebounds_per_game:.1f}")
    col4.metric("APG", f"{assists_per_game:.1f}")
    
    
    
    chart_col1, chart_col2 = st.columns([1,1])
with chart_col1:
    if "pts" in df.columns:
        st.subheader("PPG per Month")
        # Prepare data: ensure game_date is datetime
        df_chart = df.copy()
        df_chart["game_date"] = pd.to_datetime(df_chart["game_date"], errors="coerce")
        # Compute average pts by month number
        pts_by_month_num = (
            df_chart.groupby(df_chart["game_date"].dt.month)["pts"]
                    .mean()
        )

        pts_by_month_num = pts_by_month_num.round(2)
        # Determine month order by earliest game date in season
        months_order = (
            df_chart.groupby(df_chart["game_date"].dt.month)["game_date"]
                    .min()
                    .sort_values()
                    .index
        )

        pts_by_month = (
            pts_by_month_num.reindex(months_order)
                            .rename(index=months)
                            .dropna()
        )
        st.bar_chart(pts_by_month)
#Average points per month


    

