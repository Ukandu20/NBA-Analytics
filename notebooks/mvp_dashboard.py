from pathlib import Path
from altair import Align
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import streamlit as st
import io

# ────────────────────────────────────
# Config
# ────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "processed"
MVP_CSV = DATA_DIR / "mvp_cleaned.csv"
PLAYERS_CSV = DATA_DIR / "all_players_cleaned.csv"
ERA_BINS = [0, 1979, 1989, 1999, 2009, 3000]
ERA_LABELS = [
    "Early Years (≤1979)",
    "Magic-Bird Era (1980s)",
    "Jordan Era (1990s)",
    "Post-Jordan (2000s)",
    "Modern Era (2010-present)",
]

# ────────────────────────────────────
# Data loading & preprocessing
# ────────────────────────────────────
@st.cache_data
def load_data():
    mvp = pd.read_csv(MVP_CSV)
    if "season_start" not in mvp.columns:
        mvp["season_start"] = mvp["season_start"] + 1

    players = pd.read_csv(PLAYERS_CSV, parse_dates=["birthdate"])
    players["position_primary"] = players["position"].str.split("-").str[0]

    df = mvp.merge(
        players[["player","position_primary","height","weight","birthdate", "headshot_url"]],
        on="player", how="left"
    )
    df["age"] = df["season_start"] - df["birthdate"].dt.year
    df["era"] = pd.cut(df["season_start"], bins=ERA_BINS, labels=ERA_LABELS)
    return df

# Streamlit layout & filters
st.set_page_config(page_title="NBA MVP Dashboard", layout="wide")
hide_streamlit_style = "<style>#MainMenu {visibility: hidden;} footer {visibility: hidden;}</style>"
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
df = load_data()
min_year = int(df["season_start"].min())
max_year = int(df["season_start"].max())
year_range = st.sidebar.slider(
    "Season end range", min_year, max_year, (max_year-19, max_year)
)
era_filter = st.sidebar.multiselect("Select Eras", options=ERA_LABELS, default=ERA_LABELS)
filtered = df.query(
    "@year_range[0] <= season_start <= @year_range[1] and era in @era_filter"
)

# ────────────────────────────────────
# Compute KPIs
# ────────────────────────────────────
total_mvp = len(filtered)
unique_winners = filtered["player"].nunique()
career_counts = filtered.groupby("player")["season_start"].count()
repeat_share = career_counts[career_counts >= 2].sum() / total_mvp * 100 if total_mvp else 0
first_time = career_counts[career_counts == 1].sum()
first_share = first_time / total_mvp * 100 if total_mvp else 0
avg_age = filtered["age"].mean()
min_age = filtered["age"].min()
max_age = filtered["age"].max()
young_row = filtered.loc[filtered["age"] == min_age].iloc[0]
youngest_name = young_row["player"]
young_year = young_row["season_end"]
old_row = filtered.loc[filtered["age"] == max_age].iloc[0]
oldest_name = old_row["player"]
old_year = old_row["season_end"]
pos_counts = filtered["position_primary"].fillna("Unknown").value_counts()
g,f,c = pos_counts.get("G",0), pos_counts.get("F",0), pos_counts.get("C",0)
# Advanced stats
avg_pts = filtered["pts"].mean()
avg_trb = filtered["trb"].mean()
avg_ast = filtered["ast"].mean()
avg_stl = filtered["stl"].mean()
avg_blk = filtered["blk"].mean()
avg_fgpct = filtered["fg_pct"].mean()*100
avg_ws = filtered["ws"].mean()
avg_ws48 = filtered["ws_48"].mean()

# ────────────────────────────────────
# Header & Volume KPIs
# ────────────────────────────────────
st.markdown(
    "<h1 style='text-align: center;'>NBA MVP Awards — Dashboard</h1>",
    unsafe_allow_html=True
)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Seasons", total_mvp)
c2.metric("Unique Winners", unique_winners)
c3.metric("Repeat-win %", f"{repeat_share:.1f}%")
c4.metric("1st-time wins", f"{first_time} ({first_share:.1f}% ) ")

# ────────────────────────────────────
# Age KPIs including per-year selection
# ────────────────────────────────────
c5, c6, c7= st.columns([1,2,2])
c5.metric("Avg Age", f"{avg_age:.1f} yrs")
c6.metric("Youngest Winner", f"{youngest_name} ({min_age:.0f} yrs, {young_year})")
c7.metric("Oldest Winner",   f"{oldest_name}   ({max_age:.0f} yrs, {old_year})")


st.markdown(
    "<h2 style='text-align: center;'>MVPs across the years</h2>",
    unsafe_allow_html=True
)
year_options = sorted(filtered["season_end"].unique())
selected_year = st.selectbox("Choose a season", year_options, index=len(year_options)-1)
selected_row = filtered[filtered["season_end"] == selected_year].iloc[0]


headshot_url = selected_row.get("headshot_url", None)
mvp_name = selected_row["player"]
age_val = selected_row["age"]
col_img, col_name, col_age, col_team = st.columns([1,2,1,1])
if headshot_url:
    col_img.image(headshot_url, width=100, caption=mvp_name)

col_name.metric("MVP of " + str(selected_year), mvp_name)
col_age.metric("Age", f"{age_val:.0f} yrs")
col_team.metric("Team", selected_row["team"])

# ────────────────────────────────────
# Advanced Stats KPIs
# ────────────────────────────────────
c9, c10, c11, c12 = st.columns(4)
c9.metric("PPG", f"{selected_row['pts']:.1f}")
c10.metric("RPG", f"{selected_row['trb']:.1f}")
c11.metric("APG", f"{selected_row['ast']:.1f}")
c12.metric("SPG", f"{selected_row['stl']:.1f}")

c13, c14, c15, c16 = st.columns(4)
c13.metric("BPG", f"{selected_row['blk']:.1f}")
c14.metric("FG%", f"{selected_row['fg_pct']* 100 :.1f}%")
c15.metric("WS", f"{selected_row['ws']:.1f}")
c16.metric("WS/48", f"{selected_row['ws_48']:.2f}")



st.markdown("---")

# ────────────────────────────────────
# Charts side by side: Position & Teams
# ────────────────────────────────────
chart_col1, chart_col2 = st.columns([1,3])
with chart_col1:
    st.subheader("MVPs by Position")
    pos_df = pos_counts.reset_index()
    pos_df.columns = ["Position", "MVP Count"]
    pos_df = pos_df.sort_values("MVP Count", ascending=True)
    fig_pos = px.bar(
        pos_df, 
        y="Position", 
        x="MVP Count",
        text="MVP Count",
        orientation="h",
    )
    st.plotly_chart(fig_pos, use_container_width=True)
with chart_col2:
    st.subheader("Top Teams")
    tdf = filtered["team"].value_counts().nlargest(10).reset_index()
    tdf.columns=["Team","Count"]
    st.plotly_chart(px.bar(tdf,x="Team",y="Count",text="Count"),use_container_width=True)


st.markdown("---")

# ────────────────────────────────────
# Age vs Season Chart
# ────────────────────────────────────
chart_col3, chart_col4, chart_col5= st.columns([1,1,1])
with chart_col3:
    st.subheader("Age vs Season")
    age_season_df = filtered[["season_end", "age"]].drop_duplicates().sort_values("season_end")
    fig = px.line(age_season_df, x="season_end", y="age", title="Age per Season")
    fig.update_traces(marker=dict(size=10, color='steelblue'))
    fig.update_layout(xaxis_title="Year", yaxis_title="Age of MVP Winner")
    st.plotly_chart(fig, use_container_width=True)



st.markdown("---")
# Suggested extra KPIs (no extra data needed)
st.write("• Games Played per season: mean of 'g' column if available")
st.write("• Minutes per Game: mean of 'mp' column if available")
st.write("• True Shooting %: compute if 'ts_pct' present")
st.write("• Win Shares per Game: (ws / g) mean")
st.write("• Era-by-era PPG & WS: filtered.groupby('era')[['pts','ws']].mean()")

# Footer
st.markdown(
    "<small>Data source: cleaned MVP & roster data</small>", unsafe_allow_html=True
)
