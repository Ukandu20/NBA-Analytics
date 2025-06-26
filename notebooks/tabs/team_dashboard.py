import streamlit as st
from pathlib import Path
import pandas as pd
from io import BytesIO
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import glob
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

# ─────────────────────────────────────────────────────────────────────────────
# Load game-by-game data for this season
# ─────────────────────────────────────────────────────────────────────────────
base_dir = (TEAM_DIR    / DEFAULT_METRIC    / season    / DEFAULT_MEASURE    / DEFAULT_SEASON_TYPE)

df_all = pd.concat([
    pd.read_csv(f).assign(season=season)
    for f in base_dir.glob("*.csv")
], ignore_index=True)

# drop any stray 'team' col, then merge in canonical team info
df_all = df_all.drop(columns=["team"], errors="ignore")
df_all = df_all.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

# ─────────────────────────────────────────────────────────────────────────────
# Load per-team summary ("general") data for this season
# ─────────────────────────────────────────────────────────────────────────────
general_dir = (    TEAM_DIR    / "general"    / season    / DEFAULT_MEASURE    / DEFAULT_SEASON_TYPE)
gen_trad = (general_dir / "traditional.csv")
df_gen = pd.concat([
    pd.read_csv(f).assign(season=season)
    for f in general_dir.glob("*.csv")
], ignore_index=True)

df_gen_trad = pd.read_csv(gen_trad)

df_gen_trad = df_gen_trad.rename(columns=str.lower)
df_gen_trad = df_gen_trad.drop(columns=["team"], errors="ignore")
df_gen_trad = df_gen_trad.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)
df_gen = df_gen.drop(columns=["team", "team_name"], errors="ignore")
df_gen = df_gen.merge(
    team_bios[["team_id","team","team_name","logo_url"]],
    on="team_id", how="left"
)

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
df_team_metric  = df_all[df_all["team_id"] == team_id]
df_team_general = df_gen[df_gen["team_id"] == team_id]
df_team_gen_trad = df_gen_trad[df_gen_trad["team_id"] == team_id]

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
ts_pct_sup = to_superscript(ts_pct_rank) if ts_pct_rank is not None else ""
win_loss_pct_color = rank_color(win_loss_pct_rank if win_loss_pct_rank is not None else 30, total_teams=30)
pts_color = rank_color(pts_rank if pts_rank is not None else 30, total_teams=30)
trb_color = rank_color(trb_rank if trb_rank is not None else 30, total_teams=30)
ast_color = rank_color(ast_rank if ast_rank is not None else 30, total_teams=30)
net_rating_color = rank_color(net_rating_rank if net_rating_rank is not None else 30, total_teams=30)
ts_pct_color = rank_color(ts_pct_rank if ts_pct_rank is not None else 30, total_teams=30)

    # Wins & losses (one row per team in df_team_general)

wins   = int(df_team_general["w"].iloc[0])
losses = int(df_team_general["l"].iloc[0])


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
w_color = rank_color(w_rank if w_rank is not None else 30, total_teams=30)
l_color = rank_color(l_rank if l_rank is not None else 30, total_teams=30)

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
    col1, col2, col3 = container.columns(3)
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

    # True Shooting Percentage (TS%)
    col3.markdown(
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
    

    col1, col2, col3, col4 = st.columns(4)
    col1.markdown(
        f"""
        <div>Pace</div>
        <span style="font-size:2rem;">{win_loss_pct:.3}</span>
        <sup style="color:{win_loss_pct_color}; font-size:1rem;">{win_loss_pct_sup}</sup>        
        </div>
        """,
        unsafe_allow_html=True,
    )
    col2.markdown(
        f"""
        <div>PPG</div>
        <span style="font-size:2rem;">{points_per_game}</span>
        <sup style="color:{pts_color}; font-size:1rem;">{pts_sup}</sup>        
        </div>
        """,
        unsafe_allow_html=True,
    )
    col3.markdown(
        f"""
        <div>RPG</div>
        <span style="font-size:2rem;">{rebounds_per_game}</span>
        <sup style="color:{trb_color}; font-size:1rem;">{trb_sup}</sup>
        
        </div>
        """,
        unsafe_allow_html=True,
    )
    col4.markdown(
        f"""
        <div>AST</div>
        <span style="font-size:2rem;">{assists_per_game}</span>
        <sup style="color:{ast_color}; font-size:1rem;">{ast_sup}</sup>        
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # PPG per Month chart
    # ─────────────────────────────────────────────────────────────────────────────
    chart_col1, chart_col2, chart_col3 = st.columns([1,1,1])

    # 1) PPG per Month
    with chart_col1:
        if "pts" in df_team_metric.columns:
            
            st.subheader("PPG per Month")
            df_chart = df_team_metric.copy()
            df_chart["game_date"] = pd.to_datetime(df_chart["game_date"], errors="coerce")
            pts_by_month = (
                df_chart
                .groupby(df_chart["game_date"].dt.month)["pts"]
                .mean()
                .round(2)
            )
            order = (
                df_chart
                .groupby(df_chart["game_date"].dt.month)["game_date"]
                .min()
                .sort_values()
                .index
            )
            season_month_names = [months[m] for m in order]
            pts_by_month = pts_by_month.reindex(order).rename(index=months).dropna()

            pts_by_month.index = pd.CategoricalIndex(
                pts_by_month.index,
                categories=season_month_names,
                ordered=True
            )
            st.bar_chart(pts_by_month)

    # 2) Net Rating per Month
    with chart_col2:
        if "net_rating" in df_team_metric.columns:
            st.subheader("Net Rating per Month")
            df_chart = df_team_metric.copy()
            df_chart["game_date"] = pd.to_datetime(df_chart["game_date"], errors="coerce")            
            
            nr_by_month = (
                df_chart
                .groupby(df_chart["game_date"].dt.month)["net_rating"]
                .mean()
                .round(2)
            )
            order = (
                df_chart 
                .groupby(df_chart["game_date"].dt.month)["game_date"]
                .min()
                .sort_values()
                .index
            )
            season_month_names = [months[m] for m in order]
            nr_by_month = nr_by_month.reindex(order).rename(index=months).dropna()

            nr_by_month.index = pd.CategoricalIndex(
                nr_by_month.index,
                categories=season_month_names,
                ordered=True
            )

            league_med = league_median["net_rating"]
             # Now plot with matplotlib
            # Prepare x positions
            vals = nr_by_month.values
            x = np.arange(len(vals))

            fig, ax = plt.subplots(figsize=(8, 7))

            # Plot each segment in its own color
            for i in range(len(vals) - 1):
                seg_x = x[i : i + 2]
                seg_y = vals[i : i + 2]
                # decide color by the midpoint of the segment
                seg_color = "tab:green" if seg_y.mean() >= league_med else "tab:red"
                ax.plot(seg_x, seg_y, color=seg_color, linewidth=2)

            # Plot markers also colored
            marker_cols = ["tab:green" if v >= league_med else "tab:red" for v in vals]
            ax.scatter(x, vals, color=marker_cols, s=20, zorder=3)

            # Dashed league-median line
            ax.axhline(league_med, color="gray", linestyle="--", label="League Median")

            # Decorations
            ax.set_xticks(x)
            ax.set_xticklabels(nr_by_month.index)
            ax.set_ylim(min(vals.min(), league_med) * 0.9,
                        max(vals.max(), league_med) * 1.1)
            ax.set_xlabel("Month")
            ax.set_ylabel("Net Rating")
            ax.legend(loc="upper right")

            # 4) Render it
            st.pyplot(fig)

    # 3) Clutch‐benchmarks
        # 3) Clutch-benchmarks radar
    # 3) Efficiency benchmarks → Radar (Spider) chart
    with chart_col3:
        st.subheader("Efficiency")

        # 1) Labels and column keys
        labels = list(eff_benchmark_metrics.keys())
        cols   = list(eff_benchmark_metrics.values())

        # 2) Build angles (one per axis) and close the loop
        angles = np.linspace(0, 2*np.pi, len(labels), endpoint=False).tolist()
        angles += angles[:1]

        # 3) Pull out percentile values (0–1) for team & median
        team_pct = [ league_percentile.loc[team_id, c] for c in cols ]
        med_pct  = [ 0.5 for _ in cols ]   # 50th percentile = league median
        team_pct += team_pct[:1]
        med_pct  += med_pct[:1]

        # 4) Make the polar plot
        fig, ax = plt.subplots(figsize=(8,8), subplot_kw=dict(polar=True))

        # league‐median polygon (light fill)
        ax.plot(angles, med_pct,    marker="o", label="League")
        ax.fill(angles, med_pct,    alpha=0.10)

        # team polygon (darker fill)
        ax.plot(angles, team_pct,   marker="o", label=team_name)
        ax.fill(angles, team_pct,   alpha=0.25)

        # 5) Decorations
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_ylim(0, 1)  # percentiles span exactly 0–1
        ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1))

        # 6) Render in Streamlit
        st.pyplot(fig)


    # ────────────────────────────────────────────────────────────────────────────
    # Advanced Stats KPIs
    # ────────────────────────────────────────────────────────────────────────────
    c9, c10, c11, c12 = st.columns(4)

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
            title="Shooting Efficiency Metrics"
        )

        # — 4) Add dashed vertical lines for league medians —
        n = len(metrics)
        for idx, m in enumerate(metrics):
            med = median_vals[m]
            # band is [idx/n, (idx+1)/n] in paper‐coords
            y0, y1 = idx/n, (idx+1)/n
            fig.add_shape(
                type="line",
                y0=med, y1=med,
                xref="paper", x0=y0, x1=y1,
                line=dict(color="gray", dash="dot")
            )

        st.plotly_chart(fig, use_container_width=True)
        
    
    
