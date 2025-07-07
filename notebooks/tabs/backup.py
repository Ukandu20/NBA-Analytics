import streamlit as st
from pathlib import Path
import pandas as pd
import altair as alt
import numpy as np
import math

# ─────────────────────────────────────────────────────────────────────────────
# Data Paths and Directories
# ─────────────────────────────────────────────────────────────────────────────
DATA_DIR   = Path(__file__).resolve().parents[2] / "data" / "processed"
TEAM_DIR   = DATA_DIR / "team_stats"
TEAM_DATA  = DATA_DIR / "teams_cleaned.csv"
PLAYER_DIR = DATA_DIR / "player_stats"
PLAYER_DATA = DATA_DIR / "all_players_cleaned.csv"

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

try:
    prev_season = seasons[seasons.index(season) + 1]
except (ValueError, IndexError):
    prev_season = None
# ─────────────────────────────────────────────────────────────────────────────
# Team picker
# ─────────────────────────────────────────────────────────────────────────────
team_bios = pd.read_csv(TEAM_DATA).rename(columns=str.lower)
team_codes = sorted(team_bios["team"].unique())  # three-letter codes
team_abbr = st.sidebar.selectbox("Team", team_codes, index=0)
team_info = team_bios.loc[team_bios["team"] == team_abbr].iloc[0]
team_id   = int(team_info["team_id"])
team_name = team_info["team_name"]

player_bios = pd.read_csv(PLAYER_DATA).rename(columns=str.lower).drop(columns=["player_id"]).rename(columns={"pid": "player_id"}, errors="ignore")
player_info = player_bios.loc[player_bios["team"] == team_abbr].iloc[0]
player_id = int(player_info["player_id"])
player_name = player_info["player"]

def render_tab(season_type: str):


    # ─────────────────────────────────────────────────────────────────────────────
    # Constants for folder structure
    # ─────────────────────────────────────────────────────────────────────────────
    DEFAULT_METRIC      = "adv_boxscores"
    DEFAULT_MEASURE     = "pergame"
    SEASON_TYPE = season_type
    DEFAULT_MODE = "advanced"

    #Base directory for team stats
    base_dir = (TEAM_DIR    / DEFAULT_METRIC    / season    / DEFAULT_MEASURE    / SEASON_TYPE)
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
    # ─────────────────────────────────────────────────────────────────────────────
    # Load advanced boxscores for this season
    # ─────────────────────────────────────────────────────────────────────────────

    boxscore_dir = (TEAM_DIR / DEFAULT_METRIC / season / DEFAULT_MEASURE / SEASON_TYPE)
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
        .loc[:, ["game_id","matchup","plus_minus","pts", "ast", "reb"]]     
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
    general_dir = (    TEAM_DIR    / "general"    / season    / DEFAULT_MEASURE    / SEASON_TYPE)
    gen_trad = (general_dir / "traditional.csv")
    gen_adv = (general_dir / "advanced.csv")
    gen_score = (general_dir / "scoring.csv")
    df_gen = pd.concat([
        pd.read_csv(f).assign(season=season)
        for f in general_dir.glob("*.csv")
    ], ignore_index=True)

    df_gen_trad = pd.read_csv(gen_trad)
    df_gen_adv = pd.read_csv(gen_adv)
    df_gen_scoring = pd.read_csv(gen_score)

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

    df_gen_scoring = df_gen_scoring.rename(columns=str.lower)
    df_gen_scoring = df_gen_scoring.drop(columns=["team"], errors="ignore")
    df_gen_scoring = df_gen_scoring.merge(
        team_bios[["team_id","team","logo_url"]],
        on="team_id", how="left"
    )
    df_gen_scoring = df_gen_scoring.merge(
        df_gen_trad[["team_id", "pts"]],
        on="team_id",
        how="left"
        )


    df_gen = df_gen.drop(columns=["team", "team_name"], errors="ignore")
    df_gen = df_gen.merge(
        team_bios[["team_id","team","team_name","logo_url"]],
        on="team_id", how="left"
    )

    # Load clutch stats for this season
    clutch_dir = (TEAM_DIR / "clutch" / season / DEFAULT_MEASURE / SEASON_TYPE)
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


    #base dir for player Data
    player_dir = (PLAYER_DIR    / DEFAULT_METRIC    / season    / DEFAULT_MEASURE    / SEASON_TYPE)

    df_player_all = pd.concat([
        pd.read_csv(f).assign(season=season)
        for f in player_dir.glob("*.csv")
    ], ignore_index=True)


    # drop any stray 'team' col, then merge in canonical team info
    df_player_all = df_player_all.drop(columns=["team"], errors="ignore")
    df_player_all = df_player_all.merge(
        player_bios[["player_id","team","headshot_url", "country","birthdate", "position_primary","position_alt", "experience", "draft_year", "draft_round", "draft_pick", "is_active", "is_free_agent","is_retired"]],
        on="player_id", how="left"
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # Load advanced boxscores for this season
    # ─────────────────────────────────────────────────────────────────────────────

    players_boxscore_dir = (PLAYER_DIR / DEFAULT_METRIC / season / DEFAULT_MEASURE / SEASON_TYPE)
    players_box_adv = (players_boxscore_dir / "advanced.csv")
    players_box_trad = (players_boxscore_dir / "traditional.csv")
    df_players_boxscore_adv = pd.read_csv(players_box_adv)
    df_players_boxscore_trad = pd.read_csv(players_box_trad)
    df_players_boxscore = pd.concat([
        pd.read_csv(f).assign(season=season)
        for f in players_boxscore_dir.glob("*.csv")
    ], ignore_index=True)

    df_players_boxscore = df_players_boxscore.rename(columns=str.lower)
    df_players_boxscore = df_players_boxscore.drop(columns=["team"], errors="ignore")
    df_players_boxscore = df_players_boxscore.merge(
        player_bios[["player_id","team","headshot_url", "country","birthdate", "position_primary","position_alt", "experience", "draft_year", "draft_round", "draft_pick", "is_active", "is_free_agent","is_retired"]],
        on="player_id", how="left"
    )

    df_players_boxscore_adv = df_players_boxscore_adv.rename(columns=str.lower)
    df_players_boxscore_adv = df_players_boxscore_adv.drop(columns=["team"], errors="ignore")
    df_players_boxscore_adv = df_players_boxscore_adv.merge(
        player_bios[["player_id","team","headshot_url", "country","birthdate", "position_primary","position_alt", "experience", "draft_year", "draft_round", "draft_pick", "is_active", "is_free_agent","is_retired"]],
        on="player_id", how="left"
    )

    df_players_boxscore_trad = df_players_boxscore_trad.rename(columns=str.lower)
    df_players_boxscore_trad = df_players_boxscore_trad.drop(columns=["team"], errors="ignore")
    df_players_boxscore_trad = df_players_boxscore_trad.merge(
        player_bios[["player_id","team","headshot_url", "country","birthdate", "position_primary","position_alt", "experience", "draft_year", "draft_round", "draft_pick", "is_active", "is_free_agent","is_retired"]],
        on="player_id", how="left"
    )

    pm = (
        df_players_boxscore_trad
        .loc[:, ["game_id","matchup","plus_minus","pts", "ast", "reb"]]     
    )
    df_players_boxscore_adv = df_players_boxscore_adv.merge(
        pm,
        on=["game_id", "matchup"],
        how="left"    
    )
    df_players_boxscore_adv = df_players_boxscore_adv.drop_duplicates(subset=["game_id", "matchup"])

    players_general_dir = (    PLAYER_DIR    / "general"    / season    / DEFAULT_MEASURE    / SEASON_TYPE)
    players_gen_trad = (players_general_dir / "traditional.csv")
    players_gen_adv = (players_general_dir / "advanced.csv")
    players_gen_score = (players_general_dir / "scoring.csv")
    df_players_gen = pd.concat([
        pd.read_csv(f).assign(season=season)
        for f in players_general_dir.glob("*.csv")
    ], ignore_index=True)

    df_players_gen_trad = pd.read_csv(players_gen_trad)
    df_players_gen_adv = pd.read_csv(players_gen_adv)
    df_players_gen_scoring = pd.read_csv(players_gen_score)

    df_players_gen_trad = df_players_gen_trad.rename(columns=str.lower)
    df_players_gen_trad = df_players_gen_trad.drop(columns=["team"], errors="ignore")
    df_players_gen_trad = df_players_gen_trad.merge(
        player_bios[["player_id","team","headshot_url", "country","birthdate", "position_primary","position_alt", "experience", "draft_year", "draft_round", "draft_pick", "is_active", "is_free_agent","is_retired"]],
        on="player_id", how="left"
    )

    df_players_gen_adv = df_players_gen_adv.rename(columns=str.lower)
    df_players_gen_adv = df_players_gen_adv.drop(columns=["team"], errors="ignore")
    df_players_gen_adv = df_players_gen_adv.merge(
        player_bios[["player_id","team","headshot_url", "country","birthdate", "position_primary","position_alt", "experience", "draft_year", "draft_round", "draft_pick", "is_active", "is_free_agent","is_retired"]],
        on="player_id", how="left"
    )
    # bring only the plus_minus column into your advanced‐summary table
    df_players_gen_adv = df_players_gen_adv.merge(
        df_players_gen_trad[["player_id", "plus_minus"]],
        on="player_id",
        how="left"
    )

    df_players_gen_scoring = df_players_gen_scoring.rename(columns=str.lower)
    df_players_gen_scoring = df_players_gen_scoring.drop(columns=["team"], errors="ignore")
    df_players_gen_scoring = df_players_gen_scoring.merge(
        player_bios[["player_id","team","headshot_url", "country","birthdate", "position_primary","position_alt", "experience", "draft_year", "draft_round", "draft_pick", "is_active", "is_free_agent","is_retired"]],
        on="player_id", how="left"
    )
    df_players_gen_scoring = df_players_gen_scoring.merge(
        df_players_gen_trad[["player_id", "pts"]],
        on="player_id",
        how="left"
        )


    df_players_gen = df_players_gen.drop(columns=["team", "team_name"], errors="ignore")
    df_players_gen = df_players_gen.merge(
        player_bios[["player_id","team","headshot_url", "country","birthdate", "position_primary","position_alt", "experience", "draft_year", "draft_round", "draft_pick", "is_active", "is_free_agent","is_retired"]],
        on="player_id", how="left"
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


    pra_metrics = {
            "pts": "pts",
            "rebounds": "reb",
            "assists": "ast"
            }
    league_pra = (
        df_boxscore_adv
        .set_index("team_id")[list(pra_metrics.values())]
    )
    pra_median = league_pra.median()
    pra_percentile = league_pra.rank(pct=True).round(3)

    if prev_season:
        # a) Last season’s summary
        prev_gen_dir = TEAM_DIR / "general" / prev_season / DEFAULT_MEASURE / SEASON_TYPE
        df_prev_gen_trad = (
            pd.read_csv(prev_gen_dir / "traditional.csv")
            .rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
            .merge(team_bios[["team_id","team","team_name"]], on="team_id")
        )
        prev_team_gen = df_prev_gen_trad[df_prev_gen_trad["team_id"] == team_id]
        prev_win_pct = round((prev_team_gen["w_pct"].iloc[0])*100, 3) if not prev_team_gen.empty else None
        
        df_prev_gen_adv = (
            pd.read_csv(prev_gen_dir / "advanced.csv")
            .rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
            .merge(team_bios[["team_id","team","team_name"]], on="team_id")
        )
        prev_team_gen_adv = df_prev_gen_adv[df_prev_gen_adv["team_id"] == team_id]
        # b) Last season’s clutch adv for net rating
        prev_clutch_dir = TEAM_DIR / "clutch" / prev_season / DEFAULT_MEASURE / SEASON_TYPE
        df_prev_clutch_adv = (
            pd.read_csv(prev_clutch_dir / "advanced.csv")
            .rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
            .merge(team_bios[["team_id","team","team_name"]], on="team_id")
        )
        prev_net_rating = (
            df_prev_clutch_adv.loc[df_prev_clutch_adv["team_id"]==team_id, "net_rating"]
            .iloc[0]
            if team_id in df_prev_clutch_adv["team_id"].values else None
        )

        prev_off_rating = (
            df_prev_gen_adv.loc[df_prev_gen_adv["team_id"]==team_id, "off_rating"]
            .iloc[0]
            if team_id in df_prev_gen_adv["team_id"].values else None
        )

        prev_def_rating = (
            df_prev_gen_adv.loc[df_prev_gen_adv["team_id"]==team_id, "def_rating"]
            .iloc[0]
            if team_id in df_prev_gen_adv["team_id"].values else None
        )
    else:
        prev_win_pct = prev_net_rating = prev_off_rating = prev_def_rating = None


    @st.cache_data
    def get_team_leaders(
        df_player_all: pd.DataFrame,
        team_id: int,
        *,
        min_games: int | None = None,
        season_type: str = "regular",
    ) -> dict[str, pd.DataFrame]:
        """Return top-3 PTS/REB/AST leaders for ``team_id`` along with league
        ranks.

        Parameters
        ----------
        df_player_all : pd.DataFrame
            Per-game player box score data.
        team_id : int
            Identifier for the team to compute leaders for.
        min_games : int | None, optional
            Minimum games played required to qualify. If ``None``, the threshold
            defaults based on ``season_type``.
        season_type : str, optional
            ``"regular"`` for regular-season data or ``"playoffs"`` for
            postseason data. When ``min_games`` is ``None``, the default
            threshold is ``50`` games for the regular season and ``2`` for the
            playoffs.

        Returns
        -------
        dict[str, pd.DataFrame]
            Three tables keyed by ``"pts"``, ``"reb"`` and ``"ast"``. Each table
            contains ``player``, ``headshot_url``, the per-game value, the
            player’s league ``rank`` and ``total_players`` indicating how many
            players met the game threshold.
        """

        if min_games is None:
            min_games = 4 if season_type.lower() == "playoffs" else 50

        # ── Compute league-wide averages and ranks ────────────────────────
        league_games = (
            df_player_all
            .groupby(["player_id", "player", "headshot_url"])["game_id"]
            .nunique()
            .reset_index(name="games")
        )
        league_avgs = (
            df_player_all
            .groupby(["player_id", "player", "headshot_url"])[["pts", "reb", "ast"]]
            .mean()
            .reset_index()
        )
        league_df = league_avgs.merge(league_games, on=["player_id", "player", "headshot_url"])
        league_df = league_df.loc[league_df["games"] >= min_games]
        eligible_players = len(league_df)
        for stat in ["pts", "reb", "ast"]:
            league_df[f"{stat}_rank"] = league_df[stat].rank(method="min", ascending=False).astype(int)

        # ── Compute team-only averages ────────────────────────────────────
        team_df = df_player_all.loc[df_player_all["team_id"] == team_id]
        team_games = (
            team_df
            .groupby(["player_id", "player", "headshot_url"])["game_id"]
            .nunique()
            .reset_index(name="games")
        )
        team_avgs = (
            team_df
            .groupby(["player_id", "player", "headshot_url"])[["pts", "reb", "ast"]]
            .mean()
            .reset_index()
        )
        team_stats = team_avgs.merge(team_games, on=["player_id", "player", "headshot_url"])
        team_stats = team_stats.loc[team_stats["games"] >= min_games]

        # attach league ranks
        team_stats = team_stats.merge(
            league_df[["player_id", "pts_rank", "reb_rank", "ast_rank"]],
            on="player_id",
            how="left",
        )

        # ── pick top 3 for each stat with rank ────────────────────────────
        leaders: dict[str, pd.DataFrame] = {}
        for stat in ["pts", "reb", "ast"]:
            cols = ["player", "headshot_url", stat, f"{stat}_rank"]
            top3 = (
                team_stats
                .nlargest(3, stat)[cols]
                .rename(columns={f"{stat}_rank": "rank"})
                .reset_index(drop=True)
            )
            top3["total_players"] = eligible_players
            leaders[stat] = top3

        return leaders

    # ─────────────────────────────────────────────────────────────────────────────
    # Filter both tables to the selected team
    # ─────────────────────────────────────────────────────────────────────────────
    df_team_metric  = df_boxscore_adv[df_boxscore_adv["team_id"] == team_id]

    df_team_general = df_gen[df_gen["team_id"] == team_id]
    df_team_gen_trad = df_gen_trad[df_gen_trad["team_id"] == team_id]
    df_team_gen_adv = df_gen_adv[df_gen_adv["team_id"] == team_id]
    df_team_adv_box = df_boxscore_adv[df_boxscore_adv["team_id"] == team_id]
    df_team_gen_scoring = df_gen_scoring[df_gen_scoring["team_id"] == team_id]

    df_clutch = df_clutch[df_clutch["team_id"] == team_id]
    df_clutch_adv = df_clutch_adv[df_clutch_adv["team_id"] == team_id]
    df_clutch_trad = df_clutch_trad[df_clutch_trad["team_id"] == team_id]

    df_players_boxscore_adv = df_players_boxscore_adv[df_players_boxscore_adv["team_id"] == team_id]
    df_players_boxscore_trad = df_players_boxscore_trad[df_players_boxscore_trad["team_id"] == team_id]

    # ─────────────────────────────────────────────────────────────
    # Stat Leaders (Per Game, min 50 games) – use the helper func
    # ─────────────────────────────────────────────────────────────
    leaders = get_team_leaders(
        df_player_all,
        team_id,
        season_type=season_type,
    )
    top3_pts = leaders["pts"]   # columns: ["player","headshot_url","pts","rank"]
    top3_reb = leaders["reb"]   # columns: ["player","headshot_url","reb","rank"]
    top3_ast = leaders["ast"]   # columns: ["player","headshot_url","ast","rank"]

    
    df_players_gen_adv = df_players_gen_adv[df_players_gen_adv["team_id"] == team_id]
    df_players_gen_trad = df_players_gen_trad[df_players_gen_trad["team_id"] == team_id]
    df_players_gen_scoring = df_players_gen_scoring[df_players_gen_scoring["team_id"] == team_id]


    


    

        # … after computing `league` …
    if team_id not in league.index:
        st.warning(f"{team_name} didn’t make the {season} {season_type.replace('_',' ')}.")
        return
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
    headshot_url = player_info["headshot_url"]

    # Helpers (as before)
    _SUP_MAP = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")
    @st.cache_data
    def to_superscript(n: int) -> str:
        return str(n).translate(_SUP_MAP)

    @st.cache_data
    def rank_color(rank: int, total_teams: int = 30) -> str:
        third = total_teams // 3
        if   rank <= third:     return "green"
        elif rank <= 2*third:   return "orange"
        else:                   return "red"

    # 1) First, make sure every row has explicit home/away teams
    @st.cache_data
    def parse_matchup(m: str) -> tuple[str,str]:
        m = m.replace("vs.", "vs").replace(" @ ", "@").replace(" vs ", "vs").strip()
        if "@" in m:
            away, home = m.split("@")
        else:
            home, away = m.split("vs")
        return away.strip(), home.strip()

    @st.cache_data
    def add_home_away(df: pd.DataFrame) -> pd.DataFrame:
        
        """Add away_team, home_team and opp_team columns to ``df``."""

        df[["away_team", "home_team"]] = df["matchup"].apply(parse_matchup).tolist()

        # add opp_team which is the opponent the team faced
        # Opponent is the other side of the venue
        df["opp_team"] = df.apply(
            lambda r: r.away_team if r.home_team == r.team else r.home_team,
            axis=1,
        )
        return df


    df_boxscore_adv = add_home_away(df_boxscore_adv)


    df_boxscore_adv["wl"] = df_boxscore_adv["wl"].str.upper()  # normalize wins/losses

    # 2) Compute every team’s home W/L totals
    home = df_team_metric[df_team_metric["matchup"].str.contains("vs", case=False)]
    home_summary = (
        home
        .groupby("team_id")["wl"]
        .value_counts()
        .unstack(fill_value=0)
        .rename(columns={"W":"home_w","L":"home_l"})
        .reindex(columns=["home_w","home_l"], fill_value=0)
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
        .reindex(columns=["away_w","away_l"], fill_value=0)
    )
    a_wins = away_summary["away_w"].sum()
    a_losses = away_summary["away_l"].sum()


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
    win_loss_pct = round(win_loss_pct * 100, 3)

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

    player_ppg = (
        df_players_gen["pts"].mean()
        if "pts" in df_players_gen.columns and len(df_players_gen)>0
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

    player_reb = (
        df_players_gen["reb"].mean()
        if "reb" in df_players_gen.columns and len(df_players_gen)>0
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

    player_ast = (
        df_players_gen["ast"].mean()
        if "ast" in df_players_gen.columns and len(df_players_gen)>0
        else 0.0
        )

    ast_rank = (
            df_team_gen_trad["ast_rank"].iloc[0]
            if "ast_rank" in df_team_gen_trad.columns and len(df_team_gen_trad)>0
            else None
        )

    net_rating = (
            df_team_gen_adv["net_rating"].iloc[0]
            if "net_rating" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
            else None
    )

    net_rating_rank = (
            df_team_gen_adv["net_rating_rank"].iloc[0]
            if "net_rating_rank" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
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
            round((df_team_gen_adv["ts_pct"].mean())* 100, 3)
            if "ts_pct" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
            else 0.0
        )
    

    ts_pct_rank = (
            df_team_gen_adv["ts_pct_rank"].iloc[0]
            if "ts_pct_rank" in df_team_gen_adv.columns and len(df_team_gen_adv)>0
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
    # Initialize prefixes with default values to avoid unbound errors
    vic_prefix = ""
    def_prefix = ""

    if "plus_minus" in df_team_metric.columns and not df_team_metric.empty:
        # Make sure we have away_team/home_team
        # (skip re-parsing matchup if you've already called add_home_away)
        df = df_team_metric.copy()

        idx_max = df["plus_minus"].idxmax()
        vic_row = df.loc[idx_max]
        largest_victory         = int(df.at[idx_max, "plus_minus"])
        largest_victory_opponent = df.at[idx_max, "opp_team"]
        largest_victory_pts     = int(df.at[idx_max, "pts"])
        victory_opp_points      = largest_victory_pts - largest_victory
        vic_prefix = "vs. " if vic_row["home_team"] == team_abbr else "@"
        

        # Biggest defeat
        idx_min = df["plus_minus"].idxmin()
        def_row = df.loc[idx_min]
        biggest_defeat          = int(df.at[idx_min, "plus_minus"])
        biggest_defeat_opponent = df.at[idx_min, "opp_team"]
        biggest_defeat_pts      = int(df.at[idx_min, "pts"])
        defeat_opp_points       = biggest_defeat_pts - biggest_defeat
        def_prefix = "vs. " if def_row["home_team"] == team_abbr else "@"

    else:
        # Fallback if no plus_minus column or empty
        largest_victory, largest_victory_opponent = None, ""
        biggest_defeat, biggest_defeat_opponent = None, ""
        largest_victory_pts, victory_opp_points = 0, 0
        biggest_defeat_pts, defeat_opp_points = 0, 0
        vic_prefix = ""
        def_prefix = ""






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

    # 3) Compute deltas + arrows
    def make_delta(current, previous):
        if previous is None: return "", "", ""
        d = current - previous
        arrow = "↑" if d > 0 else ("↓" if d < 0 else "→")
        # for pct, multiply by 100; for net rating, show raw
        fmt = f"{abs(d)*100:.1f}%" if 0 <= current <= 1 else f"{abs(d):.1f}"
        color = "green" if d > 0 else ("red" if d < 0 else "gray")
        return arrow, fmt, color

    win_arrow, win_delta, win_color = make_delta(win_loss_pct, prev_win_pct)
    net_arrow, net_delta, net_color = make_delta(net_rating, prev_net_rating)
    off_arrow, off_delta, off_color = make_delta(off_rating, prev_off_rating)
    def_arrow, def_delta, def_color = make_delta(def_rating, prev_def_rating)

    total_teams = len(league)

    



    

    container = st.container(border=True)
    with container:
        container.markdown(
            f"""
            <div style="display: flex; align-items: center; justify-content: space-between;">
            <h1 style="margin: 0;">How good were the {team_name} in {season}?</h1>
            <img src="{logo_url}" alt="{team_abbr} logo" style="height:50px">
            </div>
            """,
            unsafe_allow_html=True
        )
        col1, col2, col3, col4, col5= container.columns(5)

        delta_win = f"{win_arrow}{win_delta}% from {prev_season} and #{win_loss_pct_rank}" if win_delta or win_loss_pct_rank else None
        info_win = "The percentage of games played that a team has won"
        col1.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Win %
                <span
                title="{info_win}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {win_loss_pct:.1f}
                </div>
                <div style="font-size: 0.9rem; color: {win_color}; font-weight: 500; margin-top: 0.25rem;">
                {win_arrow}{win_delta}% from {prev_season}, <span style="color: #28a745">#{win_loss_pct_rank} of {total_teams}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        #Net Rating
        delta_text = f"{net_rating_rank} in the league" if net_rating_rank else None
                #offensive Rating
        delta_off = f"{off_delta} from {prev_season}" if off_delta else None
        info_off = "The total number of points scored per 100 possessions"
        col2.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Offensive Rating
                <span
                title="{info_off}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {off_rating:.1f}
                </div>
                <div style="font-size: 0.9rem; color: #28a745; font-weight: 500; margin-top: 0.25rem;">
                #{off_rating_rank} of {total_teams} 
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        #Defensive Rating
        delta_def = f"{def_delta} from {prev_season}" if def_delta else None
        info_def = "The total number of points allowed per 100 possessions"
        col3.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Defensive Rating
                <span
                title="{info_def}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {def_rating:.1f}
                </div>
                <div style="font-size: 0.9rem; color: #28a745; font-weight: 500; margin-top: 0.25rem;">
                #{def_rating_rank} of {total_teams} 
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        info_net = "Team’s point differential per 100 possessions"
        col4.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Net Rating
                <span
                title="{info_net}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {net_rating:.1f}
                </div>
                <div style="font-size: 0.9rem; color: #28a745; font-weight: 500; margin-top: 0.25rem;">
                #{net_rating_rank} of {total_teams} 
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        
        # True Shooting Percentage (TS%)
        info_ts = "An efficiency metric that weights three-point shots and free throws for their extra value, alongside traditional two-point field goals"
        col5.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                True Shooting %
                <span
                title="{info_ts}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {ts_pct:.1f}
                </div>
                <div style="font-size: 0.9rem; color: #28a745; font-weight: 500; margin-top: 0.25rem;">
                #{ts_pct_rank} of {total_teams} 
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)


        # ─────────────────────────────────────────────────────────────────────────────
        # Additional team-level KPIs
        # ─────────────────────────────────────────────────────────────────────────────
        

        col1, col2, col3, col4 = st.columns([2,2,1,1])
        col1.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                margin-top:1rem;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Biggest win
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 2rem; font-weight: bold; color: #ddd; line-height: 1;">
                <span style="color:green">{largest_victory_pts} - {victory_opp_points}</span> {vic_prefix} {largest_victory_opponent}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)
        
        col2.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                margin-top:1rem;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Heaviest Defeat
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 2rem; font-weight: bold; color: #ddd; line-height: 1;">
                <span style="color:red">{biggest_defeat_pts} - {defeat_opp_points}</span> {def_prefix} {biggest_defeat_opponent}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        col3.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                margin-top:1rem;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Home Record
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 2rem; font-weight: bold; color: #ddd; line-height: 1;">
                <span style="font-size:2rem;">{h_wins}-{h_losses}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        col4.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                margin-top:1rem;
                margin-bottom:1rem;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Away Record
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 2rem; font-weight: bold; color: #ddd; line-height: 1;">
                <span style="font-size:2rem;">{a_wins}-{a_losses}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        # scale headshots down 12× then 1.5×
        orig_w, orig_h = 1040, 760
        new_w = round(orig_w / (15))
        new_h = round(orig_h / (15))

        # define titles, suffixes and their dfs
        stat_info = [
            ("Scoring Leaders", "PPG", "pts", top3_pts),
            ("Rebound Leaders", "RPG", "reb", top3_reb),
            ("Assist Leaders", "APG", "ast", top3_ast),
        ]

        cols = st.columns(3)
        for col, (title, suffix, colname, df_lead) in zip(cols, stat_info):
            with col:
                # outer KPI-style box
                html = f'''
                <div style="
                    border:1px solid #555;
                    border-radius:8px;
                    padding:1rem;
                    box-sizing:border-box;
                    margin-bottom:1rem;
                ">
                <h4 style="margin:0 0 0.75rem 0; color:#fff;">{title}</h4>
                '''
                # each of the top 3 players
                for i, row in df_lead.iterrows():
                    rank_html = (
                        "<span style='color:#daa520;font-weight:600;'>👑 led the league</span>"
                        if row["rank"] == 1
                        else f"<span style='color:#aaa;font-size:0.8rem;'>#{int(row['rank'])} of {int(row['total_players'])} players</span>"
                    )
                    html += f'''
                <div style="
                    display:flex;
                    align-items:center;
                    justify-content:space-between;
                    margin-bottom:0.75rem;
                ">
                    <div style="display:flex; align-items:center;">
                    <img src="{row['headshot_url']}"
                        style="
                            border-radius:50%;
                            width:{new_w}px;
                            height:{new_h}px;
                            object-fit:cover;
                            margin-right:0.75rem;
                        ">
                    <div style="display:flex; flex-direction:column;">
                        <span style="font-weight:600; color:#fff;">
                        {row['player']}
                        </span>
                        {rank_html}
                    </div>
                    </div>
                    <span style="font-weight:700; color:#0a7dfa;">
                    {row[colname]:.1f} {suffix}
                    </span>
                </div>
                '''
                html += "</div>"
                st.markdown(html, unsafe_allow_html=True)

                
                


        col1, col2= st.columns([1,1])
        with col1:
            months_renamed = {
                "JAN": "January", "FEB": "February", "MAR": "March", "APR": "April", "MAY": "May", "JUN": "June", "JUL": "July", "AUG": "August", "SEP": "September", "OCT": "October", "NOV": "November", "DEC": "December"
                }
            if {"pts", "reb", "ast", "game_date"}.issubset(df_team_metric.columns):
                # 1) Prepare the data
                df = df_team_metric.copy()
                df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                df["month_num"]  = df["game_date"].dt.month

                by_month = (
                    df
                    .groupby("month_num")
                    .agg(
                        PPG=("pts", "mean"),
                        RPG=("reb", "mean"),
                        APG=("ast", "mean"),
                    )
                    .round(2)
                    .reset_index()  # month_num column retained
                )

                # Preserve season order via earliest date per month
                order = (
                    df
                    .groupby("month_num")["game_date"]
                    .min()
                    .sort_values()
                    .index
                    .tolist()
                )

                # Map to labels and define x-axis domain
                by_month["month"] = by_month["month_num"].map(months)

                                #Rename the months
                by_month["month_full"] = by_month["month"].map(lambda m: months_renamed[m])
                x_domain = [months[m] for m in order]

                # Melt into long form for grouped bars
                df_long = by_month.melt(
                    id_vars=["month"],
                    value_vars=["PPG", "RPG", "APG"],
                    var_name="Metric",
                    value_name="Value",
                )

                # 2) Compute league-wide medians (replace df_gen if needed)
                medians = {
                    "PPG": df_gen["pts"].median(),
                    "RPG": df_gen["reb"].median(),
                    "APG": df_gen["ast"].median(),
                }

                medians = {k: round(v, 3) for k, v in medians.items()}

                df_plot = df_long.copy()
                df_plot["median"] = df_plot["Metric"].map(medians)
                df_plot["Diff"]   = df_plot["Value"] - df_plot["median"]

                # flag the max/min Diff _within each_ Metric
                df_plot["is_max"] = (
                    df_plot.groupby("Metric")["Diff"]
                        .transform("max") == df_plot["Diff"]
                )
                df_plot["is_min"] = (
                    df_plot.groupby("Metric")["Diff"]
                        .transform("min") == df_plot["Diff"]
                )


                        # ——— 2) Compute best/worst PPG & league median ———
                best_idx   = by_month["PPG"].idxmax()
                worst_idx  = by_month["PPG"].idxmin()
                best_row   = by_month.loc[best_idx]
                worst_row  = by_month.loc[worst_idx]

                best_month   = best_row["month_full"]
                best_points  = best_row["PPG"]
                worst_month  = worst_row["month_full"]
                worst_points = worst_row["PPG"]

                best_ast_idx   = by_month["APG"].idxmax()
                worst_ast_idx  = by_month["APG"].idxmin()
                best_ast_row   = by_month.loc[best_ast_idx]
                worst_ast_row  = by_month.loc[worst_ast_idx]

                best_ast_month   = best_ast_row["month"]
                best_ast  = best_ast_row["APG"]
                worst_ast_month  = worst_ast_row["month"]
                worst_ast = worst_ast_row["APG"]

                best_reb_idx   = by_month["RPG"].idxmax()
                worst_reb_idx  = by_month["RPG"].idxmin()
                best_reb_row   = by_month.loc[best_reb_idx]
                worst_reb_row  = by_month.loc[worst_reb_idx]

                best_reb_month   = best_reb_row["month"]
                best_reb  = best_reb_row["RPG"]
                worst_reb_month  = worst_reb_row["month"]
                worst_reb = worst_reb_row["RPG"]


                # 0) one multi‐selection for both legend & bar‐clicks
                sel = alt.selection_multi(
                    fields=["Metric"],
                    bind="legend",    # clicking legend toggles
                    on="click",       # clicking marks toggles
                    empty="all"       # when nothing’s selected, show everything
                )

                # 3) Build bars with that single sel
                bars = (
                    alt.Chart(df_long)
                    .mark_bar()
                    .add_selection(sel)
                    .encode(
                        x=alt.X("month:N", sort=x_domain, title="Month"),
                        xOffset="Metric:N",
                        y=alt.Y("Value:Q", title="Avg per Game"),
                        color=alt.Color("Metric:N", legend=alt.Legend(title="Metric")),
                        opacity=alt.condition(sel, alt.value(1.0), alt.value(0.05))
                    )
                )

                # 4) Highlights also use sel
                highlight_max = (
                    alt.Chart(df_plot[df_plot["is_max"]])
                    .mark_bar()
                    .add_selection(sel)
                    .encode(
                        x=alt.X("month:N", sort=x_domain),
                        xOffset="Metric:N",
                        y="Value:Q",
                        color=alt.value("#7defa1"),
                        opacity=alt.condition(sel, alt.value(1.0), alt.value(0.05))
                    )
                )
                highlight_min = (
                    alt.Chart(df_plot[df_plot["is_min"]])
                    .mark_bar()
                    .add_selection(sel)
                    .encode(
                        x=alt.X("month:N", sort=x_domain),
                        xOffset="Metric:N",
                        y="Value:Q",
                        color=alt.value("#ff2b2b"),
                        opacity=alt.condition(sel, alt.value(1.0), alt.value(0.05))
                    )
                )

                # 5) Median lines only show for the currently selected metric
                df_medians = pd.DataFrame({
                    "Metric": list(medians.keys()),
                    "median": list(medians.values())
                })
                rules = (
                    alt.Chart(df_medians)
                    .mark_rule(color="gray", strokeDash=[4,4])
                    .add_selection(sel)
                    .encode(
                        y="median:Q",
                        detail="Metric:N",
                        opacity=alt.condition(sel, alt.value(1), alt.value(0))
                    )
                )


                # 6) Layer and render
                chart = alt.layer(bars, highlight_max, highlight_min, rules).properties(
                    title="Monthly PPG / RPG / APG"
                )
                st.altair_chart(chart, use_container_width=True) # type: ignore[arg-type]
                st.markdown(
                    f"""
                    <div style="
                        font-size: 0.875rem;
                        color: #888;
                        line-height: 1.4;
                        margin-top: 0.2rem;
                        margin-bottom: 2rem;
                        padding: 0 0;
                    ">
                    This shows the points, rebounds and assists on a monthly average. <br>
                    
                    league Medians: {medians}

                    </div>
                    """,
                    unsafe_allow_html=True
                )





        #Point differential per month
        with col2:
            months_renamed = {
                "JAN": "January", "FEB": "February", "MAR": "March", "APR": "April", "MAY": "May", "JUN": "June", "JUL": "July", "AUG": "August", "SEP": "September", "OCT": "October", "NOV": "November", "DEC": "December"
                }
            if "plus_minus" in df_team_metric.columns:
                # 1) Prepare the data
                df = df_team_metric.copy()
                df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                diff_by_month = (
                    df
                    .groupby(df["game_date"].dt.month)["net_rating"]
                    .mean()
                    .round(1)
                    .reset_index(name="Diff")
                )
                # preserve season order (Oct→Apr)
                order = (
                    df
                    .groupby(df["game_date"].dt.month)["game_date"]
                    .min()
                    .sort_values()
                    .index
                )
                diff_by_month["month"] = diff_by_month["game_date"].map(lambda m: months[m])

                #Rename the months
                diff_by_month["month_full"] = diff_by_month["month"].map(lambda m: months_renamed[m])
                x_domain = [months[m] for m in order]

                # identify best/worst
                max_diff = diff_by_month["Diff"].max()
                min_diff = diff_by_month["Diff"].min()

                best_idx = diff_by_month["Diff"].idxmax()
                worst_idx = diff_by_month["Diff"].idxmin()
                best_month = diff_by_month.loc[best_idx, "month_full"]
                best_value = diff_by_month.loc[best_idx, "Diff"]
                worst_month = diff_by_month.loc[worst_idx, "month_full"]
                worst_value = diff_by_month.loc[worst_idx, "Diff"]

                median_diff = df_gen["net_rating"].median()

                # 2) Base bars (light gray)
                base = alt.Chart(diff_by_month).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain, title="Month"),
                    y=alt.Y("Diff:Q", title="Avg Margin"),
                    color=alt.value("#83c9ff")
                )
                # 3) Highlight best (green) & worst (red)
                highlight_max = alt.Chart(diff_by_month[diff_by_month.Diff == max_diff]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="Diff:Q",
                    color=alt.value("#7defa1")
                )
                highlight_min = alt.Chart(diff_by_month[diff_by_month.Diff == min_diff]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="Diff:Q",
                    color=alt.value("#ff2b2b")
                )
                # 4) Median rule + label
                median_rule = alt.Chart(pd.DataFrame({"Diff":[median_diff]})).mark_rule(
                    color="gray", strokeDash=[4,4]
                ).encode(y="Diff:Q")
                median_text = alt.Chart(pd.DataFrame({"Diff":[median_diff]})).mark_text(
                    align="left", dx=3, dy=-5, color="gray"
                ).encode(
                    y="Diff:Q",
                )

                chart = (
                    base
                    + highlight_max
                    + highlight_min
                    + median_rule
                    + median_text
                ).properties(
                    title="Monthly Net Rating",
                    width="container"
                )

                st.altair_chart(chart, use_container_width=True) # type: ignore[arg-type] 

                # caption with real line breaks
                st.markdown(
                    f"""
                    <div style="
                        font-size: 0.875rem;
                        color: #888;
                        line-height: 1.4;
                        margin-top: 0.2rem;
                        margin-bottom: 2rem;
                        padding: 0 0;
                    ">
                    This shows the teams net rating on a monthly average. <br>
                    The league median is {median_diff:.1f}pts → (dashed line).
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        # ─────────────────────────────────────────────────────────────────────────────
        # PPG per Month chart
        # ─────────────────────────────────────────────────────────────────────────────

        # ────────────────────────────────────────────────────────────────────────────
        # Advanced Stats KPIs
        # ────────────────────────────────────────────────────────────────────────────
        c9, c10= st.columns([1,1])

        #Shooting Efficiency Group bar Chart containing eFG%, TS%, FT% and 3PT%
        with c9:
            # 1) Read in and compute league‐wide medians
            df_trad_all = (
                pd.read_csv(gen_trad)
                .rename(columns=str.lower)
                .drop(columns=["team"], errors="ignore")
            )
            df_adv_all = (
                pd.read_csv(clutch_adv)
                .rename(columns=str.lower)
                .drop(columns=["team"], errors="ignore")
            )
            median_vals = {
                "efg_pct": df_adv_all["efg_pct"].median(),
                "ts_pct":  df_adv_all["ts_pct"].median(),
                "ft_pct":  df_trad_all["ft_pct"].median(),
                "fg3_pct": df_trad_all["fg3_pct"].median(),
            }

            # 2) Pull your team’s shooting metrics and melt to long form
            shoot_eff = (
                df_team_gen_trad[["team","team_id","ft_pct","fg3_pct"]]
                .merge(df_clutch_adv[["team_id","efg_pct","ts_pct"]], on="team_id")
            )
            metrics = ["efg_pct","ts_pct","ft_pct","fg3_pct"]
            df_long = shoot_eff.melt(
                id_vars=["team"],
                value_vars=metrics,
                var_name="Metric",
                value_name="Pct"
            )

            # 3) Build grouped bars with xOffset
            color_scale = alt.Scale(
                domain=metrics,
                range=["#83c9ff","#ffb366","#7defa1","#ff7f7f"]
            )
            bars = (
                alt.Chart(df_long)
                .mark_bar(size=40)
                .encode(
                    x=alt.X("team:N", title="Team"),
                    xOffset="Metric:N",
                    y=alt.Y("Pct:Q", title="Shooting %"),
                    color=alt.Color("Metric:N", scale=color_scale, legend=alt.Legend(title="Metric"))
                )
            )

            # 4) Add dashed‐gray median lines & labels for each metric
            rules = []
            texts = []
            for m in metrics:
                med = median_vals[m]
                df_med = pd.DataFrame({"median":[med]})
                rules.append(
                    alt.Chart(df_med)
                    .mark_rule(color="gray", strokeDash=[4,4])
                    .encode(y="median:Q")
                )
                texts.append(
                    alt.Chart(df_med)
                    .mark_text(dx=3, dy=-5, color="gray")
                    .encode(
                        y="median:Q",
                        text=alt.value(f"{m} med → {med:.1f}%")
                    )
                )

            chart = alt.layer(bars, *rules).properties(
                title="Shooting Efficiency",
                width="container",
            )

            st.altair_chart(chart, use_container_width=True)  # type: ignore[arg-type] 

            # 2) Friendly labels for each metric
            metric_info = {
                "efg_pct": ("effective field goal",   "shots"),
                "ts_pct":  ("true shooting",           "looks"),
                "ft_pct":  ("free throw",              "free throws"),
                "fg3_pct": ("three-point",             "threes"),
            }

            # 3) Build a casual sentence for each
            blurb_lines = []
            for m in metrics:
                tv = shoot_eff[m].iloc[0] * 100        # team value
                mv = median_vals[m]  * 100                  # league median
                label, noun = metric_info[m]

                if tv > mv:
                    blurb = (
                        f"{team_name} were awesome {label} shooters, "
                        f"hitting {tv:.1f}% of their {noun}, "
                        f"above the league median of {mv:.1f}%."
                    )
                else:
                    blurb = (
                        f"{team_name} struggled a bit from {label}, "
                        f"making {tv:.1f}% versus the league median of {mv:.1f}%."
                    )

                blurb_lines.append(blurb)

            summary_text = " ".join(blurb_lines)

            # 4) Plug that into your legend block
            st.markdown(
                f"""
                <div style="
                    font-size:0.9rem;
                    color:#555;
                    line-height:1.5;
                    margin:1rem 0;
                ">
                {summary_text}<br><br>
                </div>
                """,
                unsafe_allow_html=True,
            )
        # ────────────────────────────────────────────────────────────────────────────
        # Rebounding and Hustle Stats KPIs
        with c10:
            # ── Prep hustle metrics ────────────────────────────────
            df_team_gen_adv = df_team_gen_adv.merge(
                df_team_gen_trad[["team_id","blka", "blk", "stl"]],
                on="team_id", how="left"
            )
            df_team_gen_adv["blk_pct"] = (df_team_gen_adv["blk"] / df_team_gen_adv["blka"]).fillna(0)

            # get medians
            metrics = ["dreb_pct", "oreb_pct", "stl", "blk_pct"]
            median_vals = {m: df_team_gen_adv[m].median() for m in metrics}

            # melt into long form
            df_hustle = (
                df_team_gen_adv[["team"] + metrics]
                .melt(id_vars="team",
                    value_vars=metrics,
                    var_name="Hustle Metric",
                    value_name="Percentage")
            )

            # ── Base bars ───────────────────────────────────────────
            color_scale = alt.Scale(
                domain=metrics,
                range=["#83c9ff","#ffb366","#7defa1","#ff7f7f"]
            )
            bars = (
                alt.Chart(df_hustle)
                .mark_bar(size=40)
                .encode(
                    x=alt.X("team:N", title="Team"),
                    xOffset=alt.XOffset("Hustle Metric:N"),
                    y=alt.Y("Percentage:Q", title="Value"),
                    color=alt.Color("Hustle Metric:N",
                                    scale=color_scale,
                                    legend=alt.Legend(title="Metric"))
                )
            )

            # ── Median lines & labels ───────────────────────────────
            layers = [bars]
            for m in metrics:
                med = median_vals[m]
                df_med = pd.DataFrame({"median":[med]})
                # rule
                layers.append(
                    alt.Chart(df_med)
                    .mark_rule(color="gray", strokeDash=[4,4])
                    .encode(y="median:Q")
                )
                # label
                layers.append(
                    alt.Chart(df_med)
                    .mark_text(dx=3, dy=-5, color="gray")
                    .encode(
                        y="median:Q",
                        text=alt.value(f"{m} median → {med:.2f}")
                    )
                )

            # ── Compose & render ─────────────────────────────────────
            chart = (
                alt.layer(*layers)
                .properties(
                    title="Rebounding & Hustle Metrics",
                    width="container",
                )
            )

            st.altair_chart(chart, use_container_width=True) #type: ignore[arg-type]


        radar1, radar2 = st.columns([1,1])
        with radar2:
            # 1) build df_eff
            labels = list(eff_benchmark_metrics.keys())
            cols   = list(eff_benchmark_metrics.values())
            team_pct = [ league_percentile.loc[team_id, c] for c in cols ]
            df_eff = pd.DataFrame({
                "Metric": labels,
                "Value":  [round(p * 100, 1) for p in team_pct] 
            })
            median_val = 50
            # add the combined text label
            df_eff["label"] = df_eff.apply(lambda r: f"{r.Metric}: {r.Value:.1f}", axis=1)

            scale = alt.Scale(domain=[0,100], rangeMin=0, rangeMax=120)

            median_ring = (
                alt.Chart(pd.DataFrame({"m":[median_val]}))
                .mark_arc(stroke="gray", strokeDash=[4,4], fillOpacity=0)
                .encode(
                    theta=alt.value(2*math.pi),
                    radius=alt.Radius("m:Q", scale=scale)
            ))
            above = df_eff[df_eff.Value >= median_val]
            below = df_eff[df_eff.Value <  median_val]

            bars_above = (
                alt.Chart(above)
                .mark_arc(innerRadius=30, stroke="#fff")
                .encode(
                    theta=alt.Theta("Metric:N", sort=labels, title=None),
                    radius=alt.Radius("Value:Q", scale=scale),
                    color=alt.value("green")
            ))
            bars_below = (
                alt.Chart(below)
                .mark_arc(innerRadius=30, stroke="#fff")
                .encode(
                    theta=alt.Theta("Metric:N", sort=labels, title=None),
                    radius=alt.Radius("Value:Q", scale=scale),
                    color=alt.value("red")
            ))
            # now use the new label field
            labels_layer = (
                alt.Chart(df_eff)
                .mark_text(radiusOffset=30, fontSize=12)
                .encode(
                    theta=alt.Theta("Metric:N", sort=labels),
                    radius=alt.Radius("Value:Q", scale=scale),
                    text=alt.Text("label:N"),
                    color=alt.value("white")
            ))

            radar = alt.layer(
                median_ring,
                bars_below, bars_above,
                labels_layer
            ).properties(
                width="container",
                title=f"Efficiency Radar — {team_name}"
            ).configure_view(stroke=None)

            st.altair_chart(radar, use_container_width=True) #type: ignore[arg-type]
                    # --- NEW: build a casual radar summary ---
            above = df_eff[df_eff.Value >= median_val]
            below = df_eff[df_eff.Value <  median_val]

            # format lists like "eFG% (52.3%)"
            above_list = [f"{r.Metric} ({r.Value:.1f}%)" for _, r in above.iterrows()]
            below_list = [f"{r.Metric} ({r.Value:.1f}%)" for _, r in below.iterrows()]

            if above_list and below_list:
                summary2 = (
                    f"{team_name} crushed it in {', '.join(above_list)}, "
                    f"but fell behind in {', '.join(below_list)}."
                )
            elif above_list:
                summary2 = f"{team_name} were on fire across the board: {', '.join(above_list)}."
            else:
                summary2 = f"{team_name} need to pick it up: all metrics dipped below median ({', '.join(below_list)})."

            # --- NEW: render that Quick Take + your legend ---
            st.markdown(
                f"""
                <div style="
                    font-size:0.9rem;
                    color:#555;
                    line-height:1.4;
                    margin:1rem 0 2rem;
                ">
                {summary2}<br>
                </div>
                """,
                unsafe_allow_html=True,
            )


        with radar1:

            ft_pct = df_team_gen_scoring["pct_pts_ft"].iloc[0]
            fg2_pct = df_team_gen_scoring["pct_pts_2pt"].iloc[0]
            fg3_pct = df_team_gen_scoring["pct_pts_3pt"].iloc[0]
            pts = df_team_gen_scoring["pts"].iloc[0]

            ft_ppg  = round((ft_pct * pts), 1)
            fg2_ppg = round((fg2_pct * pts), 1)
            fg3_ppg = round((fg3_pct * pts), 1)

            # 2) Build a DataFrame of shot-type → ppg → share
            df_shot = pd.DataFrame({
                "Shot Type": ["Free Throws", "Two-Pointers", "Three-Pointers"],
                "PPG":        [ft_ppg, fg2_ppg, fg3_ppg],
            })
            #df_shot_share["Share"] = df_shot_share["PPG"] / df_shot_share["PPG"].sum()

                    # 2) Build a little helper array for the θ‐ordering:
            order = ["Free Throws", "Two-Pointers", "Three-Pointers"]

             # define a common scale so arcs and text share the exact same colors
            color_scale = alt.Scale(scheme="category10")

            # 1) Arc layer
            arcs = (
                alt.Chart(df_shot)
                .mark_arc(innerRadius=40, stroke="white")
                .encode(
                    theta=alt.Theta("Shot Type:N", sort=order, title=None),
                    radius=alt.Radius("PPG:Q",
                                    scale=alt.Scale(type="sqrt", zero=True, rangeMin=20),
                                    title="PPG"),
                    color=alt.Color("Shot Type:N",
                                    scale=color_scale,
                                    legend=alt.Legend(title="Shot Type")),
                    tooltip=[
                        alt.Tooltip("Shot Type:N", title="Shot Type"),
                        alt.Tooltip("PPG:Q", format=".1f", title="PPG")
                    ],
                )
            )

            # 2) Text layer, color‐mapped by the same "Shot Type"
            labels = (
                alt.Chart(df_shot)
                .mark_text(radiusOffset=20, fontSize=13)
                .encode(
                    theta=alt.Theta("Shot Type:N", sort=order),
                    radius=alt.Radius("PPG:Q",
                                    scale=alt.Scale(type="sqrt", zero=True, rangeMin=20)),
                    text=alt.Text("PPG:Q", format=".1f"),
                    color=alt.Color("Shot Type:N", scale=color_scale, legend=None)
                )
            )

            # 3) layer them
            chart = (arcs + labels).properties(
                title=f"{team_abbr} Shot-Type Breakdown (PPG share)",
                width="container"
            )


            st.altair_chart(chart, use_container_width=True) #type: ignore[arg-type]
                    # --- NEW: shot‐type breakdown summary ---
            df_shot_sorted = df_shot.sort_values("PPG", ascending=False)
            top = df_shot_sorted.iloc[0]
            rest = df_shot_sorted.iloc[1:]
            rest_list = [f"{r['Shot Type']} ({r.PPG:.1f} PPG)" for _, r in rest.iterrows()]

            summary1 = (
                f"{team_name} got most of their points from {top['Shot Type']} "
                f"({top.PPG:.1f} PPG), then {', '.join(rest_list)}."
            )

            # --- NEW: render that Quick Take below the chart ---
            st.markdown(
                f"""
                <div style="
                    font-size:0.9rem;
                    color:#555;
                    line-height:1.4;
                    margin:1rem 0 2rem;
                ">
                {summary1}
                </div>
                """,
                unsafe_allow_html=True,
            )


tab_reg, tab_play = st.tabs(["Regular Season","Playoffs"])
with tab_reg:  render_tab("regular_season")
with tab_play: render_tab("playoffs")
