import pandas as pd
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Data Paths and Directories
# ─────────────────────────────────────────────────────────────────────────────
DATA_DIR   = Path(__file__).resolve().parents[2] / "data" / "processed"
TEAM_DIR   = DATA_DIR / "team_stats"
PLAYER_DIR = DATA_DIR / "player_stats"
TEAM_DATA  = DATA_DIR / "teams_cleaned.csv"
PLAYER_DATA = DATA_DIR / "all_players_cleaned.csv"


def load_team_data(season: str, season_type: str) -> dict[str, pd.DataFrame]:
    """
    Load all team and player DataFrames for a given season and season_type.
    Returns a dict of DataFrames keyed by:
      - team_bios, player_bios
      - df_all, df_boxscore, df_boxscore_adv, df_boxscore_trad
      - df_gen, df_gen_trad, df_gen_adv, df_gen_scoring
      - df_clutch, df_clutch_adv, df_clutch_trad
      - df_player_all, df_players_boxscore, df_players_boxscore_adv, df_players_boxscore_trad
      - df_players_gen, df_players_gen_trad, df_players_gen_adv, df_players_gen_scoring
    """
    # 1) Load canonical team and player bios
    team_bios = pd.read_csv(TEAM_DATA).rename(columns=str.lower)
    player_bios = (
        pd.read_csv(PLAYER_DATA)
        .rename(columns=str.lower)
        .drop(columns=["player_id"], errors="ignore")
        .rename(columns={"pid": "player_id"}, errors="ignore")
    )

    # Constants for directory names
    DEFAULT_METRIC  = "adv_boxscores"
    DEFAULT_MEASURE = "pergame"
    ST = season_type

    # 2) Load team-wide per-game "adv_boxscores" data (df_all)
    base_dir = TEAM_DIR / DEFAULT_METRIC / season / DEFAULT_MEASURE / ST
    df_all = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in base_dir.glob("*.csv")],
        ignore_index=True
    )
    df_all = df_all.drop(columns=["team", "team_name"], errors="ignore")
    df_all = df_all.merge(
        team_bios[["team_id","team","team_name","logo_url"]],
        on="team_id", how="left"
    )

    # 3) Load team boxscores
    boxscore_dir = base_dir
    df_boxscore_adv = pd.read_csv(boxscore_dir / "advanced.csv")
    df_boxscore_trad = pd.read_csv(boxscore_dir / "traditional.csv")
    df_boxscore = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in boxscore_dir.glob("*.csv")],
        ignore_index=True
    )
    # normalize and merge team info
    for df in ("df_boxscore", "df_boxscore_adv", "df_boxscore_trad"):
        locals()[df] = (
            locals()[df]
            .rename(columns=str.lower)
            .drop(columns=["team", "team_name"], errors="ignore")
            .merge(
                team_bios[["team_id","team","team_name","logo_url"]],
                on="team_id", how="left"
            )
        )
    # propagate plus_minus & stats from traditional into advanced
    pm = df_boxscore_trad.loc[:, ["game_id","matchup","plus_minus","pts","ast","reb"]]
    df_boxscore_adv = (
        df_boxscore_adv
        .merge(pm, on=["game_id","matchup"], how="left")
        .drop_duplicates(subset=["game_id","matchup"])
    )

    # 4) Load per-team summary (general)
    general_dir = TEAM_DIR / "general" / season / DEFAULT_MEASURE / ST
    df_gen = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in general_dir.glob("*.csv")],
        ignore_index=True
    )
    df_gen_trad = pd.read_csv(general_dir / "traditional.csv")
    df_gen_adv = pd.read_csv(general_dir / "advanced.csv")
    df_gen_scoring = pd.read_csv(general_dir / "scoring.csv")
    # normalize and merge
    df_gen_trad = (
        df_gen_trad.rename(columns=str.lower)
        .drop(columns=["team"], errors="ignore")
        .merge(team_bios[["team_id","team","team_name","logo_url"]], on="team_id", how="left")
    )
    df_gen_adv = (
        df_gen_adv.rename(columns=str.lower)
        .drop(columns=["team"], errors="ignore")
        .merge(team_bios[["team_id","team","team_name","logo_url"]], on="team_id", how="left")
        .merge(df_gen_trad[["team_id","plus_minus"]], on="team_id", how="left")
    )
    df_gen_scoring = (
        df_gen_scoring.rename(columns=str.lower)
        .drop(columns=["team"], errors="ignore")
        .merge(team_bios[["team_id","team","logo_url"]], on="team_id", how="left")
        .merge(df_gen_trad[["team_id","pts"]], on="team_id", how="left")
    )
    df_gen = (
        df_gen.rename(columns=str.lower)
        .drop(columns=["team","team_name"], errors="ignore")
        .merge(team_bios[["team_id","team","team_name","logo_url"]], on="team_id", how="left")
    )

    # 5) Load clutch stats
    clutch_dir = TEAM_DIR / "clutch" / season / DEFAULT_MEASURE / ST
    df_clutch = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in clutch_dir.glob("*.csv")],
        ignore_index=True
    )
    df_clutch_adv = pd.read_csv(clutch_dir / "advanced.csv")
    df_clutch_trad = pd.read_csv(clutch_dir / "traditional.csv")
    # normalize and merge
    df_clutch = (
        df_clutch.rename(columns=str.lower)
        .drop(columns=["team"], errors="ignore")
        .merge(team_bios[["team_id","team","team_name","logo_url"]], on="team_id", how="left")
    )
    for df in ("df_clutch_adv","df_clutch_trad"):
        locals()[df] = (
            locals()[df].rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
            .merge(team_bios[["team_id","team","team_name","logo_url"]], on="team_id", how="left")
        )

    # 6) Load raw player per-game data
    player_dir = PLAYER_DIR / DEFAULT_METRIC / season / DEFAULT_MEASURE / ST
    df_player_all = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in player_dir.glob("*.csv")],
        ignore_index=True
    )
    df_player_all = (
        df_player_all
        .drop(columns=["team"], errors="ignore")
        .merge(
            player_bios[["player_id","team","headshot_url","country","birthdate",
                        "position_primary","position_alt","experience",
                        "draft_year","draft_round","draft_pick",
                        "is_active","is_free_agent","is_retired"]],
            on="player_id", how="left"
        )
    )

    # 7) Load player boxscores
    players_box_dir = PLAYER_DIR / DEFAULT_METRIC / season / DEFAULT_MEASURE / ST
    df_players_boxscore_adv = pd.read_csv(players_box_dir / "advanced.csv")
    df_players_boxscore_trad = pd.read_csv(players_box_dir / "traditional.csv")
    df_players_boxscore = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in players_box_dir.glob("*.csv")],
        ignore_index=True
    )
    for df in ("df_players_boxscore","df_players_boxscore_adv","df_players_boxscore_trad"):
        locals()[df] = (
            locals()[df].rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
            .merge(
                player_bios[["player_id","team","headshot_url","country","birthdate",
                            "position_primary","position_alt","experience",
                            "draft_year","draft_round","draft_pick",
                            "is_active","is_free_agent","is_retired"]],
                on="player_id", how="left"
            )
        )
    pm = df_players_boxscore_trad.loc[:, ["game_id","matchup","plus_minus","pts","ast","reb"]]
    df_players_boxscore_adv = (
        df_players_boxscore_adv
        .merge(pm, on=["game_id","matchup"], how="left")
        .drop_duplicates(subset=["game_id","matchup"])
    )

    # 8) Load player general stats
    players_gen_dir = PLAYER_DIR / "general" / season / DEFAULT_MEASURE / ST
    df_players_gen = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in players_gen_dir.glob("*.csv")],
        ignore_index=True
    )
    df_players_gen_trad = pd.read_csv(players_gen_dir / "traditional.csv")
    df_players_gen_adv  = pd.read_csv(players_gen_dir / "advanced.csv")
    df_players_gen_scoring = pd.read_csv(players_gen_dir / "scoring.csv")
    for df in ("df_players_gen_trad","df_players_gen_adv","df_players_gen_scoring"):
        locals()[df] = locals()[df].rename(columns=str.lower)
    # merge metadata and plus_minus/pts into adv and scoring
    df_players_gen_trad = (
        df_players_gen_trad
        .drop(columns=["team"], errors="ignore")
        .merge(
            player_bios[["player_id","team","headshot_url","country","birthdate",
                        "position_primary","position_alt","experience",
                        "draft_year","draft_round","draft_pick",
                        "is_active","is_free_agent","is_retired"]],
            on="player_id", how="left"
        )
    )
    df_players_gen_adv = (
        df_players_gen_adv
        .drop(columns=["team"], errors="ignore")
        .merge(
            player_bios[["player_id","team","headshot_url","country","birthdate",
                        "position_primary","position_alt","experience",
                        "draft_year","draft_round","draft_pick",
                        "is_active","is_free_agent","is_retired"]],
            on="player_id", how="left"
        )
        .merge(df_players_gen_trad[["player_id","plus_minus"]], on="player_id", how="left")
    )
    df_players_gen_scoring = (
        df_players_gen_scoring
        .drop(columns=["team"], errors="ignore")
        .merge(
            player_bios[["player_id","team","headshot_url","country","birthdate",
                        "position_primary","position_alt","experience",
                        "draft_year","draft_round","draft_pick",
                        "is_active","is_free_agent","is_retired"]],
            on="player_id", how="left"
        )
        .merge(df_players_gen_trad[["player_id","pts"]], on="player_id", how="left")
    )
    df_players_gen = (
        df_players_gen
        .drop(columns=["team","team_name"], errors="ignore")
        .merge(
            player_bios[["player_id","team","headshot_url","country","birthdate",
                        "position_primary","position_alt","experience",
                        "draft_year","draft_round","draft_pick",
                        "is_active","is_free_agent","is_retired"]],
            on="player_id", how="left"
        )
    )

    return {
        "team_bios": team_bios,
        "player_bios": player_bios,
        "df_all": df_all,
        "df_boxscore": df_boxscore,
        "df_boxscore_adv": df_boxscore_adv,
        "df_boxscore_trad": df_boxscore_trad,
        "df_gen": df_gen,
        "df_gen_trad": df_gen_trad,
        "df_gen_adv": df_gen_adv,
        "df_gen_scoring": df_gen_scoring,
        "df_clutch": df_clutch,
        "df_clutch_adv": df_clutch_adv,
        "df_clutch_trad": df_clutch_trad,
        "df_player_all": df_player_all,
        "df_players_boxscore": df_players_boxscore,
        "df_players_boxscore_adv": df_players_boxscore_adv,
        "df_players_boxscore_trad": df_players_boxscore_trad,
        "df_players_gen": df_players_gen,
        "df_players_gen_trad": df_players_gen_trad,
        "df_players_gen_adv": df_players_gen_adv,
        "df_players_gen_scoring": df_players_gen_scoring
    }