from __future__ import annotations

from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"
TEAM_DIR = DATA_DIR / "team_stats"



def load_team_data(season: str, season_type: str, team_bios: pd.DataFrame) -> dict:
    """Load all team related CSVs for a given season and type."""
    DEFAULT_METRIC = "adv_boxscores"
    DEFAULT_MEASURE = "pergame"
    base_dir = TEAM_DIR / DEFAULT_METRIC / season / DEFAULT_MEASURE / season_type
    df_all = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in base_dir.glob("*.csv")],
        ignore_index=True,
    )
    df_all = df_all.drop(columns=["team", "team_name"], errors="ignore")
    df_all = df_all.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    boxscore_dir = TEAM_DIR / DEFAULT_METRIC / season / DEFAULT_MEASURE / season_type
    box_adv = boxscore_dir / "advanced.csv"
    box_trad = boxscore_dir / "traditional.csv"
    df_boxscore_adv = pd.read_csv(box_adv)
    df_boxscore_trad = pd.read_csv(box_trad)
    df_boxscore = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in boxscore_dir.glob("*.csv")],
        ignore_index=True,
    )

    df_boxscore = df_boxscore.rename(columns=str.lower)
    df_boxscore = df_boxscore.drop(columns=["team", "team_name"], errors="ignore")
    df_boxscore = df_boxscore.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    df_boxscore_adv = df_boxscore_adv.rename(columns=str.lower)
    df_boxscore_adv = df_boxscore_adv.drop(columns=["team", "team_name"], errors="ignore")
    df_boxscore_adv = df_boxscore_adv.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    df_boxscore_trad = df_boxscore_trad.rename(columns=str.lower)
    df_boxscore_trad = df_boxscore_trad.drop(columns=["team", "team_name"], errors="ignore")
    df_boxscore_trad = df_boxscore_trad.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    pm = df_boxscore_trad.loc[:, ["game_id", "matchup", "plus_minus", "pts", "ast", "reb"]]
    df_boxscore_adv = df_boxscore_adv.merge(pm, on=["game_id", "matchup"], how="left")
    df_boxscore_adv = df_boxscore_adv.drop_duplicates(subset=["game_id", "matchup"])

    general_dir = TEAM_DIR / "general" / season / DEFAULT_MEASURE / season_type
    gen_trad = general_dir / "traditional.csv"
    gen_adv = general_dir / "advanced.csv"
    gen_score = general_dir / "scoring.csv"
    df_gen = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in general_dir.glob("*.csv")],
        ignore_index=True,
    )

    df_gen_trad = pd.read_csv(gen_trad)
    df_gen_adv = pd.read_csv(gen_adv)
    df_gen_scoring = pd.read_csv(gen_score)

    df_gen_trad = df_gen_trad.rename(columns=str.lower)
    df_gen_trad = df_gen_trad.drop(columns=["team"], errors="ignore")
    df_gen_trad = df_gen_trad.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    df_gen_adv = df_gen_adv.rename(columns=str.lower)
    df_gen_adv = df_gen_adv.drop(columns=["team"], errors="ignore")
    df_gen_adv = df_gen_adv.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )
    df_gen_adv = df_gen_adv.merge(
        df_gen_trad[["team_id", "plus_minus"]],
        on="team_id",
        how="left",
    )

    df_gen_scoring = df_gen_scoring.rename(columns=str.lower)
    df_gen_scoring = df_gen_scoring.drop(columns=["team"], errors="ignore")
    df_gen_scoring = df_gen_scoring.merge(
        team_bios[["team_id", "team", "logo_url"]],
        on="team_id",
        how="left",
    )
    df_gen_scoring = df_gen_scoring.merge(
        df_gen_trad[["team_id", "pts"]],
        on="team_id",
        how="left",
    )

    df_gen = df_gen.drop(columns=["team", "team_name"], errors="ignore")
    df_gen = df_gen.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    clutch_dir = TEAM_DIR / "clutch" / season / DEFAULT_MEASURE / season_type
    clutch_adv = clutch_dir / "advanced.csv"
    clutch_trad = clutch_dir / "traditional.csv"
    df_clutch_adv = pd.read_csv(clutch_adv)
    df_clutch_trad = pd.read_csv(clutch_trad)
    df_clutch = pd.concat(
        [pd.read_csv(f).assign(season=season) for f in clutch_dir.glob("*.csv")],
        ignore_index=True,
    )

    df_clutch = df_clutch.rename(columns=str.lower)
    df_clutch = df_clutch.drop(columns=["team"], errors="ignore")
    df_clutch = df_clutch.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    df_clutch_adv = df_clutch_adv.rename(columns=str.lower)
    df_clutch_adv = df_clutch_adv.drop(columns=["team"], errors="ignore")
    df_clutch_adv = df_clutch_adv.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    df_clutch_trad = df_clutch_trad.rename(columns=str.lower)
    df_clutch_trad = df_clutch_trad.drop(columns=["team"], errors="ignore")
    df_clutch_trad = df_clutch_trad.merge(
        team_bios[["team_id", "team", "team_name", "logo_url"]],
        on="team_id",
        how="left",
    )

    return {
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
    }