from __future__ import annotations

import pandas as pd
import streamlit as st


def compute_kpis(dfs: dict, season: str, season_type: str, team_id: int, team_abbr: str, team_name: str, prev_season: str | None, team_bios: pd.DataFrame) -> dict:
    """Compute various team KPIs used in the dashboard."""
    locals().update(dfs)

    eff_benchmark_metrics = {
        "Net Rating": "net_rating",
        "TS%": "ts_pct",
        "AST/TO Ratio": "ast_to",
        "Drb %": "dreb_pct",
        "eFG%": "efg_pct",
        "pace": "pace",
    }

    league = df_clutch_adv.set_index("team_id")[list(eff_benchmark_metrics.values())]
    league_median = league.median()
    league_percentile = league.rank(pct=True).round(3)

    pra_metrics = {"pts": "pts", "rebounds": "reb", "assists": "ast"}
    league_pra = df_boxscore_adv.set_index("team_id")[list(pra_metrics.values())]
    pra_median = league_pra.median()
    pra_percentile = league_pra.rank(pct=True).round(3)

    if prev_season:
        prev_gen_dir = TEAM_DIR / "general" / prev_season / "pergame" / season_type
        df_prev_gen_trad = (
            pd.read_csv(prev_gen_dir / "traditional.csv")
            .rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
            .merge(team_bios[["team_id", "team", "team_name"]], on="team_id")
        )
        prev_team_gen = df_prev_gen_trad[df_prev_gen_trad["team_id"] == team_id]
        prev_win_pct = round((prev_team_gen["w_pct"].iloc[0]) * 100, 3) if not prev_team_gen.empty else None

        df_prev_gen_adv = (
            pd.read_csv(prev_gen_dir / "advanced.csv")
            .rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
            .merge(team_bios[["team_id", "team", "team_name"]], on="team_id")
        )
        prev_team_gen_adv = df_prev_gen_adv[df_prev_gen_adv["team_id"] == team_id]
        prev_clutch_dir = TEAM_DIR / "clutch" / prev_season / "pergame" / season_type
        df_prev_clutch_adv = (
            pd.read_csv(prev_clutch_dir / "advanced.csv")
            .rename(columns=str.lower)
            .drop(columns=["team"], errors="ignore")
            .merge(team_bios[["team_id", "team", "team_name"]], on="team_id")
        )
        prev_net_rating = (
            df_prev_clutch_adv.loc[df_prev_clutch_adv["team_id"] == team_id, "net_rating"].iloc[0]
            if team_id in df_prev_clutch_adv["team_id"].values else None
        )
        prev_off_rating = (
            df_prev_gen_adv.loc[df_prev_gen_adv["team_id"] == team_id, "off_rating"].iloc[0]
            if team_id in df_prev_gen_adv["team_id"].values else None
        )
        prev_def_rating = (
            df_prev_gen_adv.loc[df_prev_gen_adv["team_id"] == team_id, "def_rating"].iloc[0]
            if team_id in df_prev_gen_adv["team_id"].values else None
        )
    else:
        prev_win_pct = prev_net_rating = prev_off_rating = prev_def_rating = None

    df_team_metric = df_boxscore_adv[df_boxscore_adv["team_id"] == team_id]
    df_team_general = df_gen[df_gen["team_id"] == team_id]
    df_team_gen_trad = df_gen_trad[df_gen_trad["team_id"] == team_id]
    df_team_gen_adv = df_gen_adv[df_gen_adv["team_id"] == team_id]
    df_team_adv_box = df_boxscore_adv[df_boxscore_adv["team_id"] == team_id]
    df_team_gen_scoring = df_gen_scoring[df_gen_scoring["team_id"] == team_id]

    df_clutch_local = df_clutch[df_clutch["team_id"] == team_id]
    df_clutch_adv_local = df_clutch_adv[df_clutch_adv["team_id"] == team_id]
    df_clutch_trad_local = df_clutch_trad[df_clutch_trad["team_id"] == team_id]

    if team_id not in league.index:
        st.warning(f"{team_name} didn’t make the {season} {season_type.replace('_',' ')}.")
        return {}
    team_row = league.loc[team_id]

    season_month_order = [10, 11, 12, 1, 2, 3, 4]
    months = {1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"}

    @st.cache_data
    def with_month(df: pd.DataFrame, date_col: str = "game_date") -> pd.DataFrame:
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df["month"] = df[date_col].dt.month
        df["month_name"] = df["month"].map(months)
        df["month"] = pd.Categorical(df["month"], categories=months)
        return df

    df_team_metric = with_month(df_team_metric, "game_date")

    _SUP_MAP = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")

    def to_superscript(n: int) -> str:
        return str(n).translate(_SUP_MAP)

    def rank_color(rank: int, total_teams: int = 30) -> str:
        third = total_teams // 3
        if rank <= third:
            return "green"
        elif rank <= 2 * third:
            return "orange"
        else:
            return "red"

    def parse_matchup(m: str) -> tuple[str, str]:
        m = m.replace("vs.", "vs").replace(" @ ", "@").replace(" vs ", "vs").strip()
        if "@" in m:
            away, home = m.split("@")
        else:
            home, away = m.split("vs")
        return away.strip(), home.strip()

    def add_home_away(df: pd.DataFrame) -> pd.DataFrame:
        df_boxscore_adv[["away_team", "home_team"]] = df_boxscore_adv["matchup"].apply(parse_matchup).tolist()
        df_boxscore_adv["opp_team"] = df_boxscore_adv.apply(lambda r: r.away_team if r.home_team == r.team else r.home_team, axis=1)
        return df

    df_boxscore_adv = add_home_away(df_boxscore_adv)
    df_boxscore_adv["wl"] = df_boxscore_adv["wl"].str.upper()

    home = df_team_metric[df_team_metric["matchup"].str.contains("vs", case=False)]
    home_summary = (
        home.groupby("team_id")["wl"].value_counts().unstack(fill_value=0).rename(columns={"W": "home_w", "L": "home_l"}).reindex(columns=["home_w", "home_l"], fill_value=0)
    )
    h_wins = home_summary["home_w"].sum()
    h_losses = home_summary["home_l"].sum()

    away = df_team_metric[df_team_metric["matchup"].str.contains("@")]
    away_summary = (
        away.groupby("team_id")["wl"].value_counts().unstack(fill_value=0).rename(columns={"W": "away_w", "L": "away_l"}).reindex(columns=["away_w", "away_l"], fill_value=0)
    )
    a_wins = away_summary["away_w"].sum()
    a_losses = away_summary["away_l"].sum()

    summary = (
        home_summary.join(away_summary, how="outer").fillna(0).astype(int).assign(
            h_pct=lambda d: d.home_w.div(d.home_w + d.home_l).fillna(0),
            a_pct=lambda d: d.away_w.div(d.away_w + d.away_l).fillna(0),
        )
    )
    summary["h_rank"] = summary["h_pct"].rank(ascending=False, method="min").astype(int)
    summary["a_rank"] = summary["a_pct"].rank(ascending=False, method="min").astype(int)
    df_gen_adv = df_gen_adv.merge(summary[["h_pct", "h_rank", "a_pct", "a_rank"]], left_on="team_id", right_index=True, how="left")

    df_team_gen_adv = df_gen_adv[df_gen_adv["team_id"] == team_id]
    home_pct = df_team_gen_adv["h_pct"].iloc[0]
    home_pct_rank = df_team_gen_adv["h_rank"].iloc[0]
    away_pct = df_team_gen_adv["a_pct"].iloc[0]
    away_pct_rank = df_team_gen_adv["a_rank"].iloc[0]

    games_played = df_team_metric["game_id"].nunique()
    win_loss_pct = df_team_general["w_pct"].iloc[0] if "w_pct" in df_team_general.columns and len(df_team_general) > 0 else 0.0
    win_loss_pct = round(win_loss_pct * 100, 3)
    win_loss_pct_rank = df_team_gen_trad["w_rank"].iloc[0] if "w_rank" in df_team_gen_trad.columns and len(df_team_gen_trad) > 0 else None
    points_per_game = df_team_general["pts"].mean() if "pts" in df_team_general.columns and len(df_team_general) > 0 else 0.0
    pts_rank = df_team_gen_trad["pts_rank"].iloc[0] if "pts_rank" in df_team_gen_trad.columns and len(df_team_gen_trad) > 0 else None
    rebounds_per_game = (df_team_general["reb"]).mean() if "reb" in df_team_general.columns and len(df_team_general) > 0 else 0.0
    trb_rank = df_team_gen_trad["reb_rank"].iloc[0] if "reb_rank" in df_team_gen_trad.columns and len(df_team_gen_trad) > 0 else None
    assists_per_game = df_team_general["ast"].mean() if "ast" in df_team_general.columns and len(df_team_general) > 0 else 0.0
    ast_rank = df_team_gen_trad["ast_rank"].iloc[0] if "ast_rank" in df_team_gen_trad.columns and len(df_team_gen_trad) > 0 else None
    net_rating = df_team_gen_adv["net_rating"].iloc[0] if "net_rating" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None
    net_rating_rank = df_team_gen_adv["net_rating_rank"].iloc[0] if "net_rating_rank" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None
    off_rating = df_team_gen_adv["off_rating"].iloc[0] if "off_rating" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None
    def_rating = df_team_gen_adv["def_rating"].iloc[0] if "def_rating" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None
    off_rating_rank = df_team_gen_adv["off_rating_rank"].iloc[0] if "off_rating_rank" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None
    def_rating_rank = df_team_gen_adv["def_rating_rank"].iloc[0] if "def_rating_rank" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None
    pace = df_team_gen_adv["pace"].iloc[0] if "pace" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None
    pace_rank = df_team_gen_adv["pace_rank"].iloc[0] if "pace_rank" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None
    ts_pct = round((df_team_gen_adv["ts_pct"].mean()) * 100, 3) if "ts_pct" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else 0.0
    ts_pct_rank = df_team_gen_adv["ts_pct_rank"].iloc[0] if "ts_pct_rank" in df_team_gen_adv.columns and len(df_team_gen_adv) > 0 else None

    df_boxscore_adv = add_home_away(df_boxscore_adv)
    df_team_metric = df_boxscore_adv[df_boxscore_adv.team_id == team_id].copy()

    wins = int(df_team_general["w"].iloc[0])
    losses = int(df_team_general["l"].iloc[0])

    vic_prefix = ""
    def_prefix = ""
    if "plus_minus" in df_team_metric.columns and not df_team_metric.empty:
        df = df_team_metric.copy()
        idx_max = df["plus_minus"].idxmax()
        vic_row = df.loc[idx_max]
        largest_victory = int(df.at[idx_max, "plus_minus"])
        largest_victory_opponent = df.at[idx_max, "opp_team"]
        largest_victory_pts = int(df.at[idx_max, "pts"])
        victory_opp_points = largest_victory_pts - largest_victory
        vic_prefix = "vs. " if vic_row["home_team"] == team_abbr else "@"
        idx_min = df["plus_minus"].idxmin()
        def_row = df.loc[idx_min]
        biggest_defeat = int(df.at[idx_min, "plus_minus"])
        biggest_defeat_opponent = df.at[idx_min, "opp_team"]
        biggest_defeat_pts = int(df.at[idx_min, "pts"])
        defeat_opp_points = biggest_defeat_pts - biggest_defeat
        def_prefix = "vs. " if def_row["home_team"] == team_abbr else "@"
    else:
        largest_victory = largest_victory_opponent = biggest_defeat = biggest_defeat_opponent = None
        largest_victory_pts = victory_opp_points = biggest_defeat_pts = defeat_opp_points = 0

    w_rank = df_team_general["w_rank"].iloc[0] if "w_rank" in df_team_general.columns and len(df_team_general) > 0 else None
    l_rank = df_team_general["l_rank"].iloc[0] if "l_rank" in df_team_general.columns and len(df_team_general) > 0 else None

    win_arrow, win_delta, win_color = ("", "", "")
    net_arrow, net_delta, net_color = ("", "", "")
    off_arrow, off_delta, off_color = ("", "", "")
    def_arrow, def_delta, def_color = ("", "", "")
    if prev_win_pct is not None:
        diff = win_loss_pct - prev_win_pct
        win_arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
        win_delta = f"{abs(diff):.1f}"
        win_color = "green" if diff > 0 else ("red" if diff < 0 else "gray")
    if prev_net_rating is not None:
        diff = net_rating - prev_net_rating
        net_arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
        net_delta = f"{abs(diff):.1f}"
        net_color = "green" if diff > 0 else ("red" if diff < 0 else "gray")
    if prev_off_rating is not None:
        diff = off_rating - prev_off_rating
        off_arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
        off_delta = f"{abs(diff):.1f}"
        off_color = "green" if diff > 0 else ("red" if diff < 0 else "gray")
    if prev_def_rating is not None:
        diff = def_rating - prev_def_rating
        def_arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
        def_delta = f"{abs(diff):.1f}"
        def_color = "green" if diff > 0 else ("red" if diff < 0 else "gray")

    total_teams = len(league)

    kpis = {k: v for k, v in locals().items() if not k.startswith("df_")}
    kpis.update({
        "df_team_metric": df_team_metric,
        "df_team_general": df_team_general,
        "df_team_gen_trad": df_team_gen_trad,
        "df_team_gen_adv": df_team_gen_adv,
        "df_team_adv_box": df_team_adv_box,
        "df_team_gen_scoring": df_team_gen_scoring,
        "df_clutch": df_clutch_local,
        "df_clutch_adv": df_clutch_adv_local,
        "df_clutch_trad": df_clutch_trad_local,
        "league": league,
    })
    return kpis