import pandas as pd
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Paths for prev-season lookups
# ─────────────────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"
TEAM_DIR = DATA_DIR / "team_stats"
DEFAULT_MEASURE = "pergame"


def compute_kpis(
    dfs: dict[str, pd.DataFrame],
    season: str,
    season_type: str,
    team_id: int,
    team_abbr: str,
    team_name: str,
    prev_season: str | None,
    team_bios: pd.DataFrame
) -> dict[str, any]:
    """
    Compute all numeric KPIs, league medians/ranks, season-over-season deltas,
    and top-3 player leaders for a given team.

    Returns a flat dict of values and small DataFrames.
    """
    # Unpack current-season DataFrames
    df_box_adv = dfs["df_boxscore_adv"]
    df_gen      = dfs["df_gen"]
    df_gen_trad = dfs["df_gen_trad"]
    df_gen_adv  = dfs["df_gen_adv"]
    df_gen_sc   = dfs["df_gen_scoring"]
    df_clutch_adv = dfs["df_clutch_adv"]
    df_player_all = dfs["df_player_all"]
    df_player_bs_trad = dfs["df_players_boxscore_trad"]

    # ── League-wide efficiency benchmarks ─────────────────────────────
    eff_metrics = {
        "Net Rating": "net_rating",
        "TS%":        "ts_pct",
        "AST/TO Ratio": "ast_to",
        "Drb %":      "dreb_pct",
        "eFG%":        "efg_pct",
        "pace":        "pace",
    }
    league_eff = df_clutch_adv.set_index("team_id")[list(eff_metrics.values())]
    league_median    = league_eff.median()
    league_percentile = league_eff.rank(pct=True).round(3)

    # ── League-wide P/R/A per game ───────────────────────────────────
    pra_metrics = {"pts":"pts","rebounds":"reb","assists":"ast"}
    league_pra = dfs["df_boxscore_adv"].set_index("team_id")[list(pra_metrics.values())]
    pra_median = league_pra.median()
    pra_percentile = league_pra.rank(pct=True).round(3)

    # ── Previous-season snapshot ──────────────────────────────────────
    prev_win_pct = prev_net = prev_off = prev_def = None
    if prev_season:
        prev_dir = lambda sub: TEAM_DIR / sub / prev_season / DEFAULT_MEASURE / season_type
        def prev_dir(sub: str) -> Path:
            """Return the path to previous season data for a subdirectory."""
            return TEAM_DIR / sub / prev_season / DEFAULT_MEASURE / season_type
        # load trad general
        p1 = pd.read_csv(prev_dir("general")/"traditional.csv").rename(columns=str.lower)
        prev = p1.loc[p1.team_id==team_id]
        prev_win_pct = round(prev["w_pct"].iloc[0]*100,3) if not prev.empty else None
        # load adv general
        p2 = pd.read_csv(prev_dir("general")/"advanced.csv").rename(columns=str.lower)
        prev_off = p2.loc[p2.team_id==team_id, "off_rating"].iloc[0] if team_id in p2.team_id.values else None
        prev_def = p2.loc[p2.team_id==team_id, "def_rating"].iloc[0] if team_id in p2.team_id.values else None
        # load adv clutch
        p3 = pd.read_csv(prev_dir("clutch")/"advanced.csv").rename(columns=str.lower)
        prev_net = p3.loc[p3.team_id==team_id, "net_rating"].iloc[0] if team_id in p3.team_id.values else None

    # ── Team slices ───────────────────────────────────────────────────
    df_team_bs = df_box_adv[df_box_adv.team_id==team_id]
    df_team_gen = df_gen[df_gen.team_id==team_id]
    df_team_gt  = df_gen_trad[df_gen_trad.team_id==team_id]
    df_team_ga  = df_gen_adv[df_gen_adv.team_id==team_id]
    df_team_sc  = df_gen_sc[df_gen_sc.team_id==team_id]

    # ── Home/Away summary ────────────────────────────────────────────
    df_team_bs["wl"] = df_team_bs.wl.str.upper()
    home = df_team_bs[df_team_bs.matchup.str.contains("vs",case=False)]
    away = df_team_bs[df_team_bs.matchup.str.contains("@")]
    h = (home.groupby("team_id").wl.value_counts().unstack(fill_value=0)
         .rename(columns={"W":"home_w","L":"home_l"}))
    a = (away.groupby("team_id").wl.value_counts().unstack(fill_value=0)
         .rename(columns={"W":"away_w","L":"away_l"}))
    summ = (h.join(a,how="outer").fillna(0).astype(int)
            .assign(
                h_pct=lambda d: d.home_w/(d.home_w+d.home_l),
                a_pct=lambda d: d.away_w/(d.away_w+d.away_l)
            ))
    summ["h_rank"] = summ.h_pct.rank(method="min",ascending=False).astype(int)
    summ["a_rank"] = summ.a_pct.rank(method="min",ascending=False).astype(int)

    # merge into df_team_ga
    df_team_ga = df_team_ga.merge(
        summ[["h_pct","h_rank","a_pct","a_rank"]],
        left_on="team_id", right_index=True, how="left"
    )

    # ── Core KPIs ─────────────────────────────────────────────────────
    games_played = df_team_bs.game_id.nunique()

    win_pct = round(df_team_gen.w_pct.iloc[0]*100,3) if not df_team_gen.empty else 0
    win_rank = df_team_gt.w_rank.iloc[0] if not df_team_gt.empty else None

    pts_rank = df_team_gt.pts_rank.iloc[0] if not df_team_gt.empty else None
    reb_rank = df_team_gt.reb_rank.iloc[0] if not df_team_gt.empty else None
    ast_rank = df_team_gt.ast_rank.iloc[0] if not df_team_gt.empty else None

    net       = df_team_ga.net_rating.iloc[0]      if not df_team_ga.empty else None
    net_rank  = df_team_ga.net_rating_rank.iloc[0] if not df_team_ga.empty else None
    off       = df_team_ga.off_rating.iloc[0]      if not df_team_ga.empty else None
    off_rank  = df_team_ga.off_rating_rank.iloc[0] if not df_team_ga.empty else None
    deff      = df_team_ga.def_rating.iloc[0]      if not df_team_ga.empty else None
    deff_rank = df_team_ga.def_rating_rank.iloc[0] if not df_team_ga.empty else None

    pace      = df_team_ga.pace.iloc[0]            if not df_team_ga.empty else None
    pace_rank = df_team_ga.pace_rank.iloc[0]      if not df_team_ga.empty else None

    ts_pct    = round(df_team_ga.ts_pct.mean()*100,3) if not df_team_ga.empty else 0
    ts_rank   = df_team_ga.ts_pct_rank.iloc[0]     if not df_team_ga.empty else None

    # ── Season-over-season deltas ────────────────────────────────────
    def make_delta(curr, prev):
        if prev is None: return "","",""
    def make_delta(curr: float | int | None, prev: float | int | None):
        """Return arrow, value string and color for metric change."""
        if prev is None:
            return "", "", ""
        d = curr - prev
        arrow = "↑" if d>0 else ("↓" if d<0 else "→")
        val = f"{abs(d)*100:.1f}%" if 0<=curr<=1 else f"{abs(d):.1f}"
        color = "green" if d>0 else ("red" if d<0 else "gray")
        arrow = "↑" if d > 0 else ("↓" if d < 0 else "→")
        val = f"{abs(d) * 100:.1f}%" if 0 <= curr <= 1 else f"{abs(d):.1f}"
        color = "green" if d > 0 else ("red" if d < 0 else "gray")
        return arrow, val, color

    win_arr, win_d, win_col = make_delta(win_pct, prev_win_pct)
    net_arr, net_d, net_col = make_delta(net, prev_net)
    off_arr, off_d, off_col = make_delta(off, prev_off)
    def_arr, def_d, def_col = make_delta(deff, prev_def)

    # ── Player leaders ───────────────────────────────────────────────
    from team_charts import get_team_leaders
    leaders = get_team_leaders(
        dfs["df_player_all"], team_id, season_type=season_type
    )

    # ── Package everything ─────────────────────────────────────────────
    return {
        # league stats
        "league_median": league_median,
        "league_percentile": league_percentile,
        "pra_median": pra_median,
        "pra_percentile": pra_percentile,
        # previous season
        "prev_win_pct": prev_win_pct,
        "prev_net_rating": prev_net,
        "prev_off_rating": prev_off,
        "prev_def_rating": prev_def,
    }