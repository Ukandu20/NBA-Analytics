#!/usr/bin/env python3
"""
r_sos_v0.py
─────────────────────────────
Outputs, for every season S in data/processed/schedule/<S>/ :

    data/processed/feature_store/r_schedule_feats_v0/<S>/schedule_feats.csv

columns
--------
season              "2024-25"
team_abbr           e.g. ATL
team_id             1610612737
team_hca            previous-season home-court advantage (pts)
team_rcp            previous-season road court performance (pts)
win_pct             team win%
opp_win_pct         mean win% of this season’s opponents
opp_opp_win_pct     mean win% of opponents’ opponents
sos_v0              previous-season strength of schedule (pts)

Notes
• HCA for the *first* season in your scrape falls back to the league-wide
  constant (3.0 pts).
• Neutral-site games (team ≠ home_team) contribute 0 to team HCA.
"""

from pathlib import Path
import pandas as pd

# ── paths ────────────────────────────────────────────────────────────────
ROOT      = Path("data/processed/schedule")        # raw schedules by season
OUT_BASE  = Path("data/processed/feature_store/r_schedule_feats_v0")
DEFAULT_HCA = 3.0                                 # first season default
DEFAULT_RCP = -3.0                                 # first season default

# ── helpers ───────────────────────────────────────────────────────────────
def team_hca(season_path: Path) -> pd.Series:
    """Per-team HCA (pts) from *that* season’s home games."""
    df   = pd.read_csv(season_path / "league_regular_season_schedule.csv")
    home = df[df["team"] == df["home_team"]]
    hca  = (home["pf"] - home["opp_pf"]).groupby(home["team_id"]).mean()
    return hca          # index: team_id, values: float pts

def team_rcp(season_path: Path) -> pd.Series:
    """Per-team road court performance (pts) from *that* season’s away games."""
    df   = pd.read_csv(season_path / "league_regular_season_schedule.csv")
    away = df[df["team"] == df["away_team"]]
    rcp  = (away["pf"] - away["opp_pf"]).groupby(away["team_id"]).mean()
    return rcp          # index: team_id, values: float pts

def load_games(season_path: Path, hca_prev: pd.Series, rcp_prev: pd.Series) -> pd.DataFrame:
    """Return one DataFrame with mov_adj, win_flag, etc. for the season."""
    df = pd.read_csv(season_path / "league_regular_season_schedule.csv")
    df["season"] = season_path.name                       # e.g. 2024-25

    

    ints = ["team_id", "opponent_id", "pf", "opp_pf"]
    df[ints] = df[ints].apply(pd.to_numeric, downcast="integer")
    df[["team_id", "opponent_id"]] = df[["team_id", "opponent_id"]].astype("int64")

    df["is_away"] = (df["team"] == df["away_team"]).astype("int8")
    df["is_home"]  = (df["team"] == df["home_team"]).astype("int8")
    df["win_flag"] = (df["wl"].str.upper() == "W").astype("int8")

    # map per-team HCA, fallback to league mean, then to constant
    league_hca = hca_prev.mean() if len(hca_prev) else DEFAULT_HCA
    df["hca_team"] = df["team_id"].map(hca_prev).fillna(league_hca)

    # map per-team RCP, fallback to league mean
    league_rcp = rcp_prev.mean() if len(rcp_prev) else DEFAULT_RCP
    df["rcp_team"] = df["team_id"].map(rcp_prev).fillna(league_rcp)

    # mov_adj = margin of victory adjusted for symmetric home-court
    df["mov_adj"] = (
        df["pf"] - df["opp_pf"] -
        df["hca_team"] * (2 * df["is_home"] - 1)
    )

    #mov_adj_away = margin of victory adjusted for away games
    df["mov_adj_away"] = (
        df["opp_pf"] - df["pf"] + 
        df["rcp_team"] * (2 * df["is_away"] - 1)
    )

    
    return df


# ── main build loop ──────────────────────────────────────────────────────
def build_features() -> None:
    seasons = sorted(p.name for p in ROOT.iterdir() if p.is_dir())

    for idx, season in enumerate(seasons):
        path       = ROOT / season
        prev_path  = ROOT / seasons[idx - 1] if idx else None
        hca_prev   = team_hca(prev_path) if prev_path else pd.Series(dtype=float)
        rcp_prev   = team_rcp(prev_path) if prev_path else pd.Series(dtype=float)
        games      = load_games(path, hca_prev, rcp_prev)

        # team win %
        gp_team = games.groupby("team_id")["win_flag"]
        win_pct = (gp_team.sum() / gp_team.size()).rename("win_pct")

        # 4a) opponent win %: map win_pct to each matchup
        games = games.merge(win_pct, left_on="opponent_id", right_index=True)
        opp_win_pct = games.groupby("team_id")["win_pct"].mean().rename("opp_win_pct")

        # 4b) opponent-of-opponent win %: map opp_win_pct back through opponent_id
        games = games.merge(opp_win_pct.rename("opp_wp"), left_on="opponent_id", right_index=True)
        opp_opp_win_pct = games.groupby("team_id")["opp_wp"].mean().rename("opp_opp_win_pct")

        # current-season team HCA (for reporting)
        team_hca_curr = team_hca(path).rename("team_hca")

        #current-season road court performance (for reporting)
        team_rcp_curr = team_rcp(path).rename("team_rcp")

        # ── compute v0 Strength-of-Schedule for this season ─────────────────
        if prev_path:
            # load last season’s games to get mov_adj
            prev_games = load_games(prev_path, team_hca(prev_path), team_rcp(prev_path))
            prev_mov   = prev_games.groupby("team_id")["mov_adj"] \
                                .mean() \
                                .rename("mov_prev")
            # map each opponent → its previous‐season strength, then average
            sos_v0 = (
                games[["team_id","opponent_id"]]
                .merge(prev_mov, left_on="opponent_id", right_index=True, how="left")["mov_prev"]
                .groupby(games["team_id"])
                .mean()
                .rename("sos_v0")
            )
        else:
            # first season: define SOS ≡ 0
            sos_v0 = pd.Series(0.0, index=win_pct.index, name="sos_v0")
        

        # assemble
        out = (
            pd.DataFrame({"team_id": win_pct.index})
                .merge(team_hca_curr, left_on="team_id", right_index=True, how="left")
                .merge(team_rcp_curr, left_on="team_id", right_index=True, how="left")
                .merge(win_pct,        left_on="team_id", right_index=True)
                .merge(opp_win_pct,    left_on="team_id", right_index=True)
                .merge(opp_opp_win_pct,left_on="team_id", right_index=True)
                .merge(sos_v0.rename("sos_v0"), left_on="team_id", right_index=True, how="left")
            )

        out.insert(0, "season", season)
        out = out.merge(
            games[["team_id", "team"]].drop_duplicates(), on="team_id"
        )
        out = out[["season", "team", "team_id",
                   "team_hca", "team_rcp", "win_pct", "opp_win_pct", "opp_opp_win_pct", "sos_v0"]]
        out = out.round({"team_hca": 2, "team_rcp": 2,"win_pct": 3,
                         "opp_win_pct": 3, "opp_opp_win_pct": 3, "sos_v0": 3})

        # write
        dest = OUT_BASE / season.replace("-", "_") / "schedule_feats.csv"
        dest.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(dest, index=False)
        print("✔ wrote", dest)

if __name__ == "__main__":
    build_features()
