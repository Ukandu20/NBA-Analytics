#!/usr/bin/env python3
"""
r_sos_v1.py  • V1 SRS & scaled SOS (0 = easiest, 1 = hardest)
------------------------------------------------------------------
Reads raw schedule CSVs to compute:
  • srs_v1      (pts, centered ⟨SRS⟩=0)
  • sos_v1_raw  (mean opponent SRS, pts)
  • sos_v1      (min-max scaled [0,1])

Outputs per-season:
  data/processed/feature_store/srs_v1/<SEASON>/srs_sos_v1.csv
Columns: season, team, team_id, srs_v1, sos_v1_raw, sos_v1
"""
from pathlib import Path
import pandas as pd
import numpy as np

# ── CONFIG ───────────────────────────────────────────────────────────────
ROOT = Path("data/processed/schedule")             # e.g. .../2024-25
OUT_BASE   = Path("data/processed/feature_store/srs_v1")
DEFAULT_HCA = 3.0                                        # first season default
TOL         = 1e-3
MAX_ITERS   = 500

# ── HELPERS ──────────────────────────────────────────────────────────────
def estimate_league_hca(prev_path: Path) -> float:
    """League‐wide HCA = mean(home MOV) in last season."""
    df   = pd.read_csv(prev_path / "league_regular_season_schedule.csv")
    home = df[df["team"] == df["home_team"]]
    hca = (home["pf"] - home["opp_pf"]).mean()
    return hca




def load_games(season_path: Path, hca_prev: float) -> pd.DataFrame:
    """Load schedule, cast IDs, derive symmetric neutral_mov."""
    df = pd.read_csv(season_path / "league_regular_season_schedule.csv")
    # ensure numeric IDs & scores
    df[["team_id","opponent_id","pf","opp_pf"]] = (
        df[["team_id","opponent_id","pf","opp_pf"]]
        .apply(pd.to_numeric, downcast="integer")
    )
    df[["team_id","opponent_id"]] = df[["team_id","opponent_id"]].astype("int64")
    # home flag
    df["is_home"] = (df["team"] == df["home_team"]).astype("int8")
    
    # Neutralize MOV for SRS:
    df["neutral_mov"] = df["pf"] - df["opp_pf"] - hca_prev * (2 * df["is_home"] - 1)
    return df[["team_id","opponent_id","neutral_mov"]]

def solve_srs(df_games: pd.DataFrame, teams: pd.Index) -> pd.Series:
    """Iteratively solve SRS until sup‐norm update < TOL."""
    srs = pd.Series(0.0, index=teams)
    for itr in range(MAX_ITERS):
        # mean opponent SRS per team
        opp = (
            df_games
                .merge(srs.rename("opp_srs"),
                left_on="opponent_id", right_index=True)
                .groupby("team_id")["opp_srs"]
                .mean()
                .reindex(teams)
        )
        mov = df_games.groupby("team_id")["neutral_mov"].mean().reindex(teams)
        new = mov + opp
        new -= new.mean()  # re-centre
        delta = (new - srs).abs().max()
        if delta < TOL:
            print(f"    converged in {itr+1} iters (Δ={delta:.4f})")
            return new
        srs = new
    raise RuntimeError("SRS solver did not converge")

# ── MAIN BUILD LOOP ──────────────────────────────────────────────────────
def build_all():
    seasons = sorted(p.name for p in ROOT.iterdir() if p.is_dir())
    for prev, curr in zip(seasons[:-1], seasons[1:]):
        print(f">>> Season {curr}")
        prev_path = ROOT / prev
        curr_path = ROOT / curr

        # 1) league HCA from prior season
        league_hca = estimate_league_hca(prev_path) if prev_path.exists() else DEFAULT_HCA

        # 2) load games & neutral_mov
        games = load_games(curr_path, league_hca)

        # 3) solve SRS
        # load team list + abbrev
        teams_df = (
            pd.read_csv(curr_path / "league_regular_season_schedule.csv")
              [["team_id","team"]]
              .drop_duplicates()
              .set_index("team_id")
        )
        teams = teams_df.index
        srs   = solve_srs(games, teams)

        # 4) raw SOS = mean opponent SRS
        sos_raw = (
            games
              .merge(srs.rename("opp_srs"),
                     left_on="opponent_id", right_index=True)
              .groupby("team_id")["opp_srs"]
              .mean()
              .reindex(teams)
        )

        # 5) 0–1 scaling per season
        scaled = (sos_raw - sos_raw.min()) / (sos_raw.max() - sos_raw.min())

        # 6) assemble output
        out = pd.DataFrame({
            "season"    : curr.replace("_","-"),
            "team_id"   : teams,
            "team"      : teams_df["team"].loc[teams],
            "srs_v1"    : srs.round(3),
            "sos_v1_raw": sos_raw.round(3),
            "sos_v1"    : scaled.round(4),
        }).sort_values("srs_v1", ascending=False)

        out = out.round({
            "srs_v1"    : 2,
            "sos_v1_raw": 3,
            "sos_v1"    : 3,
        })
        
        # 7) write CSV
        dest = OUT_BASE / curr.replace("-","_") / "srs_sos_v1.csv"
        dest.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(dest, index=False)
        print(f"    ✔ wrote {dest}")

if __name__ == "__main__":
    build_all()
