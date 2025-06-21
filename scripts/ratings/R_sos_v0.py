#!/usr/bin/env python3
"""
V0 SOS + season win%  • writes one CSV per season
If only the first season exists, SOS = 0 and win_pct is computed.
"""
from pathlib import Path
import pandas as pd

ROOT      = Path("data/processed/schedule")
OUT_BASE  = Path("data/processed/feature_store/r_sos_v0")
HCA       = 3.0

# ───────────────────────────────────────────────────────────────────
def load_one(season_dir: Path) -> pd.DataFrame:
    csv = season_dir / "league_regular_season_schedule.csv"
    df  = pd.read_csv(csv, parse_dates=["game_date"])
    df["season"] = season_dir.name                       # e.g. 2024-25

    req = {"season","team_id","team","opponent_id",
           "home_team","pf","opp_pf","wl"}
    miss = req.difference(df.columns)
    if miss:
        raise ValueError(f"{csv}: missing {sorted(miss)}")

    ints = ["team_id","opponent_id","pf","opp_pf"]
    df[ints] = df[ints].apply(pd.to_numeric, downcast="integer")
    df[["team_id","opponent_id"]] = df[["team_id","opponent_id"]].astype("int64")

    # win flag (0/1)
    df["win_flag"] = (df["wl"].str.upper() == "W").astype("int8")
    # home flag
    df["is_home"]  = (df["team"] == df["home_team"]).astype("int8")
    # symmetric HCA
    df["mov_adj"]  = df["pf"] - df["opp_pf"] - HCA * (2*df["is_home"] - 1)

    if abs(df["mov_adj"].mean()) > 0.05:
        raise ValueError(f"MOV drift {df['mov_adj'].mean():.3f} in {season_dir.name}")

    # quick season-level centring sanity
    eps = abs(df["mov_adj"].mean())

    return df[["season","team_id","team","opponent_id","mov_adj","win_flag"]]

def load_all() -> pd.DataFrame:
    frames = [load_one(p) for p in ROOT.iterdir() if p.is_dir()]
    return pd.concat(frames, ignore_index=True)

# ───────────────────────────────────────────────────────────────────
def sos_and_win_pct(df: pd.DataFrame) -> pd.DataFrame:
    seasons = sorted(df["season"].unique())
    out_rows = []

    for i, curr in enumerate(seasons):
        prev = seasons[i-1] if i > 0 else None

        # last-season strength map (if available)
        if prev:
            strength = (df.query("season == @prev")
                          .groupby("team_id")["mov_adj"].mean())
        else:
            strength = pd.Series(dtype="float64")

        cur = df.query("season == @curr")[["team_id","team",
                                           "opponent_id","win_flag"]]

        if prev:
            cur = cur.merge(strength.rename("mov_prev"),
                            left_on="opponent_id", right_index=True,
                            how="left")
            sos = (cur.groupby(["team_id","team"])["mov_prev"]
                       .mean().rename("sos_v0"))
        else:
            sos = (cur.groupby(["team_id","team"])
                       .size().rename("dummy")) * 0.0   # sos = 0 for first year
            sos.name = "sos_v0"

        # win %
        wins  = cur.groupby("team_id")["win_flag"].sum()
        games = cur.groupby("team_id")["win_flag"].size()
        win_pct = (wins / games).rename("win_pct")
        # 3 decimal places
        win_pct = win_pct.round(3)

        merged = (
            sos.reset_index()
               .merge(win_pct.reset_index(), on="team_id")
               .assign(season=curr)
        )
        out_rows.append(merged)

    return pd.concat(out_rows, ignore_index=True)

# ───────────────────────────────────────────────────────────────────
def main():
    df  = load_all()
    sos = sos_and_win_pct(df)

    for season, chunk in sos.groupby("season"):
        chunk = chunk[["season","team","team_id","sos_v0","win_pct"]]
        season_str = str(season)
        out   = OUT_BASE / season_str.replace("-", "_") / "sos_v0.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        chunk.to_csv(out, index=False)

    print(f"✅ wrote {len(sos)} rows to {OUT_BASE}/<season>/sos_v0.csv")

if __name__ == "__main__":
    main()
