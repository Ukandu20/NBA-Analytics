"""
Split the cleaned master roster into one CSV per “team”
=======================================================

Creates
-------
data/teams/<CODE>/players.csv
  • <CODE> is the uppercase team abbreviation
  • FA  – current free agents
  • RET – retired players

Usage
-----
# full roster, including FA & RET
python scripts/split_players_by_team.py

# keep only currently-active players in franchise files
# (RET file still generated, but franchise folders won't contain retirees)
python scripts/split_players_by_team.py --active-only
"""
import os
import argparse
import pandas as pd

INFILE  = "data/processed/players_bios_cleaned.csv"
OUTROOT = "data/teams"


def main(active_only: bool):
    if not os.path.exists(INFILE):
        raise FileNotFoundError(
            f"{INFILE} not found. Run clean_players.py first."
        )

    df = pd.read_csv(INFILE)

    # ensure special codes are upper-case strings
    df["team"] = df["team"].astype(str).str.upper()

    # filter if requested
    if active_only and "is_active" in df.columns:
        df = df[df["is_active"]]

    # make sure FA / RET always exist even if empty after filtering
    for special in ("FA", "RET"):
        if special not in df["team"].unique():
            df = pd.concat(
                [df, pd.DataFrame({"team": [special]})],
                ignore_index=True,
                sort=False,
            )

    # write one CSV per team
    n_files = 0
    for code, grp in df.groupby("team"):
        code_sanitised = str(code).strip().replace(" ", "_") or "UNKNOWN"
        out_dir  = os.path.join(OUTROOT, code_sanitised)
        os.makedirs(out_dir, exist_ok=True)
        outfile  = os.path.join(out_dir, "players.csv")
        grp.to_csv(outfile, index=False)
        n_files += 1

    print(f"✅ wrote {n_files} team file(s) under {OUTROOT}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--active-only",
        action="store_true",
        help="exclude players with is_active == False from franchise files "
             "(RET still produced separately)",
    )
    main(**vars(ap.parse_args()))
