# scripts/scrape_all_teams.py
"""
Unified NBA Team Scraper
========================

Phase 1  (always)
-----------------
Pull every franchise (active + historical) from the public
`commonteamyears` Stats-API and save → data/raw/teams_basic.csv

Fields:
    team_id • team_name • nickname • short_code • team_url • logo_url
    • first_season • last_season • is_active

Phase 2  (--detailed)
---------------------
Call TeamDetails  +  TeamInfoCommon for each TEAM_ID and enrich with
    city • state • arena • capacity • owner • head_coach
    conferenceName • divisionName
and save → data/raw/teams_detailed.csv
"""
# ---------------------------------------------------------------------------
import os, csv, time, argparse, requests, pandas as pd
from nba_api.stats.static import teams as static_teams
from nba_api.stats.endpoints import TeamDetails, TeamInfoCommon
# ── add at the top with the other imports
from nba_api.stats.endpoints import CommonTeamRoster

SEASON = "2024-25"          # current season string once, reuse everywhere

# ---------------------------------------------------------------------------
RAW_DIR      = "data/raw"
BASIC_OUT    = os.path.join(RAW_DIR, "teams_basic.csv")
DETAILED_OUT = os.path.join(RAW_DIR, "teams_detailed.csv")

TEAM_LIST_URL = "https://stats.nba.com/stats/commonteamyears?LeagueID=00"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/136.0 Safari/537.36"
    ),
    "Origin":  "https://www.nba.com",
    "Referer": "https://www.nba.com/",
}

LOGO     = "https://cdn.nba.com/logos/nba/{tid}/primary/L/logo.svg"
TEAM_URL = "https://www.nba.com/team/{tid}"
TEAM_DETAIL_URL = "https://stats.nba.com/stats/teamdetails?TeamID={tid}"
# ---------------------------------------------------------------------------
# Phase 1 ────────────────────────────────────────────────────────────────────
def scrape_basic() -> list[dict]:
    """Download franchise list and write teams_basic.csv"""
    js   = requests.get(TEAM_LIST_URL, headers=HEADERS, timeout=30).json()
    hdr  = js["resultSets"][0]["headers"]
    rows = js["resultSets"][0]["rowSet"]
    df   = pd.DataFrame(rows, columns=hdr)

    # static map → id ➜ (full, abbrev, nickname)
    static_map = {
        t["id"]: (t["full_name"], t["abbreviation"], t["nickname"])
        for t in static_teams.get_teams()
    }
    current_season = df["MAX_YEAR"].max()

    records = []
    for _, row in df.iterrows():
        tid = int(row["TEAM_ID"])
        full, abbr, nick = static_map.get(tid, (f"Team {tid}", "", ""))

        rec = {
            "team_id"     : tid,
            "team_name"   : full,
            "nickname"    : nick,
            "short_code"  : abbr,
            "team_url"    : TEAM_URL.format(tid=tid),
            "logo_url"    : LOGO.format(tid=tid),
            "first_season": row["MIN_YEAR"],
            "last_season" : row["MAX_YEAR"],
            "is_active"   : row["MAX_YEAR"] == current_season,
        }
        records.append(rec)

    os.makedirs(RAW_DIR, exist_ok=True)
    pd.DataFrame(records).to_csv(
        BASIC_OUT, index=False, quoting=csv.QUOTE_NONNUMERIC
    )
    print(f"✅ Basic franchises ({len(records)}) saved → {BASIC_OUT}")
    return records
# ---------------------------------------------------------------------------

def _get_head_coach_name(tid: int, season: str = SEASON) -> str:
    """
    Return the full name of the current head coach for one team
    using CommonTeamRoster → Coaches table.
    """
    try:
        data = CommonTeamRoster(team_id=tid, season=season) \
                 .get_normalized_dict()["Coaches"]
        for c in data:
            if (c.get("COACH_TYPE", "").lower() == "head coach"
                    or c.get("IS_ASSISTANT", "").upper() == "N"):
                # COACH_NAME is already "First Last"; fall back to split fields
                return (c.get("COACH_NAME")
                        or f"{c.get('FIRST_NAME','')} {c.get('LAST_NAME','')}".strip())
    except Exception:
        pass
    return ""


# Helpers for Phase 2
def fetch_team_details(tid: int) -> dict:
    """
    Merge TeamDetails + TeamInfoCommon into one row.
    Returns every field even if one of the endpoints fails.
    """
    out = {
        "city": "", "state": "", "arena": "", "capacity": "",
        "owner": "", "head_coach": "",
        "conferenceName": "", "divisionName": ""
    }

    # ── 1. TeamDetails  → city / arena / owner / coach ────────────────
    try:
        js   = requests.get(TEAM_DETAIL_URL.format(tid=tid),
                            headers=HEADERS, timeout=15).json()
        hdr  = js["resultSets"][0]["headers"]
        row  = js["resultSets"][0]["rowSet"][0]
        det  = dict(zip(hdr, row))
        out.update(
            city       = det.get("CITY")           or "",
            state      = det.get("STATE")          or "",
            arena      = det.get("ARENA")          or "",
            capacity   = det.get("ARENACAPACITY")  or "",
            owner      = det.get("OWNER")          or "",
            head_coach = det.get("HEADCOACH") or "",
        )
    except Exception:
        pass   # keep defaults if endpoint 1 fails

    # ── 2. TeamInfoCommon  → conference / division ────────────────────
    try:
        info = TeamInfoCommon(team_id=tid) \
                 .get_normalized_dict()["TeamInfoCommon"][0]
        out.update(
            conferenceName = info.get("TEAM_CONFERENCE") or "",
            divisionName   = info.get("TEAM_DIVISION")   or "",
        )
    except Exception:
        pass   # keep defaults if endpoint 2 fails

    return out


# ---------------------------------------------------------------------------
def scrape_detailed(basic: list[dict]):
    """Enrich with TeamDetails + TeamInfoCommon and write teams_detailed.csv"""
    detailed, counter = [], 0
    for rec in basic:
        counter += 1
        tid = rec["team_id"]
        print(f"🔍 [{counter}/{len(basic)}] {rec['short_code']} – {rec['team_name']}")
        rec.update(fetch_team_details(tid))
        detailed.append(rec)

        # batch-save every 10
        if counter % 10 == 0:
            pd.DataFrame(detailed).to_csv(
                DETAILED_OUT, index=False, quoting=csv.QUOTE_NONNUMERIC
            )
            time.sleep(0.3)   # gentle throttle

    pd.DataFrame(detailed).to_csv(
        DETAILED_OUT, index=False, quoting=csv.QUOTE_NONNUMERIC
    )
    print(f"✅ Detailed franchises saved → {DETAILED_OUT}")
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--detailed", action="store_true",
        help="also scrape city, arena, owner, coach, conference & division"
    )
    args = parser.parse_args()

    base_records = scrape_basic()
    if args.detailed:
        scrape_detailed(base_records)
