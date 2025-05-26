# scripts/scrape_all_players.py
"""
Unified NBA Player Scraper
==========================

Phase 1  (always)
-----------------
Pull *every* player ever to appear in an NBA season from the public
`commonallplayers` Stats-API and save → data/raw/players_basic.csv

Fields:
    player • team • profile_url • flags  is_active • is_free_agent • is_retired

Phase 2  (--detailed)
---------------------
Visit each profile URL, fetch (if present)
    headshot_url • birthdate • experience • draft
and save → data/raw/players_detailed.csv

Legacy “HISTADD” pages have no modern profile; they are skipped but still kept
in the CSV (headshot_url = None, legacy = True).

Run examples
------------
    py scripts\scrape_all_players.py             # basic CSV only
    py scripts\scrape_all_players.py --detailed  # + profile enrichment
"""
# ---------------------------------------------------------------------------
import os, csv, time, json, argparse, random, requests, pandas as pd
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# ---------------------------------------------------------------------------
BASE_URL  = "https://www.nba.com"
STATS_URL = (
    "https://stats.nba.com/stats/commonallplayers?"
    "IsOnlyCurrentSeason=0&LeagueID=00&Season=2024-25"
)
RAW_DIR       = "data/raw"
BASIC_OUT     = os.path.join(RAW_DIR, "players_basic.csv")
DETAILED_OUT  = os.path.join(RAW_DIR, "players_detailed.csv")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Origin":  "https://www.nba.com",
    "Referer": "https://www.nba.com/",
}

# ---------------------------------------------------------------------------
# Phase 1  ───────────────────────────────────────────────────────────────────
def scrape_basic():
    """Download roster from stats.nba.com and write players_basic.csv"""
    js   = requests.get(STATS_URL, headers=HEADERS, timeout=30).json()
    hdr  = js["resultSets"][0]["headers"]
    rows = js["resultSets"][0]["rowSet"]
    df   = pd.DataFrame(rows, columns=hdr)          # every player since 1946-47

    records = []
    for _, p in df.iterrows():
        # PLAYERCODE sometimes empty for very old IDs
        slug = (p.get("PLAYERCODE") or
                p["DISPLAY_FIRST_LAST"].lower().replace(" ", "-")).replace("_", "-")

        rec = {
            "player":        p["DISPLAY_FIRST_LAST"],
            "team":          p["TEAM_ABBREVIATION"] or "",
            "number":        "",          
            "position":      "",          
            "height":        "",
            "weight":        "",
            "last_attended": "",
            "country":       "",
            "profile_url":   f"{BASE_URL}/player/{int(p['PERSON_ID'])}/{slug}",
        }
        rec["is_active"]     = bool(p["ROSTERSTATUS"])
        rec["is_free_agent"] = rec["is_active"] and p["TEAM_ID"] == 0
        rec["is_retired"]    = not rec["is_active"]
        records.append(rec)

    os.makedirs(RAW_DIR, exist_ok=True)
    pd.DataFrame(records).to_csv(
        BASIC_OUT, index=False, quoting=csv.QUOTE_NONNUMERIC
    )
    print(f"✅ Basic roster ({len(records)}) saved → {BASIC_OUT}")
    return records

# ---------------------------------------------------------------------------
# Helpers for Phase 2
API_INFO = "https://stats.nba.com/stats/commonplayerinfo?PlayerID={pid}"

def fetch_api_info(pid: str) -> dict:
    """Fallback: grab birthdate / draft / experience via Stats API."""
    try:
        js = requests.get(
            API_INFO.format(pid=pid), headers=HEADERS, timeout=10
        ).json()
        row = js["resultSets"][0]["rowSet"][0]
        hdr = js["resultSets"][0]["headers"]
        d   = dict(zip(hdr, row))
        return {
            "birthdate":  d.get("BIRTHDATE"),
            "draft":      f"{d.get('DRAFT_YEAR','').strip()} "
                          f"#{d.get('DRAFT_NUMBER','').strip()}".strip(),
            "experience": f"{d.get('SEASON_EXP','')} Years".strip()
        }
    except Exception:
        return {}

def is_histadd(url: str) -> bool:
    return "histadd" in url.lower()

# ---------------------------------------------------------------------------
# Phase 2 ────────────────────────────────────────────────────────────────────
def get_profile(driver, url: str, pid: str) -> dict:
    """
    Best-effort extraction of headshot / bio.
    Legacy HISTADD pages → blank fields plus API fallback for birthdate/draft.
    """
    if is_histadd(url):
        return {"headshot_url": None, **fetch_api_info(pid), "legacy": True}

    def scrape_once() -> dict:
        driver.get(url)
        # small timeout: if headshot not found quickly, assume throttled
        try:
            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'img[alt$="Headshot"]')
                )
            )
            img = driver.find_element(By.CSS_SELECTOR, 'img[alt$="Headshot"]')
            hshot = img.get_attribute("src")
        except Exception:
            hshot = None

        out = {
            "headshot_url": hshot,
            "birthdate": None,
            "experience": None,
            "draft": None,
            "legacy": False,
        }

        labels = driver.find_elements(
            By.CSS_SELECTOR, 'p[class^="PlayerSummary_playerInfoLabel"]')
        vals = driver.find_elements(
            By.CSS_SELECTOR, 'p[class^="PlayerSummary_playerInfoValue"]')

        for lab, val in zip(labels, vals):
            key = lab.text.strip().lower()
            txt = val.text.strip()
            if key == "birthdate":
                out["birthdate"] = txt
            elif key == "experience":
                out["experience"] = txt
            elif key == "draft":
                out["draft"] = txt
        return out

    # one attempt + one retry if network fails
    try:
        return scrape_once()
    except Exception:
        time.sleep(random.uniform(2, 4))
        try:
            return scrape_once()
        except Exception as e:
            print(f"⚠️  fatal profile {url.split('/')[-1]} : {e.__class__.__name__}")
            return {"headshot_url": None, **fetch_api_info(pid), "legacy": False}

# ---------------------------------------------------------------------------
def scrape_detailed(basic: list[dict]):
    """Enrich basic roster with profile fields → players_detailed.csv"""
    opts = Options()
    opts.add_argument("--no-sandbox")
    driver = webdriver.Chrome(opts)

    detailed, counter = [], 0
    for rec in basic:
        counter += 1
        pid = urlparse(rec["profile_url"]).path.split("/")[2]
        print(f"🔍 [{counter}/{len(basic)}] {rec['player']}")
        rec.update(get_profile(driver, rec["profile_url"], pid))
        detailed.append(rec)

        # batch-save every 25
        if counter % 25 == 0:
            pd.DataFrame(detailed).to_csv(
                DETAILED_OUT, index=False, quoting=csv.QUOTE_NONNUMERIC
            )
        # restart Chrome every 500 to avoid leaks
        if counter % 500 == 0:
            driver.quit()
            driver = webdriver.Chrome(opts)

    driver.quit()
    pd.DataFrame(detailed).to_csv(
        DETAILED_OUT, index=False, quoting=csv.QUOTE_NONNUMERIC
    )
    print(f"✅ Detailed roster saved → {DETAILED_OUT}")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--detailed", action="store_true",
        help="also scrape headshot, birthdate, experience, draft"
    )
    args = parser.parse_args()

    base_records = scrape_basic()
    if args.detailed:
        scrape_detailed(base_records)
