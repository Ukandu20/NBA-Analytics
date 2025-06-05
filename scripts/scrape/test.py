# scripts/scrape_all_players.py

"""
Unified NBA Player Scraper (with centralized configuration)
===========================================================

Phase 1  (always)
-----------------
Pull *every* player ever to appear in an NBA season from the public
`commonallplayers` Stats-API and save → data/raw/players_basic.csv

Fields:
    player • team • number • position • height • weight • last_attended • country
    • profile_url • is_active • is_free_agent • is_retired

Phase 2  (--detailed)
---------------------
Visit each profile URL, fetch (if present)
    headshot_url • birthdate • experience • draft • legacy
and save → data/raw/players_detailed.csv

Legacy “HISTADD” pages have no modern profile; they are skipped but still kept
in the CSV (headshot_url = None, legacy = True).

Usage:
    python scripts/scrape_all_players.py            # only basic CSV
    python scripts/scrape_all_players.py --detailed # plus profile enrichment
"""

import os, sys
import csv
import time
import random
import argparse
from urllib.parse import urlparse

import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ──────────────────────────
# Import centralized settings
# ──────────────────────────
this_dir = os.path.dirname(__file__)                            # .../scripts/scrape
project_root = os.path.abspath(os.path.join(this_dir, "..", ".."))  # .../NBA Analytics
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    
import settings

# ──────────────────────────
# Constants from settings
# ──────────────────────────

# Base URL for constructing profile links
BASE_URL = "https://www.nba.com"

# Stats API endpoint templates (from config.yaml → settings.API_ENDPOINTS)
COMMON_PLAYERS_URL = settings.API_ENDPOINTS["common_all_players"].format(
    season="2024-25"
)
PLAYER_INFO_URL = settings.API_ENDPOINTS["common_player_info"]  # accepts {player_id}

# Output file paths (from settings)
BASIC_OUT     = settings.RAW_PLAYERS_BASIC
DETAILED_OUT  = settings.RAW_PLAYERS_DETAILED

# HTTP headers for NBA stats API (from settings)
HEADERS = settings.API_HEADERS

# ──────────────────────────
# Helper: fetch basic bio fields from Stats API
# ──────────────────────────
def bio_fields(pid: int) -> dict:
    """
    Call commonplayerinfo to retrieve:
      - number (JERSEY)
      - position (POSITION)
      - height   (HEIGHT)
      - weight   (WEIGHT)
      - last_attended (SCHOOL)
      - country  (COUNTRY)
      - birthdate (BIRTHDATE, YYYY-MM-DD)
      - experience (SEASON_EXP)
      - draft     (DRAFT_YEAR + DRAFT_NUMBER)
    Returns a dict with those keys (or blanks if any fail).
    """
    blank = {
        "number":        "",
        "position":      "",
        "height":        "",
        "weight":        "",
        "last_attended": "",
        "country":       "",
        "birthdate":     "",
        "experience":    "",
        "draft":         ""
    }
    try:
        resp = requests.get(PLAYER_INFO_URL.format(pid=pid), headers=HEADERS, timeout=10)
        js = resp.json()
        row = js["resultSets"][0]["rowSet"][0]
        hdr = js["resultSets"][0]["headers"]
        d   = dict(zip(hdr, row))
        return {
            "number":        (d.get("JERSEY") or "").strip(),
            "position":      (d.get("POSITION") or "").strip(),
            "height":        (d.get("HEIGHT") or "").strip(),
            "weight":        (d.get("WEIGHT") or "").strip(),
            "last_attended": (d.get("SCHOOL") or "").strip(),
            "country":       (d.get("COUNTRY") or "").strip(),
            "birthdate":     (d.get("BIRTHDATE") or "").split("T")[0],
            "experience":    f"{d.get('SEASON_EXP','')} Years".strip(),
            "draft":         " ".join([
                (d.get("DRAFT_YEAR") or "").strip(),
                (d.get("DRAFT_NUMBER") or "").strip()
            ]).strip()
        }
    except Exception:
        return blank

# ──────────────────────────
# Phase 1: scrape_basic()
# ──────────────────────────
def scrape_basic() -> list[dict]:
    """
    Download the full roster via stats.nba.com/commonallplayers and write
    players_basic.csv to data/raw. Returns list of record dicts.
    """
    # 1) Call the Stats API for all players
    js = requests.get(COMMON_PLAYERS_URL, headers=HEADERS, timeout=30).json()
    hdr  = js["resultSets"][0]["headers"]
    rows = js["resultSets"][0]["rowSet"]
    df   = pd.DataFrame(rows, columns=hdr)  # every player since 1946-47

    records = []
    for _, p in df.iterrows():
        pid = int(p["PERSON_ID"])
        # PLAYERCODE may be empty for very old IDs → fallback on lowercased name
        slug = (p.get("PLAYERCODE") or 
                p["DISPLAY_FIRST_LAST"].lower().replace(" ", "-")).replace("_", "-")

        # ① get static roster flags
        rec = {
            "player":        p["DISPLAY_FIRST_LAST"].strip(),
            "team":          (p["TEAM_ABBREVIATION"] or "").strip(),
            "profile_url":   f"{BASE_URL}/player/{pid}/{slug}",
            "is_active":     bool(p["ROSTERSTATUS"])
        }
        rec["is_free_agent"] = rec["is_active"] and p["TEAM_ID"] == 0
        rec["is_retired"]    = not rec["is_active"]

        # ② call commonplayerinfo for richer bio
        bio = bio_fields(pid)
        rec.update(bio)

        records.append(rec)
        time.sleep(0.3)   # polite rate-limit

    # 3) Write out to CSV
    os.makedirs(settings.RAW_DIR, exist_ok=True)
    pd.DataFrame(records).to_csv(
        BASIC_OUT,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC
    )
    print(f"✅ Basic roster ({len(records)}) saved → {BASIC_OUT}")
    return records

# ──────────────────────────
# Helpers for Phase 2
# ──────────────────────────
def fetch_api_info(pid: str) -> dict:
    """
    Fallback: pull birthdate / draft / experience via Stats API, if get_profile fails.
    Returns dict with those keys or empty.
    """
    try:
        js = requests.get(
            PLAYER_INFO_URL.format(pid=pid),
            headers=HEADERS,
            timeout=10
        ).json()
        row = js["resultSets"][0]["rowSet"][0]
        hdr = js["resultSets"][0]["headers"]
        d   = dict(zip(hdr, row))
        return {
            "birthdate":  (d.get("BIRTHDATE") or "").split("T")[0],
            "draft":      " ".join([
                              (d.get("DRAFT_YEAR") or "").strip(),
                              (d.get("DRAFT_NUMBER") or "").strip()
                          ]).strip(),
            "experience": f"{d.get('SEASON_EXP','')} Years".strip()
        }
    except Exception:
        return {}

def is_histadd(url: str) -> bool:
    """Return True if this profile URL is a legacy HISTADD page."""
    return "histadd" in url.lower()

# ──────────────────────────
# Phase 2: get_profile() + scrape_detailed()
# ──────────────────────────
def get_profile(driver, url: str, pid: str) -> dict:
    """
    Best-effort extraction of headshot / bio from an NBA.com profile.
    If HISTADD (legacy) page, skip Selenium and fallback to Stats API.
    """
    if is_histadd(url):
        return {"headshot_url": None, **fetch_api_info(pid), "legacy": True}

    def scrape_once() -> dict:
        driver.get(url)
        # Try to locate the “Headshot” <img> quickly
        try:
            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'img[alt$="Headshot"]'))
            )
            img = driver.find_element(By.CSS_SELECTOR, 'img[alt$="Headshot"]')
            hshot = img.get_attribute("src")
        except Exception:
            hshot = None

        out = {
            "headshot_url": hshot,
            "birthdate":    None,
            "experience":   None,
            "draft":        None,
            "legacy":       False
        }

        # The profile “Summary” section uses <p class="PlayerSummary_playerInfoLabel__…">
        labels = driver.find_elements(
            By.CSS_SELECTOR, 'p[class^="PlayerSummary_playerInfoLabel"]'
        )
        vals = driver.find_elements(
            By.CSS_SELECTOR, 'p[class^="PlayerSummary_playerInfoValue"]'
        )
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

    # Attempt once; if network fails, wait and retry
    try:
        return scrape_once()
    except Exception:
        time.sleep(random.uniform(1.5, 3.0))
        try:
            return scrape_once()
        except Exception as e:
            print(f"⚠️  fatal profile {url.split('/')[-1]} : {e.__class__.__name__}")
            return {"headshot_url": None, **fetch_api_info(pid), "legacy": False}

def scrape_detailed(basic: list[dict]):
    """
    Enrich basic roster with headshot_url, birthdate, experience, draft → players_detailed.csv
    """
    opts = Options()
    opts.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=opts)

    detailed, counter = [], 0
    total = len(basic)
    for rec in basic:
        counter += 1
        pid = urlparse(rec["profile_url"]).path.split("/")[2]
        print(f"🔍 [{counter}/{total}] {rec['player']}")

        rec.update(get_profile(driver, rec["profile_url"], pid))
        detailed.append(rec)

        # Batch‐save every 25 records
        if counter % 25 == 0:
            pd.DataFrame(detailed).to_csv(
                DETAILED_OUT,
                index=False,
                quoting=csv.QUOTE_NONNUMERIC
            )

        # Restart Chrome every 500 to avoid memory leaks
        if counter % 500 == 0:
            driver.quit()
            driver = webdriver.Chrome(options=opts)

    driver.quit()
    pd.DataFrame(detailed).to_csv(
        DETAILED_OUT,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC
    )
    print(f"✅ Detailed roster saved → {DETAILED_OUT}")

# ──────────────────────────
# __main__
# ──────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape NBA player basic + detailed info."
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Also scrape headshot, birthdate, experience, draft (players_detailed.csv)"
    )
    args = parser.parse_args()

    # Phase 1: always get basic roster
    basic_records = scrape_basic()

    # Phase 2: if requested, enrich with profile details
    if args.detailed:
        scrape_detailed(basic_records)
