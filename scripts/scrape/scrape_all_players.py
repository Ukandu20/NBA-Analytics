import os, csv, time, json, argparse, random, requests
from urllib.parse import urlparse

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import settings
# ---------------------------------------------------------------------------
BASE_URL = "https://www.nba.com"
STATS_URL = settings.API_ENDPOINTS["common_all_players"].format(
    season="2024-25"
)
PLAYER_INFO_URL = settings.API_ENDPOINTS["common_player_info"]  # accepts {player_id}


# head-shot CDN pattern (1040 × 760)
HEADSHOT_CDN = (
    "https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
)
CHECK_CDN = True  # HEAD-request to verify the file really exists

BASIC_OUT     = settings.RAW_PLAYERS_BASIC
DETAILED_OUT  = settings.RAW_PLAYERS_DETAILED

HEADERS = settings.API_HEADERS
# ---------------------------------------------------------------------------
def bio_fields(pid: int) -> dict:
    """
    Return dict with number / position / height / weight / school / country
              + birthdate / experience / draft.
    Blanks on failure.
    """
    blank = {k: "" for k in (
        "number", "position", "height", "weight", "last_attended", "country",
        "birthdate", "experience", "draft"
    )}
    try:
        js  = requests.get(PLAYER_INFO_URL.format(pid=pid), headers=HEADERS, timeout=8).json()
        row = dict(zip(js["resultSets"][0]["headers"],
                       js["resultSets"][0]["rowSet"][0]))

        # draft string: "2014 #41" or "" for undrafted
        dy = (row.get("DRAFT_YEAR")   or "").strip()
        dn = (row.get("DRAFT_NUMBER") or "").strip()
        draft = f"{dy} #{dn}".strip().rstrip("#").strip()

        return {
            "number":        row.get("JERSEY")  or "",
            "position":      row.get("POSITION") or "",
            "height":        row.get("HEIGHT")   or "",
            "weight":        row.get("WEIGHT")   or "",
            "last_attended": row.get("SCHOOL")   or "",
            "country":       row.get("COUNTRY")  or "",
            "birthdate":     (row.get("BIRTHDATE") or "")[:10],  # YYYY-MM-DD
            "experience":    f"{row.get('SEASON_EXP', '')} Years".strip(),
            "draft":         draft,
        }
    except Exception:
        return blank

# ────────────────────────────────────────────────────────────────────────────
# Phase 1
def scrape_basic() -> list[dict]:
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
            "player":      p["DISPLAY_FIRST_LAST"],
            "team":        p["TEAM_ABBREVIATION"] or "",
            **bio_fields(int(p["PERSON_ID"])),     # ← real number/ht/wt/…
            "profile_url": f"{BASE_URL}/player/{int(p['PERSON_ID'])}/{slug}",
        }
        rec["is_active"]     = bool(p["ROSTERSTATUS"])
        rec["is_free_agent"] = rec["is_active"] and p["TEAM_ID"] == 0
        rec["is_retired"]    = not rec["is_active"]
        records.append(rec)

    os.makedirs(RAW_DIR, exist_ok=True)
    pd.DataFrame(records).to_csv(BASIC_OUT, index=False)
    print(f"✅ Basic roster ({len(records)}) saved → {BASIC_OUT}")
    return records

# ────────────────────────────────────────────────────────────────────────────
# Helpers for Phase 2
def fetch_api_info(pid: str) -> dict:
    """Fallback: grab birthdate / draft / experience via Stats API."""
    try:
        js = requests.get(
            PLAYER_INFO_URL.format(pid=pid), headers=HEADERS, timeout=10
        ).json()
        row = js["resultSets"][0]["rowSet"][0]
        hdr = js["resultSets"][0]["headers"]
        d   = dict(zip(hdr, row))
        return {
            "birthdate":  d.get("BIRTHDATE"),
            "draft":      f"{d.get('DRAFT_YEAR', '').strip()} "
                          f"#{d.get('DRAFT_NUMBER', '').strip()}".strip(),
            "experience": f"{d.get('SEASON_EXP', '')} Years".strip()
        }
    except Exception:
        return {}

def is_histadd(url: str) -> bool:
    return "histadd" in url.lower()

# ────────────────────────────────────────────────────────────────────────────
# Phase 2
def get_profile(driver, url: str, pid: str) -> dict:
    """
    Best-effort extraction of headshot / bio.

    1. Try the <img alt*="headshot"> on the page (case-insensitive)
    2. Fallback: deterministic CDN URL 1040×760/<PID>.png
    3. HISTADD pages → no modern head-shot
    """
    if is_histadd(url):
        return {"headshot_url": None, **fetch_api_info(pid), "legacy": True}

    def scrape_once() -> dict:
        driver.get(url)

        # ---------- HEADSHOT ----------
        try:
            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'img[alt*="headshot" i]')
                )
            )
            img = driver.find_element(By.CSS_SELECTOR, 'img[alt*="headshot" i]')
            hshot = img.get_attribute("src")
        except Exception:
            hshot = None

        # fallback to CDN pattern if selector failed
        if not hshot:
            cdn_url = HEADSHOT_CDN.format(pid=pid)
            if CHECK_CDN:
                try:
                    ok = requests.head(cdn_url, headers=HEADERS, timeout=4).status_code == 200
                    hshot = cdn_url if ok else None
                except Exception:
                    hshot = None
            else:
                hshot = cdn_url

        out = {
            "headshot_url": hshot,
            "birthdate": None,
            "experience": None,
            "draft": None,
            "legacy": False,
        }

        # ---------- BIO BOX -------------
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

    # one attempt + one retry if network hiccups
    try:
        return scrape_once()
    except Exception:
        time.sleep(random.uniform(2, 4))
        try:
            return scrape_once()
        except Exception as e:
            print(f"⚠️  fatal profile {url.split('/')[-1]} : {e.__class__.__name__}")
            return {"headshot_url": None, **fetch_api_info(pid), "legacy": False}

# ────────────────────────────────────────────────────────────────────────────
def scrape_detailed(basic: list[dict]):
    """Enrich basic roster with profile fields → players_detailed.csv"""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=opts)

    detailed, counter = [], 0
    for rec in basic:
        counter += 1
        pid = urlparse(rec["profile_url"]).path.split("/")[2]
        print(f"🔍 [{counter}/{len(basic)}] {rec['player']}")
        rec.update(get_profile(driver, rec["profile_url"], pid))
        detailed.append(rec)

        # batch-save every 25
        if counter % 25 == 0:
            pd.DataFrame(detailed).to_csv(DETAILED_OUT, index=False)

        # tiny pause to avoid rate-limiting
        time.sleep(0.15)

        # restart Chrome every 500 to avoid leaks
        if counter % 500 == 0:
            driver.quit()
            driver = webdriver.Chrome(options=opts)

    driver.quit()
    pd.DataFrame(detailed).to_csv(DETAILED_OUT, index=False)
    print(f"✅ Detailed roster saved → {DETAILED_OUT}")

# ────────────────────────────────────────────────────────────────────────────
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
