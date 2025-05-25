# scripts/scrape_players.py

import os
import csv
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# File paths
MAIN_CACHE = "data/raw/players_raw_detailed.csv"
PROFILE_CACHE = "data/raw/player_profiles.csv"
BASE_URL = "https://www.nba.com"


def get_player_profile_data(driver, profile_url):
    """Fetch headshot, experience, birthdate from individual profile page."""
    data = {"headshot_url": None, "experience": None, "birthdate": None}
    # Ensure URL is absolute
    if not profile_url.startswith("http"):
        profile_url = BASE_URL + profile_url
    try:
        driver.get(profile_url)
    except Exception as e:
        print(f"⚠️ Could not navigate to {profile_url}: {e}")
        return data
    try:
        # Wait for headshot image to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'img[alt$="Headshot"]'))
        )
        time.sleep(1)
        # HEADSHOT
        try:
            img = driver.find_element(By.CSS_SELECTOR, 'img[alt$="Headshot"]')
            data['headshot_url'] = img.get_attribute('src')
        except Exception:
            pass
        # PROFILE INFO LABELS & VALUES
        labels = driver.find_elements(By.CSS_SELECTOR, 'p[class^="PlayerSummary_playerInfoLabel"]')
        values = driver.find_elements(By.CSS_SELECTOR, 'p[class^="PlayerSummary_playerInfoValue"]')
        for label, value in zip(labels, values):
            key = label.text.strip().lower()
            val = value.text.strip()
            if key == 'experience':
                data['experience'] = val
            elif key == 'birthdate':
                data['birthdate'] = val
    except Exception as e:
        print(f"⚠️ Error extracting data from {profile_url}: {e}")
    return data
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'img.PlayerImage_image'))
        )
        time.sleep(1)
        # HEADSHOT
        try:
            img = driver.find_element(By.CSS_SELECTOR, 'img.PlayerImage_image')
            data['headshot_url'] = img.get_attribute('src')
        except:
            pass
        # PROFILE INFO
        labels = driver.find_elements(By.CSS_SELECTOR, '.PlayerSummary_playerInfoLabel')
        values = driver.find_elements(By.CSS_SELECTOR, '.PlayerSummary_playerInfoValue')
        for label, value in zip(labels, values):
            key = label.text.strip().lower()
            val = value.text.strip()
            if key == 'experience':
                data['experience'] = val
            elif key == 'birthdate':
                data['birthdate'] = val
    except Exception as e:
        print(f"⚠️ Error extracting data from {profile_url}: {e}")
    return data


def scrape_nba_players():
    # --- Phase 1: Scrape roster list and URLs ---
    options = Options()
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver_main = webdriver.Chrome(options=options)
    wait_main = WebDriverWait(driver_main, 15)

    driver_main.get(BASE_URL + "/players")
    time.sleep(5)
    # Accept cookies if present
    try:
        btn = wait_main.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='I Accept']")))
        btn.click(); time.sleep(1)
    except:
        pass
    # Select "All" rows
    try:
        wait_main.until(EC.presence_of_element_located((By.CSS_SELECTOR,
            'select[title="Page Number Selection Drown Down List"]')))
        Select(driver_main.find_element(By.CSS_SELECTOR,
            'select[title="Page Number Selection Drown Down List"]')
        ).select_by_visible_text("All")
        time.sleep(3)
    except Exception as e:
        print(f"❌ Could not set 'All' view: {e}")
    # Extract all rows
    rows = driver_main.find_elements(By.CSS_SELECTOR, '.players-list tbody tr')
    player_list = []
    for idx, row in enumerate(rows, start=1):
        try:
            tds = row.find_elements(By.TAG_NAME, 'td')
            if len(tds) < 8:
                continue
            cols = [td.text for td in tds]
            a = row.find_element(By.TAG_NAME, 'a')
            link = a.get_attribute('href')
            profile_url = link
            record = {
                'player': cols[0], 'team': cols[1], 'number': cols[2],
                'position': cols[3], 'height': cols[4], 'weight': cols[5],
                'last_attended': cols[6], 'country': cols[7],
                'profile_url': profile_url
            }
            player_list.append(record)
        except Exception:
            continue
    driver_main.quit()

    # Cache main list
    if os.path.exists(MAIN_CACHE):
        main_df = pd.read_csv(MAIN_CACHE)
    else:
        main_df = pd.DataFrame(player_list)
        main_df.to_csv(MAIN_CACHE, index=False, quoting=csv.QUOTE_NONNUMERIC)

    # --- Phase 2: Enrich via individual profiles ---
    if os.path.exists(PROFILE_CACHE):
        prof_df = pd.read_csv(PROFILE_CACHE)
    else:
        prof_df = pd.DataFrame(columns=['player','headshot_url','experience','birthdate'])
    seen = set(prof_df['player'])

    driver_prof = webdriver.Chrome(options=options)

    new_entries = []
    for rec in player_list:
        name = rec['player']
        if name in seen:
            continue
        print(f"🔍 Profile: {name}")
        pdata = get_player_profile_data(driver_prof, rec['profile_url'])
        new_entries.append({ 'player': name,
                             'headshot_url': pdata['headshot_url'],
                             'experience': pdata['experience'],
                             'birthdate': pdata['birthdate'] })
        seen.add(name)
        if len(new_entries) >= 25:
            chunk = pd.DataFrame(new_entries)
            prof_df = pd.concat([prof_df, chunk], ignore_index=True)
            prof_df.to_csv(PROFILE_CACHE, index=False, quoting=csv.QUOTE_NONNUMERIC)
            new_entries = []
    # Final flush
    if new_entries:
        chunk = pd.DataFrame(new_entries)
        prof_df = pd.concat([prof_df, chunk], ignore_index=True)
        prof_df.to_csv(PROFILE_CACHE, index=False, quoting=csv.QUOTE_NONNUMERIC)
    driver_prof.quit()

    print(f"✅ Done. Roster: {MAIN_CACHE}, Profiles: {PROFILE_CACHE}")

if __name__ == '__main__':
    scrape_nba_players()
