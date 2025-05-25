# scripts/scrape_players.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import csv

def scrape_nba_players():
    url = "https://www.nba.com/players"
    options = Options()
    # Uncomment the next line to run in headless mode
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)

    driver.get(url)
    time.sleep(5)  # allow JS content to load

    wait = WebDriverWait(driver, 15)

    # Accept cookies
    try:
        accept_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='I Accept']")))
        accept_btn.click()
        time.sleep(2)
    except:
        pass



    # Select "All" rows from the dropdown
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Page Number Selection Drown Down List"]')))
        Select(driver.find_element(By.CSS_SELECTOR, 'select[title="Page Number Selection Drown Down List"]')).select_by_visible_text("All")
        time.sleep(5)
    except Exception as e:
        print("❌ Could not select 'All' from dropdown:", e)
        driver.quit()
        return

    

    data = []
    headers = []

    try:
        table = driver.find_element(By.CLASS_NAME, 'players-list')
        rows = table.find_elements(By.TAG_NAME, 'tr')

        if rows:
            headers = [th.text for th in rows[0].find_elements(By.TAG_NAME, 'th')]

        for row in rows[1:]:
            cols = row.find_elements(By.TAG_NAME, 'td')
            row_data = [col.text for col in cols]
            player = dict(zip(headers, row_data))
            # Fill in missing headers with empty string
            for h in headers:
                if h not in player:
                    player[h] = ""
            data.append(player)

    except Exception as e:
        print("❌ Error during table extraction:", e)

    driver.quit()

    if data and headers:
        df = pd.DataFrame(data)

        df = df.replace({r'\n': ' ', r'\r': ' '}, regex=True)

        df.to_csv("data/raw/players_raw.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        print("✅ Player data scraped and saved to data/raw/players_raw.csv")
    else:
        print("❌ No data scraped.")

if __name__ == "__main__":
    scrape_nba_players()
