# scripts/extract_glossary.py
"""
Extract the official NBA Stats glossary and save it as CSV
----------------------------------------------------------

* Source  : https://stats.gleague.nba.com/help/glossary/
            (identical text to the NBA.com glossary but not blocked
             by robots.txt, so `requests` works)
* Output  : data/raw/glossary.csv

Columns
-------
    code          – e.g. "%3PA", "PIE"
    name          – full stat name
    definition    – long definition
    formula       – (may be blank)
    type          – "Traditional", "Advanced", "Four Factors", …
    contexts      – pipe-delimited list (Usage│Clutch│Player)
"""

import re, os, csv, requests, pandas as pd
from bs4 import BeautifulSoup, NavigableString, Tag

URL       = "https://stats.gleague.nba.com/help/glossary/"
RAW_DIR   = "data/raw"
OUT_CSV   = os.path.join(RAW_DIR, "glossary.csv")
HEADERS   = {       # same UA bundle you use elsewhere
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/136.0 Safari/537.36"),
    "Referer": "https://www.nba.com/",
}

# ──────────────────────────────────────────────────────────────
def fetch_page(url: str) -> BeautifulSoup:
    html = requests.get(url, headers=HEADERS, timeout=30).text
    return BeautifulSoup(html, "lxml")

def parse_glossary(soup: BeautifulSoup) -> list[dict]:
    """Walk every <h2> / <h3> block and gather fields."""
    records, code, block = [], None, None

    # NBA markup: the code is in an <h2> or <h3> beginning with '##'
    for tag in soup.select("h2, h3"):
        txt = tag.get_text(strip=True)
        if not txt:
            continue
        if txt.startswith("%") or txt[0].isalnum():            # stat code
            # flush previous block
            if block:
                records.append(block)
            code  = txt
            block = {"code": code, "name": "", "definition": "", 
                     "formula": "", "type": "", "contexts": ""}
            # walk forward until next header
            for sib in tag.next_siblings:
                if isinstance(sib, Tag) and sib.name in ("h2", "h3"):
                    break
                if isinstance(sib, NavigableString):
                    continue
                label = sib.get_text(strip=True)
                if label.lower() == "name":
                    name_text = sib.find_next(text=True, recursive=False)
                    if isinstance(name_text, str):
                        block["name"] = name_text.strip()
                    else:
                        block["name"] = ""
                elif label.lower() == "definition":
                    def_text = sib.find_next(text=True, recursive=False)
                    if isinstance(def_text, str):
                        block["definition"] = def_text.strip()
                    elif def_text:
                        block["definition"] = str(def_text).strip()
                    else:
                        block["definition"] = ""
                elif label.lower() == "formula":
                    formula_text = sib.find_next(text=True, recursive=False)
                    if isinstance(formula_text, str):
                        block["formula"] = formula_text.strip()
                    elif formula_text:
                        block["formula"] = str(formula_text).strip()
                    else:
                        block["formula"] = ""
                elif label.lower() == "type":
                    type_text = sib.find_next(text=True, recursive=False)
                    if isinstance(type_text, str):
                        block["type"] = type_text.strip()
                    elif type_text:
                        block["type"] = str(type_text).strip()
                    else:
                        block["type"] = ""
                elif label.lower() == "contexts":
                    ctx = [t.strip() for t in sib.stripped_strings if t.strip()]
                    block["contexts"] = "|".join(ctx[1:])  # drop header itself
    if block:
        records.append(block)
    return records

# ──────────────────────────────────────────────────────────────
def main():
    soup = fetch_page(URL)
    glossary = parse_glossary(soup)
    df = pd.DataFrame(glossary)
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, quoting=csv.QUOTE_NONNUMERIC)
    print(f"✅ Saved {len(df)} glossary rows → {OUT_CSV}")

if __name__ == "__main__":
    main()
