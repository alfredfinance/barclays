import time
import random
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime
import json
from selenium.webdriver.chrome.options import Options
from rapidfuzz import process, fuzz
from difflib import get_close_matches
from google.cloud import bigquery
from google.oauth2 import service_account

# ─────────────────────────────────────────────
# SELENIUM OPTIONS
# ─────────────────────────────────────────────
options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--remote-debugging-port=9222")
options.add_argument("--window-size=1920,1080")

# ─────────────────────────────────────────────
# PHASE 1 — COLLECT ALL JOB URLS
# ─────────────────────────────────────────────
driver = webdriver.Chrome(options=options)
base_url = "https://search.jobs.barclays/search-jobs/"
driver.get(base_url)
time.sleep(random.uniform(4, 7))

# Dismiss cookie banner
try:
    accept_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "system-ialert-button"))
    )
    driver.execute_script("arguments[0].click();", accept_btn)
    print("✅ Accepted cookies")
    time.sleep(1)
except TimeoutException:
    print("ℹ️ No cookie banner found")

job_urls = []
MAX_LOAD_MORE_CLICKS = 3  # safety cap — adjust if needed

for click_count in range(MAX_LOAD_MORE_CLICKS):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(2, 4))

    # Force-remove the blocking overlay if still present
    driver.execute_script("""
        var el = document.getElementById('system-ialert');
        if (el) el.remove();
    """)

    try:
        next_btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.next"))
        )
        driver.execute_script("arguments[0].click();", next_btn)
        print(f"✅ Clicked 'Next' ({click_count + 1})")
        time.sleep(random.uniform(3, 5))
    except TimeoutException:
        print("ℹ️ No more 'Next' button found — all pages visited.")
        break

# Now collect all job links from the fully loaded page
job_cards = driver.find_elements(By.CSS_SELECTOR, "a.job-link, h2.job-title a, li.search-result a[href*='/job/']")

# Fallback: broader selector
if not job_cards:
    job_cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/job/']")

seen = set()
for card in job_cards:
    href = card.get_attribute("href")
    if href and href not in seen:
        seen.add(href)
        job_urls.append(href)

driver.quit()
print(f"Collected {len(job_urls)} job URLs")


#------------------------CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

# Query existing URLs from your BigQuery table
query = """
    SELECT url
    FROM `databasealfred.alfredFinance.barclays`
    WHERE url IS NOT NULL
"""
query_job = client.query(query)

# Convert results to a set for fast lookup
existing_urls = {row.url for row in query_job}

print(f"Loaded {len(existing_urls)} URLs from BigQuery")

# Filter job_urls
job_urls = [url for url in job_urls if url not in existing_urls]

print(f"✅ Remaining job URLs to scrape: {len(job_urls)}")


#------------------------ FIN CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------


# ─────────────────────────────────────────────
# PHASE 2 — SCRAPE EACH JOB PAGE
# ─────────────────────────────────────────────
options2 = Options()
options2.add_argument("--headless=new")
options2.add_argument("--disable-gpu")
options2.add_argument("--no-sandbox")
options2.add_argument("--disable-dev-shm-usage")
options2.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(options=options2)

job_data = []

for job_url in job_urls:
    try:
        driver.get(job_url)
        time.sleep(random.uniform(3, 6))

        # ── Title ──────────────────────────────────────────────────────────
        try:
            title = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
            ).text.strip()
        except TimeoutException:
            title = ""

        # ── Location ───────────────────────────────────────────────────────
        # Barclays shows location as text in a span/li near the job header
        location = ""
        for selector in [
            "p[class*='job-details--location']",
        ]:
            try:
                loc_elem = driver.find_element(By.CSS_SELECTOR, selector)
                location = loc_elem.text.strip().split(",")[0].strip()
                if location:
                    break
            except NoSuchElementException:
                continue

        # Fallback: look for a label/value pair containing "Location"
        if not location:
            try:
                location = driver.find_element(
                    By.XPATH,
                    "//*[contains(text(),'Location')]/following-sibling::*[1]"
                ).text.strip().split(",")[0].strip()
            except NoSuchElementException:
                pass

        # ── Division / Business Area ───────────────────────────────────────
        division = ""
        try:
            division = driver.find_element(
                By.XPATH,
                "//span[contains(@class,'job-info--label') and contains(.,'Area of Expertise')]"
                "/following-sibling::span[contains(@class,'job-info-label-text')]"
            ).text.strip()
        except NoSuchElementException:
            division = ""
                
        # ── Contract ───────────────────────────────────────────────────────
        contract = ""
        try:
            contract = driver.find_element(
                By.XPATH,
                "//span[contains(@class,'job-info--label') and contains(.,'Contract')]"
                "/following-sibling::span[contains(@class,'job-info-label-text')]"
            ).text.strip()
        except NoSuchElementException:
            contract = ""

        # ── Experience Level ───────────────────────────────────────────────
        experienceLevel = ""

        # ── Description ────────────────────────────────────────────────────
        description = ""
        for desc_selector in [
            "div.job-description",
            "div[class*='description']",
            "section[class*='description']",
            "div#job-description",
            "div.content-body",
        ]:
            try:
                container = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, desc_selector))
                )
                html = container.get_attribute("innerHTML")
                soup = BeautifulSoup(html, "html.parser")
                lines = []
                for element in soup.find_all(["p", "li", "h2", "h3"]):
                    text = element.get_text(" ", strip=True)
                    if text:
                        if element.name == "li":
                            lines.append(f"- {text}")
                        else:
                            lines.append(text)
                description = "\n".join(lines)
                if description:
                    break
            except TimeoutException:
                continue

        # ── Timestamps ─────────────────────────────────────────────────────
        scrappedDateTime    = datetime.datetime.now().isoformat()
        scrappedDate        = datetime.datetime.now().strftime("%Y-%m-%d")
        scrappedHour        = datetime.datetime.now().strftime("%H")
        scrappedMinutes     = datetime.datetime.now().strftime("%M")

        print(f"  → {title} | {location} | {division}")

        job_data.append({
            "title":                title,
            "location":             location,
            "scrappedDateTime":     scrappedDateTime,
            "description":          description,
            "division":             division,
            "experienceLevel":      experienceLevel,
            "url":                  job_url,
            "source":               "Barclays",
            "scrappedDate":         scrappedDate,
            "scrappedHour":         scrappedHour,
            "scrappedMinutes":      scrappedMinutes,
            "scrappedDateTimeText": scrappedDateTime,
            "contract": contract
        })

    except Exception as e:
        print(f"⚠️ Error scraping {job_url}: {e}")
        continue

driver.quit()

df_jobs = pd.DataFrame(job_data)
new_data = df_jobs
print(f"\n📦 Scraped {len(new_data)} jobs")

import re
import numpy as np

def extract_experience_level(title):
    if pd.isna(title):
        return ""
    
    title = title.lower()

    patterns = [
        (r'\bsummer\s+analyst\b|\bsummer\s+analyste\b', "Summer Analyst"),
        (r'\bsummer\s+associate\b|\bsummer\s+associé\b', "Summer Associate"),
        (r'\bvice\s+president\b|\bsvp\b|\bvp\b|\bprincipal\b', "Vice President"),
        (r'\bassistant\s+vice\s+president\b|\bsavp\b|\bavp\b', "Assistant Vice President"),
        (r'\bsenior\s+manager\b', "Senior Manager"),
        (r'\bproduct\s+manager\b|\bpm\b|\bmanager\b', "Manager"),
        (r'\bmanager\b', "Manager"),
        (r'\bengineer\b|\bengineering\b', "Engineer"),
        (r'\badministrative\s+assistant\b|\bexecutive\s+assistant\b|\badmin\b', "Assistant"),
        (r'\bassociate\b|\bassocié\b', "Associate"),
        (r'\banalyst\b|\banalyste\b|\banalist\b', "Analyst"),
        (r'\bchief\b|\bhead\b', "C-Level"),
        (r'\bV.I.E\b|\bVIE\b|\bvolontariat international\b|\bV I E\b|', "VIE"),
    ]

    for pattern, label in patterns:
        if re.search(pattern, title):
            return label

    return "" 

# Apply to dataframe
new_data["experienceLevel"] = new_data["title"].apply(extract_experience_level)
new_data.loc[new_data['contract'].isin(['Internship', 'Stage', 'Intern']), 'experienceLevel'] = 'Intern'
new_data.loc[new_data['contract'].isin(['Graduate']), 'experienceLevel'] = 'Graduate'
new_data.loc[new_data['contract'].isin(['Apprentice']), 'experienceLevel'] = 'Apprentice'


new_data = new_data.drop(columns=['contract'])


# ─────────────────────────────────────────────
# DIVISION MAPPING (fuzzy)
# ─────────────────────────────────────────────
BASE_MAPPING = {
    "investment banking": "Investment Banking (M&A / Advisory)",
    "m&a": "Investment Banking (M&A / Advisory)",
    "mergers and acquisitions": "Investment Banking (M&A / Advisory)",
    "corporate finance": "Investment Banking (M&A / Advisory)",
    "ecm": "Investment Banking (M&A / Advisory)",
    "dcm": "Investment Banking (M&A / Advisory)",
    "capital markets origination": "Investment Banking (M&A / Advisory)",
    "corporate & investment banking": "Investment Banking (M&A / Advisory)",
    "markets": "Markets (Sales & Trading)",
    "sales and trading": "Markets (Sales & Trading)",
    "trading": "Markets (Sales & Trading)",
    "sales": "Markets (Sales & Trading)",
    "structuring": "Markets (Sales & Trading)",
    "derivatives": "Markets (Sales & Trading)",
    "fixed income": "Markets (Sales & Trading)",
    "equities": "Markets (Sales & Trading)",
    "fx": "Markets (Sales & Trading)",
    "global markets": "Markets (Sales & Trading)",
    "relationship management": "Markets (Sales & Trading)",       # Barclays
    "asset management": "Asset & Wealth Management",
    "wealth management": "Asset & Wealth Management",
    "private banking": "Asset & Wealth Management",
    "portfolio management": "Asset & Wealth Management",
    "private equity": "Private Equity & Alternatives",
    "alternatives": "Private Equity & Alternatives",
    "credit": "Credit & Lending",
    "lending": "Credit & Lending",
    "leveraged finance": "Credit & Lending",
    "structured finance": "Credit & Lending",
    "banking operations": "Operations (Back/Middle Office)",       # Barclays
    "research": "Research & Strategy",
    "equity research": "Research & Strategy",
    "risk": "Risk Management",
    "risk management": "Risk Management",
    "market risk": "Risk Management",
    "credit risk": "Risk Management",
    "operational risk": "Risk Management",
    "risk and quantitative analytics": "Risk Management",         # Barclays
    "controls": "Audit & Internal Control",                       # Barclays
    "compliance": "Compliance & Financial Crime",
    "financial crime": "Compliance & Financial Crime",
    "aml": "Compliance & Financial Crime",
    "kyc": "Compliance & Financial Crime",
    "finance": "Finance (Accounting / Controlling / Tax)",
    "accounting": "Finance (Accounting / Controlling / Tax)",
    "controlling": "Finance (Accounting / Controlling / Tax)",
    "tax": "Finance (Accounting / Controlling / Tax)",
    "operations": "Operations (Back/Middle Office)",
    "middle office": "Operations (Back/Middle Office)",
    "back office": "Operations (Back/Middle Office)",
    "trade support": "Operations (Back/Middle Office)",
    "settlement": "Operations (Back/Middle Office)",
    "audit": "Audit & Internal Control",
    "internal audit": "Audit & Internal Control",
    "internal control": "Audit & Internal Control",
    "technology": "Technology (IT / Engineering)",
    "it": "Technology (IT / Engineering)",
    "data": "Technology (IT / Engineering)",
    "engineering": "Technology (IT / Engineering)",
    "software": "Technology (IT / Engineering)",
    "development and engineering": "Technology (IT / Engineering)", # Barclays
    "data & analytics": "Technology (IT / Engineering)",            # Barclays
    "design": "Technology (IT / Engineering)",                      # Barclays
    "human resources": "Corporate Functions",
    "hr": "Corporate Functions",
    "communications": "Corporate Functions",
    "marketing": "Corporate Functions",
    "procurement": "Corporate Functions",                           # Barclays
    "corporate affairs": "Corporate Functions",                     # Barclays
    "business support & administration": "Corporate Functions",     # Barclays
    "customer service": "Corporate Functions",                      # Barclays
    "legal": "Compliance & Financial Crime",                        # Barclays
    "strategy": "Executive / Strategy / Management",
    "management": "Executive / Strategy / Management",
    "business management": "Executive / Strategy / Management",     # Barclays
    "change": "Executive / Strategy / Management",                  # Barclays
    "product development & management": "Executive / Strategy / Management", # Barclays
    "real estate": "Real Estate",
    "real estate & physical security": "Real Estate",               # Barclays
    "other": "Other / Temporary",
    "miscellaneous": "Other / Temporary",
    "early careers": "Other / Temporary",                           # Barclays
    "internships": "Other / Temporary",                             # Barclays
    "third party colleagues": "Other / Temporary",                  # Barclays
}

KNOWN_DIVISIONS = list(BASE_MAPPING.keys())

def map_division_fuzzy(value: str, threshold: int = 85) -> str:
    if not value:
        return "Other / Temporary"
    v = str(value).strip().lower()
    if v in BASE_MAPPING:
        return BASE_MAPPING[v]
    result = process.extractOne(v, KNOWN_DIVISIONS, scorer=fuzz.token_sort_ratio)
    if result and result[1] >= threshold:
        return BASE_MAPPING[result[0]]
    return "Other / Temporary"

new_data["division"] = new_data["division"].apply(map_division_fuzzy)


# ─────────────────────────────────────────────
# LOCATION MAPPING (fuzzy)
# ─────────────────────────────────────────────
BASE_CITY_MAPPING = {
    "new york": "New York", "new york city": "New York", "jersey city": "Jersey City",
    "london": "London", "glasgow": "Glasgow", "birmingham": "Birmingham",
    "paris": "Paris", "frankfurt": "Frankfurt", "frankfurt am main": "Frankfurt",
    "madrid": "Madrid", "milan": "Milan", "milano": "Milan",
    "zurich": "Zurich", "zürich": "Zurich", "geneva": "Geneva",
    "amsterdam": "Amsterdam", "brussels": "Brussels", "stockholm": "Stockholm",
    "warsaw": "Warsaw", "krakow": "Krakow",
    "dubai": "Dubai", "riyadh": "Riyadh", "doha": "Doha",
    "hong kong": "Hong Kong", "singapore": "Singapore", "tokyo": "Tokyo",
    "sydney": "Sydney", "mumbai": "Mumbai", "bangalore": "Bangalore",
    "chennai": "Chennai", "delhi": "Delhi", "pune": "Pune",
    "new delhi": "Delhi",
}
CITY_CATEGORIES = set(BASE_CITY_MAPPING.values())
BASE_CITY_MAPPING.update({city.lower(): city for city in CITY_CATEGORIES})
KNOWN_LOCATIONS = list(BASE_CITY_MAPPING.keys())

def map_location(value: str, cutoff: float = 0.8) -> str:
    if not value:
        return "Other / Unknown"
    v = str(value).strip().lower()
    if v in BASE_CITY_MAPPING:
        return BASE_CITY_MAPPING[v]
    matches = get_close_matches(v, KNOWN_LOCATIONS, n=1, cutoff=cutoff)
    if matches:
        return BASE_CITY_MAPPING[matches[0]]
    return value

new_data["location"] = new_data["location"].apply(map_location)

#---------UPLOAD TO BIGQUERY-------------------------------------------------------------------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

table_id = "databasealfred.alfredFinance.barclays"

# CONFIG WITHOUT PYARROW
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

# Convert DataFrame → list of dict rows (JSON compatible)
rows = new_data.to_dict(orient="records")

# Upload
job = client.load_table_from_json(
    rows,
    table_id,
    job_config=job_config
)

job.result()
