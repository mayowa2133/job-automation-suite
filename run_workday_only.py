# run_workday_only.py
import os, json
from datetime import datetime
import pandas as pd

from src.scrapers.custom.workday import scrape_workday_jobs

KEYWORDS = [
    "new grad","university grad","entry-level","entry level","software engineer",
    "software engineer i","swe i","step","residency","early career","class of",
    "graduate program","developer","engineer"
]

CONFIG_PATH = "config/companies.json"
OUTPUT = f"data/raw/WORKDAY_smoke_{datetime.now().strftime('%Y-%m-%d_%H%M')}.xlsx"

def main():
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)
    portals = cfg.get("workday", {}) or {}
    if not portals:
        print("No Workday portals defined in config")
        return

    os.environ.setdefault("WORKDAY_ALLOWED_COUNTRIES", "US,CA")
    os.environ.setdefault("FAST_MODE", "1")

    print(f"Running Workday smoke on {len(portals)} portal(s)")
    rows = scrape_workday_jobs(KEYWORDS, portals)
    print(f"Collected {len(rows)} jobs")

    if rows:
        df = pd.DataFrame(rows)
        os.makedirs("data/raw", exist_ok=True)
        df.to_excel(OUTPUT, index=False)
        print(f"Wrote {len(df)} rows to {OUTPUT}")

if __name__ == "__main__":
    main()
