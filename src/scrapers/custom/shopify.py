# src/scrapers/custom/shopify.py
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from src.utils import generate_linkedin_links

DISCIPLINE_URLS = [
    "https://www.shopify.com/careers/disciplines/engineering-data",
    # add more discipline pages here if you want broader coverage
]

def _looks_like_job_path(path: str) -> bool:
    # Accept job detail URLs such as /careers/<slug>_<uuid>
    if not path.startswith("/careers/"):
        return False
    if "/careers/disciplines/" in path:
        return False
    return "_" in path

def _extract_title_and_location(text: str) -> tuple[str, str]:
    # Example text: "Applied ML Engineering, GenAI  AI Agent Remote  Americas"
    t = " ".join(text.split())
    # Pull a trailing location phrase if it contains Remote or a region
    m = re.search(r"(Remote.*|Americas|EMEA|APAC|Europe|Canada|United States)$", t, re.I)
    loc = m.group(0) if m else "N/A"
    # Remove the location tail from the title if present
    title = t[: t.rfind(loc)].strip() if m else t
    return title, loc

def scrape_shopify_jobs(keyword_filters):
    print("Scraping Shopify from discipline pages")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )
    }

    jobs = []
    seen = set()

    for url in DISCIPLINE_URLS:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"  > Failed to load {url}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        anchors = soup.find_all("a", href=True)
        print(f"  > {url} has {len(anchors)} anchors. Scanning for job links")

        for a in anchors:
            href = a["href"]
            abs_url = urljoin("https://www.shopify.com", href)
            parsed = urlparse(abs_url)

            if parsed.netloc and "shopify.com" not in parsed.netloc:
                continue
            if not _looks_like_job_path(parsed.path):
                continue

            raw_text = a.get_text(strip=True)
            if not raw_text:
                continue

            title, location = _extract_title_and_location(raw_text)
            if not title:
                continue

            key = (title, abs_url)
            if key in seen:
                continue
            seen.add(key)

            title_lower = title.lower()
            raw_lower = raw_text.lower()
            if not any(k in title_lower or k in raw_lower for k in keyword_filters):
                continue

            links = generate_linkedin_links("Shopify", title)
            job = {
                "Company": "Shopify",
                "Title": title,
                "URL": abs_url,
                "Location": location,
            }
            job.update(links)
            jobs.append(job)

    print(f"  > Found {len(jobs)} relevant jobs at Shopify from discipline pages")
    return jobs
