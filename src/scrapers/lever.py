# src/scrapers/lever.py

from __future__ import annotations

import os
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from src.utils import generate_linkedin_links

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

LEVER_DEBUG = os.getenv("LEVER_DEBUG", "0") == "1"


def _dbg(msg: str) -> None:
    if LEVER_DEBUG:
        print(f"    [Lever] {msg}")


def _normalize_posted_ts(created_at_ms: int | None) -> str:
    """
    Lever API returns milliseconds since epoch in createdAt.
    Convert to YYYY-MM-DD local date string if present.
    """
    if not created_at_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(int(created_at_ms) / 1000, tz=timezone.utc).astimezone()
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _title_matches(title: str, keyword_filters: list[str]) -> bool:
    t = (title or "").lower()
    return any(k.lower() in t for k in (keyword_filters or []))


def _make_networking_links(company_name: str, title: str) -> tuple[str, str]:
    """
    Your utils may return either the new keys or the old ones.
    Normalize to what main.py expects.
    """
    links = generate_linkedin_links(company_name, title) or {}
    entry_link = links.get("Entry_Level_SE_Search") or links.get("Alumni_Search_URL", "")
    role_link = links.get("General_Role_Search") or links.get("Role_Search_URL", "")
    return entry_link, role_link


def _api_fetch(company_token: str) -> list[dict]:
    """
    Try Lever's public JSON API.
    Example: https://api.lever.co/v0/postings/zoox?mode=json
    Returns a list of posting dicts on success, else [].
    """
    url = f"https://api.lever.co/v0/postings/{company_token}?mode=json"
    try:
        _dbg(f"API GET {url}")
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=20)
        if r.status_code != 200:
            _dbg(f"API status: {r.status_code}")
            return []
        data = r.json()
        if not isinstance(data, list):
            _dbg("API returned non-list JSON")
            return []
        return data
    except Exception as e:
        _dbg(f"API error: {e}")
        return []


def _html_fetch(company_token: str) -> BeautifulSoup | None:
    """
    Fallback to HTML job board: https://jobs.lever.co/{company_token}
    """
    url = f"https://jobs.lever.co/{company_token}"
    try:
        _dbg(f"HTML GET {url}")
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=20)
        r.raise_for_status()
        try:
            return BeautifulSoup(r.content, "lxml")
        except Exception:
            return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        _dbg(f"HTML error: {e}")
        return None


def _extract_location_from_html_posting(p: BeautifulSoup) -> str:
    """
    Typical Lever markup exposes one of:
      - span.sort-by-location
      - div.posting-categories > span (one is usually the location)
    """
    loc_el = p.select_one("span.sort-by-location")
    if loc_el and loc_el.get_text(strip=True):
        return loc_el.get_text(strip=True)

    cats = p.select("div.posting-categories span")
    if cats:
        # Heuristic: the one that looks like a location (has a comma, or Remote, or country-ish)
        texts = [c.get_text(" ", strip=True) for c in cats if c and c.get_text(strip=True)]
        for t in texts:
            if "remote" in t.lower() or "," in t or re.search(r"\b(US|USA|United States|Canada|UK|Germany|France|India|Japan)\b", t, re.I):
                return t
        # fallback to join all
        if texts:
            return " • ".join(texts)
    return "N/A"


def _absolute_lever_link(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return f"https://jobs.lever.co{href}"
    # Some boards embed relative anchors
    return f"https://jobs.lever.co/{href.lstrip('/')}"


def scrape_lever_jobs(company_name: str, company_token: str, keyword_filters: list[str]) -> list[dict]:
    """
    Scrape Lever postings for a company. Tries JSON API first, then HTML fallback.
    Returns a list of dicts with keys:
      Company, Title, URL, Location, Posted (optional), Entry_Level_SE_Search, General_Role_Search
    """
    print(f"Scraping {company_name} (Lever)...")

    # ---------- JSON API path ----------
    api_posts = _api_fetch(company_token)
    results: list[dict] = []

    if api_posts:
        _dbg(f"API returned {len(api_posts)} postings")
        for p in api_posts:
            title = (p.get("text") or p.get("title") or "").strip()
            if not title or not _title_matches(title, keyword_filters):
                continue

            url = _absolute_lever_link((p.get("hostedUrl") or p.get("applyUrl") or "").strip())
            if not url:
                continue

            # Location from categories if present
            loc = ""
            cats = p.get("categories") or {}
            if isinstance(cats, dict):
                loc = (cats.get("location") or "").strip()
            if not loc:
                # Some postings use 'workplaceType' or tags; leave N/A if absent
                loc = "N/A"

            posted = _normalize_posted_ts(p.get("createdAt"))

            entry_link, role_link = _make_networking_links(company_name, title)
            results.append({
                "Company": company_name,
                "Title": title,
                "URL": url,
                "Location": loc,
                "Posted": posted,
                "Entry_Level_SE_Search": entry_link,
                "General_Role_Search": role_link,
            })

        print(f"  > Found {len(results)} relevant jobs.")
        return results

    # ---------- HTML fallback path ----------
    soup = _html_fetch(company_token)
    if not soup:
        print("  > Could not load Lever board page.")
        return []

    postings = soup.select("div.posting")
    if not postings:
        print("  > No job postings found on the page.")
        return []

    for p in postings:
        # Title
        title_el = p.find("h5") or p.find("h4") or p.select_one(".posting-title")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title or not _title_matches(title, keyword_filters):
            continue

        # Link (prefer the posting anchor in list view)
        link_el = p.select_one("a.posting-title") or p.find("a", href=True)
        url = _absolute_lever_link(link_el.get("href").strip()) if link_el else ""
        if not url:
            continue

        # Location
        location = _extract_location_from_html_posting(p)

        entry_link, role_link = _make_networking_links(company_name, title)

        results.append({
            "Company": company_name,
            "Title": title,
            "URL": url,
            "Location": location,
            # No posted date reliably available in HTML list
            "Entry_Level_SE_Search": entry_link,
            "General_Role_Search": role_link,
        })

    print(f"  > Found {len(results)} relevant jobs.")
    return results