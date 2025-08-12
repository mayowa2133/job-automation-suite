# src/scrapers/custom/microsoft.py
from playwright.sync_api import sync_playwright, TimeoutError
from contextlib import suppress
from urllib.parse import urljoin
import time
import re
import json

from src.utils import generate_linkedin_links

SEARCH_URLS = [
    # Software Engineering profession search
    "https://jobs.careers.microsoft.com/global/en/search?p=Software%20Engineering&l=en_us&pg=1&pgSz=20&o=Relevance&flt=true",
    # You can add more targeted searches here if you want broader coverage
    # Program management can be useful for APM like roles
    # "https://jobs.careers.microsoft.com/global/en/search?p=Program%20Management&l=en_us&pg=1&pgSz=20&o=Relevance&flt=true",
]

BASE = "https://jobs.careers.microsoft.com"
JOB_PATH_FRAGMENT = "/job/"

def _scroll_until_stable(page, pause_sec=1.0, max_loops=40):
    last_height = -1
    stable = 0
    for _ in range(max_loops):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(pause_sec)
        with suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=3000)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            stable += 1
            if stable >= 2:
                break
        else:
            stable = 0
            last_height = new_height
        with suppress(Exception):
            btn = page.get_by_role("button", name=re.compile("Load more|Show more|See more", re.I))
            if btn.is_visible():
                btn.click()
                time.sleep(0.8)

def _collect_from_json(obj):
    """Walk any JSON and pull out title url location fields when they look like a job."""
    out = []

    def looks_like_job(d):
        if not isinstance(d, dict):
            return False
        keys = {k.lower() for k in d.keys()}
        has_title = any(k in keys for k in ["title", "jobtitle", "name"])
        has_urlish = any(k in keys for k in ["url", "joburl", "canonicalpositionurl", "applyurl", "detailsurl", "slug", "path", "jobid"])
        return has_title and has_urlish

    def first_str(d, keys):
        for k in keys:
            for dk, v in d.items():
                if dk.lower() == k and isinstance(v, str) and v.strip():
                    return v.strip()
        return ""

    def build_url(d):
        # Prefer full urls if present. Otherwise build from slug or jobId.
        for k in ["url", "jobUrl", "canonicalPositionUrl", "applyUrl", "detailsUrl", "href", "path", "slug"]:
            for dk, v in d.items():
                if dk.lower() == k and isinstance(v, str) and v.strip():
                    u = v.strip()
                    if u.startswith("http"):
                        return u
                    if u.startswith("/"):
                        return urljoin(BASE, u)
                    return urljoin(BASE, "/" + u)
        # Try jobId if exposed
        jid = None
        for dk, v in d.items():
            if dk.lower() in {"jobid", "id"} and isinstance(v, (str, int)):
                jid = str(v)
                break
        if jid:
            return urljoin(BASE, f"/us/en/job/{jid}")
        return ""

    def walk(x):
        if isinstance(x, dict):
            if looks_like_job(x):
                title = first_str(x, ["title", "jobTitle", "name"])
                url = build_url(x)
                loc = first_str(x, ["location", "jobLocation", "city", "region"]) or "N/A"
                if title and url:
                    out.append({"title": title, "url": url, "location": loc})
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(obj)
    return out

def _collect_from_dom(page):
    # Collect anchors that point at real job detail pages
    anchors = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({href: a.getAttribute('href') || '', abs: a.href || '', text: a.innerText || ''}))",
    )
    items = []
    seen = set()
    for a in anchors:
        href = a.get("href") or ""
        absu = a.get("abs") or ""
        txt = (a.get("text") or "").strip()
        target = ""
        if JOB_PATH_FRAGMENT in href:
            target = href
        elif JOB_PATH_FRAGMENT in absu:
            target = absu
        if not target:
            continue
        if not target.startswith("http"):
            target = urljoin(BASE, target)
        key = (target, txt)
        if key in seen:
            continue
        seen.add(key)
        title = txt.split("\n")[0] if txt else ""
        if title and target:
            items.append({"title": title, "url": target, "location": "N/A"})
    return items

def scrape_microsoft_jobs(keyword_filters):
    print("Scraping Microsoft with Playwright")
    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36")
        )
        # Skip heavy assets to speed up
        def route_handler(route):
            rt = route.request.resource_type
            if rt in {"image", "media", "font"}:
                return route.abort()
            return route.continue_()
        context.route("**/*", route_handler)

        # Capture JSON from the careers app
        captured = []
        def on_response(resp):
            try:
                url = resp.url
                if "jobs.careers.microsoft.com" not in url and "careers.microsoft.com" not in url:
                    return
                # lots of endpoints return JSON during search
                ct = resp.headers.get("content-type", "")
                if "application/json" not in ct:
                    return
                data = resp.json()
                found = _collect_from_json(data)
                if found:
                    captured.extend(found)
            except Exception:
                pass

        context.on("response", on_response)
        page = context.new_page()

        try:
            for url in SEARCH_URLS:
                print(f"  > Opening {url}")
                page.goto(url, timeout=60000)
                with suppress(TimeoutError):
                    page.wait_for_load_state("networkidle", timeout=10000)
                _scroll_until_stable(page)

            # Prefer network results
            network_jobs = []
            for it in captured:
                title = it.get("title", "").strip()
                url = it.get("url", "").strip()
                loc = it.get("location", "").strip() or "N/A"
                if title and url:
                    network_jobs.append({"title": title, "url": url, "location": loc})

            # Fallback to DOM
            dom_jobs = []
            if not network_jobs:
                dom_jobs = _collect_from_dom(page)

            pool = network_jobs if network_jobs else dom_jobs
            print(f"  > Parsed {len(pool)} potential Microsoft roles before keyword filtering")

            seen = set()
            for it in pool:
                title = it["title"]
                url = it["url"]
                loc = it.get("location", "N/A")
                key = (title, url)
                if key in seen:
                    continue
                seen.add(key)

                t = title.lower()
                if not any(k in t for k in keyword_filters):
                    continue

                links = generate_linkedin_links("Microsoft", title)
                row = {
                    "Company": "Microsoft",
                    "Title": title,
                    "URL": url,
                    "Location": loc,
                }
                row.update(links)
                jobs.append(row)

        except Exception as e:
            print(f"  > Error while scraping Microsoft: {e}")
        finally:
            context.close()
            browser.close()

    print(f"  > Collected {len(jobs)} Microsoft jobs matching your keywords")
    return jobs
