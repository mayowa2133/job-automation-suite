# src/scrapers/custom/meta.py
from playwright.sync_api import sync_playwright, TimeoutError
from contextlib import suppress
from urllib.parse import urljoin, quote_plus
import re
import time
import random
import json
import xml.etree.ElementTree as ET
import requests

from src.utils import generate_linkedin_links

BASE = "https://www.metacareers.com"
SEARCH_BASE = f"{BASE}/jobs/?q="
JOB_PATH_FRAGMENT = "/jobs/"

# Broad queries to maximize coverage
QUERIES = [
    "software engineer",
    "software developer",
    "production engineer",
    "data engineer",
    "machine learning engineer",
    "backend engineer",
    "platform engineer",
    "infrastructure engineer",
    "systems engineer",
    "ios engineer",
    "android engineer",
    "graphics engineer",
    "compiler engineer",
    "site reliability engineer",
    "reliability engineer",
    "security engineer",
    "tooling engineer",
    "new grad",
    "university grad",
    "early career",
    "intern",
]

RELAXED_KEYS = ["engineer", "developer"]

def _rand_sleep(a=0.25, b=0.75):
    time.sleep(random.uniform(a, b))

def _scroll_until_stable(page, pause_sec=0.9, max_loops=45):
    last_height = -1
    stable = 0
    for _ in range(max_loops):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        _rand_sleep(pause_sec * 0.6, pause_sec * 1.2)
        with suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=3500)
        # try to click any load more button if present
        with suppress(Exception):
            btn = page.get_by_role("button", name=re.compile("Load more|Show more|See more", re.I))
            if btn and btn.is_visible():
                btn.click()
                _rand_sleep()
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            stable += 1
            if stable >= 2:
                break
        else:
            stable = 0
            last_height = new_height

def _collect_from_json_like(obj):
    out = []

    def first_str(d, keys):
        for k in keys:
            for dk, v in d.items():
                if dk.lower() == k and isinstance(v, str) and v.strip():
                    return v.strip()
        return ""

    def build_url(d):
        for k in ["url", "canonicalurl", "applyurl", "detailsurl", "href", "path", "slug"]:
            for dk, v in d.items():
                if dk.lower() == k and isinstance(v, str) and v.strip():
                    u = v.strip()
                    if u.startswith("http"):
                        return u
                    if u.startswith("/"):
                        return urljoin(BASE, u)
                    return urljoin(BASE, "/" + u)
        # last chance via id field
        for dk, v in d.items():
            if dk.lower() in {"jobid", "id"} and isinstance(v, (str, int)):
                return urljoin(BASE, f"/jobs/{v}")
        return ""

    def looks_like_job(d):
        if not isinstance(d, dict):
            return False
        keys = {k.lower() for k in d.keys()}
        has_title = any(k in keys for k in ["title", "jobtitle", "name"])
        has_urlish = any(k in keys for k in ["url", "canonicalurl", "applyurl", "detailsurl", "href", "path", "slug", "jobid", "id"])
        return has_title and has_urlish

    def walk(x):
        if isinstance(x, dict):
            if looks_like_job(x):
                title = first_str(x, ["title", "jobtitle", "name"])
                url = build_url(x)
                loc = first_str(x, ["location", "joblocation", "city", "region"]) or "N/A"
                if title and url and JOB_PATH_FRAGMENT in url:
                    out.append({"title": title, "url": url, "location": loc})
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(obj)
    return out

def _collect_from_dom(frame):
    anchors = frame.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({href: a.getAttribute('href') || '', abs: a.href || '', text: a.innerText || ''}))",
    )
    items, seen = [], set()
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
        if "/jobs/?q=" in target:
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

def _job_id_from_url(url: str) -> str:
    m = re.search(r"/jobs/(\d+)", url)
    return m.group(1) if m else ""

def _fallback_sitemap_harvest():
    print("    sitemap fallback running")
    try:
        r = requests.get(f"{BASE}/sitemap.xml", timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
    except Exception as e:
        print(f"    sitemap fetch failed {e}")
        return []
    jobs = []
    try:
        root = ET.fromstring(r.text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for loc in root.findall(".//sm:loc", ns):
            url = loc.text or ""
            if JOB_PATH_FRAGMENT in url and "/jobs/?q=" not in url:
                jobs.append({"title": "", "url": url, "location": "N/A"})
    except Exception as e:
        print(f"    sitemap parse failed {e}")
    return jobs

def scrape_meta_jobs(keyword_filters):
    print("Scraping Meta with Playwright")
    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36"),
            locale="en-US",
            geolocation={"latitude": 43.6532, "longitude": -79.3832},  # Toronto
            permissions=["geolocation"],
            viewport={"width": 1366, "height": 900},
        )
        # allow fonts and scripts so the app hydrates, skip only heavy media
        def route_handler(route):
            rt = route.request.resource_type
            if rt in {"image", "media"}:
                return route.abort()
            return route.continue_()
        context.route("**/*", route_handler)

        captured = []

        def on_response(resp):
            try:
                url = resp.url
                if "metacareers.com" not in url:
                    return
                # try json first
                data = None
                with suppress(Exception):
                    data = resp.json()
                if data is None:
                    with suppress(Exception):
                        txt = resp.text()
                        if txt and txt.strip().startswith("{"):
                            data = json.loads(txt)
                if data is None:
                    return
                found = _collect_from_json_like(data)
                if found:
                    captured.extend(found)
            except Exception:
                pass

        context.on("response", on_response)
        page = context.new_page()

        try:
            for q in QUERIES:
                search_url = SEARCH_BASE + quote_plus(q)
                print(f"  > Opening {search_url}")
                page.goto(search_url, timeout=60000)
                with suppress(TimeoutError):
                    page.wait_for_load_state("networkidle", timeout=10000)
                _scroll_until_stable(page)
                _rand_sleep()

            # Prefer network results
            network_jobs = [{"title": it["title"].strip(), "url": it["url"].strip(), "location": it.get("location", "N/A").strip() or "N/A"}
                            for it in captured if it.get("title") and it.get("url")]

            # Harvest from all frames as a fallback
            dom_jobs = []
            for fr in page.context.pages[0].frames:
                dom_jobs.extend(_collect_from_dom(fr))

            if not network_jobs and not dom_jobs:
                dom_jobs = _fallback_sitemap_harvest()

            pool = network_jobs + dom_jobs
            print(f"  > Parsed {len(pool)} potential Meta roles before filtering")

            seen_ids = set()
            seen_url_title = set()

            for it in pool:
                title = it.get("title", "").strip()
                url = it.get("url", "").strip()
                loc = it.get("location", "N/A")

                if not url:
                    continue

                jid = _job_id_from_url(url)
                if jid:
                    if jid in seen_ids:
                        continue
                    seen_ids.add(jid)
                else:
                    key = (title, url)
                    if key in seen_url_title:
                        continue
                    seen_url_title.add(key)

                keep = False
                t = title.lower()
                if title:
                    if any(k in t for k in keyword_filters) or any(k in t for k in RELAXED_KEYS):
                        keep = True
                else:
                    # if we got only a URL from sitemap, keep it to surface in Networking sheet
                    keep = True

                if not keep:
                    continue

                if not title:
                    # try to derive a title from the URL slug
                    m = re.search(r"/jobs/\d+/?([^/?#]+)", url)
                    if m:
                        title = m.group(1).replace("-", " ").title()
                    else:
                        title = "Software Engineer"

                links = generate_linkedin_links("Meta", title)
                row = {
                    "Company": "Meta",
                    "Title": title,
                    "URL": url,
                    "Location": loc,
                }
                row.update(links)
                jobs.append(row)

        except Exception as e:
            print(f"  > Error while scraping Meta: {e}")
        finally:
            context.close()
            browser.close()

    print(f"  > Collected {len(jobs)} Meta jobs matching your criteria")
    return jobs
