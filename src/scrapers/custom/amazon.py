# src/scrapers/custom/amazon.py
import os
import re
import time
import random
import json
import requests
from contextlib import suppress
from urllib.parse import urljoin, quote_plus, urlencode

from playwright.sync_api import sync_playwright, TimeoutError
from src.utils import generate_linkedin_links

BASE = "https://www.amazon.jobs"
JOB_PATH_FRAGMENT = "/en/jobs/"

SEARCH_TEMPLATES = [
    BASE + "/en/search?keywords={q}",
    BASE + "/en/search?business_category=software-development&keywords={q}",
]

# Broad queries for coverage
QUERIES = [
    "software engineer",
    "software developer",
    "software development engineer",
    "sde",
    "sde i",
    "sde ii",
    "sdet",
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
    "security engineer",
    "tooling engineer",
    "new grad",
    "university grad",
    "early career",
    "intern",
]

# relaxed terms for breadth
RELAXED_KEYS = [
    "software development engineer",
    "software engineer",
    "engineer",
    "developer",
    "swe",
    "sde",
    "sde i",
    "sde ii",
    "sdet",
    "qa",
    "reliability",
    "security",
    "platform",
    "systems",
    "compiler",
]

# env flags
def _env_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}

AMZN_SENIORITY_TRIM = _env_flag("AMZN_SENIORITY_TRIM", "1")   # default on
AMZN_EARLY_ONLY     = _env_flag("AMZN_EARLY_ONLY", "0")       # default off
FAST_MODE           = _env_flag("FAST_MODE", "0")

# country filtering
def _allowed_country_codes() -> set[str]:
    raw = os.getenv("AMZN_ALLOWED_COUNTRIES", "US,CA")
    return {c.strip().upper() for c in raw.split(",") if c.strip()}

COUNTRY_SYNONYMS = {
    "US": {"US", "USA", "UNITED STATES"},
    "CA": {"CA", "CAN", "CANADA"},
}

def _loc_in_allowed(loc: str, allowed: set[str]) -> bool:
    if not loc:
        return False
    s = loc.strip().upper()
    # quick passes
    if any(f", {code}" in s for code in allowed):
        return True
    # word matches like Remote United States or Toronto Canada
    for code in list(allowed):
        if any(token in s for token in COUNTRY_SYNONYMS.get(code, {code})):
            return True
    return False

SENIOR_HINTS_RE = re.compile(r"\b(SR|SENIOR|STAFF|PRINCIPAL|LEAD|ARCHITECT|FELLOW|DISTINGUISHED)\b", re.I)
EARLY_SIGNS_RE  = re.compile(
    r"(NEW\s*GRAD|UNIVERSITY|GRADUATE|EARLY\s*CAREER|ENTRY\s*LEVEL|ENTRY|JUNIOR|ASSOC(IATE)?|INTERN|APPRENTICE|ENGINEER\s*[I1]\b)",
    re.I,
)

# manager and PM tracks we do not want unless explicitly early
MANAGER_HINTS = {
    " PROGRAM MANAGER",
    " PRODUCT MANAGER",
    " PROJECT MANAGER",
    " TECHNICAL PROGRAM MANAGER",
    " TPM",
    " PM ",
    " MANAGER",
    " DIRECTOR",
}

def _rand_sleep(a=0.15, b=0.45):
    time.sleep(random.uniform(a, b))

def _scroll_until_stable(page, pause_sec=0.9, max_loops=30):
    last_height = -1
    stable = 0
    for _ in range(max_loops):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        _rand_sleep(pause_sec * 0.6, pause_sec * 1.2)
        with suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=3000)
        with suppress(Exception):
            btn = page.get_by_role("button", name=re.compile("Load more|Show more|See more|Next", re.I))
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

def _first_str(d, keys):
    for k in keys:
        for dk, v in d.items():
            if dk.lower() == k and isinstance(v, str) and v.strip():
                return v.strip()
    return ""

def _build_url(d):
    for k in ["url", "joburl", "canonicalurl", "applyurl", "detailsurl", "href", "path", "jobpath", "slug"]:
        for dk, v in d.items():
            if dk.lower() == k and isinstance(v, str) and v.strip():
                u = v.strip()
                if u.startswith("http"):
                    return u
                if u.startswith("/"):
                    return urljoin(BASE, u)
                return urljoin(BASE, "/" + u)
    for dk, v in d.items():
        if dk.lower() in {"jobid", "id"} and isinstance(v, (str, int)):
            return urljoin(BASE, f"/en/jobs/{v}")
    return ""

def _looks_like_job_dict(d):
    if not isinstance(d, dict):
        return False
    keys = {k.lower() for k in d.keys()}
    has_title = any(k in keys for k in ["title", "jobtitle", "name"])
    has_urlish = any(k in keys for k in ["url", "joburl", "canonicalurl", "applyurl", "detailsurl", "href", "path", "jobpath", "slug", "jobid", "id"])
    return has_title and has_urlish

def _collect_from_json_like(obj):
    out = []
    def walk(x):
        if isinstance(x, dict):
            if _looks_like_job_dict(x):
                title = _first_str(x, ["title", "jobtitle", "name"])
                url = _build_url(x)
                loc = _first_str(x, ["location", "joblocation", "city", "region", "normalized_location"]) or "N/A"
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
        if not target or "/search" in target:
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

def _derive_title_from_url(url: str) -> str:
    m = re.search(r"/en/jobs/\d+/?([^/?#]+)", url)
    if not m:
        return "Software Development Engineer"
    slug = re.sub(r"\?.*$", "", m.group(1))
    return slug.replace("-", " ").replace("_", " ").strip().title()

def _job_id_from_url(url: str) -> str:
    m = re.search(r"/en/jobs/(\d+)", url)
    return m.group(1) if m else ""

def _has_early_signal(title: str) -> bool:
    return bool(EARLY_SIGNS_RE.search(title))

def _has_senior_signal(title: str) -> bool:
    return bool(SENIOR_HINTS_RE.search(title))

def _looks_manager(title_lc: str) -> bool:
    return any(h in title_lc for h in MANAGER_HINTS)

def _api_params(query: str, offset: int, limit: int = 100, business_category: str | None = None) -> dict:
    params = {
        "keywords": query,
        "result_limit": limit,
        "offset": offset,
        "sort": "recent",
    }
    if business_category:
        params["business_category"] = business_category
    return params

def _harvest_via_api(queries):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
    }
    all_items = []
    seen = set()

    for q in queries:
        for cat in [None, "software-development"]:
            offset = 0
            page_cap = 5 if FAST_MODE else 40
            while page_cap > 0:
                url = f"{BASE}/en/search.json?{urlencode(_api_params(q, offset, 100, cat))}"
                try:
                    r = requests.get(url, headers=headers, timeout=20)
                    if r.status_code != 200:
                        break
                    data = r.json()
                except Exception:
                    break

                items = _collect_from_json_like(data)
                new_count = 0
                for it in items:
                    key = (it["url"], it.get("title", ""))
                    if key in seen:
                        continue
                    seen.add(key)
                    all_items.append(it)
                    new_count += 1

                if new_count == 0:
                    break

                offset += 100
                page_cap -= 1
                _rand_sleep()

    return all_items

def scrape_amazon_jobs(keyword_filters):
    print("Scraping Amazon with Playwright + API assist")
    t0 = time.time()
    jobs = []

    allowed_codes = _allowed_country_codes()

    # First try the JSON API
    api_pool = _harvest_via_api(QUERIES)
    if api_pool:
        print(f"  > API harvested {len(api_pool)} potential Amazon roles")
    else:
        print("  > API returned nothing useful, falling back to full browser crawl")

    # Browser crawl as a secondary pass
    dom_pool = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36"),
            locale="en-US",
            geolocation={"latitude": 43.6532, "longitude": -79.3832},
            permissions=["geolocation"],
            viewport={"width": 1366, "height": 900},
        )

        def route_handler(route):
            rt = route.request.resource_type
            if rt in {"image", "media", "font"}:
                return route.abort()
            return route.continue_()
        context.route("**/*", route_handler)

        captured = []

        def on_response(resp):
            try:
                url = resp.url
                if "amazon.jobs" not in url:
                    return
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
                for tmpl in SEARCH_TEMPLATES:
                    url = tmpl.format(q=quote_plus(q))
                    print(f"  > Opening {url}")
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    with suppress(TimeoutError):
                        page.wait_for_load_state("networkidle", timeout=8000)
                    _scroll_until_stable(page)
                    _rand_sleep()

            network_jobs = [
                {
                    "title": it["title"].strip(),
                    "url": it["url"].strip(),
                    "location": (it.get("location") or "N/A").strip() or "N/A",
                }
                for it in captured
                if it.get("title") and it.get("url")
            ]

            dom_jobs = []
            for fr in page.context.pages[0].frames:
                dom_jobs.extend(_collect_from_dom(fr))

            dom_pool = network_jobs + dom_jobs

        except Exception as e:
            print(f"  > Error during browser crawl: {e}")
        finally:
            context.close()
            browser.close()

    pool = (api_pool or []) + dom_pool
    print(f"  > Parsed {len(pool)} potential Amazon roles before filtering")

    # Filter and de dupe
    seen_ids = set()
    seen_pairs = set()
    kept, dropped = 0, 0
    drop_samples = []
    kept_by_country = {"US": 0, "CA": 0, "OTHER": 0}

    for it in pool:
        raw_title = (it.get("title") or "").strip()
        url = (it.get("url") or "").strip()
        loc = it.get("location", "N/A")

        if not url:
            dropped += 1
            continue

        title = raw_title if raw_title else _derive_title_from_url(url)

        # country filter
        if not _loc_in_allowed(loc, allowed_codes):
            dropped += 1
            continue

        # de dupe
        jid = _job_id_from_url(url)
        if jid:
            if jid in seen_ids:
                dropped += 1
                continue
            seen_ids.add(jid)
        else:
            key = (title, url)
            if key in seen_pairs:
                dropped += 1
                continue
            seen_pairs.add(key)

        t = title.upper()

        # drop managers unless explicitly early
        if any(h in t for h in MANAGER_HINTS) and not _has_early_signal(title):
            dropped += 1
            if len(drop_samples) < 12:
                drop_samples.append(f"DROP manager title='{title}'")
            continue

        # base keep rule from user filters or relaxed terms
        tlc = title.lower()
        keep = any(k in tlc for k in [k.lower() for k in keyword_filters]) or any(k in tlc for k in RELAXED_KEYS)
        if not keep:
            dropped += 1
            if len(drop_samples) < 12:
                drop_samples.append(f"DROP not_eng title='{title}'")
            continue

        # seniority trim unless early
        if AMZN_SENIORITY_TRIM and _has_senior_signal(title) and not _has_early_signal(title):
            dropped += 1
            if len(drop_samples) < 12:
                drop_samples.append(f"DROP senior_trim title='{title}'")
            continue

        # early only mode
        if AMZN_EARLY_ONLY and not _has_early_signal(title):
            dropped += 1
            if len(drop_samples) < 12:
                drop_samples.append(f"DROP early_only title='{title}'")
            continue

        links = generate_linkedin_links("Amazon", title)
        row = {
            "Company": "Amazon",
            "Title": title,
            "URL": url,
            "Location": loc,
        }
        row.update(links)
        jobs.append(row)
        kept += 1

        # small country tally for your logs
        u = loc.upper()
        if "UNITED STATES" in u or ", US" in u or "USA" in u:
            kept_by_country["US"] += 1
        elif "CANADA" in u or ", CAN" in u:
            kept_by_country["CA"] += 1
        else:
            kept_by_country["OTHER"] += 1

    print(f"  > Collected {len(jobs)} Amazon jobs matching your criteria")
    print(f"    Amazon keep audit kept={kept} dropped={dropped}")
    if drop_samples:
        print("    sample drops:")
        for s in drop_samples:
            print("     - " + s)
    print(f"    country breakdown US={kept_by_country['US']} CA={kept_by_country['CA']} OTHER={kept_by_country['OTHER']}")
    print(f"    took {time.time() - t0:.1f}s")
    return jobs
