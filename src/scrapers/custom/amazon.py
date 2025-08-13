# src/scrapers/custom/amazon.py
import os
import re
import time
import random
import json
from contextlib import suppress
from urllib.parse import urljoin, quote_plus

from playwright.sync_api import sync_playwright, TimeoutError
from src.utils import generate_linkedin_links

BASE = "https://www.amazon.jobs"
SEARCH_TEMPLATES = [
    BASE + "/en/search?keywords={q}",
    BASE + "/en/search?business_category=software-development&keywords={q}",
]

# Broad queries for coverage
QUERIES = [
    "software engineer",
    "software developer",
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
    "sdet",
    "tooling engineer",
    "new grad",
    "university grad",
    "early career",
    "intern",
]

JOB_PATH_FRAGMENT = "/en/jobs/"

# relaxed terms for breadth
RELAXED_KEYS = [
    "engineer", "developer", "software", "swe", "sde",
    "sdet", "qa", "reliability", "security", "platform", "systems",
]

# env flags
def _env_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}

AMZN_SENIORITY_TRIM = _env_flag("AMZN_SENIORITY_TRIM", "1")   # default on
AMZN_EARLY_ONLY     = _env_flag("AMZN_EARLY_ONLY", "0")       # default off

SENIOR_HINTS_RE = re.compile(r"\b(sr|senior|staff|principal|lead|architect|fellow|distinguished)\b", re.I)
EARLY_SIGNS_RE  = re.compile(
    r"(new\s*grad|university|graduate|early\s*career|entry\s*level|entry|junior|assoc(iate)?|intern|apprentice|engineer\s*[i1]\b)",
    re.I,
)

# manager and PM tracks we do not want unless explicitly early
MANAGER_HINTS = {
    " program manager",
    " product manager",
    " project manager",
    " technical program manager",
    " tpm",
    " pm ",
    " manager",
    " director",
}

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

def _first_str(d, keys):
    for k in keys:
        for dk, v in d.items():
            if dk.lower() == k and isinstance(v, str) and v.strip():
                return v.strip()
    return ""

def _build_url(d):
    # common keys seen on amazon.jobs payloads
    for k in ["url", "joburl", "canonicalurl", "applyurl", "detailsurl", "href", "path", "jobpath", "slug"]:
        for dk, v in d.items():
            if dk.lower() == k and isinstance(v, str) and v.strip():
                u = v.strip()
                if u.startswith("http"):
                    return u
                if u.startswith("/"):
                    return urljoin(BASE, u)
                return urljoin(BASE, "/" + u)
    # last chance from id
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
                loc = _first_str(x, ["location", "joblocation", "city", "region"]) or "N/A"
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
        return "Software Engineer"
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

def scrape_amazon_jobs(keyword_filters):
    print("Scraping Amazon with Playwright")
    t0 = time.time()
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

        # trim heavy resources
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
            # Visit search pages and harvest after each
            for q in QUERIES:
                for tmpl in SEARCH_TEMPLATES:
                    url = tmpl.format(q=quote_plus(q))
                    print(f"  > Opening {url}")
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    with suppress(TimeoutError):
                        page.wait_for_load_state("networkidle", timeout=8000)
                    _scroll_until_stable(page)
                    _rand_sleep()

            # Pool results from network plus DOM
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

            pool = network_jobs + dom_jobs
            print(f"  > Parsed {len(pool)} potential Amazon roles before filtering")

            # Filter and de dupe
            seen_ids = set()
            seen_pairs = set()
            kept, dropped = 0, 0
            drop_samples = []

            for it in pool:
                raw_title = (it.get("title") or "").strip()
                url = (it.get("url") or "").strip()
                loc = it.get("location", "N/A")

                if not url:
                    dropped += 1
                    continue

                title = raw_title if raw_title else _derive_title_from_url(url)

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

                t = title.lower()

                # drop managers unless explicitly early
                if _looks_manager(t) and not _has_early_signal(title):
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP manager title='{title}'")
                    continue

                # base keep rule from user filters or relaxed terms
                keep = any(k in t for k in [k.lower() for k in keyword_filters]) or any(k in t for k in RELAXED_KEYS)
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

            print(f"  > Collected {len(jobs)} Amazon jobs matching your criteria")
            print(f"    Amazon keep audit kept={kept} dropped={dropped}")
            if drop_samples:
                print("    sample drops:")
                for s in drop_samples:
                    print("     - " + s)

        except Exception as e:
            print(f"  > Error while scraping Amazon: {e}")
        finally:
            context.close()
            browser.close()

    print(f"    took {time.time() - t0:.1f}s")
    return jobs
