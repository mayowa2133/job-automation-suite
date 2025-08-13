# src/scrapers/custom/microsoft.py
from playwright.sync_api import sync_playwright, TimeoutError
from contextlib import suppress
from urllib.parse import urljoin, quote_plus
import re
import time
import random
import json
import os

from src.utils import generate_linkedin_links

BASE = "https://jobs.careers.microsoft.com"
SEARCH_TEMPLATES = [
    BASE + "/global/en/search?q={q}&l=en_us",
    BASE + "/global/en/search?p=Software%20Engineering&l=en_us&q={q}",
]

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
    "tooling engineer",
    "new grad",
    "university grad",
    "early career",
    "intern",
]

JOB_PATH_FRAGMENT = "/job/"

# relaxed term check for breadth
RELAXED_KEYS = [
    "engineer", "developer", "software", "swe", "sde",
    "sdet", "qa", "reliability", "security", "platform", "systems",
]

# env flags
def _env_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}

MSFT_SENIORITY_TRIM = _env_flag("MSFT_SENIORITY_TRIM", "1")   # default on
MSFT_EARLY_ONLY     = _env_flag("MSFT_EARLY_ONLY", "0")       # default off

# patterns
SENIOR_HINTS_RE = re.compile(r"\b(sr|senior|staff|principal|lead|architect|fellow|distinguished)\b", re.I)
EARLY_SIGNS_RE  = re.compile(
    r"(new\s*grad|university|graduate|early\s*career|entry\s*level|entry|junior|assoc(iate)?|intern|apprentice|engineer\s*[i1]\b)",
    re.I,
)

# common Microsoft manager tracks we do not want
MANAGER_HINTS = {
    " program manager",   # PM
    " product manager",
    " project manager",
    " technical program manager",  # TPM
    " tpm",
    " pm ",
    " manager",           # generic manager catch
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
            last_height = new_height  # fixed stray parenthesis

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
        for dk, v in d.items():
            if dk.lower() in {"jobid", "id"} and isinstance(v, (str, int)):
                return urljoin(BASE, f"/us/en/job/{v}")
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
                title = first_str(x, ["title", "jobTitle", "name"])
                url = build_url(x)
                loc = first_str(x, ["location", "jobLocation", "city", "region"]) or "N/A"
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
        if "/search" in target and "q=" in target:
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
    m = re.search(r"/job/(\d+)", url)
    return m.group(1) if m else ""

def _has_early_signal(title: str) -> bool:
    return bool(EARLY_SIGNS_RE.search(title))

def _has_senior_signal(title: str) -> bool:
    return bool(SENIOR_HINTS_RE.search(title))

def _looks_manager(title_lc: str) -> bool:
    return any(h in title_lc for h in MANAGER_HINTS)

def scrape_microsoft_jobs(keyword_filters):
    print("Scraping Microsoft with Playwright")
    t0 = time.time()
    jobs = []

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
                if "careers.microsoft.com" not in url:
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
                    page.goto(url, timeout=60000)
                    with suppress(TimeoutError):
                        page.wait_for_load_state("networkidle", timeout=10000)
                    _scroll_until_stable(page)
                    _rand_sleep()

            network_jobs = [
                {
                    "title": it["title"].strip(),
                    "url": it["url"].strip(),
                    "location": it.get("location", "N/A").strip() or "N/A",
                }
                for it in captured
                if it.get("title") and it.get("url")
            ]

            dom_jobs = []
            for fr in page.context.pages[0].frames:
                dom_jobs.extend(_collect_from_dom(fr))

            pool = network_jobs + dom_jobs
            print(f"  > Parsed {len(pool)} potential Microsoft roles before filtering")

            seen_ids = set()
            seen_url_title = set()
            kept, dropped = 0, 0
            drop_samples = []

            for it in pool:
                title = it.get("title", "").strip()
                url = it.get("url", "").strip()
                loc = it.get("location", "N/A")

                if not url or not title:
                    dropped += 1
                    continue

                jid = _job_id_from_url(url)
                if jid:
                    if jid in seen_ids:
                        dropped += 1
                        continue
                    seen_ids.add(jid)
                else:
                    key = (title, url)
                    if key in seen_url_title:
                        dropped += 1
                        continue
                    seen_url_title.add(key)

                t = title.lower()

                # manager filter first
                if _looks_manager(t) and not _has_early_signal(title):
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP manager title='{title}'")
                    continue

                # base keep rule
                keep = any(k in t for k in [k.lower() for k in keyword_filters]) or any(k in t for k in RELAXED_KEYS)
                if not keep:
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP not_eng title='{title}'")
                    continue

                # seniority trim unless early signal
                if MSFT_SENIORITY_TRIM and _has_senior_signal(title) and not _has_early_signal(title):
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP senior_trim title='{title}'")
                    continue

                # early only gate
                if MSFT_EARLY_ONLY and not _has_early_signal(title):
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP early_only title='{title}'")
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
                kept += 1

            print(f"  > Collected {len(jobs)} Microsoft jobs matching your criteria")
            print(f"    Microsoft keep audit kept={kept} dropped={dropped}")
            if drop_samples:
                print("    sample drops:")
                for s in drop_samples:
                    print("     - " + s)

        except Exception as e:
            print(f"  > Error while scraping Microsoft: {e}")
        finally:
            context.close()
            browser.close()

    print(f"    took {time.time() - t0:.1f}s")
    return jobs
