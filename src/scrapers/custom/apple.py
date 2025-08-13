# src/scrapers/custom/apple.py
import os
import re
import time
import random
import json
from contextlib import suppress
from urllib.parse import urljoin, quote_plus

from playwright.sync_api import sync_playwright, TimeoutError
from src.utils import generate_linkedin_links

BASE = "https://jobs.apple.com"
SEARCH_TEMPLATE = BASE + "/en-us/search?search={q}"
JOB_PATH_FRAGMENT = "/details/"

FAST_MODE = os.getenv("FAST_MODE") == "1"
if FAST_MODE:
    QUERIES = [
        "software engineer",
        "software developer",
        "new grad",
        "intern",
        "early career",
    ]
    MAX_SCROLL_LOOPS = 12
    EARLY_STOP_TARGET = 350
else:
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
    MAX_SCROLL_LOOPS = 45
    EARLY_STOP_TARGET = 10_000

# Broader keep terms for Apple
RELAXED_KEYS = [
    "engineer", "engineering", "developer", "software", "machine", "ml",
    "data", "ai", "ios", "android", "security", "reliability", "sre",
    "platform", "backend", "front end", "frontend", "full stack", "devops",
    "swe", "sde",
]

# Generic link texts that are not real titles
GENERIC_TEXT = {
    "apply", "apply now", "learn more", "view role", "see role", "details",
    "learn", "read more", "more", "view job", "view more", "see more",
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

def _collect_from_json_like(obj):
    out = []

    def first_str(d, keys):
        for k in keys:
            for dk, v in d.items():
                if dk.lower() == k and isinstance(v, str) and v.strip():
                    return v.strip()
        return ""

    def build_url(d):
        for k in ["url", "applyurl", "detailsurl", "canonicalurl", "href", "path", "slug"]:
            for dk, v in d.items():
                if dk.lower() == k and isinstance(v, str) and v.strip():
                    u = v.strip()
                    if u.startswith("http"):
                        return u
                    if u.startswith("/"):
                        return urljoin(BASE, u)
                    return urljoin(BASE, "/" + u)
        for dk, v in d.items():
            if dk.lower() in {"jobid", "id", "rolenumber", "reqid"} and isinstance(v, (str, int)):
                return urljoin(BASE, f"/en-us/details/{v}")
        return ""

    def looks_like_job(d):
        if not isinstance(d, dict):
            return False
        keys = {k.lower() for k in d.keys()}
        has_title = any(k in keys for k in ["title", "jobtitle", "postingtitle", "name"])
        has_urlish = any(k in keys for k in ["url", "applyurl", "detailsurl", "canonicalurl", "href", "path", "slug", "jobid", "id", "rolenumber", "reqid"])
        return has_title and has_urlish

    def walk(x):
        if isinstance(x, dict):
            if looks_like_job(x):
                title = first_str(x, ["title", "jobtitle", "postingtitle", "name"])
                url = build_url(x)
                loc = first_str(x, ["location", "joblocation", "city", "region"]) or "N/A"
                if url and JOB_PATH_FRAGMENT in url:
                    out.append({"title": title or "", "url": url, "location": loc})
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(obj)
    return out

def _best_text(txt, aria, title_attr):
    # Choose the best display string among inner text, aria label, title attribute
    for s in [txt, aria, title_attr]:
        s = (s or "").strip()
        if s:
            return s
    return ""

def _collect_from_dom(frame):
    anchors = frame.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({href: a.getAttribute('href') || '', abs: a.href || '', "
        "text: a.innerText || '', aria: a.getAttribute('aria-label') || '', title: a.getAttribute('title') || ''}))",
    )
    items, seen = [], set()
    for a in anchors:
        href = a.get("href") or ""
        absu = a.get("abs") or ""
        txt = _best_text(a.get("text"), a.get("aria"), a.get("title"))
        target = ""
        if JOB_PATH_FRAGMENT in href:
            target = href
        elif JOB_PATH_FRAGMENT in absu:
            target = absu
        if not target:
            continue
        if "/search" in target and "search=" in target:
            continue
        if not target.startswith("http"):
            target = urljoin(BASE, target)
        key = (target, txt)
        if key in seen:
            continue
        seen.add(key)
        items.append({"title": txt or "", "url": target, "location": "N/A"})
    return items

def _job_id_from_url(url: str) -> str:
    m = re.search(r"/details/(\d+)", url)
    return m.group(1) if m else ""

def _derive_title_from_url(url: str) -> str:
    m = re.search(r"/details/\d+/?([^/?#]+)", url)
    if not m:
        return "Software Engineer"
    slug = m.group(1)
    # Remove common tracking suffixes if present
    slug = re.sub(r"\?.*$", "", slug)
    return slug.replace("-", " ").replace("_", " ").title()

def _looks_generic(s: str) -> bool:
    s_norm = re.sub(r"\s+", " ", s.strip().lower())
    return s_norm in GENERIC_TEXT or s_norm in {g + " »" for g in GENERIC_TEXT}

def _title_matches_any(title: str, keyword_filters) -> bool:
    t = title.lower()
    return any(k in t for k in keyword_filters) or any(k in t for k in RELAXED_KEYS)

def scrape_apple_jobs(keyword_filters):
    print("Scraping Apple with Playwright")
    t_start = time.time()
    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            geolocation={"latitude": 43.6532, "longitude": -79.3832},
            permissions=["geolocation"],
            viewport={"width": 1366, "height": 900},
        )

        def route_initial(route):
            rt = route.request.resource_type
            if rt in {"image", "media"}:
                return route.abort()
            return route.continue_()

        context.route("**/*", route_initial)

        captured = []

        def on_response(resp):
            try:
                if "jobs.apple.com" not in resp.url:
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
            hydrated = False
            for q in QUERIES:
                url = SEARCH_TEMPLATE.format(q=quote_plus(q))
                print(f"  > Opening {url}")
                page.goto(url, timeout=60000)
                with suppress(TimeoutError):
                    page.wait_for_load_state("networkidle", timeout=8000)

                if not hydrated:
                    with suppress(Exception):
                        context.unroute("**/*")
                    def route_after_hydration(route):
                        rt = route.request.resource_type
                        if rt in {"image", "media", "font"}:
                            return route.abort()
                        return route.continue_()
                    context.route("**/*", route_after_hydration)
                    hydrated = True

                _scroll_until_stable(page, max_loops=MAX_SCROLL_LOOPS)
                _rand_sleep()

                if FAST_MODE and len(captured) >= EARLY_STOP_TARGET:
                    print("    early stop reached in fast mode")
                    break

            network_jobs = [
                {"title": it.get("title", "").strip(), "url": it["url"].strip(), "location": it.get("location", "N/A").strip() or "N/A"}
                for it in captured
                if it.get("url")
            ]

            dom_jobs = []
            for fr in page.context.pages[0].frames:
                dom_jobs.extend(_collect_from_dom(fr))

            pool = network_jobs + dom_jobs
            print(f"  > Parsed {len(pool)} potential Apple roles before filtering")

            seen_ids = set()
            seen_url_title = set()

            kept = 0
            for it in pool:
                raw_title = it.get("title", "").strip()
                url = it.get("url", "").strip()
                loc = it.get("location", "N/A")
                if not url:
                    continue

                # Build the best possible title for filtering
                derived = _derive_title_from_url(url)
                title_for_filter = raw_title if raw_title and not _looks_generic(raw_title) else derived

                jid = _job_id_from_url(url)
                if jid:
                    if jid in seen_ids:
                        continue
                    seen_ids.add(jid)
                else:
                    key = (title_for_filter, url)
                    if key in seen_url_title:
                        continue
                    seen_url_title.add(key)

                if not _title_matches_any(title_for_filter, keyword_filters):
                    # Try the other one if we used raw first
                    other = derived if title_for_filter == raw_title else raw_title
                    if not _title_matches_any(other, keyword_filters):
                        continue
                    title_for_filter = other if other else title_for_filter

                # Final title to save. Prefer the non generic human label if it is informative.
                final_title = raw_title if raw_title and not _looks_generic(raw_title) else title_for_filter

                links = generate_linkedin_links("Apple", final_title)
                row = {
                    "Company": "Apple",
                    "Title": final_title,
                    "URL": url,
                    "Location": loc,
                }
                row.update(links)
                jobs.append(row)
                kept += 1

        except Exception as e:
            print(f"  > Error while scraping Apple: {e}")
        finally:
            context.close()
            browser.close()

    print(f"  > Collected {len(jobs)} Apple jobs matching your criteria")
    print(f"    took {time.time() - t_start:.1f}s")
    return jobs
