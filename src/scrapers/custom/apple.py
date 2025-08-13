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

# Fast daily runs
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

# During the catch everything phase allow any engineer or developer
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
        # click any load more style control if present
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
    """
    Walk any JSON structure and pull things that look like jobs.
    Apple feeds vary by page, so we match common field names.
    """
    out = []

    def first_str(d, keys):
        for k in keys:
            for dk, v in d.items():
                if dk.lower() == k and isinstance(v, str) and v.strip():
                    return v.strip()
        return ""

    def build_url(d):
        # common urlish keys
        for k in ["url", "applyurl", "detailsurl", "canonicalurl", "href", "path", "slug"]:
            for dk, v in d.items():
                if dk.lower() == k and isinstance(v, str) and v.strip():
                    u = v.strip()
                    if u.startswith("http"):
                        return u
                    if u.startswith("/"):
                        return urljoin(BASE, u)
                    return urljoin(BASE, "/" + u)
        # fallback via an id style field
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
        if "/search" in target and "search=" in target:
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
    m = re.search(r"/details/(\d+)", url)
    return m.group(1) if m else ""


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

        # Let the app hydrate first. Skip heavy media only at first.
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

                # after first hydration, block fonts too for speed
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

            # Prefer network found jobs
            network_jobs = [
                {"title": it["title"].strip(), "url": it["url"].strip(), "location": it.get("location", "N/A").strip() or "N/A"}
                for it in captured
                if it.get("title") and it.get("url")
            ]

            # Fallback to DOM from all frames
            dom_jobs = []
            for fr in page.context.pages[0].frames:
                dom_jobs.extend(_collect_from_dom(fr))

            pool = network_jobs + dom_jobs
            print(f"  > Parsed {len(pool)} potential Apple roles before filtering")

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

                # Relax filters for Apple during the breadth phase
                t = title.lower()
                keep = bool(title) and (any(k in t for k in keyword_filters) or any(k in t for k in RELAXED_KEYS))
                if not keep:
                    continue

                links = generate_linkedin_links("Apple", title)
                row = {
                    "Company": "Apple",
                    "Title": title,
                    "URL": url,
                    "Location": loc,
                }
                row.update(links)
                jobs.append(row)

        except Exception as e:
            print(f"  > Error while scraping Apple: {e}")
        finally:
            context.close()
            browser.close()

    print(f"  > Collected {len(jobs)} Apple jobs matching your criteria")
    print(f"    took {time.time() - t_start:.1f}s")
    return jobs
