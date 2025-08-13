# src/scrapers/custom/apple.py
import os
import re
import time
import random
import json
from contextlib import suppress
from urllib.parse import urljoin, quote_plus, urlparse, parse_qs

from playwright.sync_api import sync_playwright, TimeoutError
from src.utils import generate_linkedin_links

BASE = "https://jobs.apple.com"
SEARCH_TEMPLATE = BASE + "/en-us/search?{params}"
JOB_PATH_FRAGMENT = "/details/"

# Team filtered entry points that skew to software engineering
TEAM_PAGES = [
    # Software and Services group
    "apps-and-frameworks-SFTWR-AF",
    "core-operating-systems-SFTWR-COS",
    "security-and-privacy-SFTWR-SEC",
    "cloud-and-infrastructure-SFTWR-CLD",
    "devops-and-site-reliability-SFTWR-DSR",
    "wireless-software-SFTWR-WSFT",
    "information-systems-and-technology-SFTWR-ISTECH",
    "machine-learning-and-ai-SFTWR-MCHLN",
    # AIML divisions
    "applied-research-MLAI-AR",
    "natural-language-processing-and-speech-technologies-MLAI-NLP",
    "machine-learning-infrastructure-MLAI-MLI",
    "deep-learning-and-reinforcement-learning-MLAI-DLRL",
]

# Extra keyword searches as fallbacks
QUERY_TERMS = [
    "software engineer",
    "software developer",
    "machine learning engineer",
    "platform engineer",
    "backend engineer",
    "site reliability engineer",
    "security engineer",
    "sdet",
    "ios engineer",
    "android engineer",
    "data engineer",
]

# Broad engineering keep terms for title or slug checks
RELAXED_KEYS = [
    "engineer", "engineering", "developer", "software", "swe", "sde",
    "sdet", "qa engineer", "machine learning", "ml", "ai",
    "ios", "android", "security", "sre", "devops", "platform",
    "backend", "frontend", "front end", "full stack", "systems",
]

# Obvious non engineering words to down rank
EXCLUDE_HINTS = [
    "retail", "specialist", "manager", "advisor", "genius", "business expert",
    "operations", "sales", "marketing", "finance", "store", "accommodation",
    "support team manager", "online support",
]

FAST_MODE = os.getenv("FAST_MODE") == "1"
MAX_SCROLL_LOOPS = 14 if FAST_MODE else 45
EARLY_STOP_TARGET = 600 if FAST_MODE else 10_000

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

def _best_text(txt, aria, title_attr):
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
        if "/search" in target:
            continue
        if not target.startswith("http"):
            target = urljoin(BASE, target)
        key = (target, txt)
        if key in seen:
            continue
        seen.add(key)
        items.append({"title": txt or "", "url": target, "location": "N/A"})
    return items

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
                title = first_str(x, ["title", "jobtitle", "postingtitle", "name"]) or ""
                url = build_url(x)
                loc = first_str(x, ["location", "joblocation", "city", "region"]) or "N/A"
                if url and JOB_PATH_FRAGMENT in url:
                    out.append({"title": title, "url": url, "location": loc})
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(obj)
    return out

def _job_id_from_url(url: str) -> str:
    m = re.search(r"/details/(\d+)", url)
    return m.group(1) if m else ""

def _derive_title_from_url(url: str) -> str:
    m = re.search(r"/details/\d+/?([^/?#]+)", url)
    if not m:
        return ""
    slug = re.sub(r"\?.*$", "", m.group(1))
    # clean common noise
    slug = slug.replace("%E2%80%93", "-")
    return slug.replace("-", " ").replace("_", " ").strip().title()

def _is_engineering_like(title: str) -> bool:
    t = title.lower()
    if any(h in t for h in EXCLUDE_HINTS):
        return False
    return any(k in t for k in RELAXED_KEYS)

def _page_team(url: str) -> str:
    try:
        q = parse_qs(urlparse(url).query)
        vals = q.get("team", [])
        return vals[0] if vals else ""
    except Exception:
        return ""

def scrape_apple_jobs(keyword_filters):
    print("Scraping Apple with Playwright")
    t0 = time.time()
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

        # Trim heavy resources
        def route_handler(route):
            rt = route.request.resource_type
            if rt in {"image", "media", "font"}:
                return route.abort()
            return route.continue_()
        context.route("**/*", route_handler)

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
            # 1 team filtered passes that bias toward engineering
            for team in TEAM_PAGES:
                url = SEARCH_TEMPLATE.format(params=f"team={quote_plus(team)}")
                print(f"  > Opening {url}")
                page.goto(url, timeout=60000)
                with suppress(TimeoutError):
                    page.wait_for_load_state("networkidle", timeout=8000)
                _scroll_until_stable(page, max_loops=MAX_SCROLL_LOOPS)
                _rand_sleep()
                if FAST_MODE and len(captured) >= EARLY_STOP_TARGET:
                    print("    early stop reached in fast mode")
                    break

            # 2 a couple keyword passes as safety nets
            if not FAST_MODE:
                for q in QUERY_TERMS[:6]:
                    url = SEARCH_TEMPLATE.format(params=f"search={quote_plus(q)}")
                    print(f"  > Opening {url}")
                    page.goto(url, timeout=60000)
                    with suppress(TimeoutError):
                        page.wait_for_load_state("networkidle", timeout=8000)
                    _scroll_until_stable(page, max_loops=MAX_SCROLL_LOOPS // 2)
                    _rand_sleep()

            # Build pool from network captures and DOM
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
            seen_pairs = set()
            kept, dropped = 0, 0

            for it in pool:
                raw_title = (it.get("title") or "").strip()
                url = (it.get("url") or "").strip()
                loc = it.get("location", "N/A")
                if not url:
                    dropped += 1
                    continue

                # Derive a useful title when the link text is generic
                title = raw_title or _derive_title_from_url(url)
                title = title if title else _derive_title_from_url(url)

                # De dupe by job id when possible
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

                # Positive filter for engineering
                title_lc = title.lower()
                keep_by_relaxed = any(k in title_lc for k in RELAXED_KEYS)
                keep_by_user = any(k in title_lc for k in [k.lower() for k in keyword_filters])

                # Easy negative filter for retail or support like pages
                url_lc = url.lower()
                obvious_non_eng = any(h in title_lc for h in EXCLUDE_HINTS) or "/retail/" in url_lc or "CUST" in _page_team(url)

                if not (keep_by_relaxed or keep_by_user) or obvious_non_eng:
                    dropped += 1
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
                kept += 1

            print(f"    Apple keep audit kept={kept} dropped={dropped}")

        except Exception as e:
            print(f"  > Error while scraping Apple: {e}")
        finally:
            context.close()
            browser.close()

    print(f"  > Collected {len(jobs)} Apple jobs matching your criteria")
    print(f"    took {time.time() - t0:.1f}s")
    return jobs
