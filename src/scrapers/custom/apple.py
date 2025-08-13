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

# Software heavy team pages
TEAM_PAGES = [
    "apps-and-frameworks-SFTWR-AF",
    "core-operating-systems-SFTWR-COS",
    "security-and-privacy-SFTWR-SEC",
    "cloud-and-infrastructure-SFTWR-CLD",
    "devops-and-site-reliability-SFTWR-DSR",
    "wireless-software-SFTWR-WSFT",
    "information-systems-and-technology-SFTWR-ISTECH",
    "machine-learning-and-ai-SFTWR-MCHLN",
    "applied-research-MLAI-AR",
    "natural-language-processing-and-speech-technologies-MLAI-NLP",
    "machine-learning-infrastructure-MLAI-MLI",
    "deep-learning-and-reinforcement-learning-MLAI-DLRL",
]

QUERY_TERMS = [
    "software engineer",
    "software developer",
    "machine learning engineer",
    "site reliability engineer",
    "security engineer",
    "sdet",
]

# Treat these team codes as software engineering
KEEP_TEAM_CODES = {"SFTWR", "MLAI", "ML", "AIML"}

# Guardrails
EXCLUDE_HINTS = {"retail", "specialist", "genius", "advisor", "manager", "store", "accommodation"}

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


def _derive_title_from_url(url: str) -> str:
    m = re.search(r"/details/\d+/?([^/?#]+)", url)
    if not m:
        return "Software Engineer"
    slug = re.sub(r"\?.*$", "", m.group(1))
    return slug.replace("-", " ").replace("_", " ").strip().title()


def _job_id_from_url(url: str) -> str:
    m = re.search(r"/details/(\d+)", url)
    return m.group(1) if m else ""


def _team_code_from_url(url: str) -> str:
    try:
        q = parse_qs(urlparse(url).query)
        v = q.get("team", [""])[0]
        return v.split("-")[-1] if v else ""
    except Exception:
        return ""


def _looks_engineering(title: str, url: str) -> bool:
    t = title.lower()
    if any(h in t for h in EXCLUDE_HINTS):
        return False
    if any(k in t for k in ["engineer", "engineering", "developer", "software", "swe", "sde", "sdet", "qa engineer",
                             "machine learning", "ml", "ai", "ios", "android", "security", "sre",
                             "devops", "platform", "backend", "frontend", "full stack", "systems", "data engineer"]):
        return True
    # fall back to team code in URL
    code = _team_code_from_url(url).upper()
    return any(tag in code for tag in KEEP_TEAM_CODES)


def _collect_from_dom(page):
    """
    Collect detail links from both anchors and elements that store the target
    in data attributes. Apple uses data-analytics-link-destination on cards.
    """
    # 1. Anchors with real hrefs
    a_items = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({href: a.getAttribute('href') || '', abs: a.href || '', "
        "text: a.innerText || '', aria: a.getAttribute('aria-label') || '', title: a.getAttribute('title') || ''}))",
    )

    items = []
    seen = set()
    for a in a_items:
        href = a.get("href") or ""
        absu = a.get("abs") or ""
        txt = _best_text(a.get("text"), a.get("aria"), a.get("title"))
        candidate = ""
        if JOB_PATH_FRAGMENT in href:
            candidate = href
        elif JOB_PATH_FRAGMENT in absu:
            candidate = absu
        if not candidate:
            continue
        if "/search" in candidate:
            continue
        if not candidate.startswith("http"):
            candidate = urljoin(BASE, candidate)
        key = ("a", candidate, txt)
        if key in seen:
            continue
        seen.add(key)
        items.append({"title": txt, "url": candidate, "location": "N/A"})

    # 2. Cards or buttons with data attributes
    data_cards = page.eval_on_selector_all(
        "[data-analytics-link-destination]",
        "els => els.map(e => ({dest: e.getAttribute('data-analytics-link-destination') || '', "
        "text: e.innerText || '', aria: e.getAttribute('aria-label') || '', title: e.getAttribute('title') || ''}))",
    )
    for c in data_cards:
        dest = c.get("dest") or ""
        if JOB_PATH_FRAGMENT not in dest:
            continue
        url = dest if dest.startswith("http") else urljoin(BASE, dest)
        txt = _best_text(c.get("text"), c.get("aria"), c.get("title"))
        key = ("d", url, txt)
        if key in seen:
            continue
        seen.add(key)
        items.append({"title": txt, "url": url, "location": "N/A"})

    return items


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

        def route_handler(route):
            rt = route.request.resource_type
            if rt in {"image", "media", "font"}:
                return route.abort()
            return route.continue_()

        context.route("**/*", route_handler)
        page = context.new_page()

        try:
            # Team filtered passes first
            for team in TEAM_PAGES:
                url = SEARCH_TEMPLATE.format(params=f"team={quote_plus(team)}")
                print(f"  > Opening {url}")
                page.goto(url, timeout=60000)
                with suppress(TimeoutError):
                    page.wait_for_load_state("networkidle", timeout=8000)
                _scroll_until_stable(page, max_loops=MAX_SCROLL_LOOPS)
                _rand_sleep()
                if FAST_MODE:
                    # Small safety net of a couple keyword passes in fast mode
                    break
            # Keyword passes
            for q in (QUERY_TERMS if not FAST_MODE else QUERY_TERMS[:3]):
                url = SEARCH_TEMPLATE.format(params=f"search={quote_plus(q)}")
                print(f"  > Opening {url}")
                page.goto(url, timeout=60000)
                with suppress(TimeoutError):
                    page.wait_for_load_state("networkidle", timeout=8000)
                _scroll_until_stable(page, max_loops=MAX_SCROLL_LOOPS // 2)
                _rand_sleep()

            # Build pool only from DOM which includes anchors and data attribute cards
            pool = _collect_from_dom(page)
            print(f"  > Parsed {len(pool)} potential Apple roles before filtering")

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

                # De dupe
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

                is_eng = _looks_engineering(title, url)

                if not is_eng:
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP reason=not_eng title='{title}' url='{url[:120]}'")
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
            if drop_samples:
                print("    sample drops:")
                for s in drop_samples:
                    print("     - " + s)

            # If we somehow filtered everything, relax to team code only
            if kept == 0:
                relaxed = []
                seen_ids.clear()
                for it in pool:
                    url = (it.get("url") or "").strip()
                    if not url:
                        continue
                    code = _team_code_from_url(url).upper()
                    if not any(tag in code for tag in KEEP_TEAM_CODES):
                        continue
                    title = (it.get("title") or "").strip() or _derive_title_from_url(url)
                    jid = _job_id_from_url(url)
                    if jid and jid in seen_ids:
                        continue
                    if jid:
                        seen_ids.add(jid)
                    links = generate_linkedin_links("Apple", title)
                    relaxed.append({
                        "Company": "Apple",
                        "Title": title,
                        "URL": url,
                        "Location": it.get("location", "N/A"),
                        **links,
                    })
                if relaxed:
                    print(f"    relaxed pass rescued {len(relaxed)} engineering team items")
                    jobs.extend(relaxed)

        except Exception as e:
            print(f"  > Error while scraping Apple: {e}")
        finally:
            context.close()
            browser.close()

    print(f"  > Collected {len(jobs)} Apple jobs matching your criteria")
    print(f"    took {time.time() - t0:.1f}s")
    return jobs
