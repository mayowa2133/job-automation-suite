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
JOB_PATH_FRAGMENT = "/details/"

# Locales to try
LOCALES = ["en-us", "en-ca"]

def _search_template(locale: str) -> str:
    return f"{BASE}/{locale}/search?{{params}}"

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
    "data engineer",
    "ios engineer",
    "android engineer",
]

# Treat these team codes as software engineering
KEEP_TEAM_CODES = {"SFTWR", "MLAI", "ML", "AIML"}

# Exclude obvious non engineering buckets
EXCLUDE_HINTS_NON_ENG = {
    "retail",
    "specialist",
    "genius",
    "advisor",
    "accommodation",
}

# Explicit manager filters
MANAGER_HINTS = {
    " program manager",
    " project manager",
    " product manager",
    " engineering program manager",
    " technical program manager",
    " tpm",
    " epm",
    " mgr",
}

# Seniority and early career controls
def _env_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}

APPLE_SENIORITY_TRIM = _env_flag("APPLE_SENIORITY_TRIM", "1")   # default on
APPLE_EARLY_ONLY     = _env_flag("APPLE_EARLY_ONLY", "0")       # default off

SENIOR_HINTS_RE = re.compile(r"\b(sr|senior|staff|principal|lead|architect|fellow|distinguished)\b", re.I)
EARLY_SIGNS_RE = re.compile(
    r"(new\s*grad|university|graduate|early\s*career|entry\s*level|entry|junior|assoc(iate)?|intern|apprentice|engineer\s*[i1]\b)",
    re.I,
)

FAST_MODE = os.getenv("FAST_MODE") == "1"
MAX_SCROLL_LOOPS = 12 if FAST_MODE else 40
EARLY_STOP_TARGET = 600 if FAST_MODE else 10_000

# Country filtering
def _allowed_country_codes() -> set[str]:
    raw = os.getenv("APPLE_ALLOWED_COUNTRIES", "US,CA")
    return {c.strip().upper() for c in raw.split(",") if c.strip()}

COUNTRY_SYNONYMS = {
    "US": {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"},
    "CA": {"CA", "CAN", "CANADA"},
}

def _loc_in_allowed(loc: str, allowed: set[str], url: str = "", locale_hint: str = "") -> bool:
    """Return True if location or url locale implies an allowed country."""
    if not allowed:
        return True
    s = (loc or "").strip().upper()

    # direct string hits like Toronto, ON, Canada or Remote, United States
    if s:
        if any(f", {code}" in s for code in allowed):
            return True
        for code in allowed:
            if any(token in s for token in COUNTRY_SYNONYMS.get(code, {code})):
                return True

    # infer from url path locale
    u = url.lower()
    if "/en-us/" in u and "US" in allowed:
        return True
    if "/en-ca/" in u and "CA" in allowed:
        return True

    # infer from page locale hint
    if locale_hint:
        if locale_hint.lower() == "en-us" and "US" in allowed:
            return True
        if locale_hint.lower() == "en-ca" and "CA" in allowed:
            return True

    return False


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


def _extract_location_from_text(txt: str) -> str:
    """Heuristic: many Apple cards include a line with location."""
    if not txt:
        return ""
    lines = [l.strip() for l in txt.split("\n") if l.strip()]
    for l in lines[::-1]:
        u = l.upper()
        if any(tok in u for tok in ["UNITED STATES", "CANADA", ", US", ", CA"]):
            return l
        # generic city state pattern
        if "," in l and len(l) <= 80:
            return l
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

    # do not confuse App Store with retail
    if "app store" in t:
        pass
    else:
        if any(h in t for h in EXCLUDE_HINTS_NON_ENG):
            return False

    # hard filter for manager roles
    if any(h in t for h in MANAGER_HINTS):
        return False

    eng_terms = [
        "engineer", "engineering", "developer", "software", "swe", "sde",
        "sdet", "qa engineer", "quality engineer",
        "machine learning", "ml", "ai",
        "ios", "android",
        "security", "sre", "devops",
        "platform", "backend", "frontend", "front end", "full stack",
        "systems", "data engineer", "compiler", "kernel", "graphics",
        "infrastructure", "cloud",
    ]
    if any(k in t for k in eng_terms):
        return True

    code = _team_code_from_url(url).upper()
    if any(tag in code for tag in KEEP_TEAM_CODES):
        return True

    if "/retail/" in url.lower():
        return False

    return False


def _collect_from_dom(page, locale_hint: str):
    """Collect detail links and try to pull a location hint from visible text."""
    items, seen = [], set()

    # Anchors with real hrefs
    a_items = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({href: a.getAttribute('href') || '', abs: a.href || '', "
        "text: a.innerText || '', aria: a.getAttribute('aria-label') || '', title: a.getAttribute('title') || ''}))",
    )
    for a in a_items:
        href = a.get("href") or ""
        absu = a.get("abs") or ""
        txt = _best_text(a.get("text"), a.get("aria"), a.get("title"))
        candidate = ""
        if JOB_PATH_FRAGMENT in href:
            candidate = href
        elif JOB_PATH_FRAGMENT in absu:
            candidate = absu
        if not candidate or "/search" in candidate:
            continue
        if not candidate.startswith("http"):
            candidate = urljoin(BASE, candidate)
        key = ("a", candidate, txt)
        if key in seen:
            continue
        seen.add(key)
        loc = _extract_location_from_text(txt) or ("United States" if locale_hint == "en-us" else "Canada" if locale_hint == "en-ca" else "N/A")
        items.append({"title": txt or "", "url": candidate, "location": loc, "locale": locale_hint})

    # Cards or buttons with data attributes
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
        loc = _extract_location_from_text(txt) or ("United States" if locale_hint == "en-us" else "Canada" if locale_hint == "en-ca" else "N/A")
        items.append({"title": txt or "", "url": url, "location": loc, "locale": locale_hint})

    return items


def _has_senior_signal(title: str) -> bool:
    return bool(SENIOR_HINTS_RE.search(title))


def _has_early_signal(title: str) -> bool:
    return bool(EARLY_SIGNS_RE.search(title))


def scrape_apple_jobs(keyword_filters):
    print("Scraping Apple with Playwright")
    t0 = time.time()
    jobs = []
    allowed_codes = _allowed_country_codes()

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

        page = context.new_page()
        context.set_default_navigation_timeout(90000)

        pooled_raw = []

        try:
            # Visit team filtered pages for both locales and harvest after each
            for locale in LOCALES:
                for team in TEAM_PAGES:
                    url = _search_template(locale).format(params=f"team={quote_plus(team)}")
                    print(f"  > Opening {url}")
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    except TimeoutError:
                        print("    timeout opening page, continuing")
                        continue
                    with suppress(TimeoutError):
                        page.wait_for_load_state("networkidle", timeout=6000)
                    _scroll_until_stable(page, max_loops=MAX_SCROLL_LOOPS)
                    items = _collect_from_dom(page, locale_hint=locale)
                    print(f"    harvested {len(items)} from this page")
                    pooled_raw.extend(items)
                    _rand_sleep()
                    if FAST_MODE and len(pooled_raw) >= EARLY_STOP_TARGET:
                        break
                if FAST_MODE and len(pooled_raw) >= EARLY_STOP_TARGET:
                    break

            # A few keyword passes as safety nets
            for locale in LOCALES:
                for q in (QUERY_TERMS if not FAST_MODE else QUERY_TERMS[:3]):
                    url = _search_template(locale).format(params=f"search={quote_plus(q)}")
                    print(f"  > Opening {url}")
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    except TimeoutError:
                        print("    timeout opening page, continuing")
                        continue
                    with suppress(TimeoutError):
                        page.wait_for_load_state("networkidle", timeout=6000)
                    _scroll_until_stable(page, max_loops=MAX_SCROLL_LOOPS // 2)
                    items = _collect_from_dom(page, locale_hint=locale)
                    print(f"    harvested {len(items)} from this page")
                    pooled_raw.extend(items)
                    _rand_sleep()
                    if FAST_MODE and len(pooled_raw) >= EARLY_STOP_TARGET:
                        break
                if FAST_MODE and len(pooled_raw) >= EARLY_STOP_TARGET:
                    break

            print(f"  > Parsed {len(pooled_raw)} potential Apple roles before filtering")

            # Filter and de dupe
            seen_ids = set()
            seen_pairs = set()
            kept, dropped = 0, 0
            drop_samples = []

            for it in pooled_raw:
                raw_title = (it.get("title") or "").strip()
                url = (it.get("url") or "").strip()
                loc = it.get("location", "N/A")
                locale_hint = it.get("locale", "")

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

                # Country filter
                if not _loc_in_allowed(loc, allowed_codes, url=url, locale_hint=locale_hint):
                    dropped += 1
                    continue

                is_eng = _looks_engineering(title, url)
                if not is_eng:
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP not_eng title='{title}' url='{url[:120]}'")
                    continue

                # Seniority trim unless early signal
                if APPLE_SENIORITY_TRIM and _has_senior_signal(title) and not _has_early_signal(title):
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP senior_trim title='{title}'")
                    continue

                # Early career only toggle
                if APPLE_EARLY_ONLY and not _has_early_signal(title):
                    dropped += 1
                    if len(drop_samples) < 12:
                        drop_samples.append(f"DROP early_only title='{title}'")
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

            # If strict pass yields nothing, relax to team code only from pooled_raw
            if kept == 0:
                relaxed = []
                seen_ids.clear()
                for it in pooled_raw:
                    url = (it.get("url") or "").strip()
                    if not url:
                        continue
                    if not _loc_in_allowed(it.get("location", ""), allowed_codes, url=url, locale_hint=it.get("locale", "")):
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
