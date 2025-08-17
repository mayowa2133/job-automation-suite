# src/scrapers/custom/workday.py
import os
import re
import time
import random
from contextlib import suppress
from urllib.parse import urlparse, urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError

from src.utils import generate_linkedin_links

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

def _flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}

FAST_MODE  = _flag("FAST_MODE", "0")
KEEP_UNKNOWN_COUNTRY = _flag("WORKDAY_KEEP_UNKNOWN_COUNTRY", "0")
DEBUG      = _flag("WORKDAY_DEBUG", "0")
HEADLESS   = not _flag("WD_HEADLESS", "0")  # WD_HEADLESS=1 shows the browser
FACET_DUMP = _flag("WD_FACET_DUMP", "0")

# Greedy city-level selection (Intel benefits from this)
GREEDY_LOCATIONS = _flag("WD_LOCATIONS_GREEDY", "1")
# Force using the popover-search typing path (best for Intel)
FORCE_SEARCH_LOC = _flag("WD_LOCATIONS_FORCE_SEARCH", "0")
# Optional cap when tenants have thousands of labels
LOCATIONS_LABEL_CAP = int(os.getenv("WD_LOCATIONS_MAX_LABELS", "3500"))

# Optional: skip flaky tenants temporarily, e.g. "AMD,Qualcomm"
SKIP_TENANTS = {t.strip().lower() for t in (os.getenv("WD_SKIP_TENANTS", "") or "").split(",") if t.strip()}

KICK_QUERY = os.getenv("WORKDAY_FORCE_QUERY", "engineer").strip()

_raw_countries = (os.getenv("WORKDAY_ALLOWED_COUNTRIES", "US,CA") or "").strip()
if _raw_countries in {"*", "ALL", "all"}:
    ALLOWED_COUNTRIES = set()
else:
    ALLOWED_COUNTRIES = {c.strip().upper() for c in _raw_countries.split(",") if c.strip()} or {"US", "CA"}

PAGE_LIMIT = 50
EARLY_CAP  = 300 if FAST_MODE else 2000

ENGINEERING_HINTS = [
    "engineer","developer","software","swe","sde","sdet","qa engineer","quality engineer",
    "machine learning","ml","ai","ios","android","security","sre","devops","platform",
    "backend","frontend","front end","full stack","systems","data engineer","compiler",
    "kernel","graphics","infrastructure","cloud",
]
SENIOR_RE = re.compile(r"\b(sr|senior|staff|principal|lead|architect|fellow|distinguished)\b", re.I)

US_STATES = {
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
    "Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky",
    "Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi",
    "Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico",
    "New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
    "Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming",
}
CA_PROVINCES = {
    "Alberta","British Columbia","Manitoba","New Brunswick","Newfoundland and Labrador",
    "Nova Scotia","Ontario","Prince Edward Island","Quebec","Saskatchewan",
    "NL","PE","NS","NB","QC","ON","MB","SK","AB","BC","YT","NT","NU",
}
US_STATE_ABBR = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY",
    "LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND",
    "OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
}

REMOTE_LABEL_REXS = [
    re.compile(r"\bVirtual US\b", re.I),
    re.compile(r"\bUS,?\s*Virtual\b", re.I),
    re.compile(r"\bRemote\s*-\s*United States\b", re.I),
    re.compile(r"\bUnited States\b.*\b(Remote|Virtual)\b", re.I),
    re.compile(r"\b(Remote|Virtual)\b.*\bUnited States\b", re.I),
    re.compile(r"\bUS\b.*\b(Remote|Virtual)\b", re.I),
    re.compile(r"\bUnited States - Remote\b", re.I),
]

def _rand_sleep(a=0.12, b=0.35):
    time.sleep(random.uniform(a, b))

def _norm_country(s: str) -> str:
    s = (s or "").strip().upper()
    if not s: return ""
    if s in {"US","USA","UNITED STATES","UNITED STATES OF AMERICA"}: return "US"
    if s in {"CA","CAN","CANADA"}: return "CA"
    return s

def _parse_portal(base_url: str):
    pr = urlparse(base_url)
    host = pr.netloc
    path = pr.path
    parts = [p for p in path.split("/") if p]
    if not parts:
        raise ValueError(f"Bad Workday portal url {base_url}")
    locale = parts[0] if re.match(r"^[a-z]{2}-[A-Z]{2}$", parts[0]) else "en-US"
    portal = parts[-1]
    tenant = host.split(".")[0].lower()
    endpoint = f"https://{host}/wday/cxs/{tenant}/{portal}/jobs"
    referer  = f"https://{host}{path}"
    return endpoint, referer, host, portal, locale, tenant

def _allowed_country_from_posting(p: dict) -> str | None:
    with suppress(Exception):
        for loc in p.get("locations", []):
            for key in ("country","countryCode","countryIsoCode","isoAlpha3","addressCountry"):
                val = _norm_country(loc.get(key, ""))
                if val:
                    return "US" if val.startswith("US") else "CA" if val.startswith("CA") else val
    txt = (p.get("locationsText") or "").lower()
    if any(s in txt for s in ["united states"," usa",", us"," us)"]): return "US"
    if "canada" in txt or ", ca" in txt or " ca)" in txt:            return "CA"
    if re.search(r"\b(us|united states)\b.*\b(remote|virtual)\b", txt): return "US"
    return None

def _looks_engineering(title: str) -> bool:
    return any(k in title.lower() for k in ENGINEERING_HINTS)

def _is_senior(title: str) -> bool:
    return bool(SENIOR_RE.search(title))

# ---------------- API + HTML inlining ----------------

def _cxs_fetch(endpoint: str, referer: str):
    headers = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": referer.split("/en-")[0],
        "Referer": referer,
        "Accept-Language": "en-US,en;q=0.8",
    }
    offset = 0
    total  = 0
    payloads = [
        {"limit": PAGE_LIMIT, "offset": offset, "searchText": ""},
        {"appliedFacets": {}, "limit": PAGE_LIMIT, "offset": offset, "searchText": ""},
    ]
    while True:
        ok = False
        data = None
        for pl in payloads:
            pl["offset"] = offset
            for url in (endpoint, endpoint + "?sourceLocale=en-US"):
                try:
                    resp = requests.post(url, headers=headers, json=pl, timeout=25)
                    if resp.status_code < 400:
                        ok = True
                        data = resp.json()
                        break
                    if DEBUG: print(f"    api status {resp.status_code} for {url}")
                except Exception as e:
                    if DEBUG: print(f"    api error {e}")
        if not ok or not data:
            break
        postings = data.get("jobPostings") or []
        if not postings:
            break
        for p in postings: yield p
        total  += len(postings)
        offset += len(postings)
        if FAST_MODE and total >= EARLY_CAP:
            break
        _rand_sleep()

def _html_fallback(host: str, path: str):
    url = f"https://{host}{path}"
    headers = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml", "Referer": url, "Accept-Language": "en-US,en;q=0.8"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
    except Exception:
        return []
    if r.status_code >= 400 or not r.text: return []
    html = r.text
    m = re.search(r"jobPostings\"\s*:\s*(\[\s*\{.*?\}\s*\])", html, re.S)
    postings = []
    if m:
        import json
        with suppress(Exception): postings = json.loads(m.group(1))
    if DEBUG: print(f"    html fallback extracted {len(postings)} postings")
    return postings

# ---------------- Page prep ----------------

def _kick_results(page):
    with suppress(Exception):
        btn = page.get_by_role("button", name=re.compile("Accept( All| Cookies)?|I Accept", re.I))
        if btn and btn.is_visible():
            btn.click(); _rand_sleep()
    with suppress(Exception):
        for lab in ["View All Jobs","See All Jobs","Show All Jobs","Explore All Jobs"]:
            va = page.get_by_role("link", name=re.compile(lab, re.I)).first
            if va and va.is_visible():
                va.click()
                with suppress(TimeoutError): page.wait_for_load_state("networkidle", timeout=4000)
                _rand_sleep(); break
    selectors = ["[data-automation-id='keywordSearchInput']","input[placeholder*='Search']","input[aria-label*='Search']"]
    typed, el = False, None
    for sel in selectors:
        try:
            el = page.wait_for_selector(sel, timeout=2500)
            if el: el.fill(KICK_QUERY); typed = True; break
        except Exception: continue
    if typed and el:
        with suppress(Exception): el.press("Enter"); _rand_sleep()
    with suppress(Exception):
        b = page.get_by_role("button", name=re.compile("^Search$", re.I))
        if b and b.is_visible(): b.click()
    with suppress(TimeoutError): page.wait_for_load_state("networkidle", timeout=5000)
    _rand_sleep(0.3, 0.7)

# ---------------- Facet helpers (frames + shadow DOM + search) ----------------

def _all_scopes(page):
    scopes = [page]
    with suppress(Exception):
        for fr in page.frames:
            if fr not in scopes:
                scopes.append(fr)
    return scopes

def _open_facet_header(scope, pattern: re.Pattern):
    for sel in ["button","summary","[role='button']","[data-automation-id*='facet'] [role='button']"]:
        with suppress(Exception):
            hdr = scope.locator(f":light({sel})").filter(has_text=pattern).first
            if hdr and hdr.is_visible():
                hdr.click(); _rand_sleep()
                with suppress(Exception):
                    sec = hdr.locator("xpath=ancestor::section[1]")
                    if sec and sec.count() > 0: return sec.first
                with suppress(Exception):
                    div = hdr.locator("xpath=ancestor::div[contains(@data-automation-id,'facet')][1]")
                    if div and div.count() > 0: return div.first
                return scope
    return None

def _open_location_popover(page):
    """Click a Location/Locations opener that spawns a popover/dialog and return its container."""
    scopes = _all_scopes(page)
    openers = []
    for sc in scopes:
        with suppress(Exception):
            cand = sc.get_by_role("button", name=re.compile(r"\bLocation(s)?\b", re.I)).all()
            openers.extend(cand)
        with suppress(Exception):
            cand = sc.locator(":light([aria-label*='Location']), :light([data-automation-id*='location'])").all()
            openers.extend(cand)
    for op in openers:
        with suppress(Exception):
            if not op.is_visible(): continue
            op.click(); _rand_sleep(0.15, 0.35)
            for sel in ["[role='dialog']",
                        ":light([data-automation-id*='facetPanel'])",
                        ":light([data-automation-id*='popover'])",
                        ":light([data-automation-id*='filterPanel'])"]:
                with suppress(Exception):
                    cont = page.locator(sel).last
                    if cont and cont.is_visible():
                        return cont
    return None

def _facet_scroll_all(container):
    if not container: return
    for _ in range(16):
        with suppress(Exception):
            container.evaluate("el => { el.scrollTop = el.scrollHeight; }")
        _rand_sleep(0.04, 0.1)

def _click_all_more(container):
    if not container: return
    for _ in range(20):
        did = False
        for lab in [r"Show more", r"More", r"See more", r"View more", r"Show all", r"View all"]:
            with suppress(Exception):
                btn = container.locator(":light(button), :light(a)").filter(has_text=re.compile(lab, re.I)).first
                if btn and btn.is_visible():
                    btn.click(); _rand_sleep(0.12, 0.22); did = True; break
        if not did: break

def _candidate_label_nodes(container):
    return container.locator(
        ":light(label), "
        ":light([role='checkbox']), "
        ":light(div[aria-checked]), "
        ":light(button[role='checkbox']), "
        ":light([data-automation-id*='checkbox']), "
        ":light([role='option']), "
        ":light(li[role='option']), "
        ":light(div[role='option']), "
        ":light([data-automation-id='singleSelectFacetOptionButton']), "
        ":light(button[data-automation-id*='Pill']), "
        ":light(div[data-uxi-widget='filterPill'])"
    )

def _node_text(node):
    txt = ""
    with suppress(Exception): txt = (node.inner_text() or "").strip()
    if not txt:
        with suppress(Exception): txt = (node.get_attribute("aria-label") or "").strip()
    return txt or ""

def _click_node_force(node):
    # Try an internal checkbox
    with suppress(Exception):
        inp = node.locator(":light(input[type='checkbox'])")
        if inp and inp.count() > 0:
            try:
                inp.first.check()
                return True
            except Exception:
                pass
    # Try the node itself
    with suppress(Exception):
        node.scroll_into_view_if_needed()
        node.click()
        return True
    # Try a programmatic click
    with suppress(Exception):
        node.evaluate("el => el.click && el.click()")
        return True
    # Walk up a few ancestors to find a clickable wrapper
    cur = node
    for _ in range(4):
        with suppress(Exception):
            cur = cur.locator("xpath=..")
            if cur and cur.is_visible():
                try:
                    cur.click()
                    return True
                except Exception:
                    continue
    return False

def _dump_some_labels(container, how_many=120):
    if not (FACET_DUMP and container): return
    try:
        items = _candidate_label_nodes(container)
        n = min(items.count(), how_many)
        print("    [facet dump] sample of selectable items:")
        for i in range(n):
            with suppress(Exception):
                t = _node_text(items.nth(i))
                if t: print("     -", t)
    except Exception:
        pass

def _searchbox(scope_or_container):
    sels = [
        ":light(input[type='search'])",
        ":light(input[aria-label*='Search'])",
        ":light(input[placeholder*='Search'])",
        ":light([data-automation-id='keywordFacetInput'])",
        ":light([data-automation-id='textInput']) input",
        ":light([role='searchbox'])",
        ":light([contenteditable='true'])",
        ":light(div[role='combobox']) input",
    ]
    for sel in sels:
        with suppress(Exception):
            el = scope_or_container.locator(sel).first
            if el and el.is_visible():
                return el
    return None

def _type_text(el, text: str) -> bool:
    # Try fill()
    with suppress(Exception):
        el.fill(text)
        return True
    # Try keyboard typing
    with suppress(Exception):
        el.click()
        with suppress(Exception): el.press("Control+A")
        with suppress(Exception): el.press("Meta+A")
        with suppress(Exception): el.press("Backspace")
        el.type(text, delay=20)
        return True
    # Try programmatic set for contenteditable / custom inputs
    with suppress(Exception):
        el.evaluate(
            """(n, t) => {
                n.focus && n.focus();
                if ('value' in n) {
                    n.value = t;
                    n.dispatchEvent(new Event('input', {bubbles: true}));
                    n.dispatchEvent(new Event('change', {bubbles: true}));
                } else {
                    n.textContent = t;
                    n.dispatchEvent(new Event('input', {bubbles: true}));
                }
            }""",
            text,
        )
        return True
    return False

def _facet_type_and_click(container, queries, per_q_cap=50):
    if not container: return 0
    sb = _searchbox(container) or _searchbox(container.page if hasattr(container, "page") else container)
    if not sb:
        return 0
    clicked = 0
    for q in queries:
        if not _type_text(sb, q):
            continue
        _rand_sleep(0.25, 0.45)
        # Find any node that *starts with* the query text, then walk to a clickable ancestor.
        patt = re.compile(rf"^{re.escape(q)}", re.I)
        options = container.locator(":light(*)").filter(has_text=patt)
        count = 0
        with suppress(Exception):
            count = options.count()
        n = min(count, per_q_cap)
        if DEBUG:
            print(f"    facet search '{q}': options={count} (taking {n})")
        for i in range(n):
            with suppress(Exception):
                node = options.nth(i)
                target = node.locator(
                    "xpath=ancestor-or-self::label | "
                    "xpath=ancestor-or-self::*[@role='checkbox'] | "
                    "xpath=ancestor-or-self::*[@role='option'] | "
                    "xpath=ancestor-or-self::button | "
                    "xpath=ancestor-or-self::*[@data-automation-id='singleSelectFacetOptionButton']"
                ).first
                if target and target.is_visible():
                    if _click_node_force(target):
                        clicked += 1
                        _rand_sleep(0.06, 0.14)
    return clicked

def _check_labels_matching(container, matchers, cap: int | None = None) -> int:
    if not container: return 0
    _click_all_more(container)
    _facet_scroll_all(container)
    clicked = 0
    try:
        nodes = _candidate_label_nodes(container)
        n = nodes.count()
        limit = min(n, cap or LOCATIONS_LABEL_CAP)
        for i in range(limit):
            node = nodes.nth(i)
            txt = _node_text(node)
            if not txt: continue
            if any(m(txt) for m in matchers):
                if _click_node_force(node):
                    clicked += 1
                    _rand_sleep(0.03, 0.09)
    except Exception:
        pass
    return clicked

def _apply_country_filters(page):
    if not ALLOWED_COUNTRIES or not {"US","CA"} & ALLOWED_COUNTRIES:
        return

    scopes = _all_scopes(page)
    country_container = None

    # Classic "Country/Region" facet (any frame)
    for sc in scopes:
        for lab in [r"Country/Region", r"Country and Region", r"Country & Region",
                    r"Country or Region", r"Country", r"Location", r"Locations"]:
            country_container = _open_facet_header(sc, re.compile(lab, re.I))
            if country_container: break
        if country_container: break

    # Tick United States / Canada anywhere we can
    for code, rex in [("US", re.compile(r"\bUnited States\b", re.I)),
                      ("CA", re.compile(r"\bCanada\b", re.I))]:
        if ALLOWED_COUNTRIES and code not in ALLOWED_COUNTRIES: continue
        did = False
        with suppress(Exception):
            cb = page.get_by_role("checkbox", name=rex)
            if cb and cb.is_visible():
                cb.scroll_into_view_if_needed(); cb.check(); _rand_sleep()
                did = True
        if not did and country_container:
            did = _check_labels_matching(country_container, [lambda t, r=rex: bool(r.search(t))], cap=200) > 0
        if not did:
            _check_labels_matching(page, [lambda t, r=rex: bool(r.search(t))], cap=200)

    # Prefer the **Location popover/dialog**
    loc_container = _open_location_popover(page)
    if not loc_container:
        # Fallback: other location facets
        for sc in scopes:
            for h in [r"Locations", r"Location", r"State/Region", r"State/Province", r"State", r"Province", r"Province/State", r"City"]:
                loc_container = _open_facet_header(sc, re.compile(h, re.I))
                if loc_container: break
            if loc_container: break
    if not loc_container:
        loc_container = country_container or page

    if FACET_DUMP:
        _dump_some_labels(loc_container)

    clicked_states = 0
    clicked_cities = 0
    clicked_remote = 0

    def _do_search_type(container):
        queries = []
        for s in sorted(US_STATES):
            queries.append(f"US, {s}")
            queries.append(f"United States, {s}")
        for p in sorted(list({x for x in CA_PROVINCES if len(x) > 2})):
            queries.append(f"Canada, {p}")
            queries.append(f"CA, {p}")
        queries += ["United States - Remote", "Virtual US", "US Virtual"]
        return _facet_type_and_click(container, queries, per_q_cap=12)

    if FORCE_SEARCH_LOC:
        got = _do_search_type(loc_container)
        clicked_cities += got
    else:
        # 1) states/provinces via label scan
        def _state_matcher(txt: str) -> bool:
            t = txt.strip()
            if t in US_STATES or t in CA_PROVINCES: return True
            if re.search(r"\(([A-Z]{2})\)$", t) and re.sub(r".*\(([A-Z]{2})\)$", r"\1", t) in US_STATE_ABBR:
                return True
            if t.startswith("United States") and any((" - " + s) in t for s in US_STATES): return True
            if t.startswith("Canada")        and any((" - " + p) in t for p in CA_PROVINCES): return True
            return False
        clicked_states = _check_labels_matching(loc_container, [_state_matcher])

        # 2) city-level chips (Intel)
        if GREEDY_LOCATIONS:
            def _city_matcher(txt: str) -> bool:
                t = txt.strip()
                return t.startswith(("US,", "United States,")) or t.startswith(("CA,", "Canada,"))
            clicked_cities = _check_labels_matching(loc_container, [_city_matcher])

        # 3) remote US
        def _remote_matcher(txt: str) -> bool:
            return any(rx.search(txt) for rx in REMOTE_LABEL_REXS)
        clicked_remote = _check_labels_matching(loc_container, [_remote_matcher])

        # 4) If nothing clicked, try page-wide + typing
        if (clicked_states + clicked_cities + clicked_remote) == 0:
            if DEBUG: print("    facet fallback: page-wide label scan")
            def _pw_matcher(txt: str) -> bool:
                t = txt.strip()
                if t.startswith(("US,", "United States,")) or t.startswith(("CA,", "Canada,")): return True
                if any(rx.search(t) for rx in REMOTE_LABEL_REXS): return True
                return _state_matcher(t)
            _check_labels_matching(page, [_pw_matcher])
            got = _do_search_type(loc_container)
            clicked_cities += got

    if DEBUG:
        print(f"    facet clicks: states={clicked_states} cities={clicked_cities} remote={clicked_remote}")

    # Apply / Close
    with suppress(Exception):
        apply_btn = page.get_by_role("button", name=re.compile(r"\bApply\b", re.I)).first
        if apply_btn and apply_btn.is_visible():
            apply_btn.click(); _rand_sleep()
    with suppress(TimeoutError): page.wait_for_load_state("networkidle", timeout=5000)
    _rand_sleep(0.2, 0.5)

# ---------------- Pagination ----------------

def _click_next(page) -> bool:
    candidates = [
        "[data-automation-id='searchResultNextButton']",
        "[data-automation-id='navigationPageNext']",
        "button[aria-label*='Next']",
        "button[title*='Next']",
        "button:has-text('Next')",
        "button:has-text('Load more')",
        "button:has-text('Show more')",
        "[data-uxi-widget='pagination'] button:last-child",
    ]
    for sel in candidates:
        with suppress(Exception):
            el = page.query_selector(sel)
            if el and el.is_enabled() and el.is_visible():
                el.click(); return True
    return False

# ---------------- Portal juggling ----------------

def _alt_portal_urls(base_url: str, max_count: int = 2):
    try:
        _, _, host, portal, locale, _ = _parse_portal(base_url)
    except Exception:
        return []
    candidates = [
        portal, portal.lower(), portal.capitalize(),
        "External","external","Careers","careers",
        "CandidateExperience","candidateExperience","candidateexperience",
    ]
    seen, urls = set(), []
    for p in candidates:
        if p in seen: continue
        seen.add(p)
        urls.append(f"https://{host}/{locale}/{p}")
        if len(urls) >= max_count: break
    return urls

def _is_maintenance_or_error(page) -> bool:
    with suppress(Exception):
        if "community.workday.com/maintenance-page" in page.url:
            return True
        txt = (page.content() or "")
        if re.search(r"Oops, an error occurred", txt, re.I):
            return True
    return False

# ---------------- Playwright fallback (network capture + DOM) ----------------

def _playwright_fallback(base_url: str, allowed_cap: int = 2000):
    captured, dom_items = [], []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(user_agent=UA, locale="en-US", viewport={"width": 1366, "height": 900})
        def route_handler(route):
            rt = route.request.resource_type
            if rt in {"image","media","font"}: return route.abort()
            return route.continue_()
        context.route("**/*", route_handler)

        def on_response(resp):
            try:
                if "/wday/cxs/" not in resp.url: return
                data = None
                with suppress(Exception): data = resp.json()
                if not data: return
                postings = data.get("jobPostings") or []
                if postings: captured.extend(postings)
            except Exception:
                pass
        context.on("response", on_response)

        def harvest_from(url_to_open: str):
            nonlocal captured, dom_items
            page = context.new_page()
            try:
                page.goto(url_to_open, wait_until="domcontentloaded", timeout=60000)
                with suppress(TimeoutError): page.wait_for_load_state("networkidle", timeout=7000)
                if _is_maintenance_or_error(page):
                    if DEBUG: print("    maintenance/error page detected; skipping this URL")
                    return
                _kick_results(page)
                _apply_country_filters(page)

                last_height, stable, loops = -1, 0, 0
                while loops < (12 if FAST_MODE else 40):
                    loops += 1
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    with suppress(Exception): page.wait_for_load_state("networkidle", timeout=2500)
                    clicked = _click_next(page)
                    if clicked:
                        with suppress(Exception): page.wait_for_load_state("networkidle", timeout=3500)
                    new_height = page.evaluate("document.body.scrollHeight")
                    if new_height == last_height and not clicked:
                        stable += 1
                        if stable >= 2: break
                    else:
                        stable = 0; last_height = new_height
                    if FAST_MODE and len(captured) >= EARLY_CAP: break

                if not captured and not _is_maintenance_or_error(page):
                    cards = page.eval_on_selector_all(
                        "a[data-automation-id='jobTitle'], a[href*='/job/']",
                        "els => els.map(a => ({href: a.getAttribute('href') || '', abs: a.href || '', text: a.innerText || ''}))",
                    )
                    for a in cards:
                        href = a.get("href") or ""; absu = a.get("abs") or ""; txt = (a.get("text") or "").strip()
                        target = href if "/job/" in href else absu if "/job/" in absu else ""
                        if not target: continue
                        if not target.startswith("http"): target = urljoin(url_to_open, target)
                        dom_items.append({"title": txt, "externalPath": urlparse(target).path, "locationsText": "", "url": target})
            finally:
                page.close()

        tried = set()
        for u in [base_url] + _alt_portal_urls(base_url, max_count=2):
            if u in tried: continue
            tried.add(u)
            harvest_from(u)
            if captured or dom_items: break

        context.close(); browser.close()

    if captured: return captured
    return [{"title": it["title"], "externalPath": it.get("externalPath",""), "locationsText": ""} for it in dom_items]

# ---------------- URL composer ----------------

def _compose_url(host: str, portal: str, locale: str, tenant: str, path_part: str, external_url: str | None) -> str:
    if external_url and external_url.startswith("http"): return external_url
    p = (path_part or "").strip()
    if not p: return ""
    if not p.startswith("/"): p = "/" + p
    p = re.sub(rf"^/{re.escape(tenant)}/", "/", p)  # drop leading tenant
    if not re.match(r"^/([a-z]{2}-[A-Z]{2})/", p): p = f"/{locale}{p}"
    if f"/{portal}/" not in p:
        p = re.sub(r"^/([a-z]{2}-[A-Z]{2})/", rf"/\1/{portal}/", p, count=1)
    p = re.sub(r"/{2,}", "/", p)
    return f"https://{host}{p}"

# ---------------- Public entry ----------------

def scrape_workday_jobs(keyword_filters: list[str], portals: dict[str, str]) -> list[dict]:
    rows: list[dict] = []
    for company, base_url in portals.items():
        print(f"Scraping Workday for {company}")
        try:
            endpoint, referer, host, portal, locale, tenant = _parse_portal(base_url)
        except Exception as e:
            print(f"  > Could not parse portal for {company} reason {e}")
            continue

        if tenant.lower() in SKIP_TENANTS:
            print("  > Skipping tenant due to WD_SKIP_TENANTS")
            continue

        fetched = kept = country_pass = title_pass = 0

        postings = list(_cxs_fetch(endpoint, referer))
        used = "cxs api"

        if not postings:
            postings = _html_fallback(host, f"/{locale}/{portal}")
            if postings: used = "html inline"

        if not postings:
            used = "playwright"
            postings = _playwright_fallback(base_url, allowed_cap=EARLY_CAP)

        for p in postings:
            fetched += 1
            path_part = (p.get("externalPath") or p.get("externalUrl") or "").strip()
            title     = (p.get("title") or "").strip()
            url = _compose_url(host, portal, locale, tenant, path_part, p.get("url") or p.get("externalUrl"))
            if not url:  continue
            if not title and p.get("url"): title = (p.get("url") or "").split("/")[-1].replace("-", " ").title()
            if not title: continue

            loc_text = (p.get("locationsText") or p.get("locations", "") or "N/A")
            if isinstance(loc_text, list): loc_text = ", ".join(loc_text) or "N/A"
            loc_text = str(loc_text)

            ctry = _allowed_country_from_posting(p)
            if ALLOWED_COUNTRIES:
                if ctry is None and not KEEP_UNKNOWN_COUNTRY: continue
                if ctry is not None and ctry not in ALLOWED_COUNTRIES: continue
            country_pass += 1

            tl = title.lower()
            base_keep = any(k in tl for k in [k.lower() for k in keyword_filters]) or _looks_engineering(title)
            if not base_keep: continue
            if _is_senior(title) and not any(k in tl for k in ["new grad","university","entry","junior","intern"]):
                continue
            title_pass += 1

            links = generate_linkedin_links(company, title)
            rows.append({
                "Company": company,
                "Title": title,
                "URL": url,
                "Location": loc_text,
                **links,
            })
            kept += 1
            if FAST_MODE and kept >= EARLY_CAP: break

        if DEBUG:
            print(f"    {used} fetched {fetched} after country {country_pass} after title {title_pass} kept {kept}")
        print(f"  > Collected {kept} jobs for {company}")

    return rows
