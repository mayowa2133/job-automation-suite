# src/scrapers/ashby.py

import os
import re
import json
import requests
from contextlib import suppress
from datetime import datetime, timezone
from src.utils import generate_linkedin_links

# =======================
# Flags / configuration
# =======================

def _flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}

ASHBY_DEBUG = _flag("ASHBY_DEBUG", "0")

# Countries to keep (default US & CA). Set ASHBY_ALLOWED_COUNTRIES=ALL to disable.
_raw_countries = (os.getenv("ASHBY_ALLOWED_COUNTRIES", "US,CA") or "").strip()
if _raw_countries in {"*", "ALL", "all"}:
    ASHBY_ALLOWED_COUNTRIES: set[str] = set()  # empty => keep all
else:
    ASHBY_ALLOWED_COUNTRIES: set[str] = {c.strip().upper() for c in _raw_countries.split(",") if c.strip()} or {"US", "CA"}

# If we can't infer country at all from the location strings, keep only if this flag is set.
ASHBY_KEEP_UNKNOWN_COUNTRY = _flag("ASHBY_KEEP_UNKNOWN_COUNTRY", "0")

# New-grad/early-career gating (on by default), internships excluded by default
ASHBY_NEWGRAD_ONLY    = _flag("ASHBY_NEWGRAD_ONLY", "1")
ASHBY_INCLUDE_INTERNS = _flag("ASHBY_INCLUDE_INTERNS", "0")

# Preferred public endpoints (we try these in order).
GET_TPL     = "https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
POST_URL    = "https://api.ashbyhq.com/posting-api/job-board"
GQL_GLOBAL  = "https://jobs.ashbyhq.com/api/non-user-graphql?op=JobBoardWithOpenings"
# Per-org GQL (not all orgs have a subdomain): https://{slug}.ashbyhq.com/api/non-user-graphql?op=JobBoardWithOpenings

UA_HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (compatible; job-automation-suite/1.0)",
    "Accept": "application/json, text/plain, */*",
}

def _ashby_headers(slug: str, origin_host: str = "jobs.ashbyhq.com", extra: dict | None = None) -> dict:
    """
    Build headers that satisfy CORS/origin checks on many Ashby tenants.
    """
    h = dict(UA_HEADERS_BASE)
    h.update({
        "Origin": f"https://{origin_host}",
        "Referer": f"https://{origin_host}/{slug}",
    })
    if extra:
        h.update(extra)
    return h


# =======================
# Location inference (US/CA)
# =======================

_US_STATE_ABBR = set(
    "AL AK AZ AR CA CO CT DC DE FL GA HI ID IL IN IA KS KY "
    "LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI "
    "SC SD TN TX UT VT VA WA WV WI WY".split()
)
_US_STATE_NAMES = {
    "ALABAMA","ALASKA","ARIZONA","ARKANSAS","CALIFORNIA","COLORADO","CONNECTICUT","DELAWARE","FLORIDA","GEORGIA",
    "HAWAII","IDAHO","ILLINOIS","INDIANA","IOWA","KANSAS","KENTUCKY","LOUISIANA","MAINE","MARYLAND","MASSACHUSETTS",
    "MICHIGAN","MINNESOTA","MISSISSIPPI","MISSOURI","MONTANA","NEBRASKA","NEVADA","NEW HAMPSHIRE","NEW JERSEY",
    "NEW MEXICO","NEW YORK","NORTH CAROLINA","NORTH DAKOTA","OHIO","OKLAHOMA","OREGON","PENNSYLVANIA","RHODE ISLAND",
    "SOUTH CAROLINA","SOUTH DAKOTA","TENNESSEE","TEXAS","UTAH","VERMONT","VIRGINIA","WASHINGTON","WEST VIRGINIA",
    "WISCONSIN","WYOMING","DISTRICT OF COLUMBIA","WASHINGTON, D.C.","WASHINGTON DC","D.C."
}
_CA_PROV_ABBR = {"AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"}
_CA_PROV_NAMES = {
    "ALBERTA","BRITISH COLUMBIA","MANITOBA","NEW BRUNSWICK","NEWFOUNDLAND AND LABRADOR","NOVA SCOTIA",
    "ONTARIO","PRINCE EDWARD ISLAND","QUEBEC","SASKATCHEWAN","YUKON","NORTHWEST TERRITORIES","NUNAVUT"
}

_REMOTE_RE      = re.compile(r"\b(remote|virtual|work\s*from\s*home|wfh)\b", re.I)
_US_TOKEN_RE    = re.compile(r"\b(UNITED STATES|U\.?S\.?A?\.?|U\.?S\.?)\b", re.I)
_CA_TOKEN_RE    = re.compile(r"\bCANADA\b", re.I)
_CA_PROV_TOKEN  = re.compile(r"(?:^|,\s*)(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)(?:\s|,|$)", re.I)
_US_STATE_TOKEN = re.compile(r"(?:^|,\s*)(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(?:\s|,|$)", re.I)

def _infer_countries_from_location(text: str) -> set[str]:
    s = (text or "").strip()
    if not s:
        return set()

    up = s.upper()
    countries: set[str] = set()

    if _US_TOKEN_RE.search(up):
        countries.add("US")
    if _CA_TOKEN_RE.search(up):
        countries.add("CA")

    if _REMOTE_RE.search(up):
        if _US_TOKEN_RE.search(up):
            countries.add("US")
        if _CA_TOKEN_RE.search(up):
            countries.add("CA")

    m_us = _US_STATE_TOKEN.search(up)
    if m_us and m_us.group(1).upper() in _US_STATE_ABBR:
        countries.add("US")
    m_ca = _CA_PROV_TOKEN.search(up)
    if m_ca and m_ca.group(1).upper() in _CA_PROV_ABBR:
        countries.add("CA")

    for name in _US_STATE_NAMES:
        if re.search(rf"\b{re.escape(name)}\b", up):
            countries.add("US")
            break
    for name in _CA_PROV_NAMES:
        if re.search(rf"\b{re.escape(name)}\b", up):
            countries.add("CA")
            break

    return countries

def _locations_allowed(candidates: list[str]) -> bool:
    if not ASHBY_ALLOWED_COUNTRIES:
        return True

    inferred_any = False
    for cand in candidates:
        found = _infer_countries_from_location(cand)
        if found:
            inferred_any = True
            if found & ASHBY_ALLOWED_COUNTRIES:
                if ASHBY_DEBUG:
                    print(f"    [Ashby filter] allowed by '{cand}' -> {found & ASHBY_ALLOWED_COUNTRIES}")
                return True

    if not inferred_any:
        if ASHBY_DEBUG:
            print("    [Ashby filter] no inference; "
                  f"{'keeping' if ASHBY_KEEP_UNKNOWN_COUNTRY else 'dropping'} (ASHBY_KEEP_UNKNOWN_COUNTRY={int(ASHBY_KEEP_UNKNOWN_COUNTRY)})")
        return ASHBY_KEEP_UNKNOWN_COUNTRY

    if ASHBY_DEBUG:
        print("    [Ashby filter] inferred country/countries but none allowed; dropping")
    return False


# =======================
# New-grad heuristics
# =======================

SENIOR_RE = re.compile(r"\b(sr\.?|senior|staff|principal|lead|manager|director|head|vp|iii|iv|v)\b", re.I)
NEW_GRAD_HINTS = [
    "new grad", "new graduate", "university grad", "university graduate",
    "recent graduate", "graduate program", "grad program", "graduate scheme",
    "entry-level", "entry level", "early career", "early talent",
    "junior", "jr ", "jr.", "associate",
    "engineer i", "software engineer i", "developer i", "swe i", "se i",
    "engineer 1", "developer 1", "software engineer 1",
]

def _is_new_grad_friendly(title: str) -> bool:
    t = (title or "").lower()
    if not ASHBY_INCLUDE_INTERNS and re.search(r"\b(intern|internship|co[-\s]?op)\b", t):
        return False
    if SENIOR_RE.search(t):
        return False
    return any(k in t for k in NEW_GRAD_HINTS)


# =======================
# Fetchers / Normalizers
# =======================

def _get_json(url: str, method: str = "GET", payload: dict | None = None, headers: dict | None = None) -> dict | None:
    try:
        if method == "GET":
            if ASHBY_DEBUG:
                print(f"    [Ashby fetch] GET {url}")
            r = requests.get(url, timeout=30, headers=headers)
        elif method == "POST":
            if ASHBY_DEBUG:
                print(f"    [Ashby fetch] POST {url}")
            h = dict(headers or {})
            h.setdefault("Content-Type", "application/json")
            r = requests.post(url, timeout=30, json=payload or {}, headers=h)
        else:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if ASHBY_DEBUG:
            print(f"    [Ashby fetch] {method} {url} -> {e}")
        return None

_NEXT_DATA_RE = re.compile(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', re.S | re.I)

def _fetch_from_html(html_url: str, slug: str) -> list[dict]:
    """
    Last-resort fallback: fetch public board HTML and extract jobs from
    Next.js __NEXT_DATA__ JSON.
    """
    if ASHBY_DEBUG:
        print(f"    [Ashby HTML] GET {html_url}")
    try:
        r = requests.get(html_url, timeout=30, headers=_ashby_headers(slug))
        r.raise_for_status()
        m = _NEXT_DATA_RE.search(r.text or "")
        if not m:
            if ASHBY_DEBUG:
                print("    [Ashby HTML] __NEXT_DATA__ not found")
            return []
        with suppress(Exception):
            data = json.loads(m.group(1))
            # Try common paths
            props = (data or {}).get("props") or {}
            page = props.get("pageProps") or {}
            jb = page.get("jobBoard") or page.get("board") or {}
            jobs = jb.get("jobs") or jb.get("jobPostings") or []

            out: list[dict] = []

            def push(item: dict):
                if not isinstance(item, dict):
                    return
                title = item.get("title") or item.get("jobTitle") or ""
                url   = item.get("jobPostUrl") or item.get("jobUrl") or item.get("url") or item.get("applyUrl")
                loc   = item.get("location") or {}
                if isinstance(loc, dict):
                    loc = loc.get("name") or loc.get("displayName") or ""
                created = item.get("createdAt") or item.get("publishedAt") or item.get("postDate") or item.get("openedAt")
                if title and url:
                    out.append({
                        "title": title,
                        "url": url,
                        "location": (loc or "").strip(),
                        "createdAt": created,
                    })

            for j in jobs:
                push(j)

            # Nested groups/sections with openings
            for key in ("groups", "sections", "departments", "teams", "categories"):
                arr = jb.get(key)
                if isinstance(arr, list):
                    for g in arr:
                        openings = (g or {}).get("openings") or []
                        for op in openings:
                            push(op)

            if ASHBY_DEBUG:
                print(f"    [Ashby HTML] extracted {len(out)} posting(s)")
            return out
    except Exception as e:
        if ASHBY_DEBUG:
            print(f"    [Ashby HTML] {html_url} -> {e}")
    return []

def _gql_payload(slug: str) -> dict:
    return {
        "operationName": "JobBoardWithOpenings",
        "variables": {"organizationSlug": slug},
        "query": (
            "query JobBoardWithOpenings($organizationSlug: String!) {"
            "  jobBoard(organizationSlug: $organizationSlug) {"
            "    jobs { id title createdAt jobPostUrl location { name } }"
            "    jobPostings { id title createdAt jobPostUrl location { name } }"
            "    groups {"
            "      name"
            "      openings { id title createdAt jobPostUrl location { name } }"
            "    }"
            "  }"
            "}"
        ),
    }

def _normalize_jobs(data: dict | None) -> list[dict]:
    """
    Convert a known Ashby response shape into a list of job dicts.
    NON-RECURSIVE: handle the public shapes we’ve seen.
    """
    jobs: list[dict] = []
    if not isinstance(data, dict):
        return jobs

    def push(item: dict):
        if not isinstance(item, dict):
            return
        title = (
            item.get("title")
            or item.get("jobTitle")
            or item.get("postingTitle")
            or ""
        )
        url = (
            item.get("jobPostUrl")
            or item.get("jobPostingUrl")
            or item.get("jobUrl")
            or item.get("applyUrl")
            or item.get("url")
            or ""
        )
        raw_loc = item.get("location") or item.get("primaryLocation") or item.get("office")
        loc = ""
        if isinstance(raw_loc, str):
            loc = raw_loc.strip()
        elif isinstance(raw_loc, dict):
            loc = (raw_loc.get("name") or raw_loc.get("location") or raw_loc.get("displayName") or "").strip()
        elif isinstance(raw_loc, list):
            locs = []
            for x in raw_loc:
                if isinstance(x, str) and x.strip():
                    locs.append(x.strip())
                elif isinstance(x, dict):
                    nm = (x.get("name") or x.get("location") or x.get("displayName") or "").strip()
                    if nm:
                        locs.append(nm)
            loc = ", ".join(locs)
        created = (
            item.get("createdAt")
            or item.get("publishedAt")
            or item.get("postedAt")
            or item.get("openDate")
            or item.get("openedAt")
            or item.get("updatedAt")
        )
        if title and url:
            jobs.append({
                "title": title,
                "url": url,
                "location": loc,
                "createdAt": created,
            })

    # 1) direct jobs list
    if isinstance(data.get("jobs"), list):
        for j in data["jobs"]:
            push(j)

    # 2) jobPostings list
    if isinstance(data.get("jobPostings"), list):
        for j in data["jobPostings"]:
            push(j)

    # 3) jobBoard -> jobs / jobPostings / groups[*].openings
    jb = data.get("jobBoard")
    if isinstance(jb, dict):
        if isinstance(jb.get("jobs"), list):
            for j in jb["jobs"]:
                push(j)
        if isinstance(jb.get("jobPostings"), list):
            for j in jb["jobPostings"]:
                push(j)
        for key in ("groups", "sections", "departments", "teams", "categories"):
            arr = jb.get(key)
            if isinstance(arr, list):
                for g in arr:
                    openings = (g or {}).get("openings") or []
                    for op in openings:
                        push(op)

    # 4) GraphQL: data -> jobBoard -> same shapes
    d2 = data.get("data")
    if isinstance(d2, dict):
        jb2 = d2.get("jobBoard")
        if isinstance(jb2, dict):
            if isinstance(jb2.get("jobs"), list):
                for j in jb2["jobs"]:
                    push(j)
            if isinstance(jb2.get("jobPostings"), list):
                for j in jb2["jobPostings"]:
                    push(j)
            for key in ("groups", "sections", "departments", "teams", "categories"):
                arr = jb2.get(key)
                if isinstance(arr, list):
                    for g in arr:
                        openings = (g or {}).get("openings") or []
                        for op in openings:
                            push(op)

    return jobs

def _fetch_for_slug(slug: str) -> list[dict]:
    """
    Try GET, POST, global GraphQL, per-org GraphQL, then HTML fallbacks.
    Returns a list of normalized jobs.
    """
    if ASHBY_DEBUG:
        print(f"    [Ashby] trying slug '{slug}'")

    # A) GET endpoint
    data = _get_json(GET_TPL.format(slug=slug), "GET", headers=_ashby_headers(slug))
    jobs = _normalize_jobs(data)
    if ASHBY_DEBUG:
        print(f"    [Ashby] GET normalized {len(jobs)} job(s)")
    if jobs:
        return jobs

    # B) POST endpoint (often 401 without origin/referrer)
    data = _get_json(POST_URL, "POST", payload={"organizationSlug": slug}, headers=_ashby_headers(slug))
    jobs = _normalize_jobs(data)
    if ASHBY_DEBUG:
        print(f"    [Ashby] POST normalized {len(jobs)} job(s)")
    if jobs:
        return jobs

    # C1) Global GraphQL (jobs.ashbyhq.com)
    data = _get_json(GQL_GLOBAL, "POST", payload=_gql_payload(slug), headers=_ashby_headers(slug))
    jobs = _normalize_jobs(data)
    if ASHBY_DEBUG:
        print(f"    [Ashby] GQL global normalized {len(jobs)} job(s)")
    if jobs:
        return jobs

    # C2) Per-org GraphQL endpoint: https://{slug}.ashbyhq.com/api/non-user-graphql
    gql_org = f"https://{slug}.ashbyhq.com/api/non-user-graphql?op=JobBoardWithOpenings"
    data = _get_json(gql_org, "POST", payload=_gql_payload(slug), headers=_ashby_headers(slug, origin_host=f"{slug}.ashbyhq.com"))
    jobs = _normalize_jobs(data)
    if ASHBY_DEBUG:
        print(f"    [Ashby] GQL org normalized {len(jobs)} job(s)")
    if jobs:
        return jobs

    # D) HTML page parsing fallback (Next.js __NEXT_DATA__)
    html_urls = [
        f"https://jobs.ashbyhq.com/{slug}",
        f"https://{slug}.ashbyhq.com",
        f"https://jobs.ashbyhq.com/{slug}/jobs",
    ]
    for u in html_urls:
        jobs = _fetch_from_html(u, slug)
        if jobs:
            return jobs

    return []

def _slugify_company_name(company: str) -> list[str]:
    """
    Build a few reasonable slug candidates from a company name.
    """
    if not company:
        return []
    base = company.lower()
    base = re.sub(r"\(.*?\)", "", base).strip()              # drop parentheses content
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")       # normalize to hyphens
    candidates = [base, base.replace("-", "")]
    parts = [p for p in re.split(r"[^a-z0-9]+", base) if p]
    if parts:
        candidates.append(parts[0])
    # de-dupe preserving order
    seen, out = set(), []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out

def _fetch_jobs(company_name: str, org_slug: str) -> tuple[list[dict], str]:
    """
    Try the provided slug first, then sanitized candidates from the company name.
    Returns (jobs, used_slug).
    """
    tried = []
    for slug in [org_slug] + _slugify_company_name(company_name):
        if not slug or slug in tried:
            continue
        tried.append(slug)
        jobs = _fetch_for_slug(slug)
        if jobs:
            return jobs, slug
    return [], org_slug


# =======================
# Main scrape
# =======================

def _collect_location_strings(raw_location) -> list[str]:
    out: list[str] = []
    if isinstance(raw_location, str) and raw_location.strip():
        out.append(raw_location.strip())
    elif isinstance(raw_location, list):
        for x in raw_location:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
            elif isinstance(x, dict):
                name = (x.get("name") or x.get("location") or x.get("displayName") or "").strip()
                if name:
                    out.append(name)
    elif isinstance(raw_location, dict):
        name = (raw_location.get("name") or raw_location.get("location") or raw_location.get("displayName") or "").strip()
        if name:
            out.append(name)

    # de-dupe
    seen, uniq = set(), []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq or ["N/A"]

def _fmt_posted(created_at) -> str:
    """
    Accepts ISO strings, epoch ms, or epoch seconds. Returns YYYY-MM-DD.
    """
    if not created_at:
        return ""
    with suppress(Exception):
        if isinstance(created_at, (int, float)):
            if created_at > 10_000_000_000:  # ms
                dt = datetime.fromtimestamp(created_at / 1000.0, tz=timezone.utc)
            else:
                dt = datetime.fromtimestamp(created_at, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d")
    with suppress(Exception):
        s = str(created_at).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.date().isoformat()
    return ""

def scrape_ashby_jobs(company_name: str, org_slug: str, keyword_filters: list[str]) -> list[dict]:
    """
    Scrape a single Ashby org’s public job board with robust fallbacks.
    - Filters to US/CA by default (env configurable)
    - Filters to new-grad friendly roles by default (env configurable)
    - Adds a "Posted" date when available
    """
    print(f"Scraping {company_name} (Ashby)...")

    jobs_raw, used_slug = _fetch_jobs(company_name, org_slug)
    if not jobs_raw:
        print(f"  > Could not fetch jobs for {company_name}.")
        return []
    if ASHBY_DEBUG and used_slug != org_slug:
        print(f"    [Ashby] fetched using candidate slug '{used_slug}'")

    kw = [k.lower() for k in (keyword_filters or [])]
    out: list[dict] = []

    for j in jobs_raw:
        title = j.get("title", "") or ""
        t_lower = title.lower()

        # 1) baseline title filter using your KEYWORD_FILTERS from main.py
        if not any(k in t_lower for k in kw):
            continue

        # 2) new-grad gating
        if ASHBY_NEWGRAD_ONLY and not _is_new_grad_friendly(title):
            continue

        # 3) location gating
        locs = _collect_location_strings(j.get("location"))
        if not _locations_allowed(locs):
            if ASHBY_DEBUG:
                print(f"    [Ashby drop] {title} :: locations={locs}")
            continue

        display_location = ", ".join(locs) if locs else "N/A"
        posted = _fmt_posted(j.get("createdAt"))

        links = generate_linkedin_links(company_name, title)
        row = {
            "Company": company_name,
            "Title": title,
            "URL": j.get("url"),
            "Location": display_location,
            "Posted": posted,
            **links,
        }
        out.append(row)

    print(f"  > Found {len(out)} relevant jobs.")
    return out
