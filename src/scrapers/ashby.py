import os
import re
import time
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
GET_TPL  = "https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
POST_URL = "https://api.ashbyhq.com/posting-api/job-board"
GQL_URL  = "https://jobs.ashbyhq.com/api/non-user-graphql?op=JobBoardWithOpenings"


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
_NA_TOKEN_RE    = re.compile(r"\b(NORTH AMERICA|AMERICAS)\b", re.I)
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
    if _NA_TOKEN_RE.search(up):
        countries.update({"US", "CA"})

    if _REMOTE_RE.search(up):
        if _US_TOKEN_RE.search(up) or _NA_TOKEN_RE.search(up):
            countries.add("US")
        if _CA_TOKEN_RE.search(up) or _NA_TOKEN_RE.search(up):
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
# Slug utilities + retries
# =======================

_slug_clean_re = re.compile(r"[^a-z0-9]+")
def _slugify(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("&", "and")
    s = _slug_clean_re.sub("-", s)
    return s.strip("-")

def _candidate_slugs(company_name: str, org_slug: str) -> list[str]:
    # Generate a compact set of likely slugs to try
    base = (org_slug or "").strip()
    name = (company_name or "").strip()
    cands = [
        base,
        base.lower(),
        _slugify(base),
        _slugify(name),
        _slugify(name.replace("inc", "")),
        _slugify(name.replace("labs", "")),
        _slugify(name.replace(" ai", "")),
        _slugify(name + " ai"),
    ]
    # Remove empties & dups, preserve order
    seen, out = set(), []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out

def _request(method: str, url: str, payload: dict | None = None, max_retries: int = 2) -> requests.Response | None:
    # Lightweight retry/backoff for 429/5xx/timeouts
    backoff = 0.7
    for attempt in range(max_retries + 1):
        try:
            if method == "GET":
                r = requests.get(url, timeout=30)
            else:
                r = requests.post(url, json=payload or {}, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt < max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                time.sleep(backoff)
                backoff *= 2
                continue
            if ASHBY_DEBUG:
                print(f"    [Ashby fetch] {method} {url} -> {e}")
            return None
    return None

def _get_json(method: str, url: str, payload: dict | None = None) -> dict | None:
    r = _request(method, url, payload)
    if not r:
        return None
    with suppress(Exception):
        return r.json()
    return None


# =======================
# Normalization helpers
# =======================

def _push_job(item: dict, sink: list[dict]) -> None:
    if not isinstance(item, dict):
        return
    title = item.get("title") or item.get("jobTitle") or ""
    url   = (
        item.get("jobPostUrl") or item.get("jobUrl") or item.get("url")
        or item.get("applyUrl") or item.get("postingUrl")
    )
    # location fields can vary wildly
    loc = item.get("location")
    if not loc:
        loc = item.get("locationText") or item.get("displayLocation") or item.get("officeLocation")
    if not loc:
        locs = item.get("locations") or item.get("offices")  # may be list[str|dict]
        if isinstance(locs, list) and locs:
            parts = []
            for x in locs:
                if isinstance(x, str):
                    parts.append(x.strip())
                elif isinstance(x, dict):
                    parts.append((x.get("name") or x.get("location") or x.get("displayName") or "").strip())
            loc = ", ".join([p for p in parts if p])

    created = (
        item.get("createdAt") or item.get("publishedAt") or item.get("postDate")
        or item.get("openedAt") or item.get("updatedAt") or item.get("updated_at")
    )

    if not (title and url):
        return

    sink.append({
        "title": title,
        "url": url,
        "location": loc or "",
        "createdAt": created,
    })

def _normalize_jobs(raw: dict) -> list[dict]:
    if not isinstance(raw, dict):
        return []

    jobs: list[dict] = []

    # Common shapes:

    # A) Direct lists
    for key in ("jobs", "jobPostings", "openJobs", "openings"):
        lst = raw.get(key)
        if isinstance(lst, list):
            for j in lst:
                _push_job(j, jobs)

    # B) jobBoard wrapper (REST & GraphQL)
    jb = raw.get("jobBoard") or {}
    if isinstance(jb, dict):
        for key in ("jobs", "openings", "openJobs"):
            lst = jb.get(key)
            if isinstance(lst, list):
                for j in lst:
                    _push_job(j, jobs)

        # Nested departments/teams with jobs
        for k in ("departments", "groups", "teams", "categories"):
            depts = jb.get(k)
            if isinstance(depts, list):
                for d in depts:
                    for key in ("jobs", "openings", "openJobs"):
                        lst = (d or {}).get(key)
                        if isinstance(lst, list):
                            for j in lst:
                                _push_job(j, jobs)

    # C) GraphQL data wrapper
    data = raw.get("data") or {}
    if isinstance(data, dict):
        jobs += _normalize_jobs(data)  # recurse into same patterns

    # D) Sometimes everything is under 'result' or 'payload'
    result = raw.get("result") or raw.get("payload") or {}
    if isinstance(result, dict):
        jobs += _normalize_jobs(result)

    # Deduplicate by (title,url)
    seen, deduped = set(), []
    for j in jobs:
        key = (j.get("title"), j.get("url"))
        if key not in seen:
            seen.add(key)
            deduped.append(j)
    return deduped


# =======================
# Fetchers
# =======================

def _fetch_for_slug(slug: str) -> tuple[list[dict], str | None]:
    # 1) GET
    data = _get_json("GET", GET_TPL.format(slug=slug))
    if not data:
        # 2) POST
        data = _get_json("POST", POST_URL, payload={"organizationSlug": slug})
    if not data:
        # 3) GraphQL
        gql_body = {
            "operationName": "JobBoardWithOpenings",
            "variables": {"organizationSlug": slug},
            "query": (
                "query JobBoardWithOpenings($organizationSlug: String!) {"
                "  jobBoard(organizationSlug: $organizationSlug) {"
                "    jobs { id title createdAt jobPostUrl location { name } }"
                "    departments { name jobs { id title createdAt jobPostUrl location { name } } }"
                "  }"
                "}"
            ),
        }
        data = _get_json("POST", GQL_URL, payload=gql_body)

    if not data:
        return [], None

    jobs = _normalize_jobs(data)
    return jobs, slug

def _fetch_jobs(company_name: str, org_slug: str) -> tuple[list[dict], str | None]:
    last_err_slug = None
    for slug in _candidate_slugs(company_name, org_slug):
        jobs, used = _fetch_for_slug(slug)
        if jobs:
            if ASHBY_DEBUG:
                print(f"    [Ashby] slug '{used}' yielded {len(jobs)} jobs (pre-filter)")
            return jobs, used
        last_err_slug = slug
    if ASHBY_DEBUG and last_err_slug:
        print(f"    [Ashby] no data for tried slugs; last tried '{last_err_slug}'")
    return [], None


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
        if s and s not in seen:
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
    Output rows match your main.py expectations.
    """
    print(f"Scraping {company_name} (Ashby)...")

    jobs_raw, used_slug = _fetch_jobs(company_name, org_slug)
    if not jobs_raw:
        print(f"  > Could not fetch jobs for {company_name}.")
        return []
    if ASHBY_DEBUG and used_slug:
        print(f"    [Ashby] using slug: {used_slug}")

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
