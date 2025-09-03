# src/scrapers/greenhouse.py
import os
import re
import requests
from datetime import datetime
from src.utils import generate_linkedin_links

# =======================
# Flags / configuration
# =======================

def _gh_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}

GH_DEBUG = _gh_flag("GH_DEBUG", "0")

# Countries to keep (default US & CA). Set GH_ALLOWED_COUNTRIES=ALL to disable country gating.
_raw_countries = (os.getenv("GH_ALLOWED_COUNTRIES", "US,CA") or "").strip()
if _raw_countries in {"*", "ALL", "all"}:
    GH_ALLOWED_COUNTRIES: set[str] = set()  # empty => keep all
else:
    GH_ALLOWED_COUNTRIES: set[str] = {c.strip().upper() for c in _raw_countries.split(",") if c.strip()} or {"US", "CA"}

# If we can't infer country at all from the location strings, keep only if this flag is set.
# Default to 1 so "Remote" posts aren't dropped in tests or typical runs.
GH_KEEP_UNKNOWN_COUNTRY = _gh_flag("GH_KEEP_UNKNOWN_COUNTRY", "1")

# New-grad/early-career gating (off by default so generic "Software Engineer" roles aren't filtered out in tests).
GH_NEWGRAD_ONLY = _gh_flag("GH_NEWGRAD_ONLY", "0")
# Internships are excluded by default; flip to 1 to include.
GH_INCLUDE_INTERNS = _gh_flag("GH_INCLUDE_INTERNS", "0")

# Preferred API hosts: canonical boards API first; legacy host as fallback.
GH_URL_TEMPLATES = [
    "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true",
    "https://api.greenhouse.io/v1/boards/{token}/jobs?content=true",
]

# Modest headers for better compatibility (not necessary for tests, but harmless).
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
_HDRS = {"User-Agent": _UA, "Accept": "application/json"}


# =======================
# US/CA location helpers
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
_US_STATE_TOKEN = re.compile(r"(?:^|,\s*)(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(?:\s|,|$)")

def _infer_countries_from_location(text: str) -> set[str]:
    """
    Return set of country codes found in a free-form Greenhouse location string.
    Recognizes:
      - 'City, ST' (US), 'City, ON' (CA)
      - Full state/province names
      - 'Remote - US', 'Remote (Canada)', 'North America (Remote)' etc.
      - Explicit 'United States' / 'Canada'
    """
    s = (text or "").strip()
    if not s:
        return set()

    up = s.upper()
    countries: set[str] = set()

    # explicit country keywords
    if _US_TOKEN_RE.search(up):
        countries.add("US")
    if _CA_TOKEN_RE.search(up):
        countries.add("CA")

    # remote + country hints
    if _REMOTE_RE.search(up):
        if _US_TOKEN_RE.search(up):
            countries.add("US")
        if _CA_TOKEN_RE.search(up):
            countries.add("CA")

    # token patterns like ", NY" or ", ON"
    m_us = _US_STATE_TOKEN.search(up)
    if m_us and m_us.group(1).upper() in _US_STATE_ABBR:
        countries.add("US")
    m_ca = _CA_PROV_TOKEN.search(up)
    if m_ca and m_ca.group(1).upper() in _CA_PROV_ABBR:
        countries.add("CA")

    # full names
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
    """
    Decide if a job should be kept based on *all* its location strings.
    Logic:
      - If GH_ALLOWED_COUNTRIES is empty: keep all
      - If any candidate infers a country that intersects allowed => keep
      - If none infer any country => keep only if GH_KEEP_UNKNOWN_COUNTRY=1
      - If some infer countries but none intersect allowed => drop
    """
    if not GH_ALLOWED_COUNTRIES:
        return True

    inferred_any = False
    for cand in candidates:
        found = _infer_countries_from_location(cand)
        if found:
            inferred_any = True
            if found & GH_ALLOWED_COUNTRIES:
                if GH_DEBUG:
                    print(f"    [GH filter] allowed by '{cand}' -> {found & GH_ALLOWED_COUNTRIES}")
                return True

    if not inferred_any:
        if GH_DEBUG:
            print("    [GH filter] no inference from any location; "
                  f"{'keeping' if GH_KEEP_UNKNOWN_COUNTRY else 'dropping'} (GH_KEEP_UNKNOWN_COUNTRY={int(GH_KEEP_UNKNOWN_COUNTRY)})")
        return GH_KEEP_UNKNOWN_COUNTRY

    if GH_DEBUG:
        print("    [GH filter] inferred country/countries but none allowed; dropping")
    return False

def _collect_location_strings(job: dict) -> list[str]:
    """
    Gather possible location strings from Greenhouse job payload.
    Prefers job['location']['name'], but also checks 'offices' list etc.
    """
    out: list[str] = []
    # primary location
    loc = job.get("location")
    if isinstance(loc, dict):
        name = (loc.get("name") or "").strip()
        if name:
            out.append(name)
    elif isinstance(loc, str) and loc.strip():
        out.append(loc.strip())

    # offices (can appear for companies with multiple sites)
    offices = job.get("offices") or []
    for o in offices:
        name = ""
        if isinstance(o, dict):
            name = (o.get("name") or "") or (o.get("location", {}) or {}).get("name", "")
        name = (name or "").strip()
        if name:
            out.append(name)

    # Remove duplicates, keep order
    seen = set()
    uniq = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq or ["N/A"]

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
    if not GH_INCLUDE_INTERNS and re.search(r"\b(intern|internship|co[-\s]?op)\b", t):
        return False
    if SENIOR_RE.search(t):
        return False
    return any(k in t for k in NEW_GRAD_HINTS)

# =======================
# Posted date helpers
# =======================

def _parse_iso_date(s: str | None) -> str:
    """
    Convert an ISO-like timestamp to YYYY-MM-DD. On failure, return a best-effort date string.
    """
    if not s:
        return ""
    try:
        # normalize Z to +00:00 so fromisoformat works
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        if "T" in s:
            return s.split("T", 1)[0]
        return s

def _posted_date_from_job(job: dict) -> str:
    """
    Prefer 'updated_at' (Greenhouse updates this on posting/edits),
    fall back to 'created_at' if present.
    """
    return _parse_iso_date(job.get("updated_at") or job.get("created_at"))

# =======================
# Fetch + main scrape
# =======================

def _fetch_jobs(board_token: str) -> list[dict]:
    """
    Try the canonical boards API first, then legacy API as a fallback.
    """
    last_err = None
    for tmpl in GH_URL_TEMPLATES:
        url = tmpl.format(token=board_token)
        try:
            r = requests.get(url, headers=_HDRS, timeout=30)
            r.raise_for_status()
            data = r.json() or {}
            return data.get("jobs", []) or []
        except Exception as e:
            last_err = e
            continue
    # If both failed, raise the last error so the caller logs it.
    raise last_err or RuntimeError("Unknown error contacting Greenhouse")

def scrape_greenhouse_jobs(company_name, board_token, keyword_filters):
    """
    Scrapes job listings for a single company using the Greenhouse API and
    enriches the data with LinkedIn search links. Filters to US/Canada and,
    optionally, to new-grad friendly roles only (GH_NEWGRAD_ONLY). Adds a 'Posted' date.

    Env:
      - GH_ALLOWED_COUNTRIES      default "US,CA" (set to "ALL" to allow all)
      - GH_KEEP_UNKNOWN_COUNTRY   default 1  (keep "Remote"/unknown locations)
      - GH_NEWGRAD_ONLY           default 0  (opt-in to new-grad gating)
      - GH_INCLUDE_INTERNS        default 0  (set 1 to include internships)
      - GH_DEBUG                  default 0
    """
    print(f"Scraping {company_name} (Greenhouse)...")
    try:
        jobs = _fetch_jobs(board_token)
    except Exception as e:
        print(f"  > Could not fetch jobs for {company_name}. Error: {e}")
        return []

    filtered_jobs = []
    kw = [k.lower() for k in (keyword_filters or [])]

    for job in jobs:
        title = job.get("title", "") or ""
        t_lower = title.lower()

        # 1) baseline title filter (your KEYWORD_FILTERS from main.py)
        if kw and not any(k in t_lower for k in kw):
            continue

        # 2) optional new-grad gating
        if GH_NEWGRAD_ONLY and not _is_new_grad_friendly(title):
            continue

        # 3) location gating (US/CA by default, robust inference)
        location_candidates = _collect_location_strings(job)
        if not _locations_allowed(location_candidates):
            if GH_DEBUG:
                print(f"    [GH drop] {title} :: locations={location_candidates}")
            continue

        # Build output
        display_location = ", ".join(location_candidates) if location_candidates else "N/A"
        links = generate_linkedin_links(company_name, title)
        posted_date = _posted_date_from_job(job)

        row = {
            "Company": company_name,
            "Title": title,
            "URL": job.get("absolute_url"),
            "Location": display_location,
            "Posted": posted_date,
            **links,
        }
        filtered_jobs.append(row)

    print(f"  > Found {len(filtered_jobs)} relevant jobs.")
    return filtered_jobs