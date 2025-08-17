import os
import re
import requests
from src.utils import generate_linkedin_links

# -----------------------
# US/CA location filtering helpers
# -----------------------

def _gh_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}

_GH_DEBUG = _gh_flag("GH_DEBUG", "0")

_raw_countries = (os.getenv("GH_ALLOWED_COUNTRIES", "US,CA") or "").strip()
if _raw_countries in {"*", "ALL", "all"}:
    GH_ALLOWED_COUNTRIES: set[str] = set()  # empty set => keep all
else:
    GH_ALLOWED_COUNTRIES: set[str] = {c.strip().upper() for c in _raw_countries.split(",") if c.strip()} or {"US", "CA"}

GH_KEEP_UNKNOWN_COUNTRY = _gh_flag("GH_KEEP_UNKNOWN_COUNTRY", "0")

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

_REMOTE_RE = re.compile(r"\b(remote|virtual|work\s*from\s*home|wfh)\b", re.I)
_US_TOKEN_RE = re.compile(r"\b(UNITED STATES|U\.?S\.?A?\.?)\b", re.I)
_CA_TOKEN_RE = re.compile(r"\bCANADA\b", re.I)

_CA_PROV_TOKEN_RE = re.compile(
    r"(?:^|,\s*)(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)(?:\s|,|$)", re.I
)
_US_STATE_TOKEN_RE = re.compile(
    r"(?:^|,\s*)(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(?:\s|,|$)"
)

def _infer_countries_from_location(text: str) -> set[str]:
    """
    Return set of country codes found in a free-form Greenhouse location string.
    Recognizes:
      - 'City, ST' (US), 'City, ON' (CA)
      - Full state/province names
      - 'Remote - US', 'Remote (Canada)', etc.
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
    m_us = _US_STATE_TOKEN_RE.search(up)
    if m_us and m_us.group(1) in _US_STATE_ABBR:
        countries.add("US")
    m_ca = _CA_PROV_TOKEN_RE.search(up)
    if m_ca and m_ca.group(1).upper() in _CA_PROV_ABBR:
        countries.add("CA")

    # full names
    for name in _US_STATE_NAMES:
        if re.search(rf"\b{name}\b", up):
            countries.add("US")
            break
    for name in _CA_PROV_NAMES:
        if re.search(rf"\b{name}\b", up):
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
                if _GH_DEBUG:
                    print(f"    [GH filter] allowed by '{cand}' -> {found & GH_ALLOWED_COUNTRIES}")
                return True

    if not inferred_any:
        if _GH_DEBUG:
            print("    [GH filter] no inference from any location; "
                  f"{'keeping' if GH_KEEP_UNKNOWN_COUNTRY else 'dropping'} (GH_KEEP_UNKNOWN_COUNTRY={int(GH_KEEP_UNKNOWN_COUNTRY)})")
        return GH_KEEP_UNKNOWN_COUNTRY

    if _GH_DEBUG:
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
        # typical structure: {'id':..., 'name': 'New York, NY', 'location': {'name': 'New York, NY', ...}}
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


# -----------------------
# Main scrape
# -----------------------

def scrape_greenhouse_jobs(company_name, board_token, keyword_filters):
    """
    Scrapes job listings for a single company using the Greenhouse API and
    enriches the data with LinkedIn search links, while filtering to US/CA.
    Env:
      - GH_ALLOWED_COUNTRIES (default "US,CA"; set "ALL" to disable)
      - GH_KEEP_UNKNOWN_COUNTRY (default 0)
      - GH_DEBUG (default 0)
    """
    print(f"Scraping {company_name} (Greenhouse)...")
    url = f"https://api.greenhouse.io/v1/boards/{board_token}/jobs?content=true"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  > Could not fetch jobs for {company_name}. Error: {e}")
        return []

    jobs = (response.json() or {}).get("jobs", []) or []
    filtered_jobs = []

    for job in jobs:
        title = job.get("title", "") or ""
        title_lower = title.lower()

        # keyword/title pre-filter
        if not any(keyword in title_lower for keyword in (keyword_filters or [])):
            continue

        # collect all candidate location strings
        location_candidates = _collect_location_strings(job)

        # US/CA location filter (robust inference)
        if not _locations_allowed(location_candidates):
            if _GH_DEBUG:
                print(f"    [GH drop] {title} :: locations={location_candidates}")
            continue

        # Choose a display location (join if multiple)
        display_location = ", ".join(location_candidates) if location_candidates else "N/A"

        linkedin_links = generate_linkedin_links(company_name, title)

        job_data = {
            "Company": company_name,
            "Title": title,
            "URL": job.get("absolute_url"),
            "Location": display_location,
        }
        job_data.update(linkedin_links)

        filtered_jobs.append(job_data)

    print(f"  > Found {len(filtered_jobs)} relevant jobs.")
    return filtered_jobs
