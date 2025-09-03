# src/selfheal.py
from __future__ import annotations

import json
import os
import re
import socket
from typing import Optional, Tuple
from urllib.parse import quote

import requests

# -----------------------------
# Tunables / headers
# -----------------------------
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
HDRS = {
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}

SELFHEAL_DEBUG = os.getenv("SELFHEAL_DEBUG", "0") == "1"
SELFHEAL_TRY_LEVER = os.getenv("SELFHEAL_TRY_LEVER", "1") == "1"

# Global default timeout for simple calls
TIMEOUT = float(os.getenv("SELFHEAL_TIMEOUT", "8"))

# Fast/JS-only Ashby handling
# If a jobs.ashbyhq.com page returns 200 OK but we can't see __NEXT_DATA__,
# treat it as an existing Ashby board with unknown job count (-1).
ASHBY_TREAT_200_AS_EXIST = os.getenv("SELFHEAL_ASHBY_200_OK", "1") == "1"

# Skip the heavy posting-api probe unless explicitly enabled.
USE_ASHBY_POSTING_API = os.getenv("SELFHEAL_USE_POSTING_API", "0") == "1"

# Tight timeouts for heavy calls (connect, read)
POSTING_API_CONNECT_TO = float(os.getenv("SELFHEAL_POSTING_API_CONNECT_TO", "4"))
POSTING_API_READ_TO = float(os.getenv("SELFHEAL_POSTING_API_READ_TO", "6"))

def _dbg(msg: str) -> None:
    if SELFHEAL_DEBUG:
        print(f"[SelfHeal] {msg}")

# -----------------------------
# Slug helpers
# -----------------------------

def _norm_variants_from_text(s: str) -> list[str]:
    s = s.strip()
    low = s.lower()
    dashy = re.sub(r"[^a-z0-9]+", "-", low).strip("-")
    nodash = dashy.replace("-", "")
    return [s, low, dashy, nodash]

def _slug_variants(name: str, initial_slug: str) -> list[str]:
    """
    Reasonable slug variants to try (order matters).
    Also tries the text inside parentheses as its own base (e.g., "Arc (JoinArc)").
    """
    bases: list[str] = []
    for src in (initial_slug, name):
        if not src:
            continue
        src = str(src).strip()
        if not src:
            continue
        # original & normalized forms
        for v in _norm_variants_from_text(src):
            if v and v not in bases:
                bases.append(v)

        # try inside-paren alias as a separate base
        inner = re.findall(r"\(([^)]+)\)", src)
        for inner_text in inner:
            for v in _norm_variants_from_text(inner_text):
                if v and v not in bases:
                    bases.append(v)

    # de-dup while preserving order
    seen = set()
    out = []
    for v in bases:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out

# -----------------------------
# Greenhouse probe
# -----------------------------

def _check_greenhouse(slug: str) -> Tuple[bool, int]:
    """
    Return (exists, jobs_count). Consider GH 'exists' if status==200 even if 0 jobs.
    Try both canonical and legacy endpoints.
    """
    urls = [
        f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
        f"https://api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
    ]
    for u in urls:
        try:
            r = requests.get(u, headers=HDRS, timeout=TIMEOUT)
            if r.status_code == 200:
                try:
                    data = r.json()
                    jobs = len(data.get("jobs", []) if isinstance(data, dict) else [])
                except Exception:
                    jobs = 0
                _dbg(f"GH OK slug={slug} jobs={jobs} via {u}")
                return True, jobs
            else:
                _dbg(f"GH {r.status_code} slug={slug} via {u}")
        except Exception as e:
            _dbg(f"GH error slug={slug}: {e}")
    return False, 0

# -----------------------------
# Ashby probes
# -----------------------------

def _parse_next_data(html_text: str) -> tuple[bool, int]:
    """
    Extract whether a jobBoard exists and how many jobs it has from __NEXT_DATA__.
    Returns (has_job_board, jobs_count).
    """
    try:
        m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_text, re.S | re.I)
        if not m:
            return False, 0
        data = json.loads(m.group(1))
        job_board = (
            (((data or {}).get("props") or {}).get("pageProps") or {}).get("jobBoard")
            or (data.get("jobBoard") if isinstance(data, dict) else None)
        )
        if not job_board:
            return False, 0
        sections = job_board.get("sections") or []
        jobs = sum(len((s or {}).get("jobs", [])) for s in sections)
        return True, jobs
    except Exception:
        return False, 0

def _ashby_light_probe(slug: str) -> Tuple[bool, int]:
    """
    Very fast check against jobs.ashbyhq.com. If we find __NEXT_DATA__, return a count.
    If we only see a 200 OK shell (JS-only), optionally treat as exists with unknown jobs (-1).
    """
    path_slug = quote(slug, safe="")
    urls = [
        f"https://jobs.ashbyhq.com/{path_slug}",
        f"https://jobs.ashbyhq.com/{path_slug}/jobs",
    ]

    any_200 = False
    for u in urls:
        try:
            r = requests.get(u, headers=HDRS, timeout=TIMEOUT, allow_redirects=True)
            if r.status_code != 200:
                _dbg(f"Ashby light {r.status_code} slug={slug} via {u}")
                continue
            any_200 = True
            has_board, jobs = _parse_next_data(r.text)
            if has_board:
                _dbg(f"Ashby light OK slug={slug} jobs={jobs} via {u}")
                return True, jobs
        except Exception as e:
            _dbg(f"Ashby light error slug={slug} via {u}: {e}")

    if any_200 and ASHBY_TREAT_200_AS_EXIST:
        _dbg(f"Ashby light 200-only slug={slug} -> assuming exists (jobs unknown)")
        return True, -1  # -1 means "exists, unknown job count"
    return False, 0

def _check_ashby_posting_api(slug: str) -> Tuple[bool, int]:
    """
    Optional Ashby posting API probe (can be slow). Exists only if jobBoard key present.
    """
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
    try:
        r = requests.get(
            url,
            headers=HDRS,
            timeout=(POSTING_API_CONNECT_TO, POSTING_API_READ_TO),
        )
        if r.status_code != 200:
            _dbg(f"Ashby posting-api {r.status_code} slug={slug}")
            return False, 0
        data = r.json()
        jb = data.get("jobBoard") if isinstance(data, dict) else None
        if not jb:
            _dbg(f"Ashby posting-api missing jobBoard slug={slug}")
            return False, 0
        sections = jb.get("sections") or []
        jobs = sum(len((s or {}).get("jobs", [])) for s in sections)
        _dbg(f"Ashby posting-api OK slug={slug} jobs={jobs}")
        return True, jobs
    except Exception as e:
        _dbg(f"Ashby posting-api error slug={slug}: {e}")
        return False, 0

def _check_ashby_subdomain(slug: str) -> Tuple[bool, int]:
    """
    Rare: some orgs host at <slug>.ashbyhq.com. Treat 200 OK as exist, with NEXT_DATA parse if available.
    """
    if not re.fullmatch(r"[a-z0-9-]+", slug.lower()):
        return False, 0
    host = f"{slug.lower()}.ashbyhq.com"
    try:
        socket.gethostbyname(host)
    except socket.gaierror:
        _dbg(f"Ashby subdomain DNS miss slug={slug} host={host}")
        return False, 0

    url = f"https://{host}"
    try:
        r = requests.get(url, headers=HDRS, timeout=TIMEOUT)
        if r.status_code != 200:
            _dbg(f"Ashby subdomain {r.status_code} slug={slug} via {url}")
            return False, 0
        has_board, jobs = _parse_next_data(r.text)
        if has_board:
            _dbg(f"Ashby subdomain OK slug={slug} jobs={jobs}")
            return True, jobs
        if ASHBY_TREAT_200_AS_EXIST:
            _dbg(f"Ashby subdomain 200-only slug={slug} -> assuming exists")
            return True, -1
    except Exception as e:
        _dbg(f"Ashby subdomain error slug={slug} via {url}: {e}")
    return False, 0

# -----------------------------
# Lever probe
# -----------------------------

def _check_lever(slug: str) -> Tuple[bool, int]:
    """
    Simple Lever probe (public API). Returns (exists, jobs_count).
    Treat Lever as 'exists' only if jobs > 0 (to avoid noisy 0-job boards).
    """
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        r = requests.get(url, headers=HDRS, timeout=TIMEOUT)
        if r.status_code != 200:
            _dbg(f"Lever {r.status_code} slug={slug}")
            return False, 0
        data = r.json()
        if isinstance(data, list):
            jobs = len(data)
            if jobs > 0:
                _dbg(f"Lever OK slug={slug} jobs={jobs}")
                return True, jobs
            else:
                _dbg(f"Lever 200 slug={slug} jobs=0")
                return False, 0
        return False, 0
    except Exception as e:
        _dbg(f"Lever error slug={slug}: {e}")
        return False, 0

# -----------------------------
# Resolver
# -----------------------------

def resolve_company_source(company_name: str, slug: str) -> tuple[Optional[str], Optional[str]]:
    """
    Decide which ATS a company uses and return (provider, discovered_slug).
    Provider in {"greenhouse","ashby","lever"} or (None, None) if unknown.

    Selection rules (revised for JS-only Ashby pages):
      1) Probe GH and Ashby-light (path pages) with slug variants.
         - Ashby 'exists' if we see NEXT_DATA, OR (200 OK and ASHBY_TREAT_200_AS_EXIST=1).
         - If NEXT_DATA seen, we get job counts; otherwise jobs=-1 (unknown).
      2) If GH exists and jobs>0 -> use GH immediately.
      3) Else if Ashby exists and (jobs>0 or jobs==-1) -> use Ashby immediately.
      4) Else if GH exists (even with 0) -> use GH.
      5) Else optionally try Ashby posting-api (if enabled) and/or Lever.
    """
    for candidate in _slug_variants(company_name, slug):
        # Probe Greenhouse
        gh_exists, gh_jobs = _check_greenhouse(candidate)

        # Fast Ashby path pages
        ab_exists, ab_jobs = _ashby_light_probe(candidate)
        if not ab_exists:
            # Rare subdomain form
            ab_exists, ab_jobs = _check_ashby_subdomain(candidate)

        # Early wins (positive job counts)
        if gh_exists and gh_jobs > 0:
            print(f"[SelfHeal] {company_name}: Greenhouse OK (slug={candidate}, jobs={gh_jobs})")
            return "greenhouse", candidate

        if ab_exists and (ab_jobs > 0 or ab_jobs == -1):
            # If we at least know it's Ashby (even jobs unknown), prefer it now.
            jobs_txt = "unknown" if ab_jobs == -1 else str(ab_jobs)
            print(f"[SelfHeal] {company_name}: Ashby OK (slug={candidate}, jobs={jobs_txt})")
            return "ashby", candidate

        # If GH exists (0 jobs), prefer it next
        if gh_exists:
            print(f"[SelfHeal] {company_name}: Greenhouse OK (slug={candidate}, jobs={gh_jobs})")
            return "greenhouse", candidate

        # If still nothing firm and posting-api enabled, try it as a last Ashby check
        if USE_ASHBY_POSTING_API:
            api_exists, api_jobs = _check_ashby_posting_api(candidate)
            if api_exists:
                print(f"[SelfHeal] {company_name}: Ashby OK via posting-api (slug={candidate}, jobs={api_jobs})")
                return "ashby", candidate

        # Finally try Lever if enabled
        if SELFHEAL_TRY_LEVER:
            lv_ok, lv_jobs = _check_lever(candidate)
            if lv_ok:
                print(f"[SelfHeal] {company_name}: Lever OK (slug={candidate}, jobs={lv_jobs})")
                return "lever", candidate

    print(f"[SelfHeal] {company_name}: No valid ATS found for slug={slug}")
    return None, None

# -----------------------------
# CLI helper
# -----------------------------
if __name__ == "__main__":
    import sys
    if not sys.argv[1:]:
        print("Usage: python -m src.selfheal Name[:slug] ...")
        sys.exit(2)

    for arg in sys.argv[1:]:
        if ":" in arg:
            name, slug = arg.split(":", 1)
        else:
            name = slug = arg
        provider, discovered = resolve_company_source(name, slug)
        print(f"{name}\tprovider={provider}\tdiscovered_slug={discovered}")