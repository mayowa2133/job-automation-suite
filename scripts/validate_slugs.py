# scripts/validate_slugs.py
import argparse, json, os, sys
import requests
from urllib.parse import quote

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = float(os.getenv("SLUG_CHECK_TIMEOUT", "8"))

# ----------------------------
# Greenhouse
# ----------------------------
def check_greenhouse(slug: str, fast: bool):
    urls = [
        f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
        f"https://api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
    ]
    last_err = None
    for u in urls:
        try:
            r = requests.get(u, headers=HEADERS, timeout=TIMEOUT, stream=fast)
            if r.status_code == 200:
                if fast:
                    return True, None, "200"
                try:
                    data = r.json() or {}
                    jobs = len(data.get("jobs", []) if isinstance(data, dict) else [])
                except Exception as e:
                    return True, None, f"200 (json parse fail: {e})"
                return True, jobs, "200"
            last_err = f"{r.status_code}"
        except Exception as e:
            last_err = str(e)
    return False, None, last_err or "unknown"


# ----------------------------
# Ashby
# ----------------------------
def _parse_next_data(html_text: str) -> tuple[bool, int | None]:
    import re, json as _json
    m = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_text, re.S | re.I
    )
    if not m:
        return False, None
    try:
        data = _json.loads(m.group(1))
        job_board = (((data or {}).get("props") or {}).get("pageProps") or {}).get(
            "jobBoard"
        )
        if not job_board:
            return True, None  # treat as valid, unknown jobs
        sections = job_board.get("sections") or []
        jobs = sum(len((s or {}).get("jobs", [])) for s in sections)
        return True, jobs
    except Exception:
        return True, None  # page exists, parsing failed but slug is valid


def check_ashby(slug: str, fast: bool):
    # 1) Posting API
    api = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
    try:
        r = requests.get(api, headers=HEADERS, timeout=TIMEOUT, stream=fast)
        if r.status_code == 200:
            if fast:
                return True, None, "200"
            try:
                data = r.json() or {}
                jb = data.get("jobBoard")
                if jb:
                    sections = jb.get("sections") or []
                    jobs = sum(len((s or {}).get("jobs", [])) for s in sections)
                    return True, jobs, "200 (posting-api)"
            except Exception:
                pass  # fallback to HTML
    except Exception:
        pass

    # 2) HTML path fallback
    path_slug = quote(slug, safe="")
    for u in (
        f"https://jobs.ashbyhq.com/{path_slug}",
        f"https://jobs.ashbyhq.com/{path_slug}/jobs",
    ):
        try:
            r = requests.get(u, headers=HEADERS, timeout=TIMEOUT, stream=fast)
            if r.status_code == 200:
                if fast:
                    return True, None, "200 (html)"
                ok, jobs = _parse_next_data(r.text)
                if ok:
                    return True, jobs, "200 (html)"
        except Exception:
            pass

    return False, None, "no 200"


# ----------------------------
# Lever
# ----------------------------
def check_lever(slug: str, fast: bool):
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=fast)
        if r.status_code != 200:
            return False, None, str(r.status_code)
        if fast:
            return True, None, "200"
        try:
            data = r.json()
            jobs = len(data) if isinstance(data, list) else None
            return True, jobs, "200"
        except Exception as e:
            return True, None, f"200 (json parse fail: {e})"
    except Exception as e:
        return False, None, str(e)


# ----------------------------
# Config + Validation Runner
# ----------------------------
CHECKERS = {
    "greenhouse": check_greenhouse,
    "ashby": check_ashby,
    "lever": check_lever,
}


def load_config(path: str):
    with open(path, "r") as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser(description="Validate ATS slugs from companies.json")
    ap.add_argument("--config", default="config/companies.json")
    ap.add_argument(
        "--providers", default="greenhouse,ashby,lever",
        help="Comma list: greenhouse,ashby,lever"
    )
    ap.add_argument("--only", default="", help="Comma-separated company names to test")
    ap.add_argument("--fast", action="store_true", help="Skip JSON parsing for speed")
    args = ap.parse_args()

    cfg = load_config(args.config)
    providers = [p.strip().lower() for p in args.providers.split(",") if p.strip()]
    only = {s.strip().lower() for s in args.only.split(",") if s.strip()}

    rows = []
    for prov in providers:
        block = cfg.get(prov, {}) or {}
        for company, slug in block.items():
            if only and company.lower() not in only:
                continue
            fn = CHECKERS.get(prov)
            if not fn:
                continue
            ok, jobs, note = fn(slug, args.fast)
            rows.append(
                (prov, company, slug, "PASS" if ok else "FAIL", jobs if jobs is not None else "-", note)
            )

    if not rows:
        print("No companies matched your filters.")
        sys.exit(0)

    # Print table
    colw = [10, 28, 28, 6, 6, 20]
    header = (
        "Provider".ljust(colw[0]) +
        "Company".ljust(colw[1]) +
        "Slug".ljust(colw[2]) +
        "OK".ljust(colw[3]) +
        "Jobs".ljust(colw[4]) +
        "Note"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            str(r[0]).ljust(colw[0]) +
            str(r[1]).ljust(colw[1]) +
            str(r[2]).ljust(colw[2]) +
            str(r[3]).ljust(colw[3]) +
            str(r[4]).ljust(colw[4]) +
            str(r[5])
        )


if __name__ == "__main__":
    main()