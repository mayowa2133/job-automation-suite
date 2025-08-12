# src/scrapers/custom/shopify.py
from playwright.sync_api import sync_playwright, TimeoutError
from src.utils import generate_linkedin_links
from contextlib import suppress
import time
import re
import json

SHOPIFY_CAREERS_URL = "https://www.shopify.com/careers/search"
JOB_PATH_FRAGMENT = "/careers/jobs/"

def _accept_cookies_if_present(page):
    with suppress(Exception):
        page.get_by_role("button", name=re.compile("Accept|Agree|Allow|Got it|OK", re.I)).click(timeout=3000)

def _scroll_until_stable(page, pause_sec=1.0, max_loops=40):
    last_height = -1
    stable = 0
    for _ in range(max_loops):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(pause_sec)
        with suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=3000)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            stable += 1
            if stable >= 2:
                break
        else:
            stable = 0
            last_height = new_height
        # Click load more type buttons if present
        with suppress(Exception):
            btn = page.get_by_role("button", name=re.compile("Load more|Show more|See more", re.I))
            if btn.is_visible():
                btn.click()
                time.sleep(0.8)

def _find_job_links_via_dom(page):
    # Scan all anchors and keep those that point at a job detail page
    anchors = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({href: a.getAttribute('href') || '', abs: a.href || '', text: a.innerText || ''}))",
    )
    links = []
    seen = set()
    for a in anchors:
        href = a.get("href") or ""
        abs_url = a.get("abs") or ""
        text = (a.get("text") or "").strip()
        target = ""
        if JOB_PATH_FRAGMENT in href:
            target = href
        elif JOB_PATH_FRAGMENT in abs_url:
            target = abs_url
        if not target:
            continue
        if not target.startswith("http"):
            target = "https://www.shopify.com" + target
        key = (target, text)
        if key in seen:
            continue
        seen.add(key)
        links.append({"url": target, "title": text})
    return links

def _collect_jobs_from_json_like(obj):
    """Heuristic JSON walker. Yields dicts with title url location when found."""
    results = []

    def looks_like_job(d):
        if not isinstance(d, dict):
            return False
        keys = {k.lower() for k in d.keys()}
        has_title = any(k in keys for k in ["title", "jobtitle", "name"])
        has_url = any(k in keys for k in ["url", "applyurl", "canonicalurl", "href", "path", "slug"])
        return has_title and has_url

    def get_val(d, keys, default=""):
        for k in keys:
            if k in d and isinstance(d[k], str) and d[k].strip():
                return d[k].strip()
            # case insensitive
            for dk in d.keys():
                if dk.lower() == k:
                    v = d[dk]
                    if isinstance(v, str) and v.strip():
                        return v.strip()
        return default

    def walk(x):
        if isinstance(x, dict):
            if looks_like_job(x):
                title = get_val(x, ["title", "jobTitle", "name"])
                url = get_val(x, ["url", "applyUrl", "canonicalUrl", "href", "path", "slug"])
                if url and not url.startswith("http"):
                    url = "https://www.shopify.com" + (url if url.startswith("/") else "/" + url)
                loc = get_val(x, ["location", "jobLocation", "city"])
                results.append({"title": title, "url": url, "location": loc or "N/A"})
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(obj)
    return results

def scrape_shopify_jobs(keyword_filters):
    """
    Scrapes Shopify careers using Playwright.
    Strategy
      1 Load page and accept cookies
      2 Scroll until content stabilizes and click any load more buttons
      3 Try DOM harvesting of anchors that point to job detail pages
      4 In parallel listen for XHR or fetch responses and parse JSON if a feed appears
    """
    print("Scraping Shopify with Playwright")
    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36")
        )
        # Only skip heavy media to keep scripts working
        def route_handler(route):
            rt = route.request.resource_type
            if rt in {"image", "media"}:
                return route.abort()
            return route.continue_()
        context.route("**/*", route_handler)

        page = context.new_page()

        # Response listener to catch JSON feeds
        captured = []
        def on_response(resp):
            try:
                url = resp.url
                if "careers" not in url and "jobs" not in url and "search" not in url:
                    return
                ct = resp.headers.get("content-type", "")
                if "application/json" not in ct:
                    return
                data = resp.json()
                found = _collect_jobs_from_json_like(data)
                if found:
                    captured.extend(found)
            except Exception:
                pass

        page.on("response", on_response)

        try:
            print("  > Navigating to Shopify careers")
            page.goto(SHOPIFY_CAREERS_URL, timeout=60000)
            _accept_cookies_if_present(page)

            with suppress(TimeoutError):
                page.wait_for_load_state("networkidle", timeout=10000)

            print("  > Scrolling to load results")
            _scroll_until_stable(page)

            # First try JSON captured from network
            network_jobs = []
            for item in captured:
                title = item.get("title", "").strip()
                url = item.get("url", "").strip()
                location = item.get("location", "").strip() or "N/A"
                if not title or not url:
                    continue
                network_jobs.append({"title": title, "url": url, "location": location})

            # Then try DOM if network yielded nothing
            dom_jobs = []
            if not network_jobs:
                dom_links = _find_job_links_via_dom(page)
                # try to enrich with nearby location text
                for link in dom_links:
                    url = link["url"]
                    title_text = link["title"]
                    title = title_text.strip().split("\n")[0] if title_text else ""
                    # try to grab a nearby location node
                    location = "N/A"
                    with suppress(Exception):
                        # Locate the anchor again and search around it
                        a = page.locator(f"a[href='{url.replace('https://www.shopify.com','')}'], a[href='{url}']").first
                        if a and a.count() > 0:
                            card = a.locator("xpath=ancestor-or-self::*[self::article or self::a or self::div][1]")
                            loc_node = card.locator("css=span[class*='location'], [data-testid='job-location'], .job-posting-item__location").first
                            if loc_node and loc_node.count() > 0:
                                location = loc_node.inner_text().strip()
                    if title and url:
                        dom_jobs.append({"title": title, "url": url, "location": location})

            chosen = network_jobs if network_jobs else dom_jobs
            print(f"  > Parsed {len(chosen)} potential Shopify jobs before keyword filtering")

            seen = set()
            for item in chosen:
                title = item["title"]
                url = item["url"]
                location = item.get("location", "N/A")
                key = (title, url)
                if key in seen:
                    continue
                seen.add(key)

                if not any(k in title.lower() for k in keyword_filters):
                    continue

                linkedin_links = generate_linkedin_links("Shopify", title)
                job = {
                    "Company": "Shopify",
                    "Title": title,
                    "URL": url,
                    "Location": location,
                }
                job.update(linkedin_links)
                jobs.append(job)

        except Exception as e:
            print(f"  > Error while scraping Shopify: {e}")
        finally:
            context.close()
            browser.close()

    print(f"  > Collected {len(jobs)} Shopify jobs matching your keywords")
    return jobs
