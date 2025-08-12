# src/scrapers/custom/shopify.py
from playwright.sync_api import sync_playwright, TimeoutError
from src.utils import generate_linkedin_links
import time
import re
from contextlib import suppress

SHOPIFY_CAREERS_URL = "https://www.shopify.com/careers/search"

def _accept_cookies_if_present(page):
    with suppress(Exception):
        # Common cookie buttons
        page.get_by_role("button", name=re.compile("Accept|Agree|Allow|Got it", re.I)).click(timeout=3000)

def _scroll_to_load_all(page, pause_sec=1.2, max_loops=30):
    last_count = -1
    stable_rounds = 0
    for _ in range(max_loops):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(pause_sec)
        # Let any XHR finish
        with suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=3000)
        # Count likely cards after each scroll
        count = 0
        for sel in _CANDIDATE_CARD_SELECTORS:
            count = max(count, page.locator(sel).count())
        if count == last_count:
            stable_rounds += 1
            if stable_rounds >= 2:
                break
        else:
            stable_rounds = 0
            last_count = count

# Fallback selectors since the site is a client app and changes markup
_CANDIDATE_CARD_SELECTORS = [
    "a.job-posting-item",
    "a[href*='/careers/jobs/']",
    "[data-testid='job-card'] a[href]",
    "a[data-e2e='job-card']",
    "article a[href*='/careers/']",
]

_TITLE_WITHIN_CARD = "h3, h4, [data-testid='job-title'], .job-title"
_LOCATION_WITHIN_CARD = "span[class*='location'], [data-testid='job-location'], .job-posting-item__location, .job-location"

def _extract_job_from_element(el, base_url):
    title = None
    url = None
    location = "N/A"

    with suppress(Exception):
        url = el.get_attribute("href")
    if url and not url.startswith("http"):
        url = base_url.rstrip("/") + url

    with suppress(Exception):
        t_el = el.locator(_TITLE_WITHIN_CARD).first
        if t_el and t_el.count() > 0:
            title = t_el.inner_text().strip()

    if not title:
        with suppress(Exception):
            title = el.inner_text().strip().split("\n")[0]

    with suppress(Exception):
        l_el = el.locator(_LOCATION_WITHIN_CARD).first
        if l_el and l_el.count() > 0:
            location = l_el.inner_text().strip()

    return title, url, location

def scrape_shopify_jobs(keyword_filters):
    """
    Scrapes Shopify careers using Playwright with robust waits and selectors.
    Returns a list of dicts with the standard job schema.
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
        # Speed up page load a bit by skipping heavy assets
        context.route("**/*", lambda route: route.abort() if route.request.resource_type in {"image", "font", "media"} else route.continue_())
        page = context.new_page()

        try:
            print("  > Navigating to Shopify careers")
            page.goto(SHOPIFY_CAREERS_URL, timeout=60000)
            _accept_cookies_if_present(page)

            # Wait for any initial search results to render
            with suppress(TimeoutError):
                # Try several likely anchors to ensure the app hydrated
                page.wait_for_selector(",".join(_CANDIDATE_CARD_SELECTORS), timeout=10000)

            print("  > Scrolling to load results")
            _scroll_to_load_all(page)

            # Collect unique anchors that look like job cards
            seen = set()
            elements = []
            for sel in _CANDIDATE_CARD_SELECTORS:
                elements.extend(page.locator(sel).all())

            print(f"  > Found {len(elements)} raw elements. Parsing and filtering")
            for el in elements:
                title, url, location = _extract_job_from_element(el, "https://www.shopify.com")
                if not title or not url:
                    continue
                key = (title, url)
                if key in seen:
                    continue
                seen.add(key)

                title_lower = title.lower()
                if not any(k in title_lower for k in keyword_filters):
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
