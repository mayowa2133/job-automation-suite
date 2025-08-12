from playwright.sync_api import sync_playwright, TimeoutError
from src.utils import generate_linkedin_links
import time

def scrape_shopify_jobs(keyword_filters):
    """
    Scrapes Shopify by using standard Playwright and scrolling down to
    trigger the "lazy loading" of all job posts.
    """
    print("Scraping Shopify (Playwright with scrolling)...")
    all_found_jobs = []
    
    url = "https://www.shopify.com/careers/search"

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        try:
            print("  > Navigating to Shopify careers...")
            page.goto(url, timeout=60000)

            # --- SCROLL TO LOAD ALL JOBS ---
            print("  > Scrolling down to load all jobs...")
            last_height = page.evaluate("document.body.scrollHeight")
            while True:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2) # Wait for content to load
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            print("  > Scrolling complete. Parsing results...")
            job_list_selector = "a.job-posting-item"
            job_elements = page.query_selector_all(job_list_selector)
            print(f"  > Found {len(job_elements)} total jobs on page, now filtering...")

            for element in job_elements:
                title_element = element.query_selector('h4')
                location_element = element.query_selector('span.job-posting-item__location')
                
                if not title_element: continue

                job_title = title_element.inner_text()
                title_lower = job_title.lower()

                if any(keyword in title_lower for keyword in keyword_filters):
                    job_url = element.get_attribute('href')
                    if job_url and not job_url.startswith('http'):
                        job_url = f"https://www.shopify.com{job_url}"
                    
                    job_location = location_element.inner_text() if location_element else "N/A"
                    linkedin_links = generate_linkedin_links("Shopify", job_title)
                    
                    job_data = {
                        'Company': "Shopify",
                        'Title': job_title,
                        'URL': job_url,
                        'Location': job_location
                    }
                    job_data.update(linkedin_links)
                    all_found_jobs.append(job_data)

        except Exception as e:
            print(f"  > An error occurred with Playwright while scraping Shopify: {e}")
        finally:
            browser.close()

    print(f"  > Found {len(all_found_jobs)} relevant jobs at Shopify.")
    return all_found_jobs