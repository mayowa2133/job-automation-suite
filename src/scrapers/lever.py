import requests
from bs4 import BeautifulSoup
from src.utils import generate_linkedin_links

def scrape_lever_jobs(company_name, company_token, keyword_filters):
    """
    Scrapes job listings for a single company by parsing the HTML of
    their public jobs.lever.co page.
    """
    print(f"Scraping {company_name} (Lever - HTML)...")
    url = f"https://jobs.lever.co/{company_token}"

    # --- NEW: Define browser-like headers ---
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # --- NEW: Pass the headers with the request ---
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  > Could not fetch jobs page for {company_name}. Error: {e}")
        return []

    # Create a BeautifulSoup object to parse the HTML
    soup = BeautifulSoup(response.content, 'lxml')
    
    # ... (the rest of the function remains exactly the same) ...
    postings = soup.find_all('div', class_='posting')

    if not postings:
        print(f"  > No job postings found on the page for {company_name}.")
        return []

    filtered_jobs = []
    for job in postings:
        title_tag = job.find('h5')
        location_tag = job.find('span', class_='sort-by-location')
        link_tag = job.find('a', class_='posting-btn-submit')

        if not all([title_tag, location_tag, link_tag]):
            continue

        job_title = title_tag.text
        title_lower = job_title.lower()

        if any(keyword in title_lower for keyword in keyword_filters):
            job_url = link_tag['href']
            job_location = location_tag.text

            linkedin_links = generate_linkedin_links(company_name, job_title)

            job_data = {
                'Company': company_name,
                'Title': job_title,
                'URL': job_url,
                'Location': job_location,
                'Alumni_Search_URL': linkedin_links['Alumni_Search_URL'],
                'Role_Search_URL': linkedin_links['Role_Search_URL']
            }
            filtered_jobs.append(job_data)
            
    print(f"  > Found {len(filtered_jobs)} relevant jobs.")
    return filtered_jobs