import requests
from src.utils import generate_linkedin_links

def scrape_greenhouse_jobs(company_name, board_token, keyword_filters):
    """
    Scrapes job listings for a single company using the Greenhouse API and
    enriches the data with LinkedIn search links.
    """
    print(f"Scraping {company_name} (Greenhouse)...")
    url = f"https://api.greenhouse.io/v1/boards/{board_token}/jobs?content=true"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  > Could not fetch jobs for {company_name}. Error: {e}")
        return []

    jobs = response.json().get('jobs', [])
    filtered_jobs = []

    for job in jobs:
        title_lower = job.get('title', '').lower()
        if any(keyword in title_lower for keyword in keyword_filters):
            job_title = job.get('title')

            linkedin_links = generate_linkedin_links(company_name, job_title)

            # --- UPDATED LOGIC ---
            # Start with the basic job data
            job_data = {
                'Company': company_name,
                'Title': job_title,
                'URL': job.get('absolute_url'),
                'Location': job.get('location', {}).get('name', 'N/A'),
            }
            # Dynamically add the generated links from the utils function
            job_data.update(linkedin_links)

            filtered_jobs.append(job_data)
            
    print(f"  > Found {len(filtered_jobs)} relevant jobs.")
    return filtered_jobs