import requests
from src.utils import generate_linkedin_links

def scrape_google_jobs(keyword_filters):
    """
    Scrapes job listings directly from Google's hidden career API.
    """
    print("Scraping Google (API)...")
    all_found_jobs = []
    # The base API endpoint discovered from your working file
    api_url = "https://careers.google.com/api/v3/search/"
    
    # We can search for multiple query terms
    search_queries = ["university graduate", "early career"]

    with requests.Session() as session:
        # Use a session and a standard User-Agent
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })

        for query in search_queries:
            print(f"  > Searching for '{query}'...")
            page = 1
            while True:
                params = {
                    "q": query,
                    "page": page
                }
                try:
                    response = session.get(api_url, params=params)
                    response.raise_for_status()
                    data = response.json()
                except requests.exceptions.RequestException as e:
                    print(f"  > Request failed for query '{query}'. Error: {e}")
                    break

                jobs_batch = data.get("jobs", [])
                if not jobs_batch:
                    # No more jobs found for this query
                    break
                
                for job in jobs_batch:
                    job_title = job.get("title", "")
                    title_lower = job_title.lower()

                    if any(keyword in title_lower for keyword in keyword_filters):
                        # Join multiple locations into a single string
                        locations = [loc.get('display', '') for loc in job.get('locations', [])]
                        job_location = ", ".join(locations)
                        job_url = f"https://careers.google.com/jobs/results/{job.get('id')}/"

                        linkedin_links = generate_linkedin_links("Google", job_title)
                        
                        job_data = {
                            'Company': "Google",
                            'Title': job_title,
                            'URL': job_url,
                            'Location': job_location
                        }
                        job_data.update(linkedin_links)
                        all_found_jobs.append(job_data)
                
                page += 1
    
    # Remove duplicates that might appear from different search queries
    unique_jobs = [dict(t) for t in {tuple(d.items()) for d in all_found_jobs}]
    print(f"  > Found {len(unique_jobs)} relevant jobs at Google.")
    return unique_jobs