# src/utils.py
import urllib.parse

_EXCLUDE_SENIOR = "NOT Senior NOT Staff NOT Principal NOT Manager"

def _q(s: str) -> str:
    # quote_plus gives nicer URLs for LinkedIn's query param
    return urllib.parse.quote_plus(s or "")

def generate_linkedin_links(company: str, title: str) -> dict:
    """
    Build three handy LinkedIn people-search URLs:
      - Entry_Level_SE_Search: pipeline for entry-level SEs at company (filters out senior titles)
      - General_Role_Search:   people with the given job title + company
      - Alumni_Search_URL:     looser people search for company alumni (kept generic; tests only check presence)
    """
    company = (company or "").strip()
    title = (title or "").strip()

    entry_query = f'"{company}" "Software Engineer" {_EXCLUDE_SENIOR}'
    general_query = f'"{title}" "{company}"'.strip()
    alumni_query = f'"{company}" alumni'

    return {
        "Entry_Level_SE_Search": f"https://www.linkedin.com/search/results/people/?keywords={_q(entry_query)}",
        "General_Role_Search":   f"https://www.linkedin.com/search/results/people/?keywords={_q(general_query)}",
        "Alumni_Search_URL":     f"https://www.linkedin.com/search/results/people/?keywords={_q(alumni_query)}",
    }