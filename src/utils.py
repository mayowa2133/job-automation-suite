import urllib.parse

def generate_linkedin_links(company, title):
    """
    Generates pre-made LinkedIn search URLs, including a targeted search for
    entry-level engineers and a general role search.
    """
    base_url = "https://www.linkedin.com/search/results/people/?keywords="

    # --- NEW: Targeted Entry-Level Search ---
    # We search for "Software Engineer" at the company but exclude senior titles.
    entry_level_keywords = f'"{company}" "Software Engineer" NOT Senior NOT Staff NOT Principal NOT Manager'
    entry_level_url = base_url + urllib.parse.quote_plus(entry_level_keywords)

    # General role search (same as before)
    role_keywords = f'"{title}" "{company}"' # Added quotes for more exact matching
    role_url = base_url + urllib.parse.quote_plus(role_keywords)

    # Return a dictionary with new, more descriptive keys
    return {
        'Entry_Level_SE_Search': entry_level_url,
        'General_Role_Search': role_url
    }