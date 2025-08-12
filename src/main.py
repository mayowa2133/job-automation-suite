import json
import pandas as pd
from datetime import datetime
import os
import smtplib
from email.mime.text import MIMEText

from src.scrapers.greenhouse import scrape_greenhouse_jobs
from src.scrapers.custom.google import scrape_google_jobs
from src.scrapers.custom.shopify import scrape_shopify_jobs

# --- Configuration ---
CONFIG_PATH = 'config/companies.json'
OUTPUT_DIR = 'data/raw/'
STATE_FILE = 'data/seen_jobs.txt'
KEYWORD_FILTERS = [
    'new grad', 'university grad', 'entry-level', 'entry level', 'software engineer',
    'software engineer i', 'swe i', 'step', 'residency', 'early career',
    'futureforce', 'class of', 'extreme blue', 'aspire', 'launchpad',
    'rotational', 'graduate program', 'developer', 'engineer'
]

# ... (All helper functions like load_seen_jobs, send_email_notification remain the same) ...
def load_seen_jobs():
    try:
        with open(STATE_FILE, 'r') as f: return set(line.strip() for line in f)
    except FileNotFoundError: return set()
def save_seen_jobs(job_urls):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        for url in job_urls: f.write(f"{url}\n")
def send_email_notification(new_jobs_df):
    sender_email, sender_password = os.environ.get('SENDER_EMAIL'), os.environ.get('SENDER_PASSWORD')
    if not all([sender_email, sender_password]):
        print("  > Email credentials not set. Skipping email notification.")
        return
    subject = f"Found {len(new_jobs_df)} New Job Postings!"
    body = "Here are the new jobs found today:\n\n"
    for index, job in new_jobs_df.iterrows():
        body += f"- {job['Title']} at {job['Company']}\n  Location: {job['Location']}\n  Link: {job['URL']}\n\n"
    msg = MIMEText(body)
    msg['Subject'], msg['From'], msg['To'] = subject, sender_email, sender_email
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as s:
            s.login(sender_email, sender_password)
            s.sendmail(sender_email, sender_email, msg.as_string())
        print("  > Email notification sent successfully!")
    except Exception as e:
        print(f"  > Failed to send email. Error: {e}")


def run_scrapers():
    seen_job_urls = load_seen_jobs()
    print(f"Loaded {len(seen_job_urls)} previously seen jobs from state file.")

    try:
        with open(CONFIG_PATH, 'r') as f:
            companies_config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at '{CONFIG_PATH}'")
        return

    greenhouse_companies = companies_config.get('greenhouse', {})
    all_current_jobs = []

    # --- Run Generic Scrapers ---
    print("--- Starting Greenhouse Scrape ---")
    for company, token in greenhouse_companies.items():
        jobs = scrape_greenhouse_jobs(company, token, KEYWORD_FILTERS)
        all_current_jobs.extend(jobs)
    
    # --- Run Custom Scrapers ---
    print("\n--- Starting Custom Scrapes ---")
    # Call the working Google scraper
    google_jobs = scrape_google_jobs(KEYWORD_FILTERS)
    all_current_jobs.extend(google_jobs)
    
    # Call the Shopify scraper we are testing
    shopify_jobs = scrape_shopify_jobs(KEYWORD_FILTERS)
    all_current_jobs.extend(shopify_jobs)

    # ... (the rest of the script for de-duplication and saving is exactly the same) ...
    if not all_current_jobs:
        print("\nNo jobs found at all during this run.")
        save_seen_jobs(seen_job_urls)
        return
    new_jobs_found = []
    current_job_urls = set()
    for job in all_current_jobs:
        current_job_urls.add(job['URL'])
        if job['URL'] not in seen_job_urls:
            new_jobs_found.append(job)
    print(f"\nScraping complete. Found {len(new_jobs_found)} new jobs.")
    if new_jobs_found:
        new_jobs_df = pd.DataFrame(new_jobs_found)
        send_email_notification(new_jobs_df)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"{OUTPUT_DIR}NEW_jobs_report_{today_str}.xlsx"
        print(f"  > Saving report to multi-sheet Excel file: '{filename}'")
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            print("    - Writing 'Networking To-Do' sheet...")
            networking_df = new_jobs_df.drop_duplicates(subset=['Company']).copy()
            networking_df['Status'], networking_df['Notes'] = '', ''
            networking_df = networking_df.rename(columns={'Title': 'Relevant Job Example','Entry_Level_SE_Search': 'Entry-Level SE Search','General_Role_Search': 'General Role Search'})
            networking_df = networking_df[['Company', 'Relevant Job Example', 'Entry-Level SE Search', 'General Role Search', 'Status', 'Notes']]
            networking_df.to_excel(writer, sheet_name='Networking To-Do', index=False)
            for company_name, company_df in new_jobs_df.groupby('Company'):
                print(f"    - Writing sheet for {company_name}...")
                cols_to_drop = [col for col in ['Entry_Level_SE_Search', 'General_Role_Search'] if col in company_df.columns]
                company_df_cleaned = company_df.drop(columns=cols_to_drop)
                company_df_cleaned.to_excel(writer, sheet_name=company_name, index=False)
                worksheet = writer.sheets[company_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            worksheet = writer.sheets['Networking To-Do']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
                adjusted_width = (max_length + 2) * 1.2
                worksheet.column_dimensions[column_letter].width = adjusted_width
        print("  > Multi-sheet Excel file with Networking To-Do list saved successfully.")
    
    save_seen_jobs(current_job_urls)
    print(f"Updated state file with {len(current_job_urls)} current jobs for next run.")


if __name__ == "__main__":
    run_scrapers()