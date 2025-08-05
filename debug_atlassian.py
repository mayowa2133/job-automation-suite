from playwright.sync_api import sync_playwright

print("--- Starting Manual Debug Session ---")
print("A browser window will open. Please wait for the page to load.")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    try:
        page.goto("https://www.atlassian.com/company/careers/all-jobs", timeout=60000)
        print("\n--- PAGE LOADED ---")
        print("The script is now paused. The browser window is under your control.")
        print("\nINSTRUCTIONS:")
        print("1. In the browser window that popped up, use your mouse to CLICK the 'Engineering' checkbox.")
        print("2. OBSERVE if the job listings load on the page after your click.")
        
        # This command halts the script and opens the Playwright Inspector,
        # giving you full manual control of the browser window.
        page.pause()

    except Exception as e:
        print(f"The script failed even to load the page. Error: {e}")
    finally:
        print("Closing browser. Debug session over.")
        browser.close()