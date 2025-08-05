try:
    import playwright_stealth.stealth
    print("--- Successfully imported playwright_stealth.stealth ---")
    
    print("\n--- Available names in the module: ---")
    # The dir() function lists all available functions/attributes in a module
    available_names = dir(playwright_stealth.stealth)
    for name in available_names:
        print(name)
    print("--------------------------------------")

except ImportError as e:
    print(f"Failed to import the module. Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")