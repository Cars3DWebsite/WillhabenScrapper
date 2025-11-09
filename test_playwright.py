"""
Simple Playwright Test - Does it work or not?
"""

print("=" * 60)
print("TESTING PLAYWRIGHT")
print("=" * 60)

# Test 1: Import
print("\n[1/4] Importing Playwright...")
try:
    from playwright.sync_api import sync_playwright
    print("✅ Import successful")
except Exception as e:
    print(f"❌ Import failed: {e}")
    exit(1)

# Test 2: Start Playwright
print("\n[2/4] Starting Playwright...")
try:
    p = sync_playwright().start()
    print("✅ Playwright started")
except Exception as e:
    print(f"❌ Start failed: {e}")
    exit(1)

# Test 3: Launch Browser
print("\n[3/4] Launching Chromium...")
try:
    browser = p.chromium.launch(
        headless=True,
        args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
    )
    print("✅ Chromium launched successfully!")
except Exception as e:
    print(f"❌ Chromium launch failed: {e}")
    print("\n💡 This means system dependencies are missing.")
    print("   Make sure you're using the Dockerfile.")
    p.stop()
    exit(1)

# Test 4: Navigate to Willhaben
print("\n[4/4] Testing scraping...")
try:
    page = browser.new_page()
    print("   → Loading Willhaben...")
    page.goto("https://www.willhaben.at/iad/gebrauchtwagen/auto/gebrauchtwagenboerse?rows=30", timeout=15000)
    print(f"   → Page title: {page.title()[:60]}")
    
    page.wait_for_timeout(2000)
    links = page.query_selector_all('a[href*="/gebrauchtwagen/"]')
    print(f"   → Found {len(links)} car links")
    
    if len(links) > 0:
        print("✅ Scraping works perfectly!")
    else:
        print("⚠️  Page loaded but no cars found")
    
    page.close()
except Exception as e:
    print(f"❌ Scraping failed: {e}")
    browser.close()
    p.stop()
    exit(1)

browser.close()
p.stop()

print("\n" + "=" * 60)
print("🎉 ALL TESTS PASSED - PLAYWRIGHT WORKS!")
print("=" * 60)
print("\nYour scraper should work now.")
print("If the database is still empty, check:")
print("  1. FAST_SCRAPE_INTERVAL_SECONDS is set to 60")
print("  2. No 'skipped' warnings in logs")
print("  3. Look for 'Starting FAST scraping job' in logs")
