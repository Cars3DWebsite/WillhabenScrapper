"""
Playwright Diagnostic Test Script
Run this to verify Playwright and Chromium are working properly.
"""

import sys
import os

print("=" * 60)
print("PLAYWRIGHT DIAGNOSTIC TEST")
print("=" * 60)

# Test 1: Check if Playwright is installed
print("\n[1/5] Checking if Playwright package is installed...")
try:
    import playwright
    print(f"✅ Playwright package installed: {playwright.__version__}")
except ImportError as e:
    print(f"❌ Playwright not installed: {e}")
    sys.exit(1)

# Test 2: Check if sync_api is available
print("\n[2/5] Checking if Playwright sync API is available...")
try:
    from playwright.sync_api import sync_playwright
    print("✅ Playwright sync_api available")
except ImportError as e:
    print(f"❌ Playwright sync_api not available: {e}")
    sys.exit(1)

# Test 3: Try to start Playwright
print("\n[3/5] Attempting to start Playwright...")
try:
    playwright_instance = sync_playwright().start()
    print("✅ Playwright started successfully")
except Exception as e:
    print(f"❌ Failed to start Playwright: {e}")
    sys.exit(1)

# Test 4: Try to launch Chromium
print("\n[4/5] Attempting to launch Chromium browser...")
try:
    browser = playwright_instance.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage'
        ]
    )
    print("✅ Chromium launched successfully")
except Exception as e:
    print(f"❌ Failed to launch Chromium: {e}")
    print("\nThis usually means Chromium dependencies are missing.")
    print("Make sure you're using the Dockerfile that installs them.")
    playwright_instance.stop()
    sys.exit(1)

# Test 5: Try to navigate to a real page
print("\n[5/5] Testing navigation to Willhaben...")
try:
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        locale='de-AT'
    )
    page = context.new_page()
    
    print("   Navigating to Willhaben...")
    page.goto(
        "https://www.willhaben.at/iad/gebrauchtwagen/auto/gebrauchtwagenboerse?rows=30",
        wait_until="domcontentloaded",
        timeout=15000
    )
    
    title = page.title()
    print(f"✅ Successfully loaded page: {title[:50]}...")
    
    # Try to find car listings
    page.wait_for_timeout(2000)  # Wait 2 seconds for page to load
    car_links = page.query_selector_all('a[href*="/gebrauchtwagen/"]')
    print(f"✅ Found {len(car_links)} car links on the page")
    
    if len(car_links) > 0:
        print("✅ Playwright is working perfectly! The scraper should work.")
    else:
        print("⚠️  Playwright works but no car links found. Page structure may have changed.")
    
    page.close()
    context.close()
    
except Exception as e:
    print(f"❌ Navigation failed: {e}")
    browser.close()
    playwright_instance.stop()
    sys.exit(1)

# Cleanup
print("\n[Cleanup] Closing browser...")
browser.close()
playwright_instance.stop()

print("\n" + "=" * 60)
print("✅ ALL TESTS PASSED!")
print("=" * 60)
print("\nPlaywright is working correctly on this system.")
print("If your scraper still isn't working, the problem is likely:")
print("  - Scraping interval is too fast (set to 60 seconds)")
print("  - Jobs are being skipped")
print("  - Check app logs for actual scraper errors")
