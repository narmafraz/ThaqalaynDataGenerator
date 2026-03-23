"""
Discover all volume view IDs for Bihar al-Anwar and Mir'at al-Uqul on books.rafed.net.

rafed.net has a catalog page per book that lists all volumes with links like /view/{id}.
This script navigates to each catalog page and extracts the view IDs.

Output is saved to:
  ThaqalaynDataSources/scraped/rafed_net/rafed_view_ids.json

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    python app/scrapers/discover_rafed_view_ids.py

    # Discover specific book only:
    python app/scrapers/discover_rafed_view_ids.py --bihar
    python app/scrapers/discover_rafed_view_ids.py --mirat

    # Try Word download for each discovered volume to verify availability:
    python app/scrapers/discover_rafed_view_ids.py --test-download
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error

sys.stdout.reconfigure(encoding="utf-8")

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SOURCE_DATA_DIR = os.environ.get("SOURCE_DATA_DIR", os.path.join(_PROJECT_ROOT, "..", "ThaqalaynDataSources"))
OUTPUT_DIR = os.path.join(_SOURCE_DATA_DIR, "scraped", "rafed_net")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "rafed_view_ids.json")

BRAVE_PATH = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
BASE_URL = "https://books.rafed.net"
DOWNLOAD_API = "https://books.rafed.net/api/download"

# Known catalog pages on rafed.net (older URL format)
# These list all volumes for a given book
BOOKS_TO_DISCOVER = {
    "bihar-al-anwar": {
        "title": "Bihar al-Anwar",
        "title_ar": "\u0628\u062d\u0627\u0631 \u0627\u0644\u0623\u0646\u0648\u0627\u0631",
        "author": "Allamah Majlisi",
        "expected_volumes": 110,
        # Known view IDs from web search (for validation)
        "known_ids": {2: 501, 8: 526, 52: 1009, 59: 1052, 90: 1167, 92: 1187},
        # Catalog page candidates to try
        "catalog_urls": [
            "/view.php?type=c_fbook&b_id=966",
            "/view.php?type=c_fbook&b_id=514",
        ],
    },
    "mirat-al-uqul": {
        "title": "Mir'at al-Uqul",
        "title_ar": "\u0645\u0631\u0622\u0629 \u0627\u0644\u0639\u0642\u0648\u0644",
        "author": "Allamah Majlisi",
        "expected_volumes": 26,
        "known_ids": {1: 944, 4: 1013, 20: 1073, 25: 1101},
        "catalog_urls": [
            "/view.php?type=c_fbook&b_id=944",
        ],
    },
}

DELAY_BETWEEN_REQUESTS = 2.0


def get_browser():
    """Launch a Playwright Chromium browser, preferring Brave if available."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright is not installed.")
        print("Install with: pip install playwright && playwright install chromium")
        sys.exit(1)

    pw = sync_playwright().start()
    launch_args = {"headless": True}
    if os.path.exists(BRAVE_PATH):
        launch_args["executable_path"] = BRAVE_PATH
    browser = pw.chromium.launch(**launch_args)
    page = browser.new_page()
    return pw, browser, page


def discover_from_catalog(browser_page, catalog_url):
    """Navigate to a catalog page and extract volume view IDs.

    Returns a list of dicts: [{"vol": N, "view_id": M, "title": "..."}, ...]
    """
    url = "{}{}".format(BASE_URL, catalog_url)
    print("  Navigating to: {}".format(url))

    try:
        browser_page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception as e:
        print("  ERROR navigating to {}: {}".format(url, e))
        return []

    browser_page.wait_for_timeout(3000)

    # Extract all links that point to /view/{id} patterns
    volumes = browser_page.evaluate("""() => {
        const results = [];
        const links = document.querySelectorAll('a[href*="/view/"]');
        for (const link of links) {
            const href = link.getAttribute('href') || '';
            const match = href.match(/\\/view\\/(\\d+)/);
            if (match) {
                const viewId = parseInt(match[1], 10);
                const text = link.textContent.trim();
                results.push({view_id: viewId, title: text, href: href});
            }
        }
        // Also check for older URL patterns with b_id
        const oldLinks = document.querySelectorAll('a[href*="b_id="]');
        for (const link of oldLinks) {
            const href = link.getAttribute('href') || '';
            const text = link.textContent.trim();
            // Only include if not already captured via /view/ pattern
            if (!href.includes('/view/')) {
                results.push({view_id: null, title: text, href: href});
            }
        }
        return results;
    }""")

    return volumes


def discover_from_known_id(browser_page, view_id):
    """Navigate to a known volume page and find links to other volumes of same book.

    Many book pages have a volume selector/sidebar listing all volumes.
    """
    url = "{}/view/{}".format(BASE_URL, view_id)
    print("  Navigating to known volume: {}".format(url))

    try:
        browser_page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception as e:
        print("  ERROR navigating to {}: {}".format(url, e))
        return []

    browser_page.wait_for_timeout(3000)

    # Look for volume selector links in sidebar/navigation
    volumes = browser_page.evaluate("""() => {
        const results = [];
        // Get all links on the page that point to /view/{id}
        const links = document.querySelectorAll('a[href*="/view/"]');
        for (const link of links) {
            const href = link.getAttribute('href') || '';
            const match = href.match(/\\/view\\/(\\d+)/);
            if (match) {
                const viewId = parseInt(match[1], 10);
                const text = link.textContent.trim();
                // Filter: likely volume links contain ج (juz/volume) or numbers
                if (text.includes('ج') || /\\d/.test(text) || text.includes('المجلد')) {
                    results.push({view_id: viewId, title: text, href: href});
                }
            }
        }
        return results;
    }""")

    return volumes


def extract_vol_number(title):
    """Try to extract a volume number from a title string.

    Handles Arabic and Western numerals, patterns like:
    - ج١  ج ١  ج1  [ج1]  المجلد ١
    - بحار الأنوار[ج2]
    """
    # Arabic numeral mapping
    ar_digits = {"٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4",
                 "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9"}

    def arabic_to_int(s):
        western = ""
        for ch in s:
            western += ar_digits.get(ch, ch)
        try:
            return int(western)
        except ValueError:
            return None

    # Try patterns
    patterns = [
        r'ج\s*(\d+)',           # ج2, ج 2
        r'ج\s*([٠-٩]+)',       # ج٢, ج ٢
        r'\[ج(\d+)\]',          # [ج2]
        r'\[ج([٠-٩]+)\]',      # [ج٢]
        r'المجلد\s*(\d+)',      # المجلد 2
        r'المجلد\s*([٠-٩]+)',  # المجلد ٢
        r'vol[.\s]*(\d+)',       # vol. 2, vol 2
    ]

    for pattern in patterns:
        m = re.search(pattern, title, re.IGNORECASE)
        if m:
            num_str = m.group(1)
            # Check if Arabic numerals
            if any(ch in ar_digits for ch in num_str):
                return arabic_to_int(num_str)
            try:
                return int(num_str)
            except ValueError:
                pass

    return None


def test_word_download(view_id):
    """Test if a Word download is available for a given view_id.

    Makes a HEAD request to avoid downloading the full file.
    Returns True if download is available, False otherwise.
    """
    url = "{}/{}/doc".format(DOWNLOAD_API, view_id)
    try:
        req = urllib.request.Request(url, method="HEAD")
        req.add_header("User-Agent", "ThaqalaynDataGenerator/1.0")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200
    except urllib.error.HTTPError:
        return False
    except Exception:
        return False


def discover_book(browser_page, book_key, book_info):
    """Discover all volume view IDs for a book using multiple strategies."""
    print("\n=== Discovering {} ({}) ===".format(book_info["title"], book_info["title_ar"]))

    all_found = {}  # vol_num -> {"view_id": N, "title": "..."}

    # Strategy 1: Try catalog pages
    for catalog_url in book_info.get("catalog_urls", []):
        print("\n  Strategy 1: Catalog page {}".format(catalog_url))
        results = discover_from_catalog(browser_page, catalog_url)
        print("  Found {} links".format(len(results)))

        for r in results:
            vol = extract_vol_number(r.get("title", ""))
            if vol is not None and r.get("view_id"):
                if vol not in all_found:
                    all_found[vol] = {"view_id": r["view_id"], "title": r["title"]}
                    print("    Vol {}: view_id={} ({})".format(vol, r["view_id"], r["title"][:60]))

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Strategy 2: Navigate to a known volume and find sibling links
    known_ids = book_info.get("known_ids", {})
    if known_ids:
        # Pick the first known ID
        first_vol = min(known_ids.keys())
        first_id = known_ids[first_vol]
        print("\n  Strategy 2: Discover from known volume {} (view_id={})".format(
            first_vol, first_id))
        results = discover_from_known_id(browser_page, first_id)
        print("  Found {} volume-like links".format(len(results)))

        for r in results:
            vol = extract_vol_number(r.get("title", ""))
            if vol is not None and r.get("view_id"):
                if vol not in all_found:
                    all_found[vol] = {"view_id": r["view_id"], "title": r["title"]}
                    print("    Vol {}: view_id={} ({})".format(vol, r["view_id"], r["title"][:60]))

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Strategy 3: Try sequential view IDs around known ones to fill gaps
    if known_ids and len(all_found) < book_info.get("expected_volumes", 999):
        print("\n  Strategy 3: Probe sequential IDs around known volumes")

        # Find the range of known IDs
        known_view_ids = sorted(known_ids.values())
        min_id = min(known_view_ids) - 5
        max_id = max(known_view_ids) + 5

        # Build set of already-found view IDs
        found_view_ids = {v["view_id"] for v in all_found.values()}

        # Probe IDs in the range that we haven't found yet
        probed = 0
        for probe_id in range(min_id, max_id + 1):
            if probe_id in found_view_ids:
                continue
            if probed > 20:  # Don't probe too many
                break

            # Quick check: try to load the page title
            try:
                url = "{}/view/{}".format(BASE_URL, probe_id)
                browser_page.goto(url, wait_until="networkidle", timeout=15000)
                browser_page.wait_for_timeout(1500)
                title = browser_page.title()
                if book_info["title_ar"] in (title or ""):
                    vol = extract_vol_number(title)
                    if vol and vol not in all_found:
                        all_found[vol] = {"view_id": probe_id, "title": title}
                        print("    Probed Vol {}: view_id={} ({})".format(
                            vol, probe_id, title[:60]))
                probed += 1
            except Exception:
                probed += 1

            time.sleep(1)

    # Add known IDs that weren't discovered
    for vol, vid in known_ids.items():
        if vol not in all_found:
            all_found[vol] = {"view_id": vid, "title": "(from known IDs)"}
            print("  Added known Vol {}: view_id={}".format(vol, vid))

    # Sort and format
    sorted_vols = []
    for vol in sorted(all_found.keys()):
        entry = all_found[vol]
        sorted_vols.append({
            "vol": vol,
            "view_id": entry["view_id"],
            "title": entry["title"],
        })

    expected = book_info.get("expected_volumes", "?")
    print("\n  Summary: Found {}/{} volumes".format(len(sorted_vols), expected))

    # Identify gaps
    if sorted_vols:
        found_nums = {v["vol"] for v in sorted_vols}
        max_vol = max(found_nums)
        missing = [v for v in range(1, max_vol + 1) if v not in found_nums]
        if missing:
            print("  Missing volumes: {}".format(missing))

    return sorted_vols


def main():
    book_filter = None
    test_download = "--test-download" in sys.argv

    if "--bihar" in sys.argv:
        book_filter = "bihar-al-anwar"
    elif "--mirat" in sys.argv:
        book_filter = "mirat-al-uqul"

    print("=" * 60)
    print("Rafed.net View ID Discovery")
    print("Output: {}".format(OUTPUT_FILE))
    if test_download:
        print("Mode: Discovery + Word download test")
    print("=" * 60)

    # Load existing results if any
    existing = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            existing = json.load(f)
        print("Loaded existing results from {}".format(OUTPUT_FILE))

    pw, browser, browser_page = get_browser()

    try:
        for book_key, book_info in BOOKS_TO_DISCOVER.items():
            if book_filter and book_key != book_filter:
                continue

            volumes = discover_book(browser_page, book_key, book_info)

            # Test Word downloads if requested
            if test_download and volumes:
                print("\n  Testing Word downloads...")
                available = 0
                unavailable = 0
                for v in volumes[:5]:  # Test first 5 only
                    ok = test_word_download(v["view_id"])
                    status = "OK" if ok else "UNAVAILABLE"
                    print("    Vol {} (view_id={}): {}".format(
                        v["vol"], v["view_id"], status))
                    if ok:
                        available += 1
                    else:
                        unavailable += 1
                    time.sleep(1)
                print("  Download test: {}/{} available".format(
                    available, available + unavailable))

            existing[book_key] = {
                "title": book_info["title"],
                "title_ar": book_info["title_ar"],
                "author": book_info["author"],
                "expected_volumes": book_info.get("expected_volumes"),
                "discovered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "volumes": volumes,
            }

    finally:
        browser.close()
        pw.stop()

    # Save results
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    print("\n" + "=" * 60)
    print("Results saved to {}".format(OUTPUT_FILE))
    print("=" * 60)


if __name__ == "__main__":
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        sys.exit(0)

    main()
