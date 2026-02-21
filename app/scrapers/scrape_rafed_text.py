"""
Scrape Arabic text page-by-page from books.rafed.net for the Four Books.

rafed.net is a Next.js SPA that renders book pages with selectable Arabic text.
Each page is at: books.rafed.net/view/{view_id}/page/{page_num}

This scraper uses Playwright to render pages and extract text content,
since the site requires JavaScript execution. It also extracts the table
of contents (chapter names + page numbers) from the sidebar.

Output is saved to:
  app/raw/rafed_net/{book-key}/toc.json          (table of contents)
  app/raw/rafed_net/{book-key}/pages/page-{N}.json (per-page text)

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    python app/scrapers/scrape_rafed_text.py

    # Scrape specific book:
    python app/scrapers/scrape_rafed_text.py --tahdhib
    python app/scrapers/scrape_rafed_text.py --istibsar

    # Scrape table of contents only (fast):
    python app/scrapers/scrape_rafed_text.py --toc-only

    # Scrape specific volume only:
    python app/scrapers/scrape_rafed_text.py --tahdhib --vol 1

    # List available books:
    python app/scrapers/scrape_rafed_text.py --list

Prerequisites:
    pip install playwright
    playwright install chromium
    # OR use Brave browser (see BRAVE_PATH below)
"""

import json
import os
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "raw", "rafed_net"
)

BASE_URL = "https://books.rafed.net"

# Brave browser path on Windows (fallback to system Chromium)
BRAVE_PATH = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"

DELAY_BETWEEN_PAGES = 1.5  # seconds - be respectful
PAGE_LOAD_WAIT = 3.0  # seconds to wait for SPA content to render

# Same volume registry as download_rafed_word.py
VOLUMES = {
    "tahdhib-al-ahkam": {
        "title": "Tahdhib al-Ahkam",
        "title_ar": "\u062a\u0647\u0630\u064a\u0628 \u0627\u0644\u0623\u062d\u0643\u0627\u0645",
        "author": "Sheikh al-Tusi",
        "vols": [
            {"vol": 1, "view_id": 722, "pages": 471},
            {"vol": 2, "view_id": 731, "pages": 408},
            {"vol": 3, "view_id": 734, "pages": 347},
            {"vol": 4, "view_id": 735, "pages": 392},
            {"vol": 5, "view_id": 736, "pages": 502},
            {"vol": 6, "view_id": 737, "pages": 437},
            {"vol": 7, "view_id": 741, "pages": 512},
            {"vol": 8, "view_id": 745, "pages": 362},
            {"vol": 9, "view_id": 747, "pages": 406},
            {"vol": 10, "view_id": 752, "pages": 448},
        ],
    },
    "al-istibsar": {
        "title": "al-Istibsar",
        "title_ar": "\u0627\u0644\u0627\u0633\u062a\u0628\u0635\u0627\u0631",
        "author": "Sheikh al-Tusi",
        "vols": [
            {"vol": 1, "view_id": 1266, "pages": 505},
            {"vol": 2, "view_id": 1307, "pages": 403},
            {"vol": 3, "view_id": 1320, "pages": 420},
            {"vol": 4, "view_id": 1321, "pages": 383},
        ],
    },
}


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


def extract_toc(page, view_id):
    """Extract table of contents from the sidebar of a volume page.

    Returns a list of dicts: [{"title": "...", "page": N}, ...]
    """
    url = "{}/view/{}/page/1".format(BASE_URL, view_id)
    page.goto(url, wait_until="networkidle")
    page.wait_for_timeout(int(PAGE_LOAD_WAIT * 1000))

    # The TOC is in the sidebar <aside> / <nav> with links like /view/{id}/page/{N}
    toc_entries = page.evaluate("""() => {
        const entries = [];
        // Find all links in the sidebar that point to page numbers
        const links = document.querySelectorAll('aside a[href*="/page/"], nav a[href*="/page/"], [role="complementary"] a[href*="/page/"]');
        for (const link of links) {
            const href = link.getAttribute('href') || '';
            const match = href.match(/\\/view\\/\\d+\\/page\\/(\\d+)/);
            if (match) {
                const pageNum = parseInt(match[1], 10);
                // Get the chapter title text (exclude the page number itself)
                const spans = link.querySelectorAll('div, span');
                let title = '';
                if (spans.length > 0) {
                    title = spans[0].textContent.trim();
                } else {
                    title = link.textContent.trim();
                }
                // Skip if title is just a number or empty
                if (title && !/^\\d+$/.test(title)) {
                    entries.push({title: title, page: pageNum});
                }
            }
        }
        // Also try the listitem-based TOC structure
        if (entries.length === 0) {
            const items = document.querySelectorAll('li h3 a[href*="/page/"]');
            for (const item of items) {
                const href = item.getAttribute('href') || '';
                const match = href.match(/\\/view\\/\\d+\\/page\\/(\\d+)/);
                if (match) {
                    const pageNum = parseInt(match[1], 10);
                    const divs = item.querySelectorAll('div');
                    let title = divs.length > 0 ? divs[0].textContent.trim() : item.textContent.trim();
                    if (title && !/^\\d+$/.test(title)) {
                        entries.push({title: title, page: pageNum});
                    }
                }
            }
        }
        return entries;
    }""")

    return toc_entries


def extract_page_text(page, view_id, page_num):
    """Extract text content from a single book page.

    Returns a dict with page text paragraphs.
    """
    url = "{}/view/{}/page/{}".format(BASE_URL, view_id, page_num)
    page.goto(url, wait_until="networkidle")
    page.wait_for_timeout(int(PAGE_LOAD_WAIT * 1000))

    # Extract paragraphs from the main content area
    result = page.evaluate("""() => {
        const paragraphs = [];
        // The page text is in <p> elements within the main content area
        // Look for the content container (not sidebar, not settings)
        const contentDivs = document.querySelectorAll('main > div > div');
        for (const div of contentDivs) {
            const ps = div.querySelectorAll('p');
            for (const p of ps) {
                const text = p.textContent.trim();
                if (text && text.length > 0) {
                    paragraphs.push(text);
                }
            }
        }
        // Fallback: try all <p> in main
        if (paragraphs.length === 0) {
            const allPs = document.querySelectorAll('main p');
            for (const p of allPs) {
                const text = p.textContent.trim();
                if (text && text.length > 1) {
                    paragraphs.push(text);
                }
            }
        }
        return paragraphs;
    }""")

    return result


def scrape_toc(book_key, book_info, browser_page):
    """Scrape and save table of contents for all volumes of a book."""
    book_dir = os.path.join(OUTPUT_DIR, book_key)
    os.makedirs(book_dir, exist_ok=True)

    all_toc = {}
    for vol_info in book_info["vols"]:
        vol = vol_info["vol"]
        view_id = vol_info["view_id"]
        print("  Extracting TOC for Vol {} (view_id={})...".format(vol, view_id))

        entries = extract_toc(browser_page, view_id)
        all_toc["vol_{}".format(vol)] = {
            "view_id": view_id,
            "entries": entries,
        }
        print("    Found {} TOC entries".format(len(entries)))
        time.sleep(DELAY_BETWEEN_PAGES)

    toc_path = os.path.join(book_dir, "toc.json")
    toc_data = {
        "source": "rafed.net",
        "title": book_info["title"],
        "title_ar": book_info["title_ar"],
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "volumes": all_toc,
    }
    with open(toc_path, "w", encoding="utf-8") as f:
        json.dump(toc_data, f, ensure_ascii=False, indent=2)
    print("  Saved TOC to {}".format(toc_path))
    return all_toc


def scrape_pages(book_key, book_info, browser_page, vol_filter=None):
    """Scrape page text for all (or selected) volumes of a book."""
    book_dir = os.path.join(OUTPUT_DIR, book_key)
    pages_dir = os.path.join(book_dir, "pages")
    os.makedirs(pages_dir, exist_ok=True)

    total_scraped = 0
    total_skipped = 0

    for vol_info in book_info["vols"]:
        vol = vol_info["vol"]
        if vol_filter is not None and vol != vol_filter:
            continue

        view_id = vol_info["view_id"]
        total_pages = vol_info["pages"]
        vol_dir = os.path.join(pages_dir, "vol-{}".format(vol))
        os.makedirs(vol_dir, exist_ok=True)

        print("\n  --- Vol {} ({} pages, view_id={}) ---".format(vol, total_pages, view_id))

        for page_num in range(1, total_pages + 1):
            page_file = os.path.join(vol_dir, "page-{}.json".format(page_num))

            if os.path.exists(page_file):
                total_skipped += 1
                if page_num % 50 == 0:
                    print("    Page {}/{}: SKIP (exists)".format(page_num, total_pages))
                continue

            paragraphs = extract_page_text(browser_page, view_id, page_num)

            page_data = {
                "view_id": view_id,
                "vol": vol,
                "page": page_num,
                "paragraphs": paragraphs,
            }

            with open(page_file, "w", encoding="utf-8") as f:
                json.dump(page_data, f, ensure_ascii=False, indent=2)

            total_scraped += 1
            if page_num % 10 == 0:
                print("    Page {}/{}: {} paragraphs".format(
                    page_num, total_pages, len(paragraphs)))

            time.sleep(DELAY_BETWEEN_PAGES)

        print("  --- Vol {} complete ---".format(vol))

    print("\n  Total: {} pages scraped, {} skipped (already on disk)".format(
        total_scraped, total_skipped))


def list_books():
    """List all available books and volumes."""
    for book_key, book_info in VOLUMES.items():
        print("\n{} ({})".format(book_info["title"], book_info["title_ar"]))
        print("  Author: {}".format(book_info["author"]))
        for vol_info in book_info["vols"]:
            url = "{}/view/{}/page/1".format(BASE_URL, vol_info["view_id"])
            print("    Vol {}: {} pages -> {}".format(
                vol_info["vol"], vol_info["pages"], url))


def main():
    toc_only = "--toc-only" in sys.argv
    vol_filter = None
    book_filter = None

    BOOK_FLAGS = {
        "--tahdhib": "tahdhib-al-ahkam",
        "--istibsar": "al-istibsar",
    }
    for flag, key in BOOK_FLAGS.items():
        if flag in sys.argv:
            book_filter = key
            break

    if "--vol" in sys.argv:
        idx = sys.argv.index("--vol")
        if idx + 1 < len(sys.argv):
            vol_filter = int(sys.argv[idx + 1])

    print("=" * 60)
    print("Rafed.net Page Text Scraper (Playwright)")
    print("Output directory: {}".format(OUTPUT_DIR))
    if toc_only:
        print("Mode: TOC only")
    if book_filter:
        print("Book filter: {}".format(book_filter))
    if vol_filter:
        print("Volume filter: {}".format(vol_filter))
    print("=" * 60)

    pw, browser, browser_page = get_browser()

    try:
        for book_key, book_info in VOLUMES.items():
            if book_filter and book_key != book_filter:
                continue

            print("\n=== {} ===".format(book_info["title"]))
            scrape_toc(book_key, book_info, browser_page)

            if not toc_only:
                scrape_pages(book_key, book_info, browser_page, vol_filter)
    finally:
        browser.close()
        pw.stop()

    print("\n" + "=" * 60)
    print("Scraping complete!")
    print("=" * 60)


if __name__ == "__main__":
    if "--list" in sys.argv:
        list_books()
        sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        sys.exit(0)

    main()
