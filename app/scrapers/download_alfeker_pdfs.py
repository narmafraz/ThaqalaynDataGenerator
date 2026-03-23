"""
Download PDF files for Bihar al-Anwar and Mir'at al-Uqul from alfeker.net.

alfeker.net catalogs Islamic books with download links hosted on:
- archive.org (Bihar al-Anwar — 110 volumes)
- mediafire.com (Mir'at al-Uqul — 28 files: 2 intro + 26 volumes)

This script:
1. Scrapes the catalog pages on alfeker.net to extract download URLs
2. Downloads the PDFs (archive.org direct, MediaFire via redirect)
3. Saves metadata alongside the downloads

Output is saved to:
  ThaqalaynDataSources/scraped/alfeker_net/bihar-al-anwar/vol-{NNN}.pdf
  ThaqalaynDataSources/scraped/alfeker_net/mirat-al-uqul/vol-{NN}.pdf

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    python app/scrapers/download_alfeker_pdfs.py

    # Download specific book:
    python app/scrapers/download_alfeker_pdfs.py --bihar
    python app/scrapers/download_alfeker_pdfs.py --mirat

    # Discover URLs only (no download):
    python app/scrapers/download_alfeker_pdfs.py --discover-only

    # List discovered URLs:
    python app/scrapers/download_alfeker_pdfs.py --list
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
OUTPUT_DIR = os.path.join(_SOURCE_DATA_DIR, "scraped", "alfeker_net")
URL_CACHE_FILE = os.path.join(OUTPUT_DIR, "discovered_urls.json")

BRAVE_PATH = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
DELAY_BETWEEN_REQUESTS = 2.0

CATALOG_PAGES = {
    "bihar-al-anwar": {
        "title": "Bihar al-Anwar",
        "title_ar": "\u0628\u062d\u0627\u0631 \u0627\u0644\u0623\u0646\u0648\u0627\u0631",
        "author": "Allamah Majlisi",
        "expected_volumes": 110,
        "catalog_url": "http://alfeker.net/library.php?id=4027",
        "hosting": "archive.org",
    },
    "mirat-al-uqul": {
        "title": "Mir'at al-Uqul",
        "title_ar": "\u0645\u0631\u0622\u0629 \u0627\u0644\u0639\u0642\u0648\u0644",
        "author": "Allamah Majlisi",
        "expected_volumes": 26,
        "catalog_url": "http://alfeker.net/library.php?id=2471",
        "hosting": "mediafire.com",
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


def discover_bihar_urls(browser_page):
    """Discover archive.org download URLs for Bihar al-Anwar."""
    info = CATALOG_PAGES["bihar-al-anwar"]
    print("  Navigating to: {}".format(info["catalog_url"]))
    browser_page.goto(info["catalog_url"], wait_until="networkidle")
    browser_page.wait_for_timeout(2000)

    # Extract archive.org PDF links
    urls = browser_page.evaluate(r"""() => {
        const results = [];
        const all = document.querySelectorAll('a[href]');
        for (const a of all) {
            const href = a.getAttribute('href') || '';
            if (href.includes('archive.org') && href.endsWith('.pdf')) {
                results.push(href);
            }
        }
        return results;
    }""")

    # Parse volume numbers from URLs
    volumes = []
    for url in urls:
        # Pattern: ج000, ج001, ..., ج110
        m = re.search(r'\u062c(\d{3})', url)
        if m:
            vol_num = int(m.group(1))
            volumes.append({"vol": vol_num, "url": url})

    volumes.sort(key=lambda v: v["vol"])
    return volumes


def discover_mirat_urls(browser_page):
    """Discover MediaFire download URLs for Mir'at al-Uqul."""
    info = CATALOG_PAGES["mirat-al-uqul"]
    print("  Navigating to: {}".format(info["catalog_url"]))
    browser_page.goto(info["catalog_url"], wait_until="networkidle")
    browser_page.wait_for_timeout(2000)

    # Extract mediafire links
    urls = browser_page.evaluate(r"""() => {
        const results = [];
        const all = document.querySelectorAll('a[href*="mediafire.com"]');
        for (const a of all) {
            const href = a.getAttribute('href') || '';
            results.push(href);
        }
        return results;
    }""")

    # Also extract the volume titles from the page text to map URLs to volumes
    titles = browser_page.evaluate(r"""() => {
        const results = [];
        const body = document.body.innerText;
        const lines = body.split('\n');
        for (const line of lines) {
            const trimmed = line.trim();
            // Match lines like: (مرآة العقول... - ج01) (9 MB)
            if (trimmed.includes('مرآة العقول') && trimmed.includes('MB')) {
                results.push(trimmed);
            }
        }
        return results;
    }""")

    # Map titles to URLs in order
    volumes = []
    for i, url in enumerate(urls):
        title = titles[i] if i < len(titles) else "Volume {}".format(i)
        # Extract volume number from title
        m = re.search(r'\u062c(\d+)', title)  # ج01, ج02, etc.
        if m:
            vol_num = int(m.group(1))
        elif '\u0627\u0644\u0645\u0642\u062f\u0645\u0629' in title:
            # المقدمة = Introduction
            vol_num = 0 if i == 0 else -1  # intro parts
        else:
            vol_num = i
        volumes.append({"vol": vol_num, "url": url, "title": title[:120]})

    return volumes


def discover_all(browser_page):
    """Discover URLs for both books."""
    results = {}

    print("\n=== Discovering Bihar al-Anwar ===")
    bihar = discover_bihar_urls(browser_page)
    results["bihar-al-anwar"] = {
        "title": "Bihar al-Anwar",
        "title_ar": "\u0628\u062d\u0627\u0631 \u0627\u0644\u0623\u0646\u0648\u0627\u0631",
        "count": len(bihar),
        "hosting": "archive.org",
        "volumes": bihar,
    }
    print("  Found {} volumes".format(len(bihar)))

    time.sleep(DELAY_BETWEEN_REQUESTS)

    print("\n=== Discovering Mir'at al-Uqul ===")
    mirat = discover_mirat_urls(browser_page)
    results["mirat-al-uqul"] = {
        "title": "Mir'at al-Uqul",
        "title_ar": "\u0645\u0631\u0622\u0629 \u0627\u0644\u0639\u0642\u0648\u0644",
        "count": len(mirat),
        "hosting": "mediafire.com",
        "volumes": mirat,
    }
    print("  Found {} volumes".format(len(mirat)))

    # Save URL cache
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    results["discovered_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(URL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("\nSaved URL cache to {}".format(URL_CACHE_FILE))

    return results


def download_archive_org(url, output_path):
    """Download a PDF from archive.org."""
    try:
        # URL-encode the Arabic characters
        from urllib.parse import quote, urlparse, urlunparse
        parsed = urlparse(url)
        encoded_path = quote(parsed.path, safe="/")
        safe_url = urlunparse(parsed._replace(path=encoded_path))

        req = urllib.request.Request(safe_url)
        req.add_header("User-Agent", "ThaqalaynDataGenerator/1.0")
        with urllib.request.urlopen(req, timeout=300) as resp:
            if resp.status == 200:
                data = resp.read()
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "wb") as f:
                    f.write(data)
                size_mb = len(data) / (1024 * 1024)
                print("    Downloaded {:.1f} MB -> {}".format(
                    size_mb, os.path.basename(output_path)))
                return len(data)
    except urllib.error.HTTPError as e:
        print("    ERROR: HTTP {} for {}".format(e.code, os.path.basename(output_path)))
    except Exception as e:
        print("    ERROR: {} for {}".format(type(e).__name__, os.path.basename(output_path)))
    return 0


def resolve_mediafire_url(page_url, browser_page):
    """Resolve a MediaFire sharing link to the direct download URL.

    MediaFire pages have a download button that triggers the actual file download.
    Returns the direct URL or None.
    """
    try:
        browser_page.goto(page_url, wait_until="networkidle", timeout=30000)
        browser_page.wait_for_timeout(3000)

        # Look for the download button/link
        dl_url = browser_page.evaluate(r"""() => {
            // Try the main download button
            const btn = document.querySelector('#downloadButton, .download_link a, a[aria-label*="Download"], a[id*="download"]');
            if (btn) return btn.getAttribute('href') || '';

            // Try finding direct download links
            const links = document.querySelectorAll('a[href]');
            for (const a of links) {
                const href = a.getAttribute('href') || '';
                if (href.includes('download') && !href.includes('mediafire.com/?')) {
                    return href;
                }
            }
            return '';
        }""")

        return dl_url if dl_url else None
    except Exception as e:
        print("    ERROR resolving MediaFire URL: {}".format(e))
        return None


def download_book(book_key, volumes, browser_page=None):
    """Download all volumes for a book."""
    book_dir = os.path.join(OUTPUT_DIR, book_key)
    os.makedirs(book_dir, exist_ok=True)

    info = CATALOG_PAGES[book_key]
    print("\n--- {} ({} volumes) ---".format(info["title"], len(volumes)))

    downloaded = 0
    skipped = 0
    failed = 0
    results = []

    for vol_info in volumes:
        vol = vol_info["vol"]
        url = vol_info["url"]

        # Determine filename
        if vol >= 0:
            filename = "vol-{:03d}.pdf".format(vol)
        else:
            filename = "intro-{}.pdf".format(abs(vol))

        output_path = os.path.join(book_dir, filename)

        if os.path.exists(output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print("  Vol {:3d}: SKIP ({:.1f} MB exists)".format(vol, size_mb))
            skipped += 1
            results.append({"vol": vol, "filename": filename, "status": "skipped"})
            continue

        print("  Vol {:3d}: Downloading...".format(vol))

        if "archive.org" in url:
            size = download_archive_org(url, output_path)
        elif "mediafire.com" in url:
            if browser_page is None:
                print("    SKIP: MediaFire downloads require Playwright browser")
                failed += 1
                results.append({"vol": vol, "filename": filename, "status": "failed"})
                continue
            direct_url = resolve_mediafire_url(url, browser_page)
            if direct_url:
                size = download_archive_org(direct_url, output_path)  # reuse downloader
            else:
                print("    SKIP: Could not resolve MediaFire URL")
                size = 0
        else:
            print("    SKIP: Unknown hosting for {}".format(url[:50]))
            size = 0

        if size > 0:
            downloaded += 1
            results.append({"vol": vol, "filename": filename, "status": "downloaded",
                           "size_bytes": size})
        else:
            failed += 1
            results.append({"vol": vol, "filename": filename, "status": "failed"})

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Save metadata
    metadata = {
        "source": "alfeker.net",
        "hosting": info["hosting"],
        "catalog_url": info["catalog_url"],
        "title": info["title"],
        "title_ar": info["title_ar"],
        "author": info["author"],
        "language": "ar",
        "format": "pdf",
        "total_volumes": len(volumes),
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "volumes": results,
    }
    metadata_path = os.path.join(book_dir, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    print("--- {} complete: {} downloaded, {} skipped, {} failed ---".format(
        info["title"], downloaded, skipped, failed))
    return downloaded, skipped, failed


def load_cached_urls():
    """Load previously discovered URLs."""
    if os.path.exists(URL_CACHE_FILE):
        with open(URL_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def list_volumes():
    """List discovered volumes."""
    cached = load_cached_urls()
    if not cached:
        print("No cached URLs found. Run --discover-only first.")
        return

    for book_key in ["bihar-al-anwar", "mirat-al-uqul"]:
        data = cached.get(book_key, {})
        vols = data.get("volumes", [])
        print("\n{} ({}) — {} volumes".format(
            data.get("title", book_key), data.get("title_ar", ""), len(vols)))
        print("  Hosting: {}".format(data.get("hosting", "?")))
        for v in vols[:5]:
            print("  Vol {:3d}: {}".format(v["vol"], v["url"][:80]))
        if len(vols) > 5:
            print("  ... ({} total)".format(len(vols)))


def main():
    book_filter = None
    discover_only = "--discover-only" in sys.argv

    if "--bihar" in sys.argv:
        book_filter = "bihar-al-anwar"
    elif "--mirat" in sys.argv:
        book_filter = "mirat-al-uqul"

    print("=" * 60)
    print("Alfeker.net PDF Downloader (Bihar al-Anwar & Mir'at al-Uqul)")
    print("Output directory: {}".format(OUTPUT_DIR))
    print("=" * 60)

    # Step 1: Discover URLs (or load from cache)
    cached = load_cached_urls()
    if cached and not discover_only:
        print("Using cached URLs from {}".format(URL_CACHE_FILE))
    else:
        print("\nDiscovering download URLs...")
        pw, browser, browser_page = get_browser()
        try:
            cached = discover_all(browser_page)
        finally:
            browser.close()
            pw.stop()

        if discover_only:
            print("\nDiscovery complete. Run without --discover-only to download.")
            return

    # Step 2: Download
    # Bihar uses archive.org (direct HTTP), Mir'at uses MediaFire (needs browser)
    need_browser = book_filter != "bihar-al-anwar"  # need browser for MediaFire
    pw = browser = browser_page = None

    if need_browser:
        pw, browser, browser_page = get_browser()

    try:
        total_d = total_s = total_f = 0
        for book_key in ["bihar-al-anwar", "mirat-al-uqul"]:
            if book_filter and book_key != book_filter:
                continue
            data = cached.get(book_key, {})
            volumes = data.get("volumes", [])
            if not volumes:
                print("\nNo URLs for {}. Run --discover-only first.".format(book_key))
                continue
            d, s, f = download_book(book_key, volumes, browser_page)
            total_d += d
            total_s += s
            total_f += f
    finally:
        if browser:
            browser.close()
        if pw:
            pw.stop()

    print("\n" + "=" * 60)
    print("Download complete!")
    print("  Downloaded: {} volumes".format(total_d))
    print("  Skipped: {} (already on disk)".format(total_s))
    print("  Failed: {}".format(total_f))
    print("=" * 60)


if __name__ == "__main__":
    if "--list" in sys.argv:
        list_volumes()
        sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        sys.exit(0)

    main()
