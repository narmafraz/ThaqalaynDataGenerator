"""
Download HTML files from ghbook.ir (Qaimiyyah Digital Library) for Tahdhib al-Ahkam
and al-Istibsar -- the remaining two of the Four Books (Al-Kutub Al-Arb'ah).

ghbook.ir provides direct download links for complete HTML files per book.
Each book is a single large HTML file containing all volumes.

Output is saved to:
  app/raw/ghbook_ir/tahdhib-al-ahkam/book.htm
  app/raw/ghbook_ir/al-istibsar/book.htm

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    python app/scrapers/download_ghbook_html.py

    # Download specific book only:
    python app/scrapers/download_ghbook_html.py --tahdhib
    python app/scrapers/download_ghbook_html.py --istibsar

    # List available downloads:
    python app/scrapers/download_ghbook_html.py --list
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

sys.stdout.reconfigure(encoding="utf-8")

DOWNLOAD_BASE = "https://download.ghbook.ir/downloads.php"
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "raw", "ghbook_ir"
)

DELAY_BETWEEN_REQUESTS = 2.0  # seconds

# Download URLs confirmed via Playwright browser inspection of ghbook.ir
BOOKS = {
    "tahdhib-al-ahkam": {
        "title": "Tahdhib al-Ahkam",
        "title_ar": "\u062a\u0647\u0630\u064a\u0628 \u0627\u0644\u0623\u062d\u0643\u0627\u0645",
        "author": "Sheikh al-Tusi",
        "book_id": 378,
        "volumes": 10,
        "pages": 4119,
        "downloads": [
            {
                "format": "htm",
                "filename": "book.htm",
                "url_params": "id=378&file=378-a-13900129-tahzebalahkam-koli.htm",
            },
            {
                "format": "epub",
                "filename": "book.epub",
                "url_params": "id=378&file=378-ar-tahzebalahkam-koli.epub",
            },
        ],
    },
    "al-istibsar": {
        "title": "al-Istibsar",
        "title_ar": "\u0627\u0644\u0627\u0633\u062a\u0628\u0635\u0627\u0631",
        "author": "Sheikh al-Tusi",
        "book_id": 2628,
        "volumes": 4,
        "pages": None,  # unknown exact count
        "downloads": [
            {
                "format": "htm",
                "filename": "book.htm",
                "url_params": "id=2628&file=2628-a-13900308-alestebsar-koli.htm",
            },
            {
                "format": "epub",
                "filename": "book.epub",
                "url_params": "id=2628&file=2628-ar-alestebsar-koli.epub",
            },
        ],
    },
}


def download_file(url, output_path):
    """Download a single file from ghbook.ir."""
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "ThaqalaynDataGenerator/1.0")
        with urllib.request.urlopen(req, timeout=300) as resp:
            if resp.status == 200:
                data = resp.read()
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "wb") as f:
                    f.write(data)
                size_mb = len(data) / (1024 * 1024)
                print("  Downloaded {:.1f} MB -> {}".format(size_mb, output_path))
                return len(data)
            else:
                print("  WARNING: {} returned status {}".format(url, resp.status))
                return 0
    except urllib.error.HTTPError as e:
        print("  ERROR: {} returned HTTP {}".format(url, e.code))
        return 0
    except Exception as e:
        print("  ERROR downloading {}: {}".format(url, e))
        return 0


def save_metadata(book_key, book_info, results):
    """Save download metadata alongside the files."""
    book_dir = os.path.join(OUTPUT_DIR, book_key)
    os.makedirs(book_dir, exist_ok=True)

    metadata = {
        "source": "ghbook.ir",
        "source_url": "https://ghbook.ir/books/{}".format(book_info["book_id"]),
        "title": book_info["title"],
        "title_ar": book_info["title_ar"],
        "author": book_info["author"],
        "book_id": book_info["book_id"],
        "language": "ar",
        "total_volumes": book_info["volumes"],
        "total_pages": book_info["pages"],
        "license": "Free distribution (ghbook.ir policy)",
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "files": results,
    }

    metadata_path = os.path.join(book_dir, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print("  Saved metadata to {}".format(metadata_path))


def download_book(book_key, book_info, formats=None):
    """Download all files for a book."""
    book_dir = os.path.join(OUTPUT_DIR, book_key)
    print("\n--- {} (book_id={}, {} volumes) ---".format(
        book_info["title"], book_info["book_id"], book_info["volumes"]))
    print("Output: {}".format(book_dir))

    results = []

    for dl_info in book_info["downloads"]:
        fmt = dl_info["format"]
        if formats and fmt not in formats:
            continue

        filename = dl_info["filename"]
        output_path = os.path.join(book_dir, filename)
        url = "{}?{}".format(DOWNLOAD_BASE, dl_info["url_params"])

        if os.path.exists(output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print("  {}: SKIP (already exists, {:.1f} MB)".format(filename, size_mb))
            results.append({
                "format": fmt,
                "filename": filename,
                "status": "skipped",
                "size_bytes": os.path.getsize(output_path),
            })
            continue

        print("  {}: Downloading from ghbook.ir...".format(filename))
        size = download_file(url, output_path)

        if size > 0:
            results.append({
                "format": fmt,
                "filename": filename,
                "status": "downloaded",
                "size_bytes": size,
            })
        else:
            results.append({
                "format": fmt,
                "filename": filename,
                "status": "failed",
            })

        time.sleep(DELAY_BETWEEN_REQUESTS)

    save_metadata(book_key, book_info, results)
    print("--- {} complete ---".format(book_info["title"]))
    return results


def list_downloads():
    """Print all available downloads."""
    for book_key, book_info in BOOKS.items():
        print("\n{} ({})".format(book_info["title"], book_info["title_ar"]))
        print("  Author: {}".format(book_info["author"]))
        print("  ghbook.ir book_id: {}".format(book_info["book_id"]))
        print("  Volumes: {}, Pages: {}".format(book_info["volumes"], book_info["pages"]))
        print("  Available formats:")
        for dl_info in book_info["downloads"]:
            url = "{}?{}".format(DOWNLOAD_BASE, dl_info["url_params"])
            print("    {} -> {}".format(dl_info["filename"], url))


def main(book_filter=None):
    print("=" * 60)
    print("ghbook.ir HTML/EPUB Downloader")
    print("Output directory: {}".format(OUTPUT_DIR))
    print("=" * 60)

    # Default: download HTM only (primary format for parsing)
    # EPUB can be added by modifying formats list
    formats = ["htm"]

    for book_key, book_info in BOOKS.items():
        if book_filter and book_key != book_filter:
            continue
        download_book(book_key, book_info, formats=formats)

    print("\n" + "=" * 60)
    print("Download complete!")
    print("=" * 60)


if __name__ == "__main__":
    if "--list" in sys.argv:
        list_downloads()
        sys.exit(0)

    book_filter = None
    if "--tahdhib" in sys.argv:
        book_filter = "tahdhib-al-ahkam"
    elif "--istibsar" in sys.argv:
        book_filter = "al-istibsar"

    main(book_filter)
