"""
Download Word (.doc) files from rafed.net (Maktabat Rafed) for Tahdhib al-Ahkam
and al-Istibsar -- the remaining two of the Four Books (Al-Kutub Al-Arb'ah).

rafed.net provides a Word download API: books.rafed.net/api/download/{id}/doc
Each volume is a single HTTP GET returning a Word document.

Output is saved to:
  app/raw/rafed_net/tahdhib-al-ahkam/vol-{N}.doc
  app/raw/rafed_net/al-istibsar/vol-{N}.doc

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    python app/scrapers/download_rafed_word.py

    # Download specific book only:
    python app/scrapers/download_rafed_word.py --tahdhib
    python app/scrapers/download_rafed_word.py --istibsar

    # List volumes:
    python app/scrapers/download_rafed_word.py --list
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

sys.stdout.reconfigure(encoding="utf-8")

BASE_URL = "https://books.rafed.net/api/download"
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "raw", "rafed_net"
)

DELAY_BETWEEN_REQUESTS = 2.0  # seconds - be respectful

# Volume IDs confirmed via Playwright browser inspection of books.rafed.net
BOOKS = {
    "tahdhib-al-ahkam": {
        "title": "Tahdhib al-Ahkam",
        "title_ar": "\u062a\u0647\u0630\u064a\u0628 \u0627\u0644\u0623\u062d\u0643\u0627\u0645",
        "author": "Sheikh al-Tusi",
        "volumes": [
            {"vol": 1, "view_id": 722},
            {"vol": 2, "view_id": 731},
            {"vol": 3, "view_id": 734},
            {"vol": 4, "view_id": 735},
            {"vol": 5, "view_id": 736},
            {"vol": 6, "view_id": 737},
            {"vol": 7, "view_id": 741},
            {"vol": 8, "view_id": 745},
            {"vol": 9, "view_id": 747},
            {"vol": 10, "view_id": 752},
        ],
    },
    "al-istibsar": {
        "title": "al-Istibsar",
        "title_ar": "\u0627\u0644\u0627\u0633\u062a\u0628\u0635\u0627\u0631",
        "author": "Sheikh al-Tusi",
        "volumes": [
            {"vol": 1, "view_id": 1266},
            {"vol": 2, "view_id": 1307},
            {"vol": 3, "view_id": 1320},
            {"vol": 4, "view_id": 1321},
        ],
    },
}


def download_volume(view_id, output_path):
    """Download a single Word file from rafed.net API."""
    url = "{}/{}/doc".format(BASE_URL, view_id)
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "ThaqalaynDataGenerator/1.0")
        with urllib.request.urlopen(req, timeout=120) as resp:
            if resp.status == 200:
                data = resp.read()
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "wb") as f:
                    f.write(data)
                size_kb = len(data) / 1024
                print("  Downloaded {:.0f} KB -> {}".format(size_kb, output_path))
                return True
            else:
                print("  WARNING: {} returned status {}".format(url, resp.status))
                return False
    except urllib.error.HTTPError as e:
        print("  ERROR: {} returned HTTP {}".format(url, e.code))
        return False
    except Exception as e:
        print("  ERROR downloading {}: {}".format(url, e))
        return False


def save_metadata(book_key, book_info, results):
    """Save download metadata alongside the Word files."""
    book_dir = os.path.join(OUTPUT_DIR, book_key)
    os.makedirs(book_dir, exist_ok=True)

    metadata = {
        "source": "rafed.net",
        "source_base_url": "https://books.rafed.net",
        "title": book_info["title"],
        "title_ar": book_info["title_ar"],
        "author": book_info["author"],
        "language": "ar",
        "format": "doc",
        "total_volumes": len(book_info["volumes"]),
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "volumes": results,
    }

    metadata_path = os.path.join(book_dir, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print("  Saved metadata to {}".format(metadata_path))


def download_book(book_key, book_info):
    """Download all volumes for a book."""
    book_dir = os.path.join(OUTPUT_DIR, book_key)
    print("\n--- {} ({} volumes) ---".format(book_info["title"], len(book_info["volumes"])))
    print("Output: {}".format(book_dir))

    results = []
    skipped = 0
    downloaded = 0
    failed = 0

    for vol_info in book_info["volumes"]:
        vol_num = vol_info["vol"]
        view_id = vol_info["view_id"]
        filename = "vol-{}.doc".format(vol_num)
        output_path = os.path.join(book_dir, filename)

        if os.path.exists(output_path):
            size_kb = os.path.getsize(output_path) / 1024
            print("  Vol {}: SKIP (already exists, {:.0f} KB)".format(vol_num, size_kb))
            results.append({
                "vol": vol_num,
                "view_id": view_id,
                "filename": filename,
                "status": "skipped",
            })
            skipped += 1
            continue

        print("  Vol {}: Downloading (view_id={})...".format(vol_num, view_id))
        success = download_volume(view_id, output_path)

        if success:
            results.append({
                "vol": vol_num,
                "view_id": view_id,
                "filename": filename,
                "status": "downloaded",
                "size_bytes": os.path.getsize(output_path),
            })
            downloaded += 1
        else:
            results.append({
                "vol": vol_num,
                "view_id": view_id,
                "filename": filename,
                "status": "failed",
            })
            failed += 1

        time.sleep(DELAY_BETWEEN_REQUESTS)

    save_metadata(book_key, book_info, results)

    print("--- {} complete: {} downloaded, {} skipped, {} failed ---".format(
        book_info["title"], downloaded, skipped, failed))
    return downloaded, skipped, failed


def list_volumes():
    """Print all volumes and their download URLs."""
    for book_key, book_info in BOOKS.items():
        print("\n{} ({})".format(book_info["title"], book_info["title_ar"]))
        print("  Author: {}".format(book_info["author"]))
        print("  Volumes: {}".format(len(book_info["volumes"])))
        for vol_info in book_info["volumes"]:
            url = "{}/{}/doc".format(BASE_URL, vol_info["view_id"])
            print("    Vol {}: view_id={} -> {}".format(
                vol_info["vol"], vol_info["view_id"], url))


def main(book_filter=None):
    print("=" * 60)
    print("Rafed.net Word Document Downloader")
    print("Output directory: {}".format(OUTPUT_DIR))
    print("=" * 60)

    total_downloaded = 0
    total_skipped = 0
    total_failed = 0

    for book_key, book_info in BOOKS.items():
        if book_filter and book_key != book_filter:
            continue
        d, s, f = download_book(book_key, book_info)
        total_downloaded += d
        total_skipped += s
        total_failed += f

    print("\n" + "=" * 60)
    print("Download complete!")
    print("  Downloaded: {} volumes".format(total_downloaded))
    print("  Skipped: {} volumes (already on disk)".format(total_skipped))
    print("  Failed: {} volumes".format(total_failed))
    print("=" * 60)


if __name__ == "__main__":
    if "--list" in sys.argv:
        list_volumes()
        sys.exit(0)

    book_filter = None
    if "--tahdhib" in sys.argv:
        book_filter = "tahdhib-al-ahkam"
    elif "--istibsar" in sys.argv:
        book_filter = "al-istibsar"

    main(book_filter)
