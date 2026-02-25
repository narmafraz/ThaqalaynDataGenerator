"""
Scrape hadith data from the ThaqalaynAPI (https://www.thaqalayn-api.net/)
for books not yet present in our raw data collection.

The ThaqalaynAPI provides REST access to hadith data originally from thaqalayn.net.
API docs: https://www.thaqalayn-api.net/api-docs/

Output is saved to: ThaqalaynDataSources/scraped/thaqalayn_api/<book-folder>/hadiths.json

Usage:
    # From ThaqalaynDataGenerator root:
    .venv/Scripts/python.exe app/scrapers/scrape_thaqalayn_api.py

    # Scrape specific books only:
    .venv/Scripts/python.exe app/scrapers/scrape_thaqalayn_api.py Fadail-al-Shia-Saduq

    # List available book slugs:
    .venv/Scripts/python.exe app/scrapers/scrape_thaqalayn_api.py --list
"""

import json
import os
import time
import sys
import urllib.request
import urllib.error

BASE_URL = "https://www.thaqalayn-api.net/api/v2"
# Output to ThaqalaynDataSources/scraped/thaqalayn_api/
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SOURCE_DATA_DIR = os.environ.get("SOURCE_DATA_DIR", os.path.join(_PROJECT_ROOT, "..", "ThaqalaynDataSources"))
OUTPUT_DIR = os.path.join(_SOURCE_DATA_DIR, "scraped", "thaqalayn_api")

# Books to scrape: slug -> (output_folder_name, hadith_count)
# Source: https://www.thaqalayn-api.net/api/v2/allbooks
BOOKS_TO_SCRAPE = {
    # === The Four Books (completing the set - Al-Kafi already exists) ===
    "Man-La-Yahduruh-al-Faqih-Volume-1-Saduq": ("man-la-yahduruhu-al-faqih-v1", 1569),
    "Man-La-Yahduruh-al-Faqih-Volume-2-Saduq": ("man-la-yahduruhu-al-faqih-v2", 1696),
    "Man-La-Yahduruh-al-Faqih-Volume-3-Saduq": ("man-la-yahduruhu-al-faqih-v3", 1758),
    "Man-La-Yahduruh-al-Faqih-Volume-4-Saduq": ("man-la-yahduruhu-al-faqih-v4", 964),
    "Man-La-Yahduruh-al-Faqih-Volume-5-Saduq": ("man-la-yahduruhu-al-faqih-v5", 395),
    # Note: Tahdhib al-Ahkam and al-Istibsar are NOT on ThaqalaynAPI yet

    # === Primary Hadith Collections (from aspiration list) ===
    "Nahj-al-Balagha-Radi": ("nahj-al-balagha", 2260),
    "Kitab-al-Mumin-Ahwazi": ("kitab-al-mumin", 201),
    "Al-Amali-Mufid": ("al-amali-mufid", 387),
    "Al-Amali-Saduq": ("al-amali-saduq", 1082),
    "Kamil-al-Ziyarat-Qummi": ("kamil-al-ziyarat", 750),
    "Kitab-al-Ghayba-Numani": ("kitab-al-ghayba-numani", 468),
    "Kitab-al-Ghayba-Tusi": ("kitab-al-ghayba-tusi", 774),

    # === Additional collections available on the API ===
    "Al-Tawhid-Saduq": ("al-tawhid-saduq", 575),
    "Uyun-akhbar-al-Rida-Volume-1-Saduq": ("uyun-akhbar-al-rida-v1", 347),
    "Uyun-akhbar-al-Rida-Volume-2-Saduq": ("uyun-akhbar-al-rida-v2", 607),
    "Al-Khisal-Saduq": ("al-khisal", 1282),
    "Maani-al-Akhbar-Saduq": ("maani-al-akhbar", 829),
    "Kamal-al-Din-wa-Tamam-al-Nima-Saduq": ("kamal-al-din", 659),
    "Thawab-al-Amal-wa-iqab-al-Amal-Saduq": ("thawab-al-amal", 1106),
    "Kitab-al-Zuhd-Ahwazi": ("kitab-al-zuhd", 290),
    "Risalat-al-Huquq-Abidin": ("risalat-al-huquq", 49),
    "Fadail-al-Shia-Saduq": ("fadail-al-shia", 45),
    "Sifat-al-Shia-Saduq": ("sifat-al-shia", 71),
    "Kitab-al-Duafa-Ghadairi": ("kitab-al-duafa", 226),
    "Mujam-al-Ahadith-al-Mutabara-Muhsini": ("mujam-al-ahadith-al-mutabara", 555),
}

DELAY_BETWEEN_REQUESTS = 0.5  # seconds - be respectful to the API


def fetch_hadith(book_slug, hadith_id):
    """Fetch a single hadith from the API using urllib."""
    url = "{}/{}/{}".format(BASE_URL, book_slug, hadith_id)
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "ThaqalaynDataGenerator/1.0")
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                data = resp.read().decode("utf-8")
                return json.loads(data)
            else:
                print("  WARNING: {} returned status {}".format(url, resp.status))
                return None
    except urllib.error.HTTPError as e:
        print("  WARNING: {} returned HTTP {}".format(url, e.code))
        return None
    except Exception as e:
        print("  ERROR fetching {}: {}".format(url, e))
        return None


def fetch_all_hadiths_for_book(book_slug, count):
    """Fetch all hadiths for a book, one by one."""
    hadiths = []
    for i in range(1, count + 1):
        hadith = fetch_hadith(book_slug, i)
        if hadith:
            hadiths.append(hadith)
        if i % 50 == 0:
            print("  Fetched {}/{} hadiths...".format(i, count))
        time.sleep(DELAY_BETWEEN_REQUESTS)
    return hadiths


def save_book_data(folder_name, hadiths, book_slug):
    """Save all hadiths for a book to a JSON file."""
    book_dir = os.path.join(OUTPUT_DIR, folder_name)
    os.makedirs(book_dir, exist_ok=True)

    output_path = os.path.join(book_dir, "hadiths.json")
    data = {
        "source": "thaqalayn-api.net",
        "source_url": "{}/{}".format(BASE_URL, book_slug),
        "original_site": "thaqalayn.net",
        "book_slug": book_slug,
        "total_hadiths": len(hadiths),
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "hadiths": hadiths,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("  Saved {} hadiths to {}".format(len(hadiths), output_path))


def get_already_scraped():
    """Check which books have already been scraped."""
    scraped = set()
    for slug, (folder_name, _count) in BOOKS_TO_SCRAPE.items():
        book_dir = os.path.join(OUTPUT_DIR, folder_name)
        hadiths_file = os.path.join(book_dir, "hadiths.json")
        if os.path.exists(hadiths_file):
            scraped.add(slug)
    return scraped


def main():
    print("=" * 60)
    print("ThaqalaynAPI Scraper")
    print("Output directory: {}".format(OUTPUT_DIR))
    print("Total books to scrape: {}".format(len(BOOKS_TO_SCRAPE)))
    print("=" * 60)

    already_scraped = get_already_scraped()
    if already_scraped:
        print("\nSkipping {} already-scraped books:".format(len(already_scraped)))
        for slug in sorted(already_scraped):
            print("  - {}".format(slug))

    remaining = {k: v for k, v in BOOKS_TO_SCRAPE.items() if k not in already_scraped}
    total_hadiths = sum(count for _folder, count in remaining.values())
    print("\nBooks remaining: {}".format(len(remaining)))
    print("Total hadiths to fetch: {}".format(total_hadiths))
    print("Estimated time: ~{:.0f} minutes".format(total_hadiths * DELAY_BETWEEN_REQUESTS / 60))
    print()

    for book_slug, (folder_name, count) in BOOKS_TO_SCRAPE.items():
        if book_slug in already_scraped:
            continue

        print("\n--- Scraping: {} ({} hadiths) ---".format(book_slug, count))
        hadiths = fetch_all_hadiths_for_book(book_slug, count)
        save_book_data(folder_name, hadiths, book_slug)
        print("--- Done: {} ({}/{} fetched) ---".format(book_slug, len(hadiths), count))

    print("\n" + "=" * 60)
    print("Scraping complete!")
    print("=" * 60)


if __name__ == "__main__":
    if "--list" in sys.argv:
        print("Available book slugs:")
        for slug, (folder, count) in sorted(BOOKS_TO_SCRAPE.items()):
            print("  {} -> {} ({} hadiths)".format(slug, folder, count))
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] != "--list":
        requested = set(sys.argv[1:])
        to_remove = [k for k in list(BOOKS_TO_SCRAPE.keys()) if k not in requested]
        for k in to_remove:
            del BOOKS_TO_SCRAPE[k]
        if not BOOKS_TO_SCRAPE:
            print("No matching books found. Use --list to see available slugs.")
            sys.exit(1)

    main()
