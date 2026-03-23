"""Scrape Arabic chapter titles for Man La Yahduruhu al-Faqih from thaqalayn.net.

The thaqalayn.net hadith pages embed JSON data containing both name_en and
name_ar for all chapters in the volume sidebar. We identify unique URL sections
across all volumes and fetch one page per section to extract all titles.

Output: ThaqalaynDataSources/ai-pipeline-data/faqih_arabic_chapter_titles.json
"""

import json
import os
import re
import sys
import time
import urllib.request

sys.stdout.reconfigure(encoding="utf-8")

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
_SOURCE_DIR = os.environ.get(
    "SOURCE_DATA_DIR",
    os.path.join(_PROJECT_ROOT, "..", "ThaqalaynDataSources"),
)
OUTPUT_PATH = os.path.join(
    _SOURCE_DIR, "ai-pipeline-data", "faqih_arabic_chapter_titles.json"
)

# Match double-escaped JSON: \"name_en\":\"...\",\"name_ar\":\"...\"
CHAPTER_PATTERN = re.compile(
    r'\\"name_en\\":\\"(.*?)\\",'
    r'\\"name_ar\\":(null|\\".*?\\")'
)


def get_sample_urls():
    """Get one sample hadith URL per unique (book, section) combo."""
    api_dir = os.path.join(_SOURCE_DIR, "scraped", "thaqalayn_api")
    seen_sections = set()
    urls = []

    for vol in range(1, 6):
        path = os.path.join(
            api_dir, f"man-la-yahduruhu-al-faqih-v{vol}", "hadiths.json"
        )
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for h in data["hadiths"]:
            parts = h["URL"].split("/")
            # URL: https://thaqalayn.net/hadith/{book}/{section}/{chapter}/{hadith}
            section = parts[5]
            if section not in seen_sections:
                seen_sections.add(section)
                urls.append((f"vol{vol}-section{section}", h["URL"]))

    return urls


def fetch_page(url):
    """Fetch a URL and return HTML content."""
    req = urllib.request.Request(url, headers={"User-Agent": "ThaqalaynScraper/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def extract_titles_from_page(html):
    """Extract all (name_en, name_ar) pairs from embedded JSON in HTML."""
    titles = {}
    for match in CHAPTER_PATTERN.finditer(html):
        name_en = match.group(1)
        name_ar_raw = match.group(2)

        if name_ar_raw == "null" or not name_ar_raw:
            continue

        # Strip surrounding escaped quotes
        name_ar = name_ar_raw
        if name_ar.startswith('\\"'):
            name_ar = name_ar[2:]
        if name_ar.endswith('\\"'):
            name_ar = name_ar[:-2]

        # Clean up any script tag leakage in name_en
        if "<" in name_en or "script" in name_en.lower():
            name_en = name_en.split('"]')[0] if '"]' in name_en else name_en
            if "<" in name_en:
                continue

        if name_en and name_ar and name_en not in titles:
            titles[name_en] = name_ar

    return titles


def main():
    urls = get_sample_urls()
    print(f"Found {len(urls)} unique sections to scrape")

    all_titles = {}

    for label, url in urls:
        print(f"  [{label}] {url}...")
        try:
            html = fetch_page(url)
        except Exception as e:
            print(f"    FAILED: {e}")
            continue

        titles = extract_titles_from_page(html)
        new_count = 0
        for en, ar in titles.items():
            if en not in all_titles:
                all_titles[en] = ar
                new_count += 1
        print(f"    {new_count} new titles (total: {len(all_titles)})")
        time.sleep(0.5)

    print(f"\nTotal: {len(all_titles)} Arabic titles extracted")

    # Show samples
    for en, ar in list(all_titles.items())[:10]:
        print(f"  {en[:60]}")
        print(f"    -> {ar}")

    # Save
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(all_titles, f, ensure_ascii=False, indent=2, sort_keys=False)
    print(f"\nSaved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
