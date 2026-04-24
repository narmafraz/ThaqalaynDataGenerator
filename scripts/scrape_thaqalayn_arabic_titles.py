"""Scrape Arabic chapter titles for any ThaqalaynAPI-sourced book from thaqalayn.net.

The ThaqalaynAPI (REST JSON) only returns English chapter titles, but the
HTML pages on thaqalayn.net embed sidebar JSON with both ``name_en`` and
``name_ar``. We fetch one hadith page per unique URL section to pick up all
chapter titles for that section, then map them onto canonical chapter
paths via the same in-memory tree the generator builds.

NOTE: thaqalayn.net only has populated ``name_ar`` values for Faqih (and
even there only ~50% of vol 1). All other ThaqalaynAPI books have ``null``
for `name_ar` across the board. See
``app/scrapers/scrape_thaqalayn_net_arabic_titles_notes.md`` for the
assessment. Use other sources (ghbook.ir, al-shia.org, etc.) for non-Faqih
books.

Usage:
    python scripts/scrape_thaqalayn_arabic_titles.py BOOK_SLUG [BOOK_SLUG...]
    python scripts/scrape_thaqalayn_arabic_titles.py --all
    python scripts/scrape_thaqalayn_arabic_titles.py --list

Output: ThaqalaynDataSources/scraped/thaqalayn_net/arabic_chapter_titles/{slug}.json
        (path-keyed: { "/books/al-khisal:1:5": "بَابُ ..." })
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from collections import defaultdict
from typing import Dict, List, Tuple

sys.stdout.reconfigure(encoding="utf-8")

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "app"))

from app import config  # noqa: E402
from app.book_registry import get_book_config  # noqa: E402
from app.models import Chapter  # noqa: E402
from app.thaqalayn_api import (  # noqa: E402
    THAQALAYN_API_BOOKS,
    load_hadiths_multi,
    transform_book,
)

OUTPUT_DIR = os.path.join(
    config.RAW_DIR, "thaqalayn_net", "arabic_chapter_titles"
)

# Match double-escaped JSON embedded in HTML:
#   \"name_en\":\"...\",\"name_ar\":\"...\"
CHAPTER_PATTERN = re.compile(
    r'\\"name_en\\":\\"(.*?)\\",'
    r'\\"name_ar\\":(null|\\".*?\\")'
)


def get_sample_urls(source_folders: List[str]) -> List[Tuple[str, str]]:
    """Get one sample hadith URL per unique URL section across all volumes.

    The thaqalayn.net page for any hadith in a section embeds the full
    sidebar JSON for that section, so a single fetch yields all titles.
    """
    seen_sections = set()
    urls = []
    for folder in source_folders:
        path = os.path.join(config.get_raw_path("thaqalayn_api", folder), "hadiths.json")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for h in data.get("hadiths", []):
            url = h.get("URL", "")
            parts = url.split("/")
            # https://thaqalayn.net/hadith/{book}/{section}/{chapter}/{hadith}
            if len(parts) < 7:
                continue
            section_key = (parts[4], parts[5])
            if section_key not in seen_sections:
                seen_sections.add(section_key)
                urls.append((f"{parts[4]}/{parts[5]}", url))
    return urls


def fetch_page(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "ThaqalaynScraper/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def extract_titles_from_page(html: str) -> Dict[str, str]:
    """Extract all (name_en, name_ar) pairs from embedded JSON in HTML."""
    titles: Dict[str, str] = {}
    for match in CHAPTER_PATTERN.finditer(html):
        name_en = match.group(1)
        name_ar_raw = match.group(2)
        if name_ar_raw == "null" or not name_ar_raw:
            continue
        name_ar = name_ar_raw
        if name_ar.startswith('\\"'):
            name_ar = name_ar[2:]
        if name_ar.endswith('\\"'):
            name_ar = name_ar[:-2]
        if "<" in name_en or "script" in name_en.lower():
            name_en = name_en.split('"]')[0] if '"]' in name_en else name_en
            if "<" in name_en:
                continue
        if name_en and name_ar and name_en not in titles:
            titles[name_en] = name_ar
    return titles


def scrape_en_to_ar(source_folders: List[str], delay: float = 0.5) -> Dict[str, str]:
    """Scrape thaqalayn.net for all (en_title, ar_title) pairs in the book."""
    urls = get_sample_urls(source_folders)
    print(f"  {len(urls)} unique sections to scrape")
    all_titles: Dict[str, str] = {}
    for label, url in urls:
        try:
            html = fetch_page(url)
        except Exception as e:
            print(f"    [{label}] FAILED: {e}")
            continue
        titles = extract_titles_from_page(html)
        new = sum(1 for en in titles if en not in all_titles)
        all_titles.update({en: ar for en, ar in titles.items() if en not in all_titles})
        print(f"    [{label}] +{new} (total {len(all_titles)})")
        time.sleep(delay)
    return all_titles


def collect_path_to_en(chapter: Chapter, mapping: Dict[str, str]) -> None:
    if chapter.path and chapter.titles and chapter.titles.get("en"):
        mapping[chapter.path] = chapter.titles["en"]
    if chapter.chapters:
        for sub in chapter.chapters:
            collect_path_to_en(sub, mapping)


def map_to_paths(slug: str, en_to_ar: Dict[str, str], api_config: dict) -> Tuple[Dict[str, str], int]:
    """Build path→ar mapping using the same in-memory tree the generator builds."""
    book_config = get_book_config(slug)
    if book_config is None:
        raise ValueError(f"No registry entry for {slug}")
    source_folders = api_config["source_folders"]
    translator_name = api_config["translator_name"]
    fr_translator_name = api_config.get("fr_translator_name")
    hadiths = load_hadiths_multi(source_folders)
    if not hadiths:
        raise ValueError(f"No raw data found for {slug}")
    book = transform_book(
        book_config, source_folders[0], translator_name, "en",
        fr_translator_name, hadiths=hadiths,
    )
    path_to_en: Dict[str, str] = {}
    collect_path_to_en(book, path_to_en)

    # Reverse: for each English title in the tree, find the matching scraped Arabic
    en_to_paths: Dict[str, List[str]] = defaultdict(list)
    for path, en in path_to_en.items():
        en_to_paths[en].append(path)

    path_keyed: Dict[str, str] = {}
    collisions = 0
    for en_title, ar_title in en_to_ar.items():
        paths = en_to_paths.get(en_title, [])
        if len(paths) == 1:
            path_keyed[paths[0]] = ar_title
        elif len(paths) > 1:
            # Apply to all paths sharing this English title (best we can do
            # without ID-level matching from the scrape).
            collisions += 1
            for p in paths:
                path_keyed[p] = ar_title
    return dict(sorted(path_keyed.items())), collisions


def scrape_book(slug: str, delay: float = 0.5) -> None:
    api_config = THAQALAYN_API_BOOKS.get(slug)
    if api_config is None:
        raise ValueError(f"Unknown book slug: {slug} (not in THAQALAYN_API_BOOKS)")
    print(f"\n=== {slug} ===")
    en_to_ar = scrape_en_to_ar(api_config["source_folders"], delay=delay)
    print(f"  {len(en_to_ar)} (en, ar) pairs scraped")
    path_keyed, collisions = map_to_paths(slug, en_to_ar, api_config)
    if collisions:
        print(f"  WARN: {collisions} English titles collided across multiple paths")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{slug}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(path_keyed, f, ensure_ascii=False, indent=2)
    print(f"  Wrote {len(path_keyed)} path-keyed entries → {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("slugs", nargs="*", help="Book slugs to scrape")
    parser.add_argument("--all", action="store_true", help="Scrape all ThaqalaynAPI books")
    parser.add_argument("--list", action="store_true", help="List supported book slugs")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests (seconds)")
    args = parser.parse_args()

    if args.list:
        for slug in sorted(THAQALAYN_API_BOOKS):
            print(slug)
        return

    if args.all:
        slugs = sorted(THAQALAYN_API_BOOKS)
    else:
        slugs = args.slugs
    if not slugs:
        parser.error("Provide one or more book slugs, or use --all / --list")

    for slug in slugs:
        try:
            scrape_book(slug, delay=args.delay)
        except Exception as e:
            print(f"  ERROR scraping {slug}: {e}")


if __name__ == "__main__":
    main()
