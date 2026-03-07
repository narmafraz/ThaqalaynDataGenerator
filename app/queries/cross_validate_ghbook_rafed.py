"""Cross-validate ghbook.ir parsed data against rafed.net scraped pages.

Compares:
1. Hadith counts per volume (ghbook parser vs rafed page text)
2. Text overlap — spot-checks that ghbook hadiths appear in rafed text
3. Chapter/bab name matching between ghbook headings and rafed TOC

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    PYTHONPATH="$PWD:$PWD/app" SOURCE_DATA_DIR="../ThaqalaynDataSources/" python app/queries/cross_validate_ghbook_rafed.py
    # Or for a single book:
    PYTHONPATH="$PWD:$PWD/app" SOURCE_DATA_DIR="../ThaqalaynDataSources/" python app/queries/cross_validate_ghbook_rafed.py --tahdhib
    PYTHONPATH="$PWD:$PWD/app" SOURCE_DATA_DIR="../ThaqalaynDataSources/" python app/queries/cross_validate_ghbook_rafed.py --istibsar
"""

import json
import os
import re
import sys
import unicodedata

sys.stdout.reconfigure(encoding="utf-8")

from app import config
from app.ghbook_parser import (
    load_html, parse_tahdhib, parse_istibsar,
    count_hadiths, count_babs,
)

RAFED_DIR = os.path.join(
    os.environ.get("SOURCE_DATA_DIR", os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "..", "ThaqalaynDataSources"
    )),
    "scraped", "rafed_net",
)

# Hadith-start patterns for rafed text (same as ghbook_parser but more lenient)
TAHDHIB_HADITH_RE = re.compile(r'^\s*(?:\((\d+)\)\s*)?(\d+)\s*[-\u2013ـ]\s*')
ISTIBSAR_HADITH_RE = re.compile(r'^\s*(\d+)\s*[-\u2013ـ]\s*')


def normalize_arabic(text):
    """Normalize Arabic text for comparison (remove diacritics, normalize letters)."""
    # Remove tashkeel (diacritics)
    result = ""
    for ch in text:
        if unicodedata.category(ch) != "Mn":  # Mark, Nonspacing
            result += ch
    # Normalize common letter variants
    result = result.replace("\u0649", "\u064a")  # alef maksura -> yeh
    result = result.replace("\u06a9", "\u0643")  # persian kaf -> arabic kaf
    result = result.replace("\u06cc", "\u064a")  # farsi yeh -> arabic yeh
    # Remove zero-width chars
    for zw in ["\u200c", "\u200d", "\u200b", "\u200f", "\u200e", "\u061c", "\ufeff"]:
        result = result.replace(zw, "")
    # Collapse whitespace
    result = re.sub(r'\s+', ' ', result).strip()
    return result


def load_rafed_pages(book_key, vol_num):
    """Load all paragraphs from rafed.net scraped pages for a volume."""
    vol_dir = os.path.join(RAFED_DIR, book_key, "pages", "vol-{}".format(vol_num))
    if not os.path.exists(vol_dir):
        return []

    all_paragraphs = []
    page_files = sorted(
        [f for f in os.listdir(vol_dir) if f.endswith(".json")],
        key=lambda f: int(re.search(r'(\d+)', f).group(1))
    )
    for filename in page_files:
        with open(os.path.join(vol_dir, filename), "r", encoding="utf-8") as f:
            data = json.load(f)
        all_paragraphs.extend(data.get("paragraphs", []))
    return all_paragraphs


def load_rafed_toc(book_key):
    """Load TOC entries from rafed.net."""
    toc_path = os.path.join(RAFED_DIR, book_key, "toc.json")
    if not os.path.exists(toc_path):
        return {}
    with open(toc_path, "r", encoding="utf-8") as f:
        return json.load(f)


def count_rafed_hadiths(paragraphs, hadith_re):
    """Count hadith-starting paragraphs in rafed text."""
    count = 0
    for p in paragraphs:
        if hadith_re.match(p.strip()):
            count += 1
    return count


def get_rafed_full_text(paragraphs):
    """Combine all paragraphs into one normalized text blob."""
    return normalize_arabic(" ".join(paragraphs))


def extract_text_snippet(verse_text, max_words=8):
    """Extract first N words from a verse's Arabic text for searching."""
    words = normalize_arabic(verse_text).split()
    return " ".join(words[:max_words])


def check_text_overlap(ghbook_verses, rafed_full_text, sample_size=20):
    """Check what fraction of ghbook hadiths can be found in rafed text.

    Searches for the first few words of each sampled hadith in the rafed text.
    """
    import random

    if not ghbook_verses or not rafed_full_text:
        return 0, 0, []

    # Sample hadiths to check
    if len(ghbook_verses) <= sample_size:
        sample = ghbook_verses
    else:
        sample = random.sample(ghbook_verses, sample_size)

    found = 0
    missing = []
    for verse in sample:
        if not verse.text:
            continue
        snippet = extract_text_snippet(verse.text[0])
        if not snippet or len(snippet) < 10:
            continue
        if snippet in rafed_full_text:
            found += 1
        else:
            # Try shorter snippet
            short = " ".join(snippet.split()[:4])
            if short in rafed_full_text:
                found += 1
            else:
                missing.append(snippet[:60])

    total = len(sample)
    return found, total, missing


def check_toc_overlap(ghbook_babs, rafed_toc_entries):
    """Check how many ghbook bab names appear in rafed TOC."""
    if not ghbook_babs or not rafed_toc_entries:
        return 0, 0

    rafed_titles_norm = set()
    for entry in rafed_toc_entries:
        rafed_titles_norm.add(normalize_arabic(entry["title"]))

    found = 0
    for bab_title in ghbook_babs:
        bab_norm = normalize_arabic(bab_title)
        # Check for substring match (TOC entries may be truncated)
        for rafed_title in rafed_titles_norm:
            if bab_norm in rafed_title or rafed_title in bab_norm:
                found += 1
                break

    return found, len(ghbook_babs)


def validate_book(book_key, parse_fn, hadith_re):
    """Run all cross-validation checks for a book."""
    print("\n" + "=" * 70)
    print("Cross-validating: {}".format(book_key))
    print("=" * 70)

    # Parse ghbook
    try:
        soup = load_html(book_key)
    except FileNotFoundError:
        print("  SKIP: ghbook.ir HTML not found")
        return
    book = parse_fn(soup)

    # Load rafed TOC
    toc_data = load_rafed_toc(book_key)

    total_ghbook = 0
    total_rafed = 0
    total_overlap_found = 0
    total_overlap_checked = 0
    total_toc_found = 0
    total_toc_checked = 0

    for vol_idx, vol in enumerate(book.chapters or [], 1):
        babs = vol.chapters or []
        ghbook_count = sum(len(bab.verses or []) for bab in babs)
        total_ghbook += ghbook_count

        # Rafed data
        paragraphs = load_rafed_pages(book_key, vol_idx)
        rafed_count = count_rafed_hadiths(paragraphs, hadith_re)
        total_rafed += rafed_count

        diff = ghbook_count - rafed_count
        pct = (diff / ghbook_count * 100) if ghbook_count else 0
        status = "OK" if abs(pct) < 15 else "WARN" if abs(pct) < 30 else "MISMATCH"

        print("\n  Vol {}: ghbook={} hadiths, rafed=~{} hadiths (diff={}, {:.1f}%) [{}]".format(
            vol_idx, ghbook_count, rafed_count, diff, pct, status))
        print("    ghbook: {} babs".format(len(babs)))

        # Text overlap check
        all_verses = []
        for bab in babs:
            all_verses.extend(bab.verses or [])
        rafed_full = get_rafed_full_text(paragraphs)
        found, checked, missing = check_text_overlap(all_verses, rafed_full)
        total_overlap_found += found
        total_overlap_checked += checked
        if checked > 0:
            print("    Text overlap: {}/{} sampled hadiths found in rafed ({:.0f}%)".format(
                found, checked, found / checked * 100))
            if missing:
                print("    Missing snippets (first 3):")
                for m in missing[:3]:
                    print("      - {}...".format(m))

        # TOC overlap check
        toc_vol_key = "vol_{}".format(vol_idx)
        toc_entries = toc_data.get("volumes", {}).get(toc_vol_key, {}).get("entries", [])
        bab_titles = [bab.titles.get("ar", "") for bab in babs if bab.titles.get("ar")]
        toc_found, toc_total = check_toc_overlap(bab_titles, toc_entries)
        total_toc_found += toc_found
        total_toc_checked += toc_total
        if toc_total > 0 and toc_entries:
            print("    TOC overlap: {}/{} ghbook babs matched in rafed TOC ({:.0f}%), rafed has {} TOC entries".format(
                toc_found, toc_total, toc_found / toc_total * 100, len(toc_entries)))

    # Summary
    print("\n  " + "-" * 60)
    print("  TOTALS:")
    print("    Hadiths: ghbook={}, rafed=~{}".format(total_ghbook, total_rafed))
    if total_overlap_checked > 0:
        print("    Text overlap: {}/{} ({:.0f}%)".format(
            total_overlap_found, total_overlap_checked,
            total_overlap_found / total_overlap_checked * 100))
    if total_toc_checked > 0:
        print("    TOC overlap: {}/{} ({:.0f}%)".format(
            total_toc_found, total_toc_checked,
            total_toc_found / total_toc_checked * 100))


def main():
    books = []
    if "--tahdhib" in sys.argv:
        books = [("tahdhib-al-ahkam", parse_tahdhib, TAHDHIB_HADITH_RE)]
    elif "--istibsar" in sys.argv:
        books = [("al-istibsar", parse_istibsar, ISTIBSAR_HADITH_RE)]
    else:
        books = [
            ("tahdhib-al-ahkam", parse_tahdhib, TAHDHIB_HADITH_RE),
            ("al-istibsar", parse_istibsar, ISTIBSAR_HADITH_RE),
        ]

    for book_key, parse_fn, hadith_re in books:
        validate_book(book_key, parse_fn, hadith_re)

    print("\n" + "=" * 70)
    print("Cross-validation complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
