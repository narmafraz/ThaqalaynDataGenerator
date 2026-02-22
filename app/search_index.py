"""Generate search document JSON files for Orama full-text search indexes.

Reads ThaqalaynData JSON files and produces:
1. titles.json - lightweight index of all book/chapter/surah titles for instant search
2. {book-slug}-docs.json - full-text search documents per book (Arabic + English translations)

All book directories under books/ are automatically discovered and indexed.

Arabic text is normalized (strip tashkeel, normalize letter forms, remove tatweel)
to enable fuzzy Arabic search. The original text is preserved for display; the
normalized version is stored in a separate field for search matching.

Output is written to ThaqalaynData/index/search/.
"""

import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional

from app.arabic_normalization import normalize_arabic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# HTML tag stripping for English translations that contain <sup>, <span>, etc.
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
# Footnote reference pattern like [1], [2] etc.
FOOTNOTE_PATTERN = re.compile(r"\[\d+\]")


def strip_html(text: str) -> str:
    """Remove HTML tags and footnote references from text."""
    text = HTML_TAG_PATTERN.sub("", text)
    text = FOOTNOTE_PATTERN.sub("", text)
    return text.strip()


def get_data_dir() -> str:
    """Get the ThaqalaynData directory path."""
    return os.environ.get(
        "DESTINATION_DIR",
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "ThaqalaynData"),
    )


def load_json_file(filepath: str) -> Optional[dict]:
    """Load and parse a JSON file, returning None on error."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError, OSError) as e:
        logger.warning("Failed to load %s: %s", filepath, e)
        return None


def _is_generic_chapter_title(title_en: str, part_type: str) -> bool:
    """Check if a title is a generic 'Chapter N' with no descriptive content."""
    if part_type != "Chapter":
        return False
    return bool(re.match(r"^Chapter \d+$", title_en))


def build_titles_index(data_dir: str) -> List[dict]:
    """Build a lightweight titles index from book index files.

    Produces one document per book/chapter/section path with titles in
    Arabic and English (both original and normalized).

    Generic chapter titles (just "Chapter N" with no Arabic title and
    no descriptive English subtitle) are excluded to keep the index
    small (~100KB target for immediate loading).
    """
    # First pass: collect all entries keyed by path
    entries_by_path: Dict[str, dict] = {}

    for lang_suffix in ["en", "ar"]:
        index_path = os.path.join(data_dir, "index", f"books.{lang_suffix}.json")
        index_data = load_json_file(index_path)
        if not index_data:
            continue

        for path, entry in index_data.items():
            title = entry.get("title", "")
            if not title:
                continue

            clean_title = strip_html(title)

            if path not in entries_by_path:
                entries_by_path[path] = {
                    "path": path,
                    "part_type": entry.get("part_type", ""),
                    "title_en": "",
                    "title_ar": "",
                    "title_ar_normalized": "",
                }

            if lang_suffix == "en":
                entries_by_path[path]["title_en"] = clean_title
            else:
                entries_by_path[path]["title_ar"] = clean_title
                entries_by_path[path]["title_ar_normalized"] = normalize_arabic(clean_title)

    # Second pass: filter out generic chapter titles that add no search value
    docs = []
    skipped = 0
    for entry in entries_by_path.values():
        title_en = entry.get("title_en", "")
        title_ar = entry.get("title_ar", "")
        part_type = entry.get("part_type", "")

        # Skip generic "Chapter N" entries that have no Arabic title
        if _is_generic_chapter_title(title_en, part_type) and not title_ar:
            skipped += 1
            continue

        # Use compact keys to minimize JSON size
        doc = {
            "p": entry["path"],
            "pt": entry["part_type"],
            "en": entry["title_en"],
            "ar": entry["title_ar"],
            "arn": entry["title_ar_normalized"],
        }
        docs.append(doc)

    logger.info(
        "Built titles index with %d entries (skipped %d generic chapters)",
        len(docs), skipped,
    )
    return docs


def extract_verse_docs(chapter_json: dict, book_slug: str) -> List[dict]:
    """Extract search documents from a verse_list chapter JSON.

    Each verse becomes a search document with:
    - p: verse path for linking (short key to save space)
    - t: chapter title (English) for result display context
    - ar: normalized Arabic text for Arabic search
    - en: primary English translation for English search (HTML stripped)
    - i: local_index (verse/hadith number within chapter)

    The original (diacritized) Arabic text is NOT stored here -- it's
    available from the verse data files and would double the index size.
    The chapter path and book slug are derivable from the verse path.
    """
    data = chapter_json.get("data", {})
    verses = data.get("verses", [])
    if not verses:
        return []

    chapter_titles = data.get("titles", {})
    chapter_title_en = strip_html(chapter_titles.get("en", ""))

    # Determine default English translation
    default_trans = data.get("default_verse_translation_ids", {})
    default_en_trans = default_trans.get("en", "")

    docs = []
    for verse in verses:
        verse_path = verse.get("path", "")
        if not verse_path:
            continue

        # Arabic text (normalized for search)
        text_parts = verse.get("text", [])
        text_ar = " ".join(text_parts) if text_parts else ""

        # English translation - use default, fallback to first en.* translation
        translations = verse.get("translations", {})
        text_en = ""
        if default_en_trans and default_en_trans in translations:
            text_en = " ".join(translations[default_en_trans])
        else:
            for tid, ttext in translations.items():
                if tid.startswith("en.") and tid != "en.transliteration":
                    text_en = " ".join(ttext)
                    break

        text_en = strip_html(text_en)

        doc = {
            "p": verse_path,
            "t": chapter_title_en,
            "ar": normalize_arabic(text_ar) if text_ar.strip() else "",
            "en": text_en.strip(),
            "i": verse.get("local_index", 0),
        }
        docs.append(doc)

    return docs


def build_book_docs(data_dir: str, book_slug: str) -> List[dict]:
    """Build full-text search documents for a book by walking its JSON files.

    Finds all verse_list JSON files for the given book and extracts
    search documents from each.
    """
    book_dir = os.path.join(data_dir, "books", book_slug)
    if not os.path.isdir(book_dir):
        logger.warning("Book directory not found: %s", book_dir)
        return []

    docs = []
    file_count = 0

    for root, _dirs, files in os.walk(book_dir):
        for filename in files:
            if not filename.endswith(".json"):
                continue
            filepath = os.path.join(root, filename)
            chapter_json = load_json_file(filepath)
            if not chapter_json:
                continue
            if chapter_json.get("kind") != "verse_list":
                continue
            file_count += 1
            verse_docs = extract_verse_docs(chapter_json, book_slug)
            docs.extend(verse_docs)

    logger.info(
        "Built %d search docs from %d verse_list files for %s",
        len(docs), file_count, book_slug,
    )
    return docs


def discover_book_slugs(data_dir: str) -> List[str]:
    """Discover all book slugs by listing directories under books/.

    Returns a sorted list of directory names (e.g. ['al-kafi', 'quran', ...]),
    excluding the 'complete' directory which contains aggregated files.
    """
    books_dir = os.path.join(data_dir, "books")
    if not os.path.isdir(books_dir):
        logger.warning("Books directory not found: %s", books_dir)
        return []

    slugs = []
    for entry in os.listdir(books_dir):
        entry_path = os.path.join(books_dir, entry)
        if os.path.isdir(entry_path) and entry != "complete":
            slugs.append(entry)

    slugs.sort()
    logger.info("Discovered %d book directories: %s", len(slugs), ", ".join(slugs))
    return slugs


def write_search_json(data_dir: str, filename: str, docs: list) -> str:
    """Write search documents to the search index directory."""
    search_dir = os.path.join(data_dir, "index", "search")
    os.makedirs(search_dir, exist_ok=True)
    filepath = os.path.join(search_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False)
    size_kb = os.path.getsize(filepath) / 1024
    logger.info("Wrote %s (%.1f KB, %d documents)", filepath, size_kb, len(docs))
    return filepath


def generate_search_indexes(data_dir: Optional[str] = None) -> dict:
    """Generate all search index document files.

    Returns a dict mapping filename to document count.
    """
    if data_dir is None:
        data_dir = get_data_dir()

    results = {}

    # 1. Titles index (small, loaded immediately)
    titles = build_titles_index(data_dir)
    write_search_json(data_dir, "titles.json", titles)
    results["titles.json"] = len(titles)

    # 2. Per-book full-text indexes (lazy-loaded on demand)
    book_slugs = discover_book_slugs(data_dir)
    book_files: Dict[str, str] = {}
    for book_slug in book_slugs:
        filename = f"{book_slug}-docs.json"
        docs = build_book_docs(data_dir, book_slug)
        write_search_json(data_dir, filename, docs)
        results[filename] = len(docs)
        book_files[book_slug] = filename

    # 3. Write metadata file documenting the schema for the frontend
    metadata = {
        "version": 1,
        "language": "arabic",
        "schemas": {
            "titles": {
                "file": "titles.json",
                "description": "Book/chapter/surah titles for instant navigation search",
                "fields": {
                    "p": {"type": "string", "description": "Path (e.g. /books/al-kafi:1:2)"},
                    "pt": {"type": "string", "description": "Part type (Volume, Book, Chapter)"},
                    "en": {"type": "string", "description": "English title", "searchable": True},
                    "ar": {"type": "string", "description": "Arabic title (with diacritics, for display)"},
                    "arn": {"type": "string", "description": "Normalized Arabic title (for search)", "searchable": True},
                },
                "orama_schema": {"p": "string", "pt": "string", "en": "string", "ar": "string", "arn": "string"},
            },
            "book": {
                "files": book_files,
                "description": "Full-text verse/hadith content for per-book search",
                "fields": {
                    "p": {"type": "string", "description": "Verse path (e.g. /books/quran:1:1)"},
                    "t": {"type": "string", "description": "Chapter title (English)"},
                    "ar": {"type": "string", "description": "Normalized Arabic text (for search)", "searchable": True},
                    "en": {"type": "string", "description": "English translation (for search)", "searchable": True},
                    "i": {"type": "number", "description": "Local index (verse/hadith number)"},
                },
                "orama_schema": {"p": "string", "t": "string", "ar": "string", "en": "string", "i": "number"},
            },
        },
        "notes": {
            "arabic_search": "Use 'arn' field for titles and 'ar' field for verses. Text is normalized: diacritics stripped, letter forms unified (hamza->alef, teh marbuta->heh, alef maksura->yeh), tatweel removed.",
            "orama_config": "Create Orama instances with language: 'arabic' for proper Arabic tokenization. This also handles English text correctly.",
            "loading_strategy": "Load titles.json immediately on app init. Load book doc files on-demand when user searches within a specific book.",
        },
    }
    write_search_json(data_dir, "search-meta.json", metadata)

    return results


def main():
    """CLI entry point for generating search indexes."""
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")

    data_dir = get_data_dir()
    logger.info("Generating search indexes from %s", data_dir)
    results = generate_search_indexes(data_dir)

    print("\nSearch index generation complete:")
    for filename, count in results.items():
        print(f"  {filename}: {count} documents")


if __name__ == "__main__":
    main()
