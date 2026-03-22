"""Fuzzy Arabic text matching between hadiths and Quran verses.

Detects hadiths that quote Quran verbatim (or near-verbatim) in Arabic text
without explicit [S:V] citations. Uses 3-word n-gram shingling with Arabic
normalization to find matches.

Creates bidirectional relations:
- Hadith gets "Quotes Quran": ["/books/quran:S:V", ...]
- Quran verse gets "Quoted In": ["/books/book-slug:...", ...]

Skips matches where the hadith already has a "Mentions" relation to that Quran
verse (from the text-based [S:V] linker in link_books.py).
"""

import logging
import re
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from fastapi.encoders import jsonable_encoder

from app.book_registry import BOOK_REGISTRY
from app.lib_db import load_chapter, write_file
from app.lib_model import get_chapters, get_verses
from app.link_books import _propagate_to_modular_files

logger = logging.getLogger(__name__)

SHINGLE_SIZE = 3       # 3-word sliding windows
MIN_QURAN_WORDS = 4    # Skip very short Quran verses
MATCH_THRESHOLD = 0.5  # At least 50% of Quran verse shingles must match

# Arabic diacritics range
_DIACRITICS_RE = re.compile(r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06E8]')
_ALIF_RE = re.compile(r'[إأآا]')
_YA_RE = re.compile(r'[يى]')


def _normalize_arabic(text: str) -> str:
    """Strip diacritics and normalize Arabic letter variants."""
    text = _DIACRITICS_RE.sub('', text)
    text = _ALIF_RE.sub('ا', text)
    text = text.replace('ة', 'ه')
    text = _YA_RE.sub('ي', text)
    return text


def _tokenize_arabic(text: str) -> List[str]:
    """Normalize Arabic text and split into words."""
    normalized = _normalize_arabic(text)
    # Split on whitespace and strip punctuation
    words = re.findall(r'[\u0621-\u064A\u0660-\u0669]+', normalized)
    return words


def _make_shingles(words: List[str], n: int = SHINGLE_SIZE) -> Set[str]:
    """Generate n-word shingle set from a word list."""
    if len(words) < n:
        return set()
    return {' '.join(words[i:i + n]) for i in range(len(words) - n + 1)}


def _build_quran_index(quran) -> Dict[str, List[Tuple[str, int]]]:
    """Build a shingle-to-verses index from all Quran verses.

    Returns: dict mapping each shingle to list of (quran_verse_path, total_shingles_count).
    Also returns a dict of path -> total shingle count for scoring.
    """
    shingle_index: Dict[str, List[str]] = defaultdict(list)
    verse_shingle_counts: Dict[str, int] = {}

    for sura in quran.chapters:
        if not sura.verses:
            continue
        for verse in sura.verses:
            if not verse.text or not verse.text[0]:
                continue

            words = _tokenize_arabic(verse.text[0])
            if len(words) < MIN_QURAN_WORDS:
                continue

            shingles = _make_shingles(words)
            if not shingles:
                continue

            verse_shingle_counts[verse.path] = len(shingles)
            for shingle in shingles:
                shingle_index[shingle].append(verse.path)

    return shingle_index, verse_shingle_counts


def _scan_hadith_verse(verse, shingle_index, verse_shingle_counts) -> Dict[str, float]:
    """Check a single hadith verse against the Quran shingle index.

    Returns: dict of quran_path -> match_score for paths above threshold.
    """
    if not verse.text or not verse.text[0]:
        return {}

    words = _tokenize_arabic(verse.text[0])
    hadith_shingles = _make_shingles(words)
    if not hadith_shingles:
        return {}

    # Count matching shingles per Quran verse
    match_counts: Dict[str, int] = defaultdict(int)
    for shingle in hadith_shingles:
        if shingle in shingle_index:
            for qpath in shingle_index[shingle]:
                match_counts[qpath] += 1

    # Score and filter
    results = {}
    for qpath, count in match_counts.items():
        total = verse_shingle_counts.get(qpath, 1)
        score = count / total
        if score >= MATCH_THRESHOLD:
            results[qpath] = score

    return results


def _process_book_fuzzy(
    book,
    quran,
    shingle_index: Dict[str, List[str]],
    verse_shingle_counts: Dict[str, int],
) -> int:
    """Recursively scan a book for fuzzy Quran matches.

    Adds "Quotes Quran" relations to hadith verses and "Quoted In" to Quran
    verses. Returns count of hadith verses that got new relations.
    """
    linked = [0]  # Use list for closure mutation

    def _walk(chapter):
        chapters = get_chapters(chapter)
        verses = get_verses(chapter)
        if chapters:
            for sub in chapters:
                _walk(sub)
        elif verses:
            for verse in verses:
                if hasattr(verse, 'part_type') and verse.part_type and verse.part_type.value == 'Heading':
                    continue

                matches = _scan_hadith_verse(verse, shingle_index, verse_shingle_counts)
                if not matches:
                    continue

                # Filter out already-linked Quran verses (from text-based [S:V] linker)
                existing_mentions = set()
                if verse.relations and "Mentions" in verse.relations:
                    existing_mentions = set(verse.relations["Mentions"])

                new_refs = set()
                for qpath in matches:
                    if qpath not in existing_mentions:
                        new_refs.add(qpath)

                if not new_refs:
                    continue

                # Add "Quotes Quran" to hadith
                if not verse.relations:
                    verse.relations = {}
                existing_quotes = set(verse.relations.get("Quotes Quran", set()))
                verse.relations["Quotes Quran"] = existing_quotes | new_refs

                # Add "Quoted In" to Quran verses
                for qpath in new_refs:
                    _add_quoted_in(quran, qpath, verse.path)

                linked[0] += 1

    _walk(book)
    return linked[0]


def _add_quoted_in(quran, quran_path: str, hadith_path: str):
    """Add a 'Quoted In' relation to a Quran verse."""
    # Parse quran path: /books/quran:S:V
    parts = quran_path.split(':')
    if len(parts) < 3:
        return
    try:
        sura_no = int(parts[1])
        verse_no = int(parts[2])
        sura = quran.chapters[sura_no - 1]
        verse = sura.verses[verse_no - 1]

        if not verse.relations:
            verse.relations = {}
        if "Quoted In" not in verse.relations:
            verse.relations["Quoted In"] = set()
        quoted = verse.relations["Quoted In"]
        if not isinstance(quoted, set):
            quoted = set(quoted)
            verse.relations["Quoted In"] = quoted
        quoted.add(hadith_path)
    except (IndexError, ValueError):
        pass


def link_fuzzy_quran() -> None:
    """Main entry point: scan all hadith books for fuzzy Quran text matches."""
    quran = load_chapter("/books/complete/quran")

    logger.info("Building Quran shingle index...")
    shingle_index, verse_shingle_counts = _build_quran_index(quran)
    logger.info(
        "Quran index: %d unique shingles from %d verses",
        len(shingle_index), len(verse_shingle_counts),
    )

    total_linked = 0
    total_patched = 0

    for book_config in BOOK_REGISTRY:
        if book_config.slug == "quran":
            continue

        complete_path = f"/books/complete/{book_config.slug}"
        try:
            book = load_chapter(complete_path)
        except (FileNotFoundError, OSError) as e:
            logger.warning("Could not load %s: %s", book_config.slug, e)
            continue

        linked = _process_book_fuzzy(book, quran, shingle_index, verse_shingle_counts)
        if linked > 0:
            logger.info("%s: %d hadiths fuzzy-matched to Quran", book_config.slug, linked)
            total_linked += linked

            # Write back with updated relations
            write_file(complete_path, jsonable_encoder(book))

            # Propagate to modular files
            patched = _propagate_to_modular_files(book)
            if patched > 0:
                total_patched += patched

    # Write back Quran with accumulated "Quoted In"
    write_file("/books/complete/quran", jsonable_encoder(quran))
    quran_patched = _propagate_to_modular_files(quran)
    total_patched += quran_patched

    logger.info("Fuzzy Quran matching: %d hadiths linked, %d modular files patched", total_linked, total_patched)
