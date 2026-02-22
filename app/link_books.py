"""Generalized cross-reference linker for all books.

Extends the original link_quran_kafi.py to scan ALL hadith books for Quran
references, not just Al-Kafi. Creates bidirectional relations:
- Hadith verse gets "Mentions": ["/books/quran:S:V", ...]
- Quran verse gets "Mentioned In": ["/books/book-slug:...", ...]

Also replaces [S:V] / (S:V) text with clickable HTML links in translations.
"""

import logging
import re
from typing import List, Set, Tuple

from fastapi.encoders import jsonable_encoder

from app.book_registry import BOOK_REGISTRY
from app.lib_db import load_chapter, write_file
from app.lib_model import get_chapters, get_verses
from app.models import Chapter, PartType, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

QURAN_QUOTE = re.compile(r'[\[\(](\d+):(\d+)[\]\)]')


def _process_translation_text(translation_text_list: List[str], quran_refs: Set[Tuple[int, int]]) -> None:
    """Scan a list of translation strings for Quran references.

    Replaces [S:V] / (S:V) with HTML links and collects (sura, verse) tuples.
    """
    for i, ttext in enumerate(translation_text_list):
        translation_text_list[i] = QURAN_QUOTE.sub(
            r'<a href="/#/books/quran:\1#h\2">[\1:\2]</a>', ttext
        )
        all_matches = QURAN_QUOTE.findall(ttext)
        for match in all_matches:
            quran_refs.add((int(match[0]), int(match[1])))


def _update_refs(quran: Chapter, hadith: Verse, quran_refs: Set[Tuple[int, int]]) -> None:
    """Create bidirectional relations between a hadith and Quran verses."""
    qrefs: Set[str] = set()
    for (sura_no, verse_no) in quran_refs:
        try:
            sura = quran.chapters[sura_no - 1]
            verse = sura.verses[verse_no - 1]
            if not verse.relations:
                verse.relations = {"Mentioned In": set()}
            if "Mentioned In" not in verse.relations:
                verse.relations["Mentioned In"] = set()
            # Ensure it's a set (Pydantic may have loaded it as set from JSON)
            mentioned = verse.relations["Mentioned In"]
            if not isinstance(mentioned, set):
                mentioned = set(mentioned)
                verse.relations["Mentioned In"] = mentioned
            mentioned.add(hadith.path)
            qrefs.add(f"/books/quran:{sura_no}:{verse_no}")
        except IndexError:
            logger.warning(
                "Quran ref does not exist. Hadith %s ref %d:%d",
                hadith.path, sura_no, verse_no,
            )
    if qrefs:
        existing = set(hadith.relations.get("Mentions", set())) if hadith.relations else set()
        hadith.relations = {"Mentions": existing | qrefs}


def _process_chapter_verses(quran: Chapter, chapter: Chapter) -> None:
    """Scan all translations in a chapter's verses for Quran references."""
    for hadith in chapter.verses:
        if hadith.part_type is PartType.Heading:
            continue

        quran_refs: Set[Tuple[int, int]] = set()
        if hadith.translations:
            for _tid, text_list in hadith.translations.items():
                if isinstance(text_list, list):
                    _process_translation_text(text_list, quran_refs)
        _update_refs(quran, hadith, quran_refs)


def _process_chapter(quran: Chapter, chapter: Chapter) -> None:
    """Recursively process a chapter tree for Quran references."""
    chapters = get_chapters(chapter)
    verses = get_verses(chapter)
    if chapters:
        for sub in chapters:
            _process_chapter(quran, sub)
    elif verses:
        _process_chapter_verses(quran, chapter)


def link_all_books_to_quran() -> None:
    """Scan all hadith books for Quran references and create cross-links.

    This replaces the old link_quran_kafi() with a generalized version that
    processes every registered book (except Quran itself).
    """
    quran = load_chapter("/books/complete/quran")
    total_refs = 0

    for book_config in BOOK_REGISTRY:
        if book_config.slug == "quran":
            continue

        slug = book_config.slug
        complete_path = f"/books/complete/{slug}"
        try:
            book = load_chapter(complete_path)
        except (FileNotFoundError, OSError) as e:
            logger.warning("Could not load complete file for %s: %s", slug, e)
            continue

        # Count refs before processing
        _process_chapter(quran, book)

        # Count how many hadiths got relations in this book
        book_refs = _count_relations(book)
        if book_refs > 0:
            logger.info("%s: %d hadiths linked to Quran verses", slug, book_refs)
            total_refs += book_refs

        # Write back the book with updated relations
        write_file(complete_path, jsonable_encoder(book))

    # Write back the Quran with all accumulated "Mentioned In" relations
    write_file("/books/complete/quran", jsonable_encoder(quran))

    logger.info("Total: %d hadiths across all books linked to Quran", total_refs)


def _count_relations(chapter: Chapter) -> int:
    """Count how many verses in a chapter tree have 'Mentions' relations."""
    count = 0
    chapters = get_chapters(chapter)
    verses = get_verses(chapter)
    if chapters:
        for sub in chapters:
            count += _count_relations(sub)
    elif verses:
        for v in verses:
            if v.relations and "Mentions" in v.relations:
                count += 1
    return count
