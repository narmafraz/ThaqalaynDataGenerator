"""Base parser module with shared utilities for book parsers.

Provides common patterns used across quran.py, kafi.py, kafi_sarwar.py,
and thaqalayn_api.py to reduce code duplication.
"""
import logging
import os
from typing import Dict, List, Optional

import fastapi.encoders

from app.lib_db import insert_chapter, write_file
from app.lib_index import add_translation, collect_indexes, update_index_files
from app.lib_model import ProcessingReport, set_index
from app.models import Chapter, Language, PartType, Translation, Verse

logger = logging.getLogger(__name__)


def make_chapter(
    part_type: PartType,
    path: str,
    titles: Optional[Dict[str, str]] = None,
    verse_translations: Optional[List[str]] = None,
) -> Chapter:
    """Create a Chapter with common defaults filled in."""
    chapter = Chapter()
    chapter.part_type = part_type
    chapter.path = path
    if titles:
        chapter.titles = titles
    if verse_translations:
        chapter.verse_translations = verse_translations
    return chapter


def make_verse(
    part_type: PartType,
    text: Optional[List[str]] = None,
    translations: Optional[Dict[str, List[str]]] = None,
    gradings: Optional[List[str]] = None,
    source_url: Optional[str] = None,
) -> Verse:
    """Create a Verse with common defaults filled in."""
    verse = Verse()
    verse.part_type = part_type
    if text:
        verse.text = text
    if translations:
        verse.translations = translations
    if gradings:
        verse.gradings = gradings
    if source_url:
        verse.source_url = source_url
    return verse


def register_translation(
    translation_id: str,
    language: Language,
    name: str,
) -> Translation:
    """Create and register a translation in the global index."""
    translation = Translation()
    translation.id = translation_id
    translation.lang = language
    translation.name = name
    add_translation(translation)
    return translation


def publish_book(
    book: Chapter,
    report: Optional[ProcessingReport] = None,
) -> None:
    """Standard pipeline for publishing a parsed book.

    1. Assign hierarchical indexes and navigation
    2. Write chapter/verse JSON files
    3. Write complete book file
    4. Update language index files
    """
    # Step 1: Set indexes
    set_index(book, [0, 0, 0], 0, report)

    # Step 2: Write individual chapter/verse files
    insert_chapter(book)

    # Step 3: Write complete book file
    book_name = book.path.replace('/books/', '')
    complete_path = f'/books/complete/{book_name}'
    complete_data = fastapi.encoders.jsonable_encoder(book)
    write_file(complete_path, {
        'index': book_name,
        'kind': 'complete_book',
        'data': complete_data,
    })
    logger.info("Published complete book: %s", complete_path)

    # Step 4: Update index files
    index_maps = collect_indexes(book)
    update_index_files(index_maps)
    logger.info("Updated indexes for book: %s", book.path)


def get_parser_raw_path(parser_file: str, *parts: str) -> str:
    """Get path to a raw data file relative to a parser module's location.

    Usage:
        get_parser_raw_path(__file__, "some_dir", "file.xml")
    """
    dirname = os.path.dirname(os.path.abspath(parser_file))
    return os.path.join(dirname, "raw", *parts)
