"""Integration module for ghbook.ir books (Tahdhib al-Ahkam, al-Istibsar).

Loads HTML from ghbook.ir, parses into Chapter/Verse hierarchy using
ghbook_parser, and publishes to ThaqalaynData via the standard pipeline.
"""

import logging

from app.base_parser import publish_book
from app.book_registry import get_book_config
from app.ghbook_parser import load_html, parse_tahdhib, parse_istibsar, count_hadiths, count_babs

logger = logging.getLogger(__name__)


def _init_book(slug, parse_fn):
    """Parse and publish a ghbook.ir book."""
    book_config = get_book_config(slug)
    if book_config is None:
        logger.warning("No book registry entry for slug: %s", slug)
        return

    try:
        soup = load_html(slug)
    except FileNotFoundError:
        logger.info("Skipping %s: HTML file not found", slug)
        return

    book = parse_fn(soup)
    book.path = book_config.path
    book.titles = book_config.titles
    book.verse_start_index = 0
    for vol in (book.chapters or []):
        vol.verse_start_index = 0

    hadiths = count_hadiths(book)
    babs = count_babs(book)
    vols = len(book.chapters or [])
    logger.info("Parsed %s: %d volumes, %d babs, %d hadiths", slug, vols, babs, hadiths)

    publish_book(book)
    logger.info("Published %s", slug)


def init_ghbook_books():
    """Parse and publish all ghbook.ir books."""
    _init_book("tahdhib-al-ahkam", parse_tahdhib)
    _init_book("al-istibsar", parse_istibsar)
