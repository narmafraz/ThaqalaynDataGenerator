import logging
import sys

# Fix Windows console encoding for Arabic text output
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from app.books import init_books
from app.kafi import init_kafi
from app.kafi_narrators import process_all_narrators
from app.kafi_sarwar import add_kafi_sarwar
from app.lib_model import ProcessingReport
from app.link_books import link_all_books_to_quran
from app.link_quran_fuzzy import link_fuzzy_quran
from app.link_quran_kafi import link_quran_kafi
from app.quran import init_quran
from app.thaqalayn_api import init_all_thaqalayn_api_books
from app.ghbook import init_ghbook_books
from app.ai_content_merger import merge_ai_content
from app.create_indices import create_indices
from app.link_chapters import link_related_chapters
from app.lib_db import write_file, shellify_complete_books
from app.verse_counts import write_manifest as write_verse_counts

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init():
    report = ProcessingReport()
    init_books()
    init_quran()
    init_kafi(report)
    add_kafi_sarwar(report)
    link_quran_kafi()
    init_all_thaqalayn_api_books()
    init_ghbook_books()
    link_all_books_to_quran()
    link_fuzzy_quran()
    process_all_narrators(report)   # Replaces kafi_narrators(); runs after all books loaded
    create_indices()
    link_related_chapters()
    merge_ai_content(report)
    shellify_complete_books()
    _write_verse_counts()
    _write_narrator_analysis()
    _write_data_version()
    report.print_summary()


def _write_data_version():
    """Write index/data_version.json with a timestamp for client cache invalidation."""
    from datetime import datetime, timezone
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    write_file("/index/data_version", {"version": version})


def _write_verse_counts():
    """Rebuild index/verse-counts.json — consumed by the Angular reading-progress tracker.

    Runs after all book parsers + shellify so chapter shells are final on disk.
    """
    import os
    from pathlib import Path
    dest = Path(os.environ.get("DESTINATION_DIR", "../ThaqalaynData/")).resolve()
    out = write_verse_counts(dest)
    logger.info("Wrote verse-counts manifest: %s", out)

def _write_narrator_analysis():
    """Write per-chapter narrator-analysis sidecars (`{chapter}.narrators.json`).

    Powers the opt-in "Narrator insights" panel on chapter pages. Runs last so
    it sees final shells + merged AI isnad data. Two-pass: a corpus-wide
    narrator role profile, then per-chapter analysis.
    """
    import os
    from pathlib import Path
    from app import narrator_analysis
    dest = Path(os.environ.get("DESTINATION_DIR", "../ThaqalaynData/")).resolve()
    written = narrator_analysis.build(dest)
    logger.info("Wrote %d narrator-analysis sidecars", len(written))


def main():
    logger.info("Creating initial data")
    init()
    logger.info("Initial data created")


if __name__ == "__main__":
    main()
