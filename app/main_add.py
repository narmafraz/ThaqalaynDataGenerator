import logging
import sys

# Fix Windows console encoding for Arabic text output
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from app.books import init_books
from app.kafi import init_kafi
from app.kafi_narrators import kafi_narrators
from app.kafi_sarwar import add_kafi_sarwar
from app.lib_model import ProcessingReport
from app.link_books import link_all_books_to_quran
from app.link_quran_kafi import link_quran_kafi
from app.quran import init_quran
from app.thaqalayn_api import init_all_thaqalayn_api_books
from app.ghbook import init_ghbook_books
from app.ai_content_merger import merge_ai_content
from app.create_indices import create_indices
from app.lib_db import write_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init():
    report = ProcessingReport()
    init_books()
    init_quran()
    init_kafi(report)
    add_kafi_sarwar(report)
    link_quran_kafi()
    kafi_narrators(report)
    init_all_thaqalayn_api_books()
    init_ghbook_books()
    link_all_books_to_quran()
    create_indices()
    merge_ai_content(report)
    _write_data_version()
    report.print_summary()


def _write_data_version():
    """Write index/data_version.json with a timestamp for client cache invalidation."""
    from datetime import datetime, timezone
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    write_file("/index/data_version", {"version": version})

def main():
    logger.info("Creating initial data")
    init()
    logger.info("Initial data created")


if __name__ == "__main__":
    main()
