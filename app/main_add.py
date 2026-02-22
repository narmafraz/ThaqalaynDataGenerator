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
from app.create_indices import create_indices

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
    link_all_books_to_quran()
    create_indices()
    report.print_summary()

def main():
    logger.info("Creating initial data")
    init()
    logger.info("Initial data created")


if __name__ == "__main__":
    main()
