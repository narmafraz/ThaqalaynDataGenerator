import logging

from app.books import init_books
from app.kafi import init_kafi
from app.kafi_narrators import kafi_narrators
from app.kafi_sarwar import add_kafi_sarwar
from app.link_quran_kafi import link_quran_kafi
from app.quran import init_quran

# from app.quran import init_quran

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init():
    init_books()
    init_quran()
    init_kafi()
    add_kafi_sarwar()
    link_quran_kafi()
    kafi_narrators()

def main():
    logger.info("Creating initial data")
    init()
    logger.info("Initial data created")


if __name__ == "__main__":
    main()
