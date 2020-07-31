import logging

from app.books import init_books
from app.db.session import db_session
from app.kafi import init_kafi
from app.quran import init_quran

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init():
    init_books(db_session)
    init_quran(db_session)
    init_kafi(db_session)


def main():
    logger.info("Creating initial data")
    init()
    logger.info("Initial data created")


if __name__ == "__main__":
    main()
