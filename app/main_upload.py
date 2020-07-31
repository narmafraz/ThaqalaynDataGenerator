import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from app.db import base
from app.db.base import Base
from app.db.session import db_session as src_db_session
from app.db.session import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate(src, dest):

    pass

def main():
    logger.info("Moving databases")

    dest_engine = create_engine(os.getenv("DATABASE_URL_DEST"), pool_pre_ping=True)
    dest_db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=dest_engine)
    )
    Base.metadata.create_all(bind=dest_engine)
    migrate(src_db_session, dest_db_session)
    logger.info("Finished moving database")


if __name__ == "__main__":
    main()
