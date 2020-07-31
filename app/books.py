import json
import logging
import os
import sqlite3
import xml.etree.ElementTree
from sqlite3 import Error
from typing import Dict, List

from sqlalchemy.orm import Session

from app import crud
from app.core import config
# make sure all SQL Alchemy models are imported before initializing DB
# otherwise, SQL Alchemy might fail to initialize relationships properly
# for more details: https://github.com/tiangolo/full-stack-fastapi-postgresql/issues/28
from app.db import base
from app.db.base import Base
from app.db.session import engine
from app.schemas.book_part import BookPartCreate
from app.kafi import BOOK_INDEX as KAFI_INDEX
from app.models import Language
from app.quran import BOOK_INDEX as QURAN_INDEX

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOK_INDEX = "books"
BOOK_PATH = "/books/"

def init_books(db_session: Session):
	data_root = {
		"titles": {
			Language.EN.value: "Books",
		},
		"descriptions": {
			Language.EN.value: "The two weighty things at your fingertips!"
		},
		"chapters": [
			{
				"index": QURAN_INDEX,
				"path": BOOK_PATH + QURAN_INDEX,
				"titles": {
					Language.EN.value: "The Holy Quran",
					Language.AR.value: "القرآن الكريم"
				}
			},
			{
				"index": KAFI_INDEX,
				"path": BOOK_PATH + KAFI_INDEX,
				"titles": {
					Language.EN.value: "Al-Kafi",
					Language.AR.value: "الكافي"
				}
			},
		]
	}

	obj_in = BookPartCreate (
		index = BOOK_INDEX,
		kind = "chapter_list",
		data = data_root,
		last_updated_id = 1
	)
	book = crud.book_part.upsert(db_session, obj_in=obj_in)
	logger.info("Inserted books list into book_part ID %i with index %s", book.id, book.index)
