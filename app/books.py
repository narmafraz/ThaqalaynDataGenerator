import json
import logging
import os
import sqlite3
import xml.etree.ElementTree
from typing import Dict, List

# make sure all SQL Alchemy models are imported before initializing DB
# otherwise, SQL Alchemy might fail to initialize relationships properly
# for more details: https://github.com/tiangolo/full-stack-fastapi-postgresql/issues/28
from app.basair import BOOK_INDEX as BASAIR_INDEX
from app.kafi import BOOK_INDEX as KAFI_INDEX
from app.lib_db import write_file
from app.models import Language
from app.quran import BOOK_INDEX as QURAN_INDEX

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOK_INDEX = "books"
BOOK_PATH = "/books/"

def init_books():
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
			{
				"index": BASAIR_INDEX,
				"path": BOOK_PATH + BASAIR_INDEX,
				"titles": {
					Language.EN.value: "Basa'ir ad-Darajat",
					Language.AR.value: "بَصَائِر ٱلدَّرَجَات"
				}
			},
		]
	}

	obj_in = {
		"index": BOOK_INDEX,
		"kind": "chapter_list",
		"data": data_root
	}
	book = write_file("/books/books", obj_in)
	logger.info("Inserted books list into book_part ID %s with index %s", book.id, book.index)
