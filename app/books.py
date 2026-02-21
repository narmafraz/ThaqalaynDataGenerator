import logging
from typing import Dict, List

from app.book_registry import BOOK_REGISTRY
from app.lib_db import write_file
from app.models import Language

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOK_INDEX = "books"
BOOK_PATH = "/books/"

def init_books():
	chapters = []
	for book_config in BOOK_REGISTRY:
		entry = {
			"index": book_config.index,
			"path": book_config.path,
			"titles": book_config.titles,
		}
		if book_config.descriptions:
			entry["descriptions"] = book_config.descriptions
		if book_config.author:
			entry["author"] = book_config.author
		if book_config.translator:
			entry["translator"] = book_config.translator
		if book_config.source_url:
			entry["source_url"] = book_config.source_url
		chapters.append(entry)

	data_root = {
		"titles": {
			Language.EN.value: "Books",
		},
		"descriptions": {
			Language.EN.value: "The two weighty things at your fingertips!"
		},
		"chapters": chapters
	}

	obj_in = {
		"index": BOOK_INDEX,
		"kind": "chapter_list",
		"data": data_root
	}
	book = write_file("/books/books", obj_in)
	logger.info("Inserted books list into book_part ID %s with index %s", book.id, book.index)
