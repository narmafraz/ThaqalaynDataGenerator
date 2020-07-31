import logging

from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app import crud
from app.schemas.book_part import BookPartCreate
from app.lib_model import has_chapters, has_verses
from app.models import Chapter, Language, Quran, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def index_from_path(path: str) -> str:
	return path[7:]

def insert_chapter(db: Session, book: Chapter):
	if has_chapters(book):
		insert_chapters_list(db, book)

	if has_verses(book):
		insert_chapter_content(db, book)

def insert_chapters_list(db: Session, book: Chapter):
	book_data = jsonable_encoder(book)
	for chapter in book_data['chapters']:
		chapter.pop('chapters', None)
		chapter.pop('verses', None)

	obj_in = BookPartCreate (
		index = index_from_path(book.path),
		kind = "chapter_list",
		data = book_data,
		last_updated_id = 1
	)
	book_part = crud.book_part.upsert(db, obj_in=obj_in)
	logger.info("Inserted chapter list into book_part ID %i with index %s", book_part.id, book_part.index)

	for chapter in book.chapters:
		insert_chapter(db, chapter)

def insert_chapter_content(db: Session, chapter: Chapter):
	obj_in = BookPartCreate (
		index = index_from_path(chapter.path),
		kind = "verse_list",
		data = chapter,
		last_updated_id = 1
	)
	book = crud.book_part.upsert(db, obj_in=obj_in)
	logger.info("Inserted chapter content into book_part ID %i with index %s", book.id, book.index)
