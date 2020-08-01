import inspect
import json
import logging
import os

from fastapi.encoders import jsonable_encoder

from app.lib_model import has_chapters, has_verses
from app.models import Chapter, Language, Quran, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DESTINATION_DIR = os.getenv("DESTINATION_DIR")

class InsertedObj():
	id: str
	index: str
	path: str

def index_from_path(path: str) -> str:
	return path[7:]

def insert_chapter(chapter: Chapter):
	if has_chapters(chapter):
		insert_chapters_list(chapter)

	if has_verses(chapter):
		insert_chapter_content(chapter)

def insert_chapters_list(chapter: Chapter):
	chapter_data = jsonable_encoder(chapter)
	for subchapter in chapter_data['chapters']:
		subchapter.pop('chapters', None)
		subchapter.pop('verses', None)

	obj_in = {
		"index": index_from_path(chapter.path),
		"kind": "chapter_list",
		"data": chapter_data
	}
	chapter_part = write_file(chapter.path, obj_in)
	logger.info("Inserted chapter list into chapter_part ID %s with index %s", chapter_part.id, chapter_part.index)

	for subchapter in chapter.chapters:
		insert_chapter(subchapter)

def insert_chapter_content(chapter: Chapter):
	obj_in = {
		"index": index_from_path(chapter.path),
		"kind": "verse_list",
		"data": jsonable_encoder(chapter)
	}
	book = write_file(chapter.path, obj_in)
	logger.info("Inserted chapter content into book_part ID %s with index %s", book.id, book.index)

def get_dest_path(filename: str) -> str:
	sanitised_file = filename.replace(":", "/")
	if sanitised_file.startswith("/"):
		sanitised_file = sanitised_file[1:]
	return os.path.join(DESTINATION_DIR, sanitised_file + ".json")

def ensure_dir(file_path):
	directory = os.path.dirname(file_path)
	if not os.path.exists(directory):
		os.makedirs(directory)
	return file_path

def write_file(path: str, obj):
	result = InsertedObj()
	result.path = path
	result.index = obj["index"]

	with open(ensure_dir(get_dest_path(path)), 'w', encoding='utf-8') as f:
		json.dump(obj, f, ensure_ascii=False, sort_keys=True) # indent=2, 
		result.id = f.name
	
	return result
