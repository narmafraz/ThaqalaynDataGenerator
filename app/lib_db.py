import inspect
import json
import logging
import os

import jsons
from fastapi.encoders import jsonable_encoder

from app.lib_model import get_chapters, get_verses
from app.models import Chapter, Language, Translation, Verse

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
	if get_chapters(chapter):
		insert_chapters_list(chapter)

	if get_verses(chapter):
		insert_chapter_content(chapter)

def insert_chapters_list(chapter):
	chapter_data = jsonable_encoder(chapter)
	for subchapter in chapter_data['chapters']:
		subchapter.pop('chapters', None)
		subchapter.pop('verses', None)
		subchapter.pop('nav', None)

	obj_in = {
		"index": index_from_path(chapter_data['path']),
		"kind": "chapter_list",
		"data": chapter_data
	}
	chapter_part = write_file(chapter_data['path'], obj_in)
	logger.info("Inserted chapter list into chapter_part ID %s with index %s", chapter_part.id, chapter_part.index)

	subchapters = get_chapters(chapter)

	for subchapter in subchapters:
		insert_chapter(subchapter)

def insert_chapter_content(chapter):
	chapter_data = jsonable_encoder(chapter)
	obj_in = {
		"index": index_from_path(chapter_data['path']),
		"kind": "verse_list",
		"data": chapter_data
	}
	book = write_file(chapter_data['path'], obj_in)
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

	clean_obj = clean_nones(obj)

	with open(ensure_dir(get_dest_path(path)), 'w', encoding='utf-8') as f:
		json.dump(clean_obj, f, ensure_ascii=False, indent=2, sort_keys=True)
		result.id = f.name
	
	return result

def load_chapter(path: str) -> Chapter :
	with open(ensure_dir(get_dest_path(path)), 'r', encoding='utf-8') as f:
		json_chapter = json.load(f)
		if 'data' in json_chapter:
			json_chapter = json_chapter['data']
		return Chapter(**json_chapter)

def clean_nones(value):
	"""
	Recursively remove all None values from dictionaries and lists, and returns
	the result as a new dictionary or list.
	"""
	if isinstance(value, list):
		return [clean_nones(x) for x in value if x is not None]
	elif isinstance(value, dict):
		return {
			key: clean_nones(val)
			for key, val in value.items()
			if val is not None
		}
	else:
		return value
