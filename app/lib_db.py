import inspect
import json
import logging
import os
import shutil

import jsons
from fastapi.encoders import jsonable_encoder

from app.lib_model import get_chapters, get_verses
from app.models import Chapter, Language, Translation, Verse
from app.models.enums import PartType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_destination_dir() -> str:
	return os.getenv("DESTINATION_DIR", "../ThaqalaynData/")

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

def insert_chapters_list(chapter: Chapter) -> None:
	chapter_data = jsonable_encoder(chapter,
		exclude={'chapters': {'__all__': {
			'chapters',
			'verses',
			'crumbs',
			'nav'
		}}}
	)

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

def insert_chapter_content(chapter: Chapter) -> None:
	chapter_data = jsonable_encoder(chapter, exclude={'verses', 'chapters'})

	verse_refs = []
	for verse in get_verses(chapter):
		ref = {"local_index": verse.local_index, "part_type": verse.part_type.value if verse.part_type else None}
		if verse.part_type == PartType.Heading:
			ref["inline"] = jsonable_encoder(verse)
		else:
			ref["path"] = verse.path
		verse_refs.append(ref)
	chapter_data["verse_refs"] = verse_refs

	obj_in = {
		"index": index_from_path(chapter_data['path']),
		"kind": "verse_list",
		"data": chapter_data
	}
	book = write_file(chapter_data['path'], obj_in)
	logger.info("Inserted chapter content into book_part ID %s with index %s", book.id, book.index)

	insert_verse_details(chapter)


def insert_verse_details(chapter: Chapter) -> None:
	"""Write individual verse_detail JSON files for each hadith/verse in a chapter."""
	verses = get_verses(chapter)
	if not verses:
		return

	from app.models.enums import PartType
	addressable_verses = [v for v in verses if v.part_type in (PartType.Hadith, PartType.Verse)]
	if not addressable_verses:
		return

	for i, verse in enumerate(addressable_verses):
		if not verse.path:
			continue

		nav = {}
		if i > 0:
			nav["prev"] = addressable_verses[i - 1].path
		if i < len(addressable_verses) - 1:
			nav["next"] = addressable_verses[i + 1].path
		nav["up"] = chapter.path

		verse_data = jsonable_encoder(verse)
		detail_data = {
			"verse": verse_data,
			"chapter_path": chapter.path,
			"chapter_title": chapter.titles,
			"nav": nav,
		}

		if hasattr(chapter, 'verse_translations') and chapter.verse_translations:
			detail_data["verse_translations"] = chapter.verse_translations

		if verse.gradings:
			detail_data["gradings"] = verse.gradings
		if verse.source_url:
			detail_data["source_url"] = verse.source_url

		obj_in = {
			"index": index_from_path(verse.path),
			"kind": "verse_detail",
			"data": detail_data,
		}
		result = write_file(verse.path, obj_in)
		logger.debug("Inserted verse detail ID %s with index %s", result.id, result.index)

def get_dest_path(filename: str) -> str:
	sanitised_file = filename.replace(":", "/")
	if sanitised_file.startswith("/"):
		sanitised_file = sanitised_file[1:]
	return os.path.join(get_destination_dir(), sanitised_file + ".json")

def ensure_dir(file_path: str) -> str:
	directory = os.path.dirname(file_path)
	if not os.path.exists(directory):
		os.makedirs(directory)
	return file_path

def write_file(path: str, obj: dict) -> InsertedObj:
	result = InsertedObj()
	result.path = path
	if 'index' in obj:
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

def load_json(path: str) -> dict:
	with open(ensure_dir(get_dest_path(path)), 'r', encoding='utf-8') as f:
		return json.load(f)

def delete_file(path: str) -> None:
	filename = get_dest_path(path)
	if os.path.exists(filename):
		os.remove(filename)

def delete_folder(path: str) -> None:
	if path.startswith("/"):
		path = path[1:]
	filename = os.path.join(get_destination_dir(), path)
	if os.path.exists(filename):
		shutil.rmtree(filename)

def clean_nones(value: object) -> object:
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
