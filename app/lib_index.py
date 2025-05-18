import os
import json
import fastapi
from app.lib_db import write_file

def collect_indexes(chapter, index_maps=None):
    if index_maps is None:
        index_maps = {}
    if chapter.titles:
        for lang, title in chapter.titles.items():
            if title:
                if lang not in index_maps:
                    index_maps[lang] = {}
                index_maps[lang][chapter.path] = {"title": title, "local_index": chapter.local_index, "part_type": chapter.part_type}
    if chapter.chapters:
        for subchapter in chapter.chapters:
            collect_indexes(subchapter, index_maps)
    return index_maps

def update_index_files(index_maps):
    for lang, idx in index_maps.items():
        filename = f"/index/books.{lang}.json"
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf8") as f:
                existing = json.load(f)
        else:
            existing = {}
        merged = {**existing, **idx}
        write_file(f"/index/books.{lang}", fastapi.encoders.jsonable_encoder(merged))
    
def add_translation(translation):
    try:
        if os.path.exists("/index/translations"):
            with open("/index/translations", "r", encoding="utf8") as f:
                translations = json.load(f)
        else:
            translations = {}
    except Exception:
        translations = {}
    translations[translation.id] = fastapi.encoders.jsonable_encoder(translation.dict())
    write_file("/index/translations", fastapi.encoders.jsonable_encoder(translations))
