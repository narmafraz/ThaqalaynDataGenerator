import fastapi
from app.lib_index import collect_indexes, update_index_files
from app.lib_db import write_file
from app.kafi import hubbeali_translation

def create_indices(book):
    index_maps = collect_indexes(book)
    update_index_files(index_maps)
    
    translations_map = {}
    def collect_translations(chapter):
        if chapter.verse_translations:
            for tid in chapter.verse_translations:
                if tid == hubbeali_translation.id and tid not in translations_map:
                    translations_map[tid] = hubbeali_translation.dict()
        if chapter.chapters:
            for subchapter in chapter.chapters:
                collect_translations(subchapter)
    collect_translations(book)
    write_file("/index/translations", fastapi.encoders.jsonable_encoder(translations_map))
