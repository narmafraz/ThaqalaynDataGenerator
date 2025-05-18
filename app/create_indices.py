import fastapi
from app.lib_index import collect_indexes, update_index_files
from app.lib_db import write_file, load_chapter
from app.kafi import hubbeali_translation

def create_indices():
    quran = load_chapter("/books/complete/quran")
    kafi = load_chapter("/books/complete/al-kafi")
    
    for book in [quran, kafi]:
        index_maps = collect_indexes(book)
        update_index_files(index_maps)
    