import logging
import re
from typing import Set

from fastapi.encoders import jsonable_encoder

from app.lib_bs4 import get_contents, is_rtl_tag
from app.lib_db import insert_chapter, load_chapter, write_file
from app.lib_model import SEQUENCE_ERRORS, get_chapters, get_verses
from app.models import Chapter, Language, PartType, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

QURAN_QUOTE = re.compile(r'[\[\(](\d+):(\d+)[\]\)]')

def process_translation_text(translation_text_list, quran_refs):
    for i, ttext in enumerate(translation_text_list):
        translation_text_list[i] = QURAN_QUOTE.sub("<a href=\"/#/books/quran:\\1#h\\2\">[\\1:\\2]</a>", ttext)
        all_matches = QURAN_QUOTE.findall(ttext)
        for match in all_matches:
            sura_no = int(match[0])
            verse_no = int(match[1])
            quran_refs.add((sura_no, verse_no))

def update_refs(quran: Chapter, hadith: Verse, quran_refs: Set):
    qrefs = set()
    for (sura_no, verse_no) in quran_refs:
        try:
            sura = quran.chapters[sura_no - 1]
            verse = sura.verses[verse_no - 1]
            if not verse.refs:
                verse.refs = { "Al-Kafi": set() }
            verse.refs["Al-Kafi"].add(hadith.path)
            qrefs.add(f"/books/quran:{sura_no}:{verse_no}")
        except IndexError:
            logger.warn(f"Quran ref does not exist. Hadith {hadith.path} ref {sura_no}:{verse_no}")
    if qrefs:
        hadith.refs = {'Quran': qrefs }

def process_chapter_verses(quran: Chapter, chapter: Chapter):
    for hadith in chapter.verses:
        quran_refs = set()
        if 'en.hubeali' in hadith.translations:
            process_translation_text(hadith.translations['en.hubeali'], quran_refs)
        if 'en.sarwar' in hadith.translations:
            process_translation_text(hadith.translations['en.sarwar'], quran_refs)
        update_refs(quran, hadith, quran_refs)

def process_chapter(quran: Chapter, kafi: Chapter):
    chapters = get_chapters(kafi)
    verses = get_verses(kafi)
    if chapters:
        for chapter in chapters:
            process_chapter(quran, chapter)
    elif verses:
        process_chapter_verses(quran, kafi)
    else:
        logger.info("Couldn't find anything to process in %s", kafi)

def link_quran_kafi():
    quran = load_chapter("/books/complete/quran")
    kafi = load_chapter("/books/complete/al-kafi")

    process_chapter(quran, kafi)

    insert_chapter(kafi)
    insert_chapter(quran) 
    # write_file("/books/complete/al-kafi", jsonable_encoder(kafi))
    # write_file("/books/complete/quran", jsonable_encoder(quran))

