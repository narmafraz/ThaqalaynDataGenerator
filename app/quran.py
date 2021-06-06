import json
import logging
import os
import xml.etree.ElementTree
from typing import Dict, List

import fastapi

# make sure all SQL Alchemy models are imported before initializing DB
# otherwise, SQL Alchemy might fail to initialize relationships properly
# for more details: https://github.com/tiangolo/full-stack-fastapi-postgresql/issues/28
from app.lib_db import index_from_path, insert_chapter, write_file
from app.lib_model import set_index
from app.models import Chapter, Crumb, Language, PartType, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOK_INDEX = 1
BOOK_PATH = "/books/quran"

def get_sajda_data(quran):
	sajdas = {}
	group = quran.find("sajdas")
	for j in group.findall("sajda"):
		meta = j.attrib
		ttype = meta['type']
		sura = int(meta['sura'])
		aya = int(meta['aya'])

		sajdas[(sura, aya)] = ttype

	return sajdas

def build_chapters(file: str, verses: List[Verse], verse_translations: List[Translation]) -> List[Chapter]:
	chapters: List[Chapter] = []
	
	quran = xml.etree.ElementTree.parse(file).getroot()

	suras = quran.find('suras')
	for s in suras.findall('sura'):
		meta = s.attrib
		index=int(meta['index'])
		ayas=int(meta['ayas'])
		start=int(meta['start'])
		name=meta['name']
		tname=meta['tname']
		ename=meta['ename']
		type=meta['type']
		order=int(meta['order'])
		rukus=int(meta['rukus'])

		titles = {
			Language.AR.value: name,
			Language.EN.value: ename,
			Language.ENT.value: tname
		}

		sura = Chapter()
		sura.part_type = PartType.Chapter
		sura.titles=titles
		sura.reveal_type=type
		sura.order=order
		sura.rukus=rukus
		sura.verses=verses[start:ayas+start]
		sura.verse_translations = verse_translations
		sura.default_verse_translation_ids = {
			"en": "en.qarai",
			"fa": "fa.makarem"
		}

		chapters.append(sura)

	sajdas = get_sajda_data(quran)
	for k, v in sajdas.items():
		(sura_index, aya_index) = k
		sajda_chapter = chapters[sura_index - 1]
		sajda_chapter.sajda_type = v
		sajda_chapter.verses[aya_index - 1].sajda_type = v

	# add_group_data(quran, ayaindex, 'juzs', 'juz')
	# add_group_data(quran, ayaindex, 'hizbs', 'quarter')
	# add_group_data(quran, ayaindex, 'manzils', 'manzil')
	# add_group_data(quran, ayaindex, 'rukus', 'ruku')
	# add_group_data(quran, ayaindex, 'pages', 'page')

	return chapters



def build_verses(file):
	logger.info("Adding Quran file %s", file)

	index = 0
	verses = []
	with open(file, 'r', encoding='utf8') as qfile:
		for line in qfile.readlines():
			text = line.strip()
			if text and not text.startswith('#'):
				index=index+1
				verse = Verse()
				verse.part_type = PartType.Verse
				# verse.index=index
				verse.text=[text]
				verse.translations={}

				verses.append(verse)
	
	return verses

def insert_quran_translation(verses, verse_translations, file, key, lang, author, bio):
	logger.info("Adding Quran translation file %s", file)

	id = lang + "." + key
	qt = Translation(lang = lang, name = author, id = id)
	verse_translations.append(qt)

	index = 0
	with open(file, 'r', encoding='utf8') as qfile:
		for line in qfile.readlines():
			text = line.strip()
			if text and not text.startswith('#'):
				verses[index].translations[id] = [text]
				index = index + 1

def get_path(file):
	return os.path.join(os.path.dirname(__file__), "raw\\" + file)


def build_quran() -> Chapter:
	verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
	verse_translations = []

	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.ansarian.txt"), "ansarian", "fa", "Hussain Ansarian", "https://fa.wikipedia.org/wiki/%D8%AD%D8%B3%DB%8C%D9%86_%D8%A7%D9%86%D8%B5%D8%A7%D8%B1%DB%8C%D8%A7%D9%86")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.ayati.txt"), "ayati", "fa", "AbdolMohammad Ayati", "https://fa.wikipedia.org/wiki/%D8%B9%D8%A8%D8%AF%D8%A7%D9%84%D9%85%D8%AD%D9%85%D8%AF_%D8%A2%DB%8C%D8%AA%DB%8C")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.bahrampour.txt"), "bahrampour", "fa", "Abolfazl Bahrampour", "https://fa.wikipedia.org/wiki/%D8%A7%D8%A8%D9%88%D8%A7%D9%84%D9%81%D8%B6%D9%84_%D8%A8%D9%87%D8%B1%D8%A7%D9%85%E2%80%8C%D9%BE%D9%88%D8%B1")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.fooladvand.txt"), "fooladvand", "fa", "Mohammad Mahdi Fooladvand", "https://fa.wikipedia.org/wiki/%D9%85%D8%AD%D9%85%D8%AF%D9%85%D9%87%D8%AF%DB%8C_%D9%81%D9%88%D9%84%D8%A7%D8%AF%D9%88%D9%86%D8%AF")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.ghomshei.txt"), "ghomshei", "fa", "Mahdi Elahi Ghomshei", "https://fa.wikipedia.org/wiki/%D9%85%D9%87%D8%AF%DB%8C_%D8%A7%D9%84%D9%87%DB%8C_%D9%82%D9%85%D8%B4%D9%87%E2%80%8C%D8%A7%DB%8C")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.khorramdel.txt"), "khorramdel", "fa", "Mostafa Khorramdel", "https://rasekhoon.net/mashahir/Show-904328.aspx")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.khorramshahi.txt"), "khorramshahi", "fa", "Baha'oddin Khorramshahi", "https://fa.wikipedia.org/wiki/%D8%A8%D9%87%D8%A7%D8%A1%D8%A7%D9%84%D8%AF%DB%8C%D9%86_%D8%AE%D8%B1%D9%85%D8%B4%D8%A7%D9%87%DB%8C")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.makarem.txt"), "makarem", "fa", "Naser Makarem Shirazi", "https://en.wikipedia.org/wiki/Naser_Makarem_Shirazi")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.moezzi.txt"), "moezzi", "fa", "Mohammad Kazem Moezzi", "")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.mojtabavi.txt"), "mojtabavi", "fa", "Sayyed Jalaloddin Mojtabavi", "http://rasekhoon.net/mashahir/Show-118481.aspx")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/fa.sadeqi.txt"), "sadeqi", "fa", "Mohammad Sadeqi Tehrani", "https://fa.wikipedia.org/wiki/%D9%85%D8%AD%D9%85%D8%AF_%D8%B5%D8%A7%D8%AF%D9%82%DB%8C_%D8%AA%D9%87%D8%B1%D8%A7%D9%86%DB%8C")

	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.ahmedali.txt"), "ahmedali", "en", "Ahmed Ali", "https://en.wikipedia.org/wiki/Ahmed_Ali_(writer)")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.ahmedraza.txt"), "ahmedraza", "en", "Ahmed Raza Khan", "https://en.wikipedia.org/wiki/Ahmed_Raza_Khan_Barelvi")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.arberry.txt"), "arberry", "en", "A. J. Arberry", "https://en.wikipedia.org/wiki/Arthur_John_Arberry")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.daryabadi.txt"), "daryabadi", "en", "Abdul Majid Daryabadi", "https://en.wikipedia.org/wiki/Abdul_Majid_Daryabadi")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.hilali.txt"), "hilali", "en", "Muhammad Taqi-ud-Din al-Hilali and Muhammad Muhsin Khan", "https://en.wikipedia.org/wiki/Noble_Quran_(Hilali-Khan)")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.itani.txt"), "itani", "en", "Talal Itani", "")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.maududi.txt"), "maududi", "en", "Abul Ala Maududi", "https://en.wikipedia.org/wiki/Abul_A%27la_Maududi")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.mubarakpuri.txt"), "mubarakpuri", "en", "Safi-ur-Rahman al-Mubarakpuri", "https://en.wikipedia.org/wiki/Safiur_Rahman_Mubarakpuri")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.pickthall.txt"), "pickthall", "en", "Mohammed Marmaduke William Pickthall", "https://en.wikipedia.org/wiki/Marmaduke_Pickthall")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.qarai.txt"), "qarai", "en", "Ali Quli Qarai", "")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.qaribullah.txt"), "qaribullah", "en", "Hasan al-Fatih Qaribullah and Ahmad Darwish", "")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.sahih.txt"), "sahih", "en", "Saheeh International", "http://www.saheehinternational.com/")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.sarwar.txt"), "sarwar", "en", "Muhammad Sarwar", "https://en.wikipedia.org/wiki/Shaykh_Muhammad_Sarwar")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.shakir.txt"), "shakir", "en", "Mohammad Habib Shakir", "https://en.wikipedia.org/wiki/Muhammad_Habib_Shakir")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.transliteration.txt"), "transliteration", "en", "English Transliteration", "")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.wahiduddin.txt"), "wahiduddin", "en", "Wahiduddin Khan", "https://en.wikipedia.org/wiki/Wahiduddin_Khan")
	insert_quran_translation(verses, verse_translations, get_path("tanzil_net/translations/en.yusufali.txt"), "yusufali", "en", "Abdullah Yusuf Ali", "https://en.wikipedia.org/wiki/Abdullah_Yusuf_Ali")

	chapters = build_chapters(get_path("tanzil_net/quran-data.xml"), verses, verse_translations)

	q = Chapter()
	q.index = BOOK_INDEX
	q.path = BOOK_PATH
	q.verse_start_index = 0
	q.part_type = PartType.Book
	q.titles = {
		Language.EN.value: "The Holy Quran",
		Language.AR.value: "القرآن الكريم"
	}
	q.descriptions = {
		Language.EN.value: ["Was revealed to the prophet SAW"]
	}
	q.chapters=chapters
	q.verse_translations = verse_translations
	q.default_verse_translation_ids = {
		"en": "en.qarai",
		"fa": "fa.makarem"
	}

	crumb = Crumb()
	crumb.titles = q.titles
	crumb.indexed_titles = q.titles
	crumb.path = q.path
	q.crumbs = [crumb]

	set_index(q, [0, 0], 0)

	return q

# def insert_verse_content(db: Session, quran: Quran):
# 	for chapter in quran.chapters:
# 		for verse in chapter.verses:
# 			obj_in = BookPartCreate (
# 				index = index_from_path(chapter.path) + ":" + str(verse.local_index),
# 				kind = "verse_content",
# 				data = verse,
# 				last_updated_id = 1
# 			)
# 			book = crud.book_part.upsert(db, obj_in=obj_in)
# 			logger.info("Inserted Quran verse content into book_part ID %s with index %s", book.id, book.index)

def init_quran():
	quran = build_quran()
	insert_chapter(quran) 
	write_file("/books/complete/quran", fastapi.encoders.jsonable_encoder(quran))
	# insert_verse_content(db_session, quran)
