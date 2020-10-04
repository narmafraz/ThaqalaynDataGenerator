import glob
import json
import logging
import os
import re
from pprint import pprint

from bs4 import BeautifulSoup, NavigableString, Tag
from fastapi.encoders import jsonable_encoder

from app.lib_bs4 import get_contents, is_rtl_tag
from app.lib_db import insert_chapter_dict, load_chapter, write_file
from app.lib_model import SEQUENCE_ERRORS, set_index
from app.models import Chapter, Language, PartType, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SARWAR_TRANSLATION_ID = "en.sarwar"
sarwar_translation = Translation()
sarwar_translation.name = "Shaykh Muhammad Sarwar (from Thaqalayn.net)"
sarwar_translation.lang = Language.EN.value
sarwar_translation.id = SARWAR_TRANSLATION_ID
sarwar_translation = jsonable_encoder(sarwar_translation)

def we_dont_care(html: str) -> bool:
	return '<body>' in html or '</body>' in html

def add_chapter_content(chapter, filepath, hadith_index = 0):
	verses = chapter['verses']

	sarwar_exists = next((item for item in chapter['verse_translations'] if item["id"] == SARWAR_TRANSLATION_ID), None)
	if not sarwar_exists:
		chapter['verse_translations'].append(sarwar_translation)

	with open(filepath, 'r', encoding='utf8') as qfile:
		file_html = qfile.read()

		if not 'en' in chapter['titles']:
			file_soup = BeautifulSoup(file_html, 'html.parser')

			card_body = file_soup.find('div', 'card-body')
			chapter_title = get_contents(card_body.find('h3'))
			chapter['titles']['en'] = chapter_title

		##### Processing each hadith separately

		hadith_htmls = re.split('<hr/?>', file_html)

		for hadith_html in hadith_htmls:
			if we_dont_care(hadith_html):
				continue
			
			soup = BeautifulSoup(hadith_html, 'html.parser')

			all_paras = soup.find_all('p')

			hadith_ar = get_contents(all_paras[0])
			hadith_en = get_contents(all_paras[1])


			if hadith_index >= len(verses):
				verse = {}
				verse['text'] = [hadith_ar]
				verse['part_type'] = PartType.Hadith.value
				verse['translations'] = {}

				verses.append(verse)
				logger.warn('Adding new hadith from Sarwar to hubeali on index %i in %s', hadith_index, chapter['titles']['en'])
			else:
				# TODO: create new verse if the verse at this index doesn't match the one being inserted
				# perhaps use https://github.com/ztane/python-Levenshtein or https://pypi.org/project/jellyfish/
				verse = verses[hadith_index]
				if verse['part_type'] != PartType.Hadith.value:
					error_msg = f"Hadith index {hadith_index} is of part_type {verse['part_type']} in {chapter['titles']['en']}"
					logger.warn(error_msg)
					SEQUENCE_ERRORS.append(error_msg)


			verse['translations'][SARWAR_TRANSLATION_ID] = [hadith_en]

			if len(all_paras) > 3:
				grading_title = get_contents(all_paras[2])
				if grading_title.startswith('Grading:'):
					grading = []
					# if len(all_paras[3:-3]) != 2 and len(all_paras[3:-3]) != 1:
					# 	raise Exception("We are in " + filepath + " and all_paras is " + str(all_paras))
					for grading_para in all_paras[3:-3]:
						grading.append(get_contents(grading_para))
					# logger.info(grading)
					verse['gradings'] = grading


			# logger.info(str(hadith_en))
			hadith_index += 1

	if hadith_index != len(verses):
		error_msg = f"Sarwar has {hadith_index} hadith but hubeali has {len(verses)} hadith in {chapter['titles']['en']}"
		logger.warn(error_msg)
		SEQUENCE_ERRORS.append(error_msg)

def replace_chapter_from_file(filename, book, chapter_index):
	filepath = os.path.join(os.path.dirname(__file__), filename)
	with open(filepath, 'r', encoding='utf8') as qfile:
		file_content = qfile.read()
		file_json = json.loads(file_content)
		book['chapters'][chapter_index] = file_json['data']

def create_chapter(title_ar):
	chapter = {
		'part_type': PartType.Chapter.value,
		'titles': {'ar': title_ar},
		'verse_translations': [],
		'verses': []
	}
	return chapter

def get_adjusted_chapter(volume, book, cfile, chapter_index):
	hadith_index = 0
	# book of hajj is split into another book on ziyarat https://thaqalayn.netlify.app/#/books/al-kafi:4:4?lang=en but this is not the case in https://thaqalayn.net/book/4
	if volume['index'] == 4 and chapter_index >= 212:
		new_chapter_index = chapter_index % 212
		# the two hadith in https://thaqalayn.netlify.app/#/books/al-kafi:4:4:15?lang=en are split into two in https://thaqalayn.net/chapter/4/3/228
		if new_chapter_index == 15:
			hadith_index = 1
		if new_chapter_index > 14:
			new_chapter_index -= 1
		return (volume['chapters'][3]['chapters'][new_chapter_index], hadith_index)
	
	# vol 5 book 2 has missing chapter 82
	if volume['index'] == 5:
		if book['local_index'] == 2:
			# reserve missing chapter from beginning since we parse chapter file 159 before 82 and cause index out of bound
			if chapter_index == 0:
				# Chapter 82 missing in hubeali
				book['chapters'].insert(81, {})
			if chapter_index == 81:
				replace_chapter_from_file("raw\\corrections\\al-kafi_v5_b2_c82.json", book, chapter_index)
		if book['local_index'] == 3:
			# TODO: hadith 3 in chapter 120 missing, based on noor
			# TODO: one hadith in chapter 124 missing, based on noor
			# Chapters on slaves missing Sarwar translations: from https://thaqalayn.net/chapter/5/3/112 to https://thaqalayn.net/chapter/5/3/137 
			if chapter_index == 0:
				# Missing chapters
				book['chapters'].insert(22, create_chapter("بَابُ تَزْوِيجِ أُمِّ كُلْثُوم‏"))
				book['chapters'].insert(121, create_chapter("بَابُ الرَّجُلِ يُزَوِّجُ عَبْدَهُ أَمَتَهُ ثُمَّ يَشْتَهِيهَا"))
				book['chapters'].insert(132, create_chapter("بَاب‏"))
				book['chapters'].insert(177, create_chapter("بَابُ أَنَّهُ لَا غَيْرَةَ فِي الْحَلَال‏"))
				book['chapters'].insert(190, create_chapter("بَابُ تَفْسِيرِ مَا يَحِلُّ مِنَ النِّكَاحِ وَ مَا يَحْرُمُ وَ الْفَرْقِ بَيْنَ النِّكَاحِ وَ السِّفَاحِ وَ الزِّنَى وَ هُوَ مِنْ كَلَامِ يُونُس‏"))
			# Hadith 2 and 3 missing in chapter 22 of hubeali 
			if chapter_index == 21:
				replace_chapter_from_file("raw\\corrections\\al-kafi_v5_b3_c22.json", book, chapter_index)
			# Hadith 4-9 missing in chapter 190 of hubeali
			if chapter_index == 189:
				replace_chapter_from_file("raw\\corrections\\al-kafi_v5_b3_c190.json", book, chapter_index)

	if volume['index'] == 6:
		if book['local_index'] == 2:
			if chapter_index == 0:
				book['chapters'].insert(28, create_chapter("بَابُ الْفَرْقِ بَيْنَ مَنْ طَلَّقَ عَلَى غَيْرِ السُّنَّةِ وَ بَيْنَ الْمُطَلَّقَةِ إِذَا خَرَجَتْ وَ هِيَ فِي عِدَّتِهَا أَوْ أَخْرَجَهَا زَوْجُهَا"))
		# thaqalayn.net is missing a whole book on slavery: https://thaqalayn.netlify.app/#/books/al-kafi:6:3
		# so we skip adding translation to this book
		if book['local_index'] >= 3:
			book = volume['chapters'][book['local_index']]
		if book['local_index'] == 6:
			# reserve missing chapter from beginning since we parse chapter file 159 before 82 and cause index out of bound
			if chapter_index == 0:
				book['chapters'].insert(86, create_chapter("بَابُ أَلْبَانِ الْإِبِل‏"))

	return (book['chapters'][chapter_index], hadith_index)
	
def add_book_content(book, dirname, volume):
	cfiles = glob.glob(os.path.join(dirname, "*.html"))
	for cfile in cfiles:
		logger.info("Processing file %s", cfile)
		chapter_index = int(os.path.basename(cfile)[:-5]) - 1

		(chapter, hadith_index) = get_adjusted_chapter(volume, book, cfile, chapter_index)
		
		add_chapter_content(chapter, cfile, hadith_index)

def add_content(chapter, dirname):
	cfiles = glob.glob(dirname + "*")

	for cfile in cfiles:
		logger.info("Processing book dir %s", cfile)
		book_index = int(os.path.basename(cfile)) - 1
		add_book_content(chapter['chapters'][book_index], cfile, chapter)

def get_path(file):
	return os.path.join(os.path.dirname(__file__), "raw\\thaqalayn_net\\Thaqalayn\\thaqalayn.net\\" + file)

def add_kafi_sarwar():
	kafi = load_chapter("/books/complete/al-kafi")
	# add_content(kafi['chapters'][0], get_path("chapter\\1\\"))
	# add_content(kafi['chapters'][1], get_path("chapter\\2\\"))
	# add_content(kafi['chapters'][2], get_path("chapter\\3\\"))
	# add_content(kafi['chapters'][3], get_path("chapter\\4\\"))
	# add_content(kafi['chapters'][4], get_path("chapter\\5\\"))
	# add_content(kafi['chapters'][5], get_path("chapter\\6\\"))
	add_content(kafi['chapters'][6], get_path("chapter\\7\\"))
	# add_content(kafi['chapters'][7], get_path("chapter\\8\\"))
	# insert_chapter_dict(kafi)

	# write_file("/books/complete/al-kafi", jsonable_encoder(kafi))
	pprint(SEQUENCE_ERRORS, width=240)
