import glob
import json
import logging
import os
import re
from pprint import pprint

from bs4 import BeautifulSoup, NavigableString, Tag
from fastapi.encoders import jsonable_encoder

from app.lib_bs4 import get_contents, is_rtl_tag
from app.lib_db import insert_chapter, load_chapter, write_file
from app.lib_model import SEQUENCE_ERRORS, set_index
from app.models import Chapter, Language, PartType, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SARWAR_TRANSLATION_ID = "en.sarwar"
sarwar_translation = Translation(name = "Shaykh Muhammad Sarwar (from Thaqalayn.net)", lang = Language.EN.value, id = SARWAR_TRANSLATION_ID)

# Created cumulative sum of verse numbers in volume 8 to adjust chapter index like this:
# from itertools import accumulate
# x = [len(x.verses) for x in book.chapters]
# print(list(accumulate(x)))
V8_HADITH_CUMSUM = [1, 3, 4, 6, 7, 15, 16, 20, 21, 22, 23, 26, 28, 29, 30, 32, 40, 51, 54, 56, 62, 66, 68, 70, 91, 93, 94, 95, 98, 102, 104, 107, 125, 142, 143, 153, 192, 193, 254, 273, 299, 313, 392, 457, 477, 532, 536, 550, 551, 584, 586, 597]

def we_dont_care(html: str) -> bool:
	return '<body>' in html or '</body>' in html

def sitepath_from_filepath(filepath: str) -> str:
	return filepath[filepath.index('\\chapter\\')+9:].replace('.html', '').replace('\\', '/')

def add_chapter_content(chapter: Chapter, filepath, hadith_index = 0):
	if filepath.endswith('\\0.html'):
		error_msg = f"Skipping zero file {filepath}"
		logger.warn(error_msg)
		SEQUENCE_ERRORS.append(error_msg)
		return

	verses = chapter.verses
	heading_count = len([x for x in verses if x.part_type == PartType.Heading])

	sarwar_exists = next((item for item in chapter.verse_translations if item.id == SARWAR_TRANSLATION_ID), None)
	if not sarwar_exists:
		chapter.verse_translations.append(sarwar_translation)

	with open(filepath, 'r', encoding='utf8') as qfile:
		file_html = qfile.read()

		if not 'en' in chapter.titles:
			file_soup = BeautifulSoup(file_html, 'html.parser')

			card_body = file_soup.find('div', 'card-body')
			chapter_title = get_contents(card_body.find('h3'))
			chapter.titles['en'] = chapter_title

		##### Processing each hadith separately

		hadith_htmls = re.split('<hr/?>', file_html)

		for hadith_html in hadith_htmls:
			if we_dont_care(hadith_html):
				continue
			
			soup = BeautifulSoup(hadith_html, 'html.parser')

			all_paras = soup.find_all('p')

			para_index = 0
			hadith_ar = []
			while is_rtl_tag(all_paras[para_index]):
				hadith_ar.append(get_contents(all_paras[para_index]))
				para_index += 1

			hadith_en = get_contents(all_paras[para_index])
			para_index += 1

			if hadith_index >= len(verses) - heading_count:
				# hubeali rightly splits first chapter in book of inheritance into two 
				# but thaqalayn.net has it as one chapter, so we'll skip adding ahadith
				if chapter.path == '/books/al-kafi:7:2:1':
					break
				
				verse = Verse()
				verse.text = hadith_ar
				verse.part_type = PartType.Hadith.value
				verse.translations = {}

				verses.append(verse)

				site_path = sitepath_from_filepath(filepath)
				if chapter.crumbs:
					my_site_path = chapter.crumbs[-1].path
				else:
					my_site_path = site_path.replace('/', ':')
				error_msg = f"Appending new hadith from Sarwar to hubeali, hadith #{hadith_index+1} from https://thaqalayn.net/chapter/{site_path} to https://thaqalayn.netlify.app/#{my_site_path}"
				logger.warn(error_msg)
				SEQUENCE_ERRORS.append(error_msg)
			else:
				# TODO: create new verse if the verse at this index doesn't match the one being inserted
				# perhaps use https://github.com/ztane/python-Levenshtein or https://pypi.org/project/jellyfish/
				verse = verses[hadith_index]

				if verse.part_type == PartType.Heading:
					hadith_index += 1
					verse = verses[hadith_index]
				
				if verse.part_type != PartType.Hadith:
					error_msg = f"Hadith index {hadith_index} is of part_type {verse.part_type} in https://thaqalayn.netlify.app/#{chapter.crumbs[-1].path}"
					logger.warn(error_msg)
					SEQUENCE_ERRORS.append(error_msg)


			verse.translations[SARWAR_TRANSLATION_ID] = [hadith_en]

			if len(all_paras) > para_index + 1:
				grading_title = get_contents(all_paras[para_index])
				para_index += 1
				if grading_title.startswith('Grading:'):
					grading = []
					# if len(all_paras[3:-3]) != 2 and len(all_paras[3:-3]) != 1:
					# 	raise Exception("We are in " + filepath + " and all_paras is " + str(all_paras))
					for grading_para in all_paras[para_index:-3]:
						grading.append(get_contents(grading_para))
					verse.gradings = grading

			hadith_index += 1

	# Volume 8 of al-kafi is one file per hadith on thaqalayn.net and it'll warn on every page 
	# since there is always more ahadith on hubeali's chapter
	if hadith_index != len(verses) - heading_count and 'al-kafi:8:1' not in chapter.path:
		site_path = sitepath_from_filepath(filepath)
		error_msg = f"Sarwar has {hadith_index} hadith but hubeali has {len(verses)} hadith: https://thaqalayn.net/chapter/{site_path} vs https://thaqalayn.netlify.app/#{chapter.crumbs[-1].path}"
		logger.warn(error_msg)
		SEQUENCE_ERRORS.append(error_msg)

def load_chapter_from_file(filename):
	filepath = os.path.join(os.path.dirname(__file__), filename)
	with open(filepath, 'r', encoding='utf8') as qfile:
		file_content = qfile.read()
		file_json = json.loads(file_content)
		return Chapter(**file_json['data'])

def create_chapter(title_ar) -> Chapter:
	chapter = {
		'part_type': PartType.Chapter.value,
		'titles': {'ar': title_ar},
		'verse_translations': [],
		'verses': []
	}
	return Chapter(**chapter)

def get_adjusted_chapter(volume: Chapter, book: Chapter, cfile, chapter_index):
	hadith_index = 0
	# book of hajj is split into another book on ziyarat https://thaqalayn.netlify.app/#/books/al-kafi:4:4?lang=en but this is not the case in https://thaqalayn.net/book/4
	if volume.local_index == 4:
		if book.local_index == 3:
			if chapter_index == 0:
				# Chapter 228 is merged into the end of chapter 227 in hubeali
				book.chapters.insert(227, create_chapter("دُعَاءٌ آخَرُ عِنْدَ قَبْرِ أَمِيرِ الْمُؤْمِنِينَ ع‏"))
				book.chapters[227].verses.append(book.chapters[226].verses.pop(1))
	
	# vol 5 book 2 has missing chapter 82
	if volume.local_index == 5:
		if book.local_index == 2:
			# reserve missing chapter from beginning since we parse chapter file 159 before 82 and cause index out of bound
			if chapter_index == 0:
				# Chapter 82 missing in hubeali
				book.chapters.insert(81, {})
			if chapter_index == 81:
				book.chapters[chapter_index] = load_chapter_from_file("raw\\corrections\\al-kafi_v5_b2_c82.json")
		if book.local_index == 3:
			# TODO: hadith 3 in chapter 120 missing, based on noor
			# TODO: one hadith in chapter 124 missing, based on noor
			# Chapters on slaves missing Sarwar translations: from https://thaqalayn.net/chapter/5/3/112 to https://thaqalayn.net/chapter/5/3/137 
			if chapter_index == 0:
				# Missing chapters
				book.chapters.insert(22, create_chapter("بَابُ تَزْوِيجِ أُمِّ كُلْثُوم‏"))
				book.chapters.insert(121, create_chapter("بَابُ الرَّجُلِ يُزَوِّجُ عَبْدَهُ أَمَتَهُ ثُمَّ يَشْتَهِيهَا"))
				book.chapters.insert(132, create_chapter("بَاب‏"))
				book.chapters.insert(177, create_chapter("بَابُ أَنَّهُ لَا غَيْرَةَ فِي الْحَلَال‏"))
				book.chapters.insert(190, create_chapter("بَابُ تَفْسِيرِ مَا يَحِلُّ مِنَ النِّكَاحِ وَ مَا يَحْرُمُ وَ الْفَرْقِ بَيْنَ النِّكَاحِ وَ السِّفَاحِ وَ الزِّنَى وَ هُوَ مِنْ كَلَامِ يُونُس‏"))
			# Hadith 2 and 3 missing in chapter 22 of hubeali 
			if chapter_index == 21:
				book.chapters[chapter_index] = load_chapter_from_file("raw\\corrections\\al-kafi_v5_b3_c22.json")
			# Hadith 4-9 missing in chapter 190 of hubeali
			if chapter_index == 189:
				book.chapters[chapter_index] = load_chapter_from_file("raw\\corrections\\al-kafi_v5_b3_c190.json")

	if volume.local_index == 6:
		if book.local_index == 2:
			if chapter_index == 0:
				book.chapters.insert(28, create_chapter("بَابُ الْفَرْقِ بَيْنَ مَنْ طَلَّقَ عَلَى غَيْرِ السُّنَّةِ وَ بَيْنَ الْمُطَلَّقَةِ إِذَا خَرَجَتْ وَ هِيَ فِي عِدَّتِهَا أَوْ أَخْرَجَهَا زَوْجُهَا"))
		# thaqalayn.net is missing a whole book on slavery: https://thaqalayn.netlify.app/#/books/al-kafi:6:3
		# so we skip adding translation to this book, note: book.local_index is 1 based
		if book.local_index >= 3:
			book = volume.chapters[book.local_index]
		if book.local_index == 6:
			# reserve missing chapter from beginning since we parse chapter file 159 before 82 and cause index out of bound
			if chapter_index == 0:
				book.chapters.insert(86, create_chapter("بَابُ أَلْبَانِ الْإِبِل‏"))
			if chapter_index == 133:
				book.chapters[chapter_index] = load_chapter_from_file("raw\\corrections\\al-kafi_v6_b6_c134.json")

	if volume.local_index == 7:
		if book.local_index == 2:
			if chapter_index == 0:
				book.chapters.insert(9, create_chapter("بَابُ الْعِلَّةِ فِي أَنَّ السِّهَامَ لَا تَكُونُ أَكْثَرَ مِنْ سِتَّةٍ وَ هُوَ مِنْ كَلَامِ يُونُس‏"))
			if chapter_index >= 1:
				chapter_index += 1
	
	if volume.local_index == 8 and chapter_index > 0:
		# There is only one book and each hadith is in its own chapter in thaqalayn.net
		cumsum_index = next(i for i,v in enumerate(V8_HADITH_CUMSUM) if v >= chapter_index + 1)
		cumsum = V8_HADITH_CUMSUM[cumsum_index - 1]
		hadith_index = chapter_index - cumsum
		chapter_index = cumsum_index

	return (book.chapters[chapter_index], hadith_index)
	
def add_book_content(book: Chapter, dirname, volume):
	cfiles = glob.glob(os.path.join(dirname, "*"))
	for cfile in cfiles:
		logger.info("Processing file %s", cfile)
		chapter_index = int(os.path.basename(cfile).replace('.html', '')) - 1

		(chapter, hadith_index) = get_adjusted_chapter(volume, book, cfile, chapter_index)
		
		add_chapter_content(chapter, cfile, hadith_index)

def add_content(volume: Chapter, dirname):
	cfiles = glob.glob(dirname + "*")

	for cfile in cfiles:
		logger.info("Processing book dir %s", cfile)
		book_index = int(os.path.basename(cfile)) - 1
		add_book_content(volume.chapters[book_index], cfile, volume)

def get_path(file):
	return os.path.join(os.path.dirname(__file__), "raw\\thaqalayn_net\\Thaqalayn\\thaqalayn.net\\" + file)

def add_kafi_sarwar():
	kafi = load_chapter("/books/complete/al-kafi")
	add_content(kafi.chapters[0], get_path("chapter\\1\\"))
	add_content(kafi.chapters[1], get_path("chapter\\2\\"))
	add_content(kafi.chapters[2], get_path("chapter\\3\\"))
	add_content(kafi.chapters[3], get_path("chapter\\4\\"))
	add_content(kafi.chapters[4], get_path("chapter\\5\\"))
	add_content(kafi.chapters[5], get_path("chapter\\6\\"))
	add_content(kafi.chapters[6], get_path("chapter\\7\\"))
	add_content(kafi.chapters[7], get_path("chapter\\8\\"))

	set_index(kafi, [0, 0, 0, 0], 0)
	insert_chapter(kafi)
	write_file("/books/complete/al-kafi", jsonable_encoder(kafi))

	pprint(SEQUENCE_ERRORS, width=240)
