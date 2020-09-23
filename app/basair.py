import glob
import json
import logging
import math
import os
import re
import xml.etree.ElementTree
from pprint import pprint
from typing import Dict, List

from bs4 import BeautifulSoup, NavigableString, Tag

# make sure all SQL Alchemy models are imported before initializing DB
# otherwise, SQL Alchemy might fail to initialize relationships properly
# for more details: https://github.com/tiangolo/full-stack-fastapi-postgresql/issues/28
from app.lib_db import insert_chapter
from app.lib_model import SEQUENCE_ERRORS, set_index
from app.models import Chapter, Crumb, Language, PartType, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOK_INDEX = "basair"
BOOK_PATH = "/books/" + BOOK_INDEX

TITLE_NUMBERING = re.compile(r' \(\d+\)')
HUBEALI_TRANSLATION_ID = "en.hubeali"
VOLUME_HEADING_PATTERN = re.compile("^AL-KAFI VOLUME")
TABLE_OF_CONTENTS_PATTERN = re.compile("^TABLE OF CONTENTS")
WHITESPACE_PATTERN = re.compile(r"^\s*$")
V8_HADITH_TITLE_PATTERN = re.compile(r"^H \d+")
V8_HADITH_BEGINNING_PATTERN = re.compile(r"^-? ?(1\d+)-?")
END_OF_HADITH_PATTERN = re.compile(r"<sup>\[\d+\]</sup>\s*$")
END_OF_HADITH_CLEANUP_PATTERN = re.compile(r'<a id="[^"]+"/?>(</a>)?<sup>\[\d+\]</sup>\s*$')

hubbeali_translation = Translation()
hubbeali_translation.name = "HubeAli.com"
hubbeali_translation.lang = Language.EN.value
hubbeali_translation.id = HUBEALI_TRANSLATION_ID

def we_dont_care(heading):
	if heading is None:
		return True
	
	htext = heading.get_text(strip=True).upper()
	if VOLUME_HEADING_PATTERN.match(htext):
		return True
	
	return False

def table_of_contents(heading):
	htext = heading.get_text(strip=True).upper()
	return TABLE_OF_CONTENTS_PATTERN.match(htext)

def get_contents(element):
	return "".join([str(x) for x in element.contents])

def join_texts(texts: List[str]) -> str:
	return "\n".join([text for text in texts])

def is_arabic_tag(element: Tag) -> bool:
	return element.has_attr('dir') and element['dir'] == 'rtl'

def is_section_break_tag(element: Tag) -> bool:
	return element.has_attr('class') and 'section-break' in element['class']

def is_book_title(element: Tag) -> bool:
	return element.has_attr('style') \
		and ("font-size: x-large" in element['style'] or "font-size: xx-large" in element['style']) \
		and "font-weight: bold" in element['style'] \
		and "text-align: center" in element['style'] \
		and ("text-decoration: underline" in element['style'] or "page-break-before: always" in element['style'])

def is_book_ending(element: Tag) -> bool:
	return element.has_attr('style') and "font-weight: bold" in element['style'] and "text-align: center" in element['style'] and "text-indent: 0" in element['style']

def is_chapter_title(element: Tag) -> bool:
	return element.has_attr('style') and "font-weight: bold" in element['style'] and "text-decoration: underline" in element['style']

def is_newline(element) -> bool:
	return isinstance(element, NavigableString) and WHITESPACE_PATTERN.match(element)

def add_hadith(chapter: Chapter, hadith_ar: List[str], hadith_en: List[str], part_type: PartType = PartType.Hadith):
	hadith = Verse()
	hadith.part_type = part_type
	hadith.text = hadith_ar

	text_en = [END_OF_HADITH_CLEANUP_PATTERN.sub('', txt) for txt in hadith_en]
	
	hadith.translations = {}
	hadith.translations[HUBEALI_TRANSLATION_ID] = text_en
	
	chapter.verses.append(hadith)


def build_hubeali_books(dirname) -> List[Chapter]:
	books: List[Chapter] = []
	logger.info("Adding Al-Kafi dir %s", dirname)

	cfiles = glob.glob(dirname + "c*.xhtml")

	book = None
	chapter = None
	book_title_ar = None
	chapter_title_ar = None
	hadith_ar = []
	hadith_en = []
	for cfile in cfiles:
		logger.info("Processing file %s", cfile)

		with open(cfile, 'r', encoding='utf8') as qfile:
			file_html = qfile.read()
			file_html = file_correction(cfile, file_html)
			soup = BeautifulSoup(file_html, 'html.parser')

			heading = soup.body.h1
			if we_dont_care(heading):
				continue

			if table_of_contents(heading):
				book_title_ar = get_contents(soup.body.contents[-2])
				continue

			heading_en = get_contents(heading.a)
			# sometimes the anchor is early terminated
			if not heading_en:
				heading_en = get_contents(heading)

			if book_title_ar:
				book = Chapter()
				book.part_type = PartType.Book
				book.titles = {}
				# Arabic title comes from previous file
				book.titles[Language.AR.value] = book_title_ar
				book.titles[Language.EN.value] = heading_en
				book_title_ar = None
				book.chapters = []

				books.append(book)

			elif (chapter_title_ar or not chapter) and heading_en.startswith('Chapter'):
				chapter = Chapter()
				chapter.part_type = PartType.Chapter
				chapter.titles = {}
				chapter.titles[Language.AR.value] = chapter_title_ar
				chapter.titles[Language.EN.value] = heading_en
				chapter_title_ar = None
				chapter.verse_translations = [hubbeali_translation]
				chapter.verses = []

				book.chapters.append(chapter)

			elif chapter_title_ar:
				add_hadith(chapter, [chapter_title_ar], [heading_en], PartType.Heading)

				chapter_title_ar = None


			last_element = soup.find('p', 'first-in-chapter')

			while last_element:
				if is_newline(last_element):
					last_element = last_element.next_sibling
					continue

				is_tag = isinstance(last_element, Tag)
				is_paragraph = is_tag and last_element.name == 'p'
				is_not_section_break_paragraph = is_paragraph and not is_section_break_tag(last_element)
				is_arabic = is_arabic_tag(last_element)

				element_content = get_contents(last_element)
				element_content = element_content.replace('style="font-style: italic; font-weight: bold"', 'class="ibTxt"')
				element_content = element_content.replace('style="font-weight: bold"', 'class="bTxt"')
				element_content = element_content.replace('style="font-style: italic"', 'class="iTxt"')

				is_end_of_hadith = END_OF_HADITH_PATTERN.search(element_content)
				
				if is_book_title(last_element):
					if hadith_ar and hadith_en:
						add_hadith(chapter, hadith_ar, hadith_en, PartType.Heading)
						hadith_ar = []
						hadith_en = []

					book_title_ar = element_content
					chapter = None
				elif is_chapter_title(last_element):
					if hadith_ar and hadith_en:
						if chapter:
							add_hadith(chapter, hadith_ar, hadith_en)
						else:
							book.descriptions = {}
							book.descriptions[Language.AR.value] = join_texts(hadith_ar)
							book.descriptions[Language.EN.value] = join_texts(hadith_en)
						hadith_ar = []
						hadith_en = []

					chapter_title_ar = element_content
				elif is_arabic:
					hadith_ar.append(element_content)
				# elif is_book_ending(last_element):
				# 	add_hadith(chapter, hadith_ar, [element_content], PartType.Heading)
				# 	hadith_ar = []
				# 	hadith_en = []
				elif is_not_section_break_paragraph:
					hadith_en.append(element_content)
				
				if is_end_of_hadith:
					add_hadith(chapter, hadith_ar, hadith_en)
					hadith_ar = []
					hadith_en = []

				last_element = last_element.next_sibling


	return books

def build_hubeali_book_8(dirname) -> List[Chapter]:
	logger.info("Adding Al-Kafi dir %s", dirname)

	cfiles = glob.glob(dirname + "c*.xhtml")

	book = Chapter()
	book.part_type = PartType.Book
	book.titles = {}
	# Arabic title comes from previous file
	book.titles[Language.AR.value] = "&#1603;&#1578;&#1575;&#1576; &#1575;&#1604;&#1585;&#1617;&#1614;&#1608;&#1618;&#1590;&#1614;&#1577;&#1616;"
	book.titles[Language.EN.value] = "The Book - Garden (of Flowers)"
	book.chapters = []
	
	is_the_end = False
	previous_hadith_num = 14449
	chapter = None
	chapter_title_ar = None
	hadith_ar = []
	hadith_en = []
	for cfile in cfiles:
		if is_the_end:
			break

		logger.info("Processing file %s", cfile)

		with open(cfile, 'r', encoding='utf8') as qfile:
			file_html = qfile.read()
			file_html = file_correction(cfile, file_html)
			soup = BeautifulSoup(file_html, 'html.parser')

			heading = soup.body.h1
			if we_dont_care(heading):
				continue

			if table_of_contents(heading):
				hadith_ar.append(get_contents(soup.body.contents[-2]))
				continue

			heading_en = get_contents(heading.a)
			is_hadith_title = V8_HADITH_TITLE_PATTERN.match(heading_en)
			# sometimes the anchor is early terminated
			if not heading_en or is_hadith_title:
				heading_en = get_contents(heading)

			if chapter_title_ar or not chapter:
				chapter = Chapter()
				chapter.part_type = PartType.Chapter
				chapter.titles = {}
				if chapter_title_ar:
					chapter.titles[Language.AR.value] = chapter_title_ar
				else:
					chapter.titles[Language.AR.value] = "&#1576;&#1616;&#1587;&#1618;&#1605;&#1616; &#1575;&#1604;&#1604;&#1617;&#1614;&#1607;&#1616; &#1575;&#1604;&#1585;&#1617;&#1614;&#1581;&#1618;&#1605;&#1614;&#1606;&#1616; &#1575;&#1604;&#1585;&#1617;&#1614;&#1581;&#1616;&#1610;&#1605;&#1616;"
				if heading_en:
					chapter.titles[Language.EN.value] = heading_en
				else:
					chapter.titles[Language.EN.value] = "In the name of Allah, the Beneficent, the Merciful"
				chapter_title_ar = None
				chapter.verses = []
				chapter.verse_translations = [hubbeali_translation]

				book.chapters.append(chapter)
			elif is_hadith_title:
				hadith_en.append(heading_en)


			last_element = soup.find('p', 'first-in-chapter')

			while last_element:
				if is_newline(last_element):
					last_element = last_element.next_sibling
					continue

				is_tag = isinstance(last_element, Tag)
				is_paragraph = is_tag and last_element.name == 'p'
				is_not_section_break_paragraph = is_paragraph and not is_section_break_tag(last_element)
				is_arabic = is_arabic_tag(last_element)

				element_content = get_contents(last_element)
				element_content = element_content.replace('style="font-style: italic; font-weight: bold"', 'class="ibTxt"')
				element_content = element_content.replace('style="font-weight: bold"', 'class="bTxt"')
				element_content = element_content.replace('style="font-style: italic"', 'class="iTxt"')

				is_new_hadith = V8_HADITH_BEGINNING_PATTERN.match(last_element.get_text(strip=True))
				is_the_end = element_content.startswith("&#1578;&#1614;&#1605;&#1617;&#1614; &#1603;&#1616;&#1578;&#1614;&#1575;&#1576;&#1615; &#1575;&#1604;&#1585;&#1617;&#1614;&#1608;&#1618;&#1590;&#1614;&#1577;&#1616; &#1605;&#1616;&#1606;&#1614;")

				# We commit the hadith that has been building up until now if we encounter a new hadith beginning
				if (is_new_hadith or is_the_end) and hadith_ar and hadith_en:
					add_hadith(chapter, hadith_ar, hadith_en)
					hadith_ar = []
					hadith_en = []

				if is_new_hadith:
					hadith_num = int(is_new_hadith.group(1))
					if previous_hadith_num + 1 != hadith_num:
						print("Skipped one hadith " + str(previous_hadith_num) + " to " + str(hadith_num) + " title: " + element_content)
					previous_hadith_num = hadith_num
				
				if is_chapter_title(last_element):
					if hadith_ar and hadith_en:
						add_hadith(chapter, hadith_ar, hadith_en)
						hadith_ar = []
						hadith_en = []

					chapter_title_ar = element_content
				elif is_arabic:
					hadith_ar.append(element_content)
				elif is_not_section_break_paragraph:
					hadith_en.append(element_content)
					if is_the_end:
						add_hadith(chapter, hadith_ar, hadith_en, PartType.Heading)
				
				last_element = last_element.next_sibling


	return [book]

def get_path(file):
	return os.path.join(os.path.dirname(__file__), "raw\\" + file)


def build_book() -> Chapter:
	kafi = Chapter()
	book.index = BOOK_INDEX
	book.path = BOOK_PATH
	book.titles = {
		Language.EN.value: "Basa'ir ad-Darajat",
		Language.AR.value: "بَصَائِر ٱلدَّرَجَات"
	}
	book.descriptions = {
			Language.EN.value: "Baṣāʾir ad-Darajāt Fī ʿUlūm ʾĀl Muḥammad wa-Mā Khaṣṣahum ʾAllāh Bihī (Arabic: بَصَائِر ٱلدَّرَجَات فِي عُلُوم آل مُحَمَّد وَمَا خَصَّهُم ٱلله بِهِ‎), alternatively known as Baṣaʾir ad-Darajāt al-Kubrā Fī Faḍāʾil ʾĀl Muḥammad (Arabic: بَصَائِر ٱلدَّرَجَات ٱلْكُبْرَىٰ فِي فَضَائِل آل مُحَمَّد‎), is a Hadith compilation considered to be one of the oldest books in Hadith among Shias. The book's author is Abū Jaʿfar (or Abūl-Hasan) Muḥammad ibn al-Hasan ibn Farrūkh al-ʾAʿraj (Arabic: ‌أبو جعفر [أو أبو الحسن] محمد بن الحسن بن فروخ الأعرج‎), popularly known as Sheikh aṣ-Ṣaffār al-Qummī (Arabic: ‌الشيخ الصفار القمي‎) (d. 290 AH / 902-903 CE) "
	}
	book.chapters = build_hubeali_books(get_path("hubeali_com\\BasaairAlDarajaat-Full\\"))

	# post_processor(kafi)
	book.verse_start_index = 0
	book.index = BOOK_INDEX
	book.path = BOOK_PATH
	
	crumb = Crumb()
	crumb.titles = book.titles
	crumb.indexed_titles = book.titles
	crumb.path = book.path
	book.crumbs = [crumb]

	set_index(book, [0, 0, 0, 0], 0)

	return book

def init_kafi():
	book = build_book()

	insert_chapter(book)

	pprint(SEQUENCE_ERRORS)
