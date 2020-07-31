import glob
import json
import logging
import math
import os
import re
import sqlite3
import xml.etree.ElementTree
from pprint import pprint
from sqlite3 import Error
from typing import Dict, List

from bs4 import BeautifulSoup, NavigableString, Tag
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app import crud
from app.core import config
# make sure all SQL Alchemy models are imported before initializing DB
# otherwise, SQL Alchemy might fail to initialize relationships properly
# for more details: https://github.com/tiangolo/full-stack-fastapi-postgresql/issues/28
from app.db import base
from app.db.base import Base
from app.db.session import engine
from app.schemas.book_part import BookPartCreate
from app.kafi_corrections import file_correction
from app.lib_db import insert_chapter
from app.lib_model import SEQUENCE_ERRORS, set_index
from app.models import (Chapter, Crumb, Language, PartType, Quran,
                         Translation, Verse)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOK_INDEX = "al-kafi"
BOOK_PATH = "/books/" + BOOK_INDEX

TITLE_NUMBERING = re.compile(r' \(\d+\)')

def extract_headings(headings):
	assert len(headings) > 0, "Did not find headings in " + str(headings)
	names = {}
	if len(headings) > 1:
		names[Language.AR.value] = headings[0].get_text(strip=True)
		names[Language.EN.value] = TITLE_NUMBERING.sub('', headings[1].get_text(strip=True))
	else:
		names[Language.EN.value] = TITLE_NUMBERING.sub('', headings[0].get_text(strip=True))
	return names


def build_alhassanain_baabs(file) -> List[Chapter]:
	baabs: List[Chapter] = []
	logger.info("Adding Al-Kafi file %s", file)

	with open(file, 'r', encoding='utf8') as qfile:
		inner_html = qfile.read()
		sections = inner_html.split("<br clear=all>")
		for section in sections:
			section_soup = BeautifulSoup(section, 'html.parser')
			
			headings = section_soup.select(".Heading1Center")
			if not headings:
				continue
			
			# process "the book of" chapter
			baab_titles = extract_headings(headings)
			
			en_title = baab_titles[Language.EN.value]
			
			baab = None
			for existing_baab in baabs:
				if existing_baab.titles[Language.EN.value] == en_title:
					baab = existing_baab
			
			if not baab:
				baab = Chapter()
				baab.part_type = PartType.Book
				baab.titles = baab_titles
				baab.chapters = []

				baabs.append(baab)

			# process chapters
			chapters = section_soup.select(".Heading2Center")
			chapters_len = len(chapters)
			for subchapter_index in range(math.ceil(chapters_len / 2)):
				subchapter_heading_index = subchapter_index * 2
				
				remaining_chapters = chapters[subchapter_heading_index:]
				if len(remaining_chapters) > 1:
					remaining_chapters = remaining_chapters[:2]
				chapter_titles = extract_headings(remaining_chapters)

				chapter = Chapter()
				chapter.part_type = PartType.Chapter
				chapter.titles = chapter_titles
				chapter.verses = []

				baab.chapters.append(chapter)

				last_element = remaining_chapters[-1]
				last_element = last_element.next_sibling

				verse: Verse = None
				while (last_element is not None
					and (isinstance(last_element, NavigableString) 
						or ( isinstance(last_element, Tag) and 'Heading2Center' not in last_element['class'])
						)
					):
					is_tag = isinstance(last_element, Tag)
					if is_tag and 'libAr' in last_element['class']:
						
						# push the last verse if its not the start of chapter
						if verse != None:
							chapter.verses.append(verse)
						
						verse = Verse()
						verse.part_type = PartType.Hadith
						translation = Translation()
						translation.name = "hubeali"
						translation.lang = Language.EN.value
						translation.text = None
						verse.translations = [translation]

						verse.text = last_element.get_text(strip=True)

					if is_tag and 'libNormal' in last_element['class']:
						if verse.translations[0].text:
							verse.translations[0].text = verse.translations[0].text + "\n" + last_element.get_text(strip=True)
						else:
							verse.translations[0].text = last_element.get_text(strip=True)

					last_element = last_element.next_sibling

				if verse != None:
					chapter.verses.append(verse)

	
	return baabs

VOLUME_HEADING_PATTERN = re.compile("^AL-KAFI VOLUME")
TABLE_OF_CONTENTS_PATTERN = re.compile("^TABLE OF CONTENTS")
WHITESPACE_PATTERN = re.compile(r"^\s*$")
V8_HADITH_TITLE_PATTERN = re.compile(r"^H \d+")
V8_HADITH_BEGINNING_PATTERN = re.compile(r"^-? ?(1\d+)-?")
END_OF_HADITH_PATTERN = re.compile(r"<sup>\[\d+\]</sup>\s*$")
END_OF_HADITH_CLEANUP_PATTERN = re.compile(r'<a id="[^"]+"/?>(</a>)?<sup>\[\d+\]</sup>\s*$')

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
	hadith.text = join_texts(hadith_ar)

	text_en = join_texts(hadith_en)
	text_en = END_OF_HADITH_CLEANUP_PATTERN.sub('', text_en)
	if END_OF_HADITH_PATTERN.search(text_en):
		raise Exception(text_en)
	
	translation = Translation()
	translation.name = "hubeali"
	translation.lang = Language.EN.value
	translation.text = text_en
	hadith.translations = [translation]
	
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

def build_volume(file, title_en: str, title_ar: str, description: str, last_volume: bool = False) -> Chapter:
	volume = Chapter()
	volume.titles = {
		Language.EN.value: title_en,
		Language.AR.value: title_ar
	}
	volume.descriptions = {
			Language.EN.value: description
	}
	if last_volume:
		volume.chapters = build_hubeali_book_8(file)
	else:
		volume.chapters = build_hubeali_books(file)
	volume.part_type = PartType.Volume

	return volume

def get_path(file):
	return os.path.join(os.path.dirname(__file__), "raw\\" + file)


def build_kafi() -> Chapter:
	kafi = Chapter()
	kafi.index = BOOK_INDEX
	kafi.path = BOOK_PATH
	kafi.titles = {
		Language.EN.value: "Al-Kafi",
		Language.AR.value: "الكافي"
	}
	kafi.descriptions = {
			Language.EN.value: "Of the majestic narrator and the scholar, the jurist, the Sheykh Muhammad Bin Yaqoub Al-Kulayni Well known as ‘The trustworthy of Al-Islam Al-Kulayni’ Who died in the year 329 H"
	}
	kafi.chapters = []

	kafi.chapters.append(build_volume(
		get_path("hubeali_com\\Al-Kafi-Volume-1\\"),
		"Volume One",
		"الجزء الأول‏",
		"First volume of Al-Kafi"))

	kafi.chapters.append(build_volume(
		get_path("hubeali_com\\Al-Kafi-Volume-2\\"),
		"Volume Two",
		"الجزء الثاني‏",
		"Second volume of Al-Kafi"))

	kafi.chapters.append(build_volume(
		get_path("hubeali_com\\Al-Kafi-Volume-3\\"),
		"Volume Three",
		"الجزء الثالث‏",
		"Third volume of Al-Kafi"))

	kafi.chapters.append(build_volume(
		get_path("hubeali_com\\Al-Kafi-Volume-4\\"),
		"Volume Four",
		"الجزء الرابع‏",
		"Forth volume of Al-Kafi"))

	kafi.chapters.append(build_volume(
		get_path("hubeali_com\\Al-Kafi-Volume-5\\"),
		"Volume Five",
		"الجزء الخامس‏",
		"Fifth volume of Al-Kafi"))

	kafi.chapters.append(build_volume(
		get_path("hubeali_com\\Al-Kafi-Volume-6\\"),
		"Volume Six",
		"الجزء السادس‏",
		"Sixth volume of Al-Kafi"))

	kafi.chapters.append(build_volume(
		get_path("hubeali_com\\Al-Kafi-Volume-7\\"),
		"Volume Seven",
		"الجزء السابع‏",
		"Seventh volume of Al-Kafi"))

	kafi.chapters.append(build_volume(
		get_path("hubeali_com\\Al-Kafi-Volume-8\\"),
		"Volume Eight",
		"الجزء الثامن‏",
		"Eighth volume of Al-Kafi", True))

	# kafi.chapters.append(build_volume(
	# 	get_path("alhassanain_org\\hubeali_com_usul_kafi_v_01_ed_html\\usul_kafi_v_01_ed.htm"),
	# 	"Volume 1",
	# 	"جلد اول",
	# 	"First volume of Al-Kafi"))

	# kafi.chapters.append(build_volume(
	# 	get_path("alhassanain_org\\hubeali_com_usul_kafi_v_02_ed_html\\usul_kafi_v_02_ed.htm"),
	# 	"Volume 2",
	# 	"جلد 2",
	# 	"Second volume of Al-Kafi"))

	# kafi.chapters.append(build_volume(
	# 	get_path("alhassanain_org\\hubeali_com_usul_kafi_v_03_ed_html\\usul_kafi_v_03_ed.htm"),
	# 	"Volume 3",
	# 	"جلد 3",
	# 	"Third volume of Al-Kafi"))

	# post_processor(kafi)
	kafi.verse_start_index = 0
	kafi.index = BOOK_INDEX
	kafi.path = BOOK_PATH
	
	crumb = Crumb()
	crumb.titles = kafi.titles
	crumb.indexed_titles = kafi.titles
	crumb.path = kafi.path
	kafi.crumbs = [crumb]

	set_index(kafi, [0, 0, 0, 0], 0)

	return kafi

def init_kafi(db_session: Session):
	book = build_kafi()

	with open(get_path('kafi.json'), 'w', encoding='utf-8') as f:
		json.dump(jsonable_encoder(book), f, ensure_ascii=False, indent=2, sort_keys=True)

	insert_chapter(db_session, book)

	pprint(SEQUENCE_ERRORS)
