# Aim is to get Quran verses in the first 30 chapters of Kitab al Hujjat of Al-Kafi

import csv
import glob
import json
import logging
import os
import re
from pprint import pprint

from app.lib_bs4 import get_contents, is_rtl_tag
from app.lib_db import insert_chapter_dict, load_chapter, write_file
from app.lib_model import SEQUENCE_ERRORS, set_index
from app.models import Chapter, Language, PartType, Translation, Verse
from bs4 import BeautifulSoup, NavigableString, Tag
from fastapi.encoders import jsonable_encoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

QURAN_QUOTE = re.compile(r'class="ibTxt">\[(\d+):(\d+)\]')

def query_chapter(quran, chapter):
	results = []
	for hadith in chapter['verses']:
		for ttext in hadith['translations']['en.hubeali']:
			all_matches = QURAN_QUOTE.findall(ttext)
			for match in all_matches:
				sura_no = int(match[0])
				verse_no = int(match[1])
				sura = quran['chapters'][sura_no - 1]
				sura_name = sura['titles']['ar']
				verse = sura['verses'][verse_no - 1]
				verse_text = verse['text'][0]
				chapter_name = chapter['titles']['ar']
				chapter_no = chapter['local_index']
				hadith_no = hadith['local_index']
				logger.info(f"{sura_name},{sura_no},{verse_no},{verse_text},http://tanzil.net/#{sura_no}:{verse_no},,,{chapter_no},{chapter_name},{hadith_no}")
				results.append([sura_name,sura_no,verse_no,verse_text,f"http://tanzil.net/#{sura_no}:{verse_no}","Sadegh","",chapter_no,chapter_name,hadith_no])
	
	return results

def query_book(quran, book):
	results = []
	for chapter in book['chapters']:
		if chapter['local_index'] > 30:
			logger.info('Done')
			break
		
		result = query_chapter(quran, chapter)
		results.extend(result)
	return results

def main():
	quran = load_chapter("/books/complete/quran")
	kafi = load_chapter("/books/complete/al-kafi")
	results = query_book(quran, kafi['chapters'][0]['chapters'][3])

	with open("kitab_hujjat_verses.csv", 'w', newline='', encoding='utf-8') as f:
		cw = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		for result in results:
			cw.writerow(result)


main()
