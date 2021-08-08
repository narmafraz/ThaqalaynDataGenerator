# Aim is to get Quran verses in the first 30 chapters of Kitab al Hujjat of Al-Kafi

import csv
import glob
import json
import logging
import os
import re
from pprint import pprint

from app.lib_db import load_chapter
from app.models import Chapter, Language, PartType, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SEARCH_TERM = "قَالَ حَدَّثَنِي"

def query_chapter(chapter: Chapter):
	results = []
	for hadith in chapter.verses:
		for text in hadith.text:
			if SEARCH_TERM in text:
				results.append(hadith.path)
				logger.info(hadith.path)
	
	return results

def query_book(book: Chapter):
	results = []
	for chapter in book.chapters:
		if chapter.chapters:
			result = query_book(chapter)
		else:
			result = query_chapter(chapter)
		results.extend(result)
	return results

def main():
	kafi = load_chapter("/books/complete/al-kafi")
	results = query_book(kafi)


main()
