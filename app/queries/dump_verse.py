# Aim is to get Quran verses in the first 30 chapters of Kitab al Hujjat of Al-Kafi

import csv
import glob
import json
import logging
import os
import re
from pprint import pprint

from app.kafi_narrators import extract_narrators
from app.lib_db import load_chapter
from app.models.quran import Verse

PATH = "6:6:48:17"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SPAN_PATTERN = re.compile(u"<\/?span[^>]*>")

def get_text(vol, book, chapter, hadith):
	kafi = load_chapter("/books/complete/al-kafi")
	result = kafi.chapters[vol].chapters[book].chapters[chapter].verses[hadith]
	text = SPAN_PATTERN.sub("", result.text[0])
	return text

def get_indices(path):
	nums = re.split(r"(?:#h|:)", path)
	indices = [int(x)-1 for x in nums]
	return indices

def write_test(text, vol, book, chapter, hadith):
	test = f"def test_{vol+1}_{book+1}_{chapter+1}_{hadith+1}():\n"
	test += "    assert_text_narrators(\n"
	test += f"        '{text}',\n"
	test += "        [\n"

	v = Verse()
	v.text = [text]
	narrators = extract_narrators(v)
	for narrator in narrators:
		test += f"            '{narrator}',\n"

	test += "        ]\n"
	test += "    )\n"
	return test


indices = get_indices(PATH)
text0 = get_text(*indices)
#print(text0)
print(write_test(text0, *indices))
