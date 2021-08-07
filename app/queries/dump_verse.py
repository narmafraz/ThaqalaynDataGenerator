# Aim is to get Quran verses in the first 30 chapters of Kitab al Hujjat of Al-Kafi

import csv
import glob
import json
import logging
import os
import re
from pprint import pprint

from app.lib_db import load_chapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SPAN_PATTERN = re.compile(u"<\/?span[^>]*>")

def main():
	kafi = load_chapter("/books/complete/al-kafi")
	result = kafi.chapters[7].chapters[0].chapters[12].verses[0]
	text = SPAN_PATTERN.sub("", result.text[0])

	print(text)



main()
