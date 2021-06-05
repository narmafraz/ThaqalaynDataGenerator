import copy
import re
from typing import Dict, List

from app.models import Chapter, Crumb, Language, Navigation, PartType

CHAPTER_TITLE_PATTERN = re.compile("Chapter (\d+)")

SEQUENCE_ERRORS = []

def get_chapters(book):
	if hasattr(book, 'chapters'):
		return book.chapters
	if 'chapters' in book:
		return book['chapters']
	return None

def get_verses(book):
	if hasattr(book, 'verses'):
		return book.verses
	if 'verses' in book:
		return book['verses']
	return None

def set_index(chapter: Chapter, indexes: List[int], depth: int) -> List[int]:
	# TODO: set before/after chapter links
	if len(indexes) < depth + 1:
		indexes.append(0)

	if get_verses(chapter):
		verse_local_index = 0
		for verse in chapter.verses:
			if verse.part_type == PartType.Hadith or verse.part_type == PartType.Verse:
				indexes[depth] = indexes[depth] + 1
				verse.index = indexes[depth]
				verse_local_index = verse_local_index + 1
				verse.local_index = verse_local_index
				verse.path = chapter.path + ":" + str(verse_local_index)
		chapter.verse_count = indexes[depth] - chapter.verse_start_index
	
	report_numbering = True
	sequence = None
	prev_chapter = None
	if get_chapters(chapter):
		chapter_local_index = 0
		for subchapter in chapter.chapters:
			indexes[depth] = indexes[depth] + 1
			subchapter.index = indexes[depth]
			chapter_local_index = chapter_local_index + 1
			subchapter.local_index = chapter_local_index
			subchapter.path = chapter.path + ":" + str(chapter_local_index)
			subchapter.verse_start_index = indexes[-1]

			if report_numbering and subchapter.part_type == PartType.Chapter:
				chapter_number_str = CHAPTER_TITLE_PATTERN.search(subchapter.titles['en'])
				if chapter_number_str:
					chapter_number = int(chapter_number_str.group(1))
					if sequence and sequence + 1 != chapter_number:
						error_msg = 'Chapter ' + str(chapter_local_index) + ' with indexes ' + str(indexes) + ' does not match title ' + str(subchapter.titles)
						print(error_msg)
						SEQUENCE_ERRORS.append(error_msg)
						# raise Exception('Chapter ' + str(chapter_local_index) + ' with indexes ' + str(indexes) + ' does not match title ' + str(subchapter.titles))
					sequence = chapter_number
					# if chapter_number != chapter_local_index:
						# print('Chapter ' + str(chapter_local_index) + ' with indexes ' + str(indexes) + ' does not match title ' + str(subchapter.titles))
						# report_numbering = False
						# raise Exception('Chapter ' + str(chapter_local_index) + ' with indexes ' + str(indexes) + ' does not match title ' + str(subchapter.titles))

			subchapter.crumbs = copy.copy(chapter.crumbs)
			crumb = Crumb()
			crumb.indexed_titles = {
				Language.EN.value: subchapter.part_type.name + ' ' + str(subchapter.local_index)
			}
			crumb.titles = subchapter.titles
			crumb.path = subchapter.path
			subchapter.crumbs.append(crumb)

			subchapter.nav = Navigation()
			if prev_chapter:
				subchapter.nav.prev = prev_chapter.crumbs[-1]
				prev_chapter.nav.next = crumb
			if len(subchapter.crumbs) >= 2:
				subchapter.nav.up = subchapter.crumbs[-2]
			prev_chapter = subchapter

			indexes = set_index(subchapter, indexes, depth + 1)
		chapter.verse_count = indexes[-1] - chapter.verse_start_index

	return indexes
