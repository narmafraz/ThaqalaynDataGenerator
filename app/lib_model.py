import copy
import logging
import re
from typing import Dict, List, Optional

from app.models import Chapter, Crumb, Language, Navigation, PartType

logger = logging.getLogger(__name__)

CHAPTER_TITLE_PATTERN = re.compile(r"Chapter (\d+)")


class ProcessingReport:
	"""Accumulates errors and counters across the data generation pipeline.

	Replaces module-level globals (SEQUENCE_ERRORS, NARRATIONS_WITHOUT_NARRATORS)
	so that state is scoped to a single pipeline run and tests can create
	isolated instances.
	"""

	def __init__(self):
		self.sequence_errors: List[str] = []
		self.narrations_without_narrators: int = 0
		self.ai_verses_merged: int = 0
		self.ai_verses_available: int = 0
		self.ai_merge_errors: List[str] = []

	def add_sequence_error(self, msg: str):
		self.sequence_errors.append(msg)

	def print_summary(self):
		if self.sequence_errors:
			logger.info("Sequence errors (%d):", len(self.sequence_errors))
			for err in self.sequence_errors:
				logger.info("  %s", err)
		if self.narrations_without_narrators:
			logger.info("Narrations without narrators: %d", self.narrations_without_narrators)
		if self.ai_verses_available:
			logger.info("AI content: %d/%d verses merged", self.ai_verses_merged, self.ai_verses_available)
		if self.ai_merge_errors:
			logger.info("AI merge errors (%d):", len(self.ai_merge_errors))
			for err in self.ai_merge_errors:
				logger.info("  %s", err)


# Global singleton for backward compatibility during migration.
# New code should create and pass explicit instances.
SEQUENCE_ERRORS = []
_default_report: Optional[ProcessingReport] = None


def get_default_report() -> ProcessingReport:
	"""Get or create the default global ProcessingReport."""
	global _default_report
	if _default_report is None:
		_default_report = ProcessingReport()
	return _default_report


def reset_default_report():
	"""Reset the default global ProcessingReport (for testing)."""
	global _default_report
	_default_report = None

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

def set_index(chapter: Chapter, indexes: List[int], depth: int, report: Optional[ProcessingReport] = None) -> List[int]:
	if report is None:
		report = get_default_report()

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

			if report_numbering and subchapter.part_type == PartType.Chapter and 'en' in subchapter.titles:
				chapter_number_str = CHAPTER_TITLE_PATTERN.search(subchapter.titles['en'])
				if chapter_number_str:
					chapter_number = int(chapter_number_str.group(1))
					if sequence and sequence + 1 != chapter_number:
						error_msg = 'Chapter ' + str(chapter_local_index) + ' with indexes ' + str(indexes) + ' does not match title ' + str(subchapter.titles)
						logger.warning(error_msg)
						report.add_sequence_error(error_msg)
						SEQUENCE_ERRORS.append(error_msg)
					sequence = chapter_number

			subchapter.nav = Navigation()
			if prev_chapter:
				subchapter.nav.prev = prev_chapter.path
				prev_chapter.nav.next = subchapter.path
			subchapter.nav.up = chapter.path
			prev_chapter = subchapter

			indexes = set_index(subchapter, indexes, depth + 1, report)
		chapter.verse_count = indexes[-1] - chapter.verse_start_index

	return indexes
