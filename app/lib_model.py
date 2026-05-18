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

def set_index(
	chapter: Chapter,
	indexes: List[int],
	depth: int,
	report: Optional[ProcessingReport] = None,
	verse_counter: Optional[List[int]] = None,
) -> List[int]:
	"""Assign hierarchical indexes + verse counts to ``chapter`` and its descendants.

	``indexes`` is the legacy per-depth global counter list. Its semantics are
	preserved so callers and consumers that rely on ``subchapter.index`` (the
	running count of chapters seen at each tree depth across the whole book)
	continue to work unchanged.

	``verse_counter`` is a single-element list used as a mutable cumulative
	count of verses processed so far in the whole book. Decoupling this from
	``indexes`` is what fixes the bug where the first chapter at each tree
	level got a wrong ``verse_start_index`` and a negative ``verse_count`` —
	the old code used ``indexes[-1]`` as a proxy for the verse counter, which
	silently broke in mixed-depth trees (e.g. man-la-yahduruhu-al-faqih, where
	some chapters have verses directly and others have grand-children).

	Callers don't need to pass ``verse_counter`` — the top-level call creates
	one. Pre-fix callers continue to work; ``verse.index``, ``verse_start_index``
	and ``verse_count`` come out semantically correct for every shape.
	"""
	if report is None:
		report = get_default_report()
	if verse_counter is None:
		# Top of the recursion — start a fresh cumulative-verse counter.
		# Single-element list because Python ints are immutable; this gives us
		# a reference we can mutate across recursive calls without sentinels.
		verse_counter = [0]

	if len(indexes) < depth + 1:
		indexes.append(0)

	if get_verses(chapter) is not None:
		# Note: `is not None` (not just truthiness) so a chapter with an
		# explicit empty `verses=[]` still gets verse_count set to 0 instead
		# of left as None.
		verse_local_index = 0
		for verse in chapter.verses:
			if verse.part_type == PartType.Hadith or verse.part_type == PartType.Verse:
				verse_counter[0] += 1
				indexes[depth] = indexes[depth] + 1
				# verse.index is globally monotonic across the book regardless
				# of tree depth — same as cumulative verse count at this point.
				verse.index = verse_counter[0]
				verse_local_index = verse_local_index + 1
				verse.local_index = verse_local_index
				verse.path = chapter.path + ":" + str(verse_local_index)
		# verse_count for a leaf is the number of countable verses directly under it.
		chapter.verse_count = verse_local_index

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
			# Cumulative-verse count snapshot at the moment this subchapter starts.
			subchapter.verse_start_index = verse_counter[0]

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

			indexes = set_index(subchapter, indexes, depth + 1, report, verse_counter)
		# Aggregate verse_count = verses seen during the recursion into this subtree.
		chapter.verse_count = verse_counter[0] - chapter.verse_start_index

	return indexes
