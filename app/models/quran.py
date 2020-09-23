from __future__ import annotations

from typing import Dict, List

from app.models.crumb import Crumb
from app.models.enums import Language, PartType
from app.models.translation import Translation


class Verse:
	chain_text: str
	index: int
	local_index: int
	part_type: PartType
	path: str
	sajda_type: str
	text: List[str]
	gradings: List[str]
	translations: Dict[str, List[str]]

class Chapter:
	chapters: List[Chapter]
	crumbs: List[Crumb]
	default_verse_translation_ids: Dict[str, str]
	descriptions: Dict[str, List[str]]
	index: int
	local_index: int
	order: int
	part_type: PartType
	path: str
	reveal_type: str
	rukus: int
	sajda_type: str
	title: str
	translations: Dict[str, str]
	verse_count: int
	verse_start_index: int
	verse_translations: List[Translation]
	verses: List[Verse]
