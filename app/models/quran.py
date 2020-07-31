from __future__ import annotations

from typing import Dict, List

from app.models.crumb import Crumb
from app.models.enums import Language, PartType
from app.models.translation import Translation


class Verse():
	index: int
	local_index: int
	path: str
	text: str
	chain_text: str
	sajda_type: str
	translations: List[Translation]
	part_type: PartType

class Chapter():
	verses: List[Verse]
	chapters: List[Chapter]
	index: int
	local_index: int
	path: str
	verse_count: int
	verse_start_index: int
	titles: Dict[str, str]
	descriptions: Dict[str, str]
	reveal_type: str
	order: int
	rukus: int
	sajda_type: str = None
	crumbs: List[Crumb]
	part_type: PartType

class Quran():
	chapters: List[Chapter]
