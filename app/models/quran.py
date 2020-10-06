from __future__ import annotations

from typing import Dict, List, Optional

from app.models.crumb import Crumb
from app.models.enums import Language, PartType
from app.models.translation import Translation
from pydantic import BaseModel


class Verse(BaseModel):
	chain_text: str = None
	index: int = None
	local_index: int = None
	part_type: PartType = None
	path: str = None
	sajda_type: str = None
	text: List[str] = None
	gradings: List[str] = None
	translations: Dict[str, List[str]] = None

class Chapter(BaseModel):
	chapters: List[Chapter] = None
	crumbs: List[Crumb] = None
	default_verse_translation_ids: Dict[str, str] = None
	descriptions: Dict[str, str] = None
	index: str = None
	local_index: int = None
	order: int = None
	part_type: PartType = None
	path: str = None
	reveal_type: str = None
	rukus: int = None
	sajda_type: str = None
	titles: Dict[str, Optional[str]] = None
	translations: Dict[str, str] = None
	verse_count: int = None
	verse_start_index: int = None
	verse_translations: List[Translation] = None
	verses: List[Verse] = None

Chapter.update_forward_refs()
