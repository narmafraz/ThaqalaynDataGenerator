from __future__ import annotations

from typing import Dict, List, Optional, Set

from app.models.crumb import Crumb, Navigation
from app.models.enums import Language, PartType
from app.models.translation import Translation
from pydantic import BaseModel


class SpecialText(BaseModel):
	kind: str = None
	text: str = None
	path: str = None

class NarratorChain(BaseModel):
	text: str = None
	parts: List[SpecialText] = None

class Verse(BaseModel):
	narrator_chain: NarratorChain = None
	gradings: List[str] = None
	index: int = None
	local_index: int = None
	part_type: PartType = None
	path: str = None
	relations: Dict[str, Set[str]] = None
	sajda_type: str = None
	text: List[str] = None
	translations: Dict[str, List[str]] = None

class Chapter(BaseModel):
	chapters: List[Chapter] = None
	crumbs: List[Crumb] = None
	default_verse_translation_ids: Dict[str, str] = None
	descriptions: Dict[str, List[str]] = None
	index: int = None
	local_index: int = None
	nav: Navigation = None
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
