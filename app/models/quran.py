from __future__ import annotations

from typing import Dict, List, Optional, Set

from app.models.crumb import Crumb, Navigation
from app.models.enums import Language, PartType
from app.models.translation import Translation
from pydantic import BaseModel


class SpecialText(BaseModel):
	kind: Optional[str] = None
	text: Optional[str] = None
	path: Optional[str] = None

class NarratorChain(BaseModel):
	text: Optional[str] = None
	parts: Optional[List[SpecialText]] = None

class Verse(BaseModel):
	narrator_chain: Optional[NarratorChain] = None
	gradings: Optional[Dict[str, str]] = None
	index: Optional[int] = None
	local_index: Optional[int] = None
	part_type: Optional[PartType] = None
	path: Optional[str] = None
	relations: Optional[Dict[str, Set[str]]] = None
	sajda_type: Optional[str] = None
	source_url: Optional[str] = None
	text: Optional[List[str]] = None
	translations: Optional[Dict[str, List[str]]] = None

class Chapter(BaseModel):
	author: Optional[Dict[str, str]] = None
	chapters: Optional[List[Chapter]] = None
	crumbs: Optional[List[Crumb]] = None
	default_verse_translation_ids: Optional[Dict[str, str]] = None
	descriptions: Optional[Dict[str, List[str]]] = None
	index: Optional[int] = None
	local_index: Optional[int] = None
	nav: Optional[Navigation] = None
	order: Optional[int] = None
	part_type: Optional[PartType] = None
	path: Optional[str] = None
	reveal_type: Optional[str] = None
	rukus: Optional[int] = None
	sajda_type: Optional[str] = None
	source_url: Optional[str] = None
	titles: Optional[Dict[str, Optional[str]]] = None
	translator: Optional[Dict[str, str]] = None
	translations: Optional[Dict[str, str]] = None
	verse_count: Optional[int] = None
	verse_start_index: Optional[int] = None
	verse_translations: Optional[List[str]] = None
	verses: Optional[List[Verse]] = None

Chapter.model_rebuild()
