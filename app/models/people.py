from typing import Dict, List, Optional, Set

from app.models.quran import Verse
from pydantic import BaseModel, field_serializer


class ChainVerses(BaseModel):
	narrator_ids: List[int] = None
	verse_paths: Set[str] = None

	@field_serializer('verse_paths')
	@classmethod
	def sort_verse_paths(cls, v):
		if v is None:
			return None
		return sorted(v)

class Narrator(BaseModel):
	titles: Dict[str, Optional[str]] = None
	index: int = None
	path: str = None
	verse_count: int = None
	verse_paths: Set[str] = None
	relations: Dict[str, Set[str]] = None
	subchains: Dict[str, ChainVerses] = None

	@field_serializer('verse_paths')
	@classmethod
	def sort_verse_paths(cls, v):
		if v is None:
			return None
		return sorted(v)

	@field_serializer('relations')
	@classmethod
	def sort_relations(cls, v):
		if v is None:
			return None
		return {k: sorted(vals) for k, vals in v.items()}

class NarratorIndex(BaseModel):
	id_name: Dict[int, str] = None
	name_id: Dict[str, int] = None
	last_id: int = 0
