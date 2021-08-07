from typing import Dict, List, Optional, Set

from app.models.quran import Verse
from pydantic import BaseModel


class ChainVerses(BaseModel):
	narrator_ids: List[int] = None
	verse_paths: Set[str] = None

class Narrator(BaseModel):
	titles: Dict[str, Optional[str]] = None
	index: int = None
	path: str = None
	verse_count: int = None
	verse_paths: Set[str] = None
	relations: Dict[str, Set[str]] = None
	subchains: Dict[str, ChainVerses] = None

class NarratorIndex(BaseModel):
	id_name: Dict[int, str] = None
	name_id: Dict[str, int] = None
	last_id: int = 0
