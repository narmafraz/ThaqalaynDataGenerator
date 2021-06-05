from typing import Dict, List, Optional

from app.models.enums import PartType
from pydantic import BaseModel


class Crumb(BaseModel):
	titles: Dict[str, Optional[str]] = None
	indexed_titles: Dict[str, Optional[str]] = None
	path: str = None

class Navigation(BaseModel):
	prev: Crumb = None
	next: Crumb = None
	up: Crumb = None
