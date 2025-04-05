from typing import Dict, List, Optional

from pydantic import BaseModel


class Crumb(BaseModel):
	titles: Dict[str, Optional[str]] = None
	indexed_titles: Dict[str, Optional[str]] = None
	path: str = None

class Navigation(BaseModel):
	prev: str = None
	next: str = None
	up: str = None
