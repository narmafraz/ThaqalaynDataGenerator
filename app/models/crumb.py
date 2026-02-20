from typing import Dict, List, Optional

from pydantic import BaseModel


class Crumb(BaseModel):
	titles: Optional[Dict[str, Optional[str]]] = None
	indexed_titles: Optional[Dict[str, Optional[str]]] = None
	path: Optional[str] = None

class Navigation(BaseModel):
	prev: Optional[str] = None
	next: Optional[str] = None
	up: Optional[str] = None
