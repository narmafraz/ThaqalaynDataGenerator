from typing import Dict, List, Optional

from app.models.crumb import Crumb
from pydantic import BaseModel


class Index(BaseModel):
	index: Dict[str, Crumb] = None
