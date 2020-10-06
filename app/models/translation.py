from pydantic import BaseModel

class Translation(BaseModel):
	name: str
	id: str
	lang: str
