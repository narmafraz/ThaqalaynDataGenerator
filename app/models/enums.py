from enum import Enum, auto


class LowerCaseAutoName(Enum):
    def _generate_next_value_(name: str, start, count, last_values):
        return name.lower()

class AutoName(Enum):
    def _generate_next_value_(name: str, start, count, last_values):
        return name

class Language(LowerCaseAutoName):
    AR = auto()
    EN = auto()
    ENT = auto()
    FA = auto()

class PartType(AutoName):
    Verse = auto()
    Volume = auto()
    Book = auto()
    Chapter = auto()
    Hadith = auto()
    Heading = auto()
