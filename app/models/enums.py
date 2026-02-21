from enum import Enum, auto


class LowerCaseAutoName(Enum):
    def _generate_next_value_(name: str, start, count, last_values):
        return name.lower()

class AutoName(Enum):
    def _generate_next_value_(name: str, start, count, last_values):
        return name

class Language(LowerCaseAutoName):
    AR = auto()
    BN = auto()
    DE = auto()
    EN = auto()
    ENT = auto()
    ES = auto()
    FA = auto()
    FR = auto()
    ID = auto()
    RU = auto()
    TR = auto()
    UR = auto()
    ZH = auto()

class PartType(AutoName):
    Verse = auto()
    Volume = auto()
    Book = auto()
    Chapter = auto()
    Hadith = auto()
    Heading = auto()
    Section = auto()
