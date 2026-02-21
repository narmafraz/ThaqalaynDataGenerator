"""Arabic text normalization utilities for name matching.

Provides functions to normalize Arabic text by stripping diacritics,
normalizing letter variants (hamza, teh marbuta, alef), and
standardizing whitespace. Used in the name matching pipeline to
compare narrator names from different sources.
"""

import re
import unicodedata


# Unicode ranges for Arabic diacritics (tashkeel)
# Fathah, Dammah, Kasrah, Sukun, Shadda, Tanwin variants, etc.
_DIACRITICS = re.compile(
    '[\u0610-\u061A'   # Small signs above/below
    '\u064B-\u065F'    # Standard tashkeel (fathatan through hamza below)
    '\u0670'           # Superscript alef
    '\u06D6-\u06DC'    # Small high ligatures
    '\u06DF-\u06E4'    # Small high marks
    '\u06E7-\u06E8'    # Small high yeh/noon
    '\u06EA-\u06ED'    # Small low marks
    '\u08D3-\u08FF'    # Extended Arabic marks
    ']'
)

# Alef variants to normalize
_ALEF_VARIANTS = {
    '\u0622': '\u0627',  # Alef with madda above -> plain alef
    '\u0623': '\u0627',  # Alef with hamza above -> plain alef
    '\u0625': '\u0627',  # Alef with hamza below -> plain alef
    '\u0671': '\u0627',  # Alef wasla -> plain alef
    '\u0672': '\u0627',  # Alef with wavy hamza above -> plain alef
    '\u0673': '\u0627',  # Alef with wavy hamza below -> plain alef
}

# Teh marbuta -> heh
_TEH_MARBUTA = '\u0629'
_HEH = '\u0647'

# Hamza variants to normalize
_HAMZA_VARIANTS = {
    '\u0624': '\u0648',  # Waw with hamza -> plain waw
    '\u0626': '\u064A',  # Yeh with hamza -> plain yeh
}

# Alef maksura -> yeh
_ALEF_MAKSURA = '\u0649'
_YEH = '\u064A'

# Tatweel (kashida) - decorative elongation
_TATWEEL = '\u0640'

# Common honorific phrases to strip for matching
_HONORIFICS = [
    '( عليه السلام )',
    '( عليهم السلام )',
    '( صلى الله عليه وآله )',
    '( صلوات الله عليه )',
    '( عليها السلام )',
    '(عليه السلام)',
    '(عليهم السلام)',
    '(صلى الله عليه وآله)',
]


def strip_diacritics(text: str) -> str:
    """Remove all Arabic diacritical marks (tashkeel) from text."""
    return _DIACRITICS.sub('', text)


def normalize_alef(text: str) -> str:
    """Normalize all alef variants to plain alef."""
    for variant, replacement in _ALEF_VARIANTS.items():
        text = text.replace(variant, replacement)
    return text


def normalize_teh_marbuta(text: str) -> str:
    """Normalize teh marbuta to heh."""
    return text.replace(_TEH_MARBUTA, _HEH)


def normalize_hamza(text: str) -> str:
    """Normalize hamza-on-carrier variants to plain carrier letters."""
    for variant, replacement in _HAMZA_VARIANTS.items():
        text = text.replace(variant, replacement)
    return text


def normalize_alef_maksura(text: str) -> str:
    """Normalize alef maksura to yeh."""
    return text.replace(_ALEF_MAKSURA, _YEH)


def strip_tatweel(text: str) -> str:
    """Remove tatweel (kashida) characters."""
    return text.replace(_TATWEEL, '')


def strip_honorifics(text: str) -> str:
    """Remove common Islamic honorific phrases from narrator names."""
    for h in _HONORIFICS:
        text = text.replace(h, '')
    return text


def normalize_whitespace(text: str) -> str:
    """Collapse multiple whitespace to single space and strip."""
    return re.sub(r'\s+', ' ', text).strip()


def normalize_arabic(text: str) -> str:
    """Apply full Arabic normalization pipeline.

    Steps:
    1. Strip diacritics (tashkeel)
    2. Normalize alef variants
    3. Normalize teh marbuta to heh
    4. Normalize hamza-on-carrier variants
    5. Normalize alef maksura to yeh
    6. Remove tatweel
    7. Normalize whitespace

    This is the standard normalization used in the name matching pipeline.
    """
    text = strip_diacritics(text)
    text = normalize_alef(text)
    text = normalize_teh_marbuta(text)
    text = normalize_hamza(text)
    text = normalize_alef_maksura(text)
    text = strip_tatweel(text)
    text = normalize_whitespace(text)
    return text


def normalize_for_matching(text: str) -> str:
    """Normalize text for name matching, including honorific removal.

    Applies full Arabic normalization plus strips honorific phrases.
    Used specifically in the name matching pipeline where we want
    to compare bare names without titles of respect.
    """
    text = strip_honorifics(text)
    text = normalize_arabic(text)
    return text
