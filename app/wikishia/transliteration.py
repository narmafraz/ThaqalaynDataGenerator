"""Arabic-to-English transliteration for narrator names.

Provides rule-based transliteration of Arabic narrator names to English
using a phonetic mapping table. This produces approximate transliterations
that are useful for display and search, though may not match scholarly
romanization conventions exactly.

The transliteration follows a simplified version of common academic
conventions (similar to IJMES/Library of Congress but simplified).

Usage:
    from app.wikishia.transliteration import transliterate_arabic

    name = "مُحَمَّدُ بْنُ يَعْقُوبَ"
    english = transliterate_arabic(name)
    # -> "Muhammad ibn Ya'qub"
"""

import re
from typing import Dict, List, Optional, Tuple

from app.wikishia.arabic_normalize import strip_diacritics


# === Letter mapping tables ===

# Basic Arabic letter -> Latin mapping
_LETTER_MAP: Dict[str, str] = {
    '\u0627': '',       # Alef (usually silent or long vowel)
    '\u0628': 'b',      # Ba
    '\u062A': 't',      # Ta
    '\u062B': 'th',     # Tha
    '\u062C': 'j',      # Jim
    '\u062D': 'h',      # Ha (emphatic)
    '\u062E': 'kh',     # Kha
    '\u062F': 'd',      # Dal
    '\u0630': 'dh',     # Dhal
    '\u0631': 'r',      # Ra
    '\u0632': 'z',      # Zay
    '\u0633': 's',      # Sin
    '\u0634': 'sh',     # Shin
    '\u0635': 's',      # Sad (emphatic)
    '\u0636': 'd',      # Dad (emphatic)
    '\u0637': 't',      # Ta (emphatic)
    '\u0638': 'z',      # Za (emphatic)
    '\u0639': "'",       # Ayn
    '\u063A': 'gh',     # Ghayn
    '\u0641': 'f',      # Fa
    '\u0642': 'q',      # Qaf
    '\u0643': 'k',      # Kaf
    '\u0644': 'l',      # Lam
    '\u0645': 'm',      # Mim
    '\u0646': 'n',      # Nun
    '\u0647': 'h',      # Ha
    '\u0648': 'w',      # Waw
    '\u064A': 'y',      # Ya
    '\u0629': 'a',      # Teh marbuta (at end of word, usually 'a')
    '\u0649': 'a',      # Alef maksura
    '\u0621': "'",       # Hamza (standalone)
    '\u0623': 'a',      # Alef with hamza above
    '\u0625': 'i',      # Alef with hamza below
    '\u0624': "'",       # Waw with hamza
    '\u0626': "'",       # Ya with hamza
    '\u0622': 'a',      # Alef with madda
    '\u0671': 'a',      # Alef wasla
}

# Diacritics that indicate short vowels
_VOWEL_MAP: Dict[str, str] = {
    '\u064E': 'a',      # Fatha
    '\u064F': 'u',      # Damma
    '\u0650': 'i',      # Kasra
    '\u064B': 'an',     # Fathatan (tanwin)
    '\u064C': 'un',     # Dammatan (tanwin)
    '\u064D': 'in',     # Kasratan (tanwin)
    '\u0651': '',       # Shadda (handled specially - doubles the consonant)
    '\u0652': '',       # Sukun (no vowel)
}

# Common Arabic name words with well-known English forms
_KNOWN_WORDS: Dict[str, str] = {
    'محمد': 'Muhammad',
    'علي': 'Ali',
    'حسن': 'Hasan',
    'حسين': 'Husayn',
    'جعفر': 'Ja\'far',
    'موسى': 'Musa',
    'عيسى': 'Isa',
    'يحيى': 'Yahya',
    'ابراهيم': 'Ibrahim',
    'اسماعيل': 'Isma\'il',
    'يعقوب': 'Ya\'qub',
    'يوسف': 'Yusuf',
    'يونس': 'Yunus',
    'سليمان': 'Sulayman',
    'داود': 'Dawud',
    'عبد الله': 'Abd Allah',
    'عبد الرحمن': 'Abd al-Rahman',
    'عبد الجبار': 'Abd al-Jabbar',
    'ابو': 'Abu',
    'ابي': 'Abi',
    'ابن': 'Ibn',
    'بن': 'ibn',
    'بنت': 'bint',
    'ام': 'Umm',
    'الله': 'Allah',
    'رسول': 'Rasul',
    'امير المومنين': 'Amir al-Mu\'minin',
    'صادق': 'Sadiq',
    'باقر': 'Baqir',
    'كاظم': 'Kazim',
    'رضا': 'Rida',
    'هادي': 'Hadi',
    'عسكري': 'Askari',
    'مهدي': 'Mahdi',
    'زيد': 'Zayd',
    'زياد': 'Ziyad',
    'عمر': 'Umar',
    'عثمان': 'Uthman',
    'احمد': 'Ahmad',
    'سهل': 'Sahl',
    'صالح': 'Salih',
    'هشام': 'Hisham',
    'حكم': 'Hakam',
    'عده': 'Idda',
    'اصحابنا': 'Ashabina',
    'بصير': 'Basir',
    'فضيل': 'Fudayl',
    'منصور': 'Mansur',
    'سنان': 'Sinan',
    'حمزه': 'Hamza',
    'جميله': 'Jamila',
    'سالم': 'Salim',
    'حمران': 'Humran',
}

# Common prefixes to handle
_ARTICLE_PREFIX = 'ال'  # al- (the)

# Honorific patterns to transliterate
_HONORIFIC_MAP: Dict[str, str] = {
    '( عليه السلام )': '(a)',
    '( عليهم السلام )': '(a)',
    '( صلى الله عليه وآله )': '(s)',
    '( صلوات الله عليه )': '(s)',
    '( عليها السلام )': '(a)',
}


def _transliterate_word(word: str) -> str:
    """Transliterate a single Arabic word to English.

    First checks the known-words dictionary, then falls back to
    character-by-character transliteration.
    """
    from app.wikishia.arabic_normalize import normalize_alef, normalize_teh_marbuta, normalize_alef_maksura

    # Strip diacritics and normalize letters for dictionary lookup
    bare = strip_diacritics(word)
    normalized = normalize_alef(normalize_teh_marbuta(normalize_alef_maksura(bare)))

    # Check known words (try both bare and normalized forms)
    if bare in _KNOWN_WORDS:
        return _KNOWN_WORDS[bare]
    if normalized in _KNOWN_WORDS:
        return _KNOWN_WORDS[normalized]

    # Handle al- prefix
    if normalized.startswith(_ARTICLE_PREFIX) and len(normalized) > 2:
        rest = normalized[2:]
        if rest in _KNOWN_WORDS:
            return "al-" + _KNOWN_WORDS[rest]
        # Sun letters: assimilate the 'l'
        sun_letters = 'تثدذرزسشصضطظنل'
        if rest and rest[0] in sun_letters:
            return "a" + _transliterate_chars(rest[0]) + "-" + _transliterate_chars(rest)
        return "al-" + _transliterate_chars(rest)

    return _transliterate_chars(word)


def _transliterate_chars(text: str) -> str:
    """Character-by-character transliteration of Arabic text."""
    result = []
    i = 0
    chars = list(text)

    while i < len(chars):
        ch = chars[i]

        # Skip diacritics (handle them contextually)
        if ch in _VOWEL_MAP:
            vowel = _VOWEL_MAP[ch]
            if vowel:
                result.append(vowel)
            i += 1
            continue

        # Map the consonant/letter
        if ch in _LETTER_MAP:
            mapped = _LETTER_MAP[ch]

            # Check if next char is shadda (doubles the consonant)
            if i + 1 < len(chars) and chars[i + 1] == '\u0651':
                result.append(mapped)
                result.append(mapped)
                i += 2
                continue

            result.append(mapped)
        elif ch == ' ':
            result.append(' ')
        elif ch == '\u0640':  # Tatweel
            pass  # Skip
        else:
            # Pass through non-Arabic characters
            result.append(ch)

        i += 1

    return ''.join(result)


def transliterate_arabic(name: str) -> str:
    """Transliterate an Arabic narrator name to English.

    Uses a combination of known-word dictionary lookup and
    character-by-character transliteration for unknown words.
    Handles common honorific phrases.

    Args:
        name: Arabic narrator name (may include diacritics and honorifics).

    Returns:
        English transliteration string.
    """
    # Handle honorifics first
    for arabic_hon, english_hon in _HONORIFIC_MAP.items():
        name = name.replace(arabic_hon, english_hon)

    # Split into words and transliterate each
    words = name.split()
    result_words = []
    for word in words:
        # Skip empty or whitespace-only
        if not word.strip():
            continue
        # Pass through already-transliterated parts (like honorific abbreviations)
        if all(c in '(abcdefghijklmnopqrstuvwxyz)' for c in word.lower()):
            result_words.append(word)
            continue
        result_words.append(_transliterate_word(word))

    result = ' '.join(result_words)

    # Clean up double spaces, leading/trailing whitespace
    result = re.sub(r'\s+', ' ', result).strip()

    # Capitalize first letter
    if result:
        result = result[0].upper() + result[1:]

    return result


def transliterate_narrator_index(id_name_map: Dict[int, str]) -> Dict[int, str]:
    """Transliterate all narrator names in an index.

    Args:
        id_name_map: Dict mapping narrator ID to Arabic name.

    Returns:
        Dict mapping narrator ID to English transliteration.
    """
    return {
        nid: transliterate_arabic(name)
        for nid, name in id_name_map.items()
    }
