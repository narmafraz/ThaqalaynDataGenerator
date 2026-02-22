"""Arabic text normalization and cross-validation engine.

Provides normalization of Arabic text for comparison purposes and a
3-tier comparison system to detect textual variants between sources.

Normalization levels:
- Full normalization: strips all diacritics, normalizes letter forms,
  removes tatweel, normalizes whitespace.
- Base-letter normalization: same as full but preserves spacing.

Comparison tiers:
- Tier 1 (Exact): Identical after full normalization.
- Tier 2 (Diacritics-only): Same base text, different tashkeel.
- Tier 3 (Substantive): Actual textual differences in base letters.
"""

import re
import unicodedata
from dataclasses import dataclass, field
from enum import IntEnum
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Unicode ranges for Arabic diacritical marks (tashkeel)
# ---------------------------------------------------------------------------

# Combining marks used as vowel marks (harakat) in Arabic
TASHKEEL_CHARS = (
    "\u064B"  # FATHATAN
    "\u064C"  # DAMMATAN
    "\u064D"  # KASRATAN
    "\u064E"  # FATHA
    "\u064F"  # DAMMA
    "\u0650"  # KASRA
    "\u0651"  # SHADDA
    "\u0652"  # SUKUN
    "\u0653"  # MADDAH ABOVE
    "\u0654"  # HAMZA ABOVE
    "\u0655"  # HAMZA BELOW
    "\u0656"  # SUBSCRIPT ALEF
    "\u0670"  # SUPERSCRIPT ALEF (dagger alef)
)

# Regex to match any tashkeel character
TASHKEEL_PATTERN = re.compile(f"[{re.escape(TASHKEEL_CHARS)}]")

# Tatweel (kashida) — the Arabic elongation character
TATWEEL = "\u0640"

# ---------------------------------------------------------------------------
# Letter normalization maps
# ---------------------------------------------------------------------------

# Hamza variants -> plain alef
HAMZA_MAP = {
    "\u0623": "\u0627",  # ALEF WITH HAMZA ABOVE -> ALEF
    "\u0625": "\u0627",  # ALEF WITH HAMZA BELOW -> ALEF
    "\u0622": "\u0627",  # ALEF WITH MADDA ABOVE -> ALEF
    "\u0671": "\u0627",  # ALEF WASLA -> ALEF
}

# Teh marbuta -> heh
TEH_MARBUTA_MAP = {
    "\u0629": "\u0647",  # TEH MARBUTA -> HEH
}

# Alef maksura -> yeh
ALEF_MAKSURA_MAP = {
    "\u0649": "\u064A",  # ALEF MAKSURA -> YEH
}

# Waw with hamza above -> waw (optional, less common)
WAW_HAMZA_MAP = {
    "\u0624": "\u0648",  # WAW WITH HAMZA ABOVE -> WAW
}

# Yeh with hamza above -> yeh
YEH_HAMZA_MAP = {
    "\u0626": "\u064A",  # YEH WITH HAMZA ABOVE -> YEH
}

# Combined normalization map for all letter forms
LETTER_NORM_MAP = {}
LETTER_NORM_MAP.update(HAMZA_MAP)
LETTER_NORM_MAP.update(TEH_MARBUTA_MAP)
LETTER_NORM_MAP.update(ALEF_MAKSURA_MAP)
LETTER_NORM_MAP.update(WAW_HAMZA_MAP)
LETTER_NORM_MAP.update(YEH_HAMZA_MAP)

# Build translation table for str.translate()
_LETTER_NORM_TABLE = str.maketrans(LETTER_NORM_MAP)

# Arabic-specific punctuation that should be normalized to standard forms
ARABIC_PUNCTUATION_MAP = {
    "\u060C": ",",   # ARABIC COMMA -> comma
    "\u061B": ";",   # ARABIC SEMICOLON -> semicolon
    "\u061F": "?",   # ARABIC QUESTION MARK -> question mark
}
_PUNCT_NORM_TABLE = str.maketrans(ARABIC_PUNCTUATION_MAP)

# Whitespace normalization: collapse multiple spaces, NBSP, ZWNJ, etc.
WHITESPACE_PATTERN = re.compile(r"[\s\u00A0\u200B\u200C\u200D\uFEFF]+")


# ---------------------------------------------------------------------------
# Normalization functions
# ---------------------------------------------------------------------------

def strip_tashkeel(text: str) -> str:
    """Remove all Arabic diacritical marks (tashkeel/harakat) from text."""
    return TASHKEEL_PATTERN.sub("", text)


def remove_tatweel(text: str) -> str:
    """Remove tatweel (kashida) elongation characters."""
    return text.replace(TATWEEL, "")


def normalize_letters(text: str) -> str:
    """Normalize Arabic letter variants to canonical forms.

    - Hamza variants (أ إ آ ٱ) -> ا
    - Teh marbuta (ة) -> ه
    - Alef maksura (ى) -> ي
    - Waw with hamza (ؤ) -> و
    - Yeh with hamza (ئ) -> ي
    """
    return text.translate(_LETTER_NORM_TABLE)


def normalize_punctuation(text: str) -> str:
    """Normalize Arabic-specific punctuation to standard forms."""
    return text.translate(_PUNCT_NORM_TABLE)


def normalize_whitespace(text: str) -> str:
    """Collapse all whitespace (including NBSP, ZWNJ, etc.) to single space and strip."""
    return WHITESPACE_PATTERN.sub(" ", text).strip()


def normalize_arabic(text: str) -> str:
    """Full Arabic text normalization for comparison.

    Applies all normalization steps:
    1. Remove tatweel
    2. Strip tashkeel (diacritical marks)
    3. Normalize letter variants
    4. Normalize punctuation
    5. Normalize whitespace
    """
    result = remove_tatweel(text)
    result = strip_tashkeel(result)
    result = normalize_letters(result)
    result = normalize_punctuation(result)
    result = normalize_whitespace(result)
    return result


def normalize_preserve_diacritics(text: str) -> str:
    """Normalize Arabic text but preserve diacritical marks.

    Used for Tier 2 comparison — detects whether differences are
    only in tashkeel placement.

    Applies:
    1. Remove tatweel
    2. Normalize letter variants
    3. Normalize punctuation
    4. Normalize whitespace
    (Does NOT strip tashkeel)
    """
    result = remove_tatweel(text)
    result = normalize_letters(result)
    result = normalize_punctuation(result)
    result = normalize_whitespace(result)
    return result


# ---------------------------------------------------------------------------
# Comparison tier system
# ---------------------------------------------------------------------------

class ComparisonTier(IntEnum):
    """Result tier for Arabic text comparison."""
    EXACT = 1       # Identical after full normalization
    DIACRITICS = 2  # Same base text, different tashkeel only
    SUBSTANTIVE = 3 # Actual textual differences


@dataclass
class ComparisonResult:
    """Result of comparing two Arabic texts."""
    tier: ComparisonTier
    confidence: float  # 0.0 to 1.0
    normalized_a: str = ""
    normalized_b: str = ""
    differences: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Serialize to dict for JSON output."""
        result = {
            "tier": self.tier.value,
            "tier_name": self.tier.name.lower(),
            "confidence": round(self.confidence, 4),
        }
        if self.differences:
            result["differences"] = self.differences
        return result


def _levenshtein_ratio(s1: str, s2: str) -> float:
    """Calculate normalized Levenshtein similarity ratio (0.0 to 1.0).

    Returns 1.0 for identical strings, 0.0 for completely different.
    Uses dynamic programming for O(m*n) time.
    """
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    len1, len2 = len(s1), len(s2)

    # Optimization: if lengths differ by more than 50%, they're very different
    if max(len1, len2) > 2 * min(len1, len2):
        return 0.0

    # Use single-row DP for memory efficiency
    prev_row = list(range(len2 + 1))
    for i in range(1, len1 + 1):
        curr_row = [i] + [0] * len2
        for j in range(1, len2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            curr_row[j] = min(
                curr_row[j - 1] + 1,      # insertion
                prev_row[j] + 1,           # deletion
                prev_row[j - 1] + cost,    # substitution
            )
        prev_row = curr_row

    distance = prev_row[len2]
    max_len = max(len1, len2)
    return 1.0 - (distance / max_len)


def _find_word_differences(text_a: str, text_b: str) -> List[str]:
    """Find word-level differences between two normalized texts.

    Returns a list of human-readable difference descriptions.
    """
    words_a = text_a.split()
    words_b = text_b.split()
    diffs = []

    # Simple word-by-word alignment using LCS approach
    # For practical purposes, just report additions and removals
    set_a = set(words_a)
    set_b = set(words_b)

    only_in_a = set_a - set_b
    only_in_b = set_b - set_a

    if only_in_a:
        diffs.append(f"source_a_only: {' '.join(sorted(only_in_a)[:5])}")
    if only_in_b:
        diffs.append(f"source_b_only: {' '.join(sorted(only_in_b)[:5])}")

    if len(words_a) != len(words_b):
        diffs.append(f"word_count: {len(words_a)} vs {len(words_b)}")

    return diffs


def compare_arabic(text_a: str, text_b: str) -> ComparisonResult:
    """Compare two Arabic texts using the 3-tier system.

    Tier 1 (Exact): Texts are identical after full normalization
        (strip tashkeel, normalize letters, normalize whitespace)
        AND their diacritics-preserved forms are also identical.
        This means the texts are the same in every way (after
        normalizing letter variants and whitespace).

    Tier 2 (Diacritics-only): Same base consonantal text, but
        different tashkeel/diacritics placement.
        Confidence: Levenshtein ratio of diacritics-preserved forms.

    Tier 3 (Substantive): Actual differences in base letter text.
        Confidence: Levenshtein ratio of fully normalized forms.
    """
    # Fully normalize both texts (strips diacritics)
    norm_a = normalize_arabic(text_a)
    norm_b = normalize_arabic(text_b)

    # Normalize preserving diacritics (for Tier 1 vs Tier 2 distinction)
    diacritics_a = normalize_preserve_diacritics(text_a)
    diacritics_b = normalize_preserve_diacritics(text_b)

    # Check if base consonantal text matches
    if norm_a == norm_b:
        # Base text is the same — check if diacritics also match
        if diacritics_a == diacritics_b:
            # Tier 1: Truly identical (same base text AND same diacritics)
            return ComparisonResult(
                tier=ComparisonTier.EXACT,
                confidence=1.0,
                normalized_a=norm_a,
                normalized_b=norm_b,
            )
        else:
            # Tier 2: Same base text, different diacritics
            confidence = _levenshtein_ratio(diacritics_a, diacritics_b)
            return ComparisonResult(
                tier=ComparisonTier.DIACRITICS,
                confidence=confidence,
                normalized_a=norm_a,
                normalized_b=norm_b,
                differences=["diacritics_differ"],
            )

    # Tier 3: Substantive differences in base letter text
    confidence = _levenshtein_ratio(norm_a, norm_b)
    differences = _find_word_differences(norm_a, norm_b)

    return ComparisonResult(
        tier=ComparisonTier.SUBSTANTIVE,
        confidence=confidence,
        normalized_a=norm_a,
        normalized_b=norm_b,
        differences=differences,
    )


# ---------------------------------------------------------------------------
# Batch validation
# ---------------------------------------------------------------------------

@dataclass
class ValidationEntry:
    """Result of validating a single verse/hadith across sources."""
    path: str
    comparison: ComparisonResult
    source_a_name: str = ""
    source_b_name: str = ""

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "source_a": self.source_a_name,
            "source_b": self.source_b_name,
            "comparison": self.comparison.to_dict(),
        }


@dataclass
class ValidationReport:
    """Aggregated results for validating a book across sources."""
    book_slug: str
    source_a_name: str
    source_b_name: str
    entries: List[ValidationEntry] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.entries)

    @property
    def exact_count(self) -> int:
        return sum(1 for e in self.entries if e.comparison.tier == ComparisonTier.EXACT)

    @property
    def diacritics_count(self) -> int:
        return sum(1 for e in self.entries if e.comparison.tier == ComparisonTier.DIACRITICS)

    @property
    def substantive_count(self) -> int:
        return sum(1 for e in self.entries if e.comparison.tier == ComparisonTier.SUBSTANTIVE)

    def summary(self) -> dict:
        return {
            "book": self.book_slug,
            "source_a": self.source_a_name,
            "source_b": self.source_b_name,
            "total": self.total,
            "exact": self.exact_count,
            "diacritics_only": self.diacritics_count,
            "substantive": self.substantive_count,
        }

    def to_dict(self) -> dict:
        return {
            "summary": self.summary(),
            "entries": [e.to_dict() for e in self.entries],
        }
