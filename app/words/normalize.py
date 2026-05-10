"""Arabic word normalization — the canonical slug-derivation function.

This module is the single source of truth for converting any Arabic
surface form (as it appears in a chunk's `arabic_text`) into the
slug used as a filename in ThaqalaynWords and a URL component in
the Angular UI. Both generator and UI use this function (TypeScript
twin: `Thaqalayn/src/app/services/word-normalize.ts`); a 1000-form
fixture asserts byte-identical output across the two.

Slug-derivation contract:
    slug(surface_form) = unicodedata.normalize("NFC", surface_form.strip())

That's the ONLY transformation. No transliteration, no diacritic
stripping, no alif/ya unification at the slug layer.

Why minimal:
- The slug must round-trip from any chunk's surface form. If we strip
  diacritics here, `قَالَ` and `قَالُ` would collapse to the same slug
  and we'd lose the per-inflection page distinction we want.
- Unicode normalization (NFC) is needed because the same character
  sequence can be represented multiple ways at the codepoint level
  (e.g. `لا` as a ligature codepoint vs `ل + ا` separately, or
  shadda+vowel as combined codepoint vs sequence). Without NFC, two
  byte-different but visually identical inputs would hash to different
  slugs.

A SEPARATE function `normalize_for_match()` does the
diacritic-insensitive normalization used for fuzzy matching (e.g.
search-bar suggestions, narrator-surface-in-chunk lookups). That's
NOT for slugs — never use it to derive a filename.

Round-trip test (locked in fixture):
    For every surface form in tests/words/fixtures/surface_forms.json,
    slug(s).encode("utf-8") must equal the JS-side output of
    s.normalize("NFC") applied to the same input.
"""
from __future__ import annotations

import unicodedata


def slug(surface_form: str) -> str:
    """Convert any Arabic surface form to its canonical slug.

    The slug is used as both the filename
    (`surfaces/{slug}.json`, `lemmas/{slug}.json`) and the URL
    component in the Angular UI. Determinism: byte-identical to the
    TypeScript twin (`s.normalize('NFC')`).

    Args:
        surface_form: Arabic text as it appears in a chunk's
            ``arabic_text`` field (or any user-provided word).
            May or may not be diacritized; this function preserves
            diacritics intact.

    Returns:
        Canonical NFC-normalized form, with leading/trailing
        whitespace stripped. Empty string if input is empty or None.

    Examples:
        >>> slug("قَالَ")
        'قَالَ'
        >>> slug("  وَقَالَ  ")
        'وَقَالَ'
        >>> slug("")
        ''
    """
    if not surface_form:
        return ""
    return unicodedata.normalize("NFC", surface_form.strip())


# ---------------------------------------------------------------------------
# Auxiliary: diacritic-insensitive match normalization
# ---------------------------------------------------------------------------
# Use this for fuzzy match-lookups (search bar, narrator-in-chunk check, etc).
# Do NOT use this to derive slugs — it collapses inflectional distinctions.

_DIACRITIC_MARKS = set(
    "ًٌٍَُِّْٰـ"
)
_ALIF_VARIANTS = "أإآٱا"  # أ إ آ ٱ ا
_YA_VARIANTS = "يى"  # ي ى
_TA_MARBUTA = "ة"


def normalize_for_match(surface_form: str) -> str:
    """Diacritic-insensitive normalization for fuzzy lookups.

    Strips tashkeel + tatweel, unifies alif variants to plain alif,
    unifies ya variants to plain ya, unifies ta-marbuta (ة) to ha (ه).
    Used for matching surface forms against e.g. narrator-name strings
    that may have different diacritization. Matches the normalization
    rules used by the existing Thaqalayn search service.

    NOT to be used for slug derivation — use :func:`slug` for that.

    Args:
        surface_form: Arabic text.

    Returns:
        Diacritic-stripped, letter-normalized form.
    """
    if not surface_form:
        return ""
    out = []
    for ch in unicodedata.normalize("NFC", surface_form.strip()):
        if ch in _DIACRITIC_MARKS:
            continue
        if ch in _ALIF_VARIANTS:
            out.append("ا")
        elif ch in _YA_VARIANTS:
            out.append("ي")
        elif ch == _TA_MARBUTA:
            out.append("ه")
        else:
            out.append(ch)
    return "".join(out)
