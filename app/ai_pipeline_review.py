"""AI pipeline review, chunked processing, and prompt builders.

This module extends ai_pipeline.py with:
- Quality review checks beyond schema validation (review_result)
- Long hadith chunked processing (structure + per-chunk detail passes)
- Prompt builders for review and fix passes
- Helper functions for the multi-pass Claude Code agent workflow

The review system catches issues that schema validation cannot:
- Summaries masquerading as translations (length ratio check)
- Arabic echo-back in word translations
- Missing European language diacritics
- Semantic coherence between chunks and verse-level content

For usage with Claude Code agents, see .claude/agents/ directory.
"""

import json
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from app.ai_pipeline import (
    VALID_CHUNK_TYPES,
    VALID_LANGUAGE_KEYS,
    PipelineRequest,
    build_system_prompt,
    build_user_message,
    reconstruct_fields,
    validate_result,
)


# ---------------------------------------------------------------------------
# ReviewWarning dataclass
# ---------------------------------------------------------------------------

@dataclass
class ReviewWarning:
    """A quality warning found during review of a pipeline result."""
    field: str          # e.g. "translations.tr", "word_analysis[5].translation.de"
    category: str       # e.g. "length_ratio", "arabic_echo", "missing_diacritics"
    severity: str       # "low", "medium", "high"
    message: str
    suggestion: str


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHUNKED_PROCESSING_THRESHOLD = 200

# Per-language min/max character ratio bounds (target_len / arabic_len).
# Latin-script languages typically expand; CJK compresses; Perso-Arabic similar.
TRANSLATION_RATIO_BOUNDS: Dict[str, Tuple[float, float]] = {
    # Latin-script languages
    "en": (0.3, 5.0),
    "tr": (0.3, 5.0),
    "id": (0.3, 5.0),
    "es": (0.3, 5.0),
    "fr": (0.3, 5.0),
    "de": (0.3, 5.0),
    "ru": (0.3, 5.0),
    "bn": (0.3, 5.0),
    # CJK
    "zh": (0.15, 3.0),
    # Perso-Arabic script
    "fa": (0.4, 4.0),
    "ur": (0.4, 4.0),
}

# Required diacritical characters per European language.
# At least one of these must appear in the translation text.
EUROPEAN_DIACRITICS: Dict[str, str] = {
    "tr": "\u00f6\u00fc\u015f\u00e7\u011f\u0131\u0130\u015e\u00c7\u011e\u00d6\u00dc",
    "fr": "\u00e9\u00e8\u00ea\u00e0\u00e7\u00e2\u00ee\u00f4\u00f9\u00fb\u00eb\u00ef\u0153\u00c9",
    "de": "\u00e4\u00f6\u00fc\u00df\u00c4\u00d6\u00dc",
    "es": "\u00f1\u00e1\u00e9\u00ed\u00f3\u00fa\u00bf\u00a1\u00c1\u00c9\u00cd\u00d3\u00da\u00d1",
}

# Minimum text length before diacritics check applies.
EUROPEAN_DIACRITICS_MIN_LENGTH: Dict[str, int] = {
    "tr": 50,
    "fr": 100,
    "de": 100,
    "es": 100,
}

# Arabic text patterns indicating back-references to previous hadith chains.
BACK_REFERENCE_PATTERNS = [
    "\u0648\u0639\u0646\u0647",      # wa-anhu (and from him)
    "\u0648\u0628\u0625\u0633\u0646\u0627\u062f\u0647",  # wa-bi-isnadihi (and with his chain)
    "\u0648\u0628\u0647\u0630\u0627 \u0627\u0644\u0625\u0633\u0646\u0627\u062f",  # wa-bi-hadha al-isnad
    "\u0648\u0639\u0646\u0647\u0645",  # wa-anhum (and from them)
]

# Unicode range for Arabic characters (base letters + diacritics/tashkeel).
_ARABIC_CHAR_RANGE = set()
for _cp in range(0x0600, 0x06FF + 1):  # Arabic block (letters + diacritics + marks)
    _ARABIC_CHAR_RANGE.add(chr(_cp))
for _cp in range(0x0750, 0x077F + 1):  # Arabic Supplement
    _ARABIC_CHAR_RANGE.add(chr(_cp))
for _cp in range(0xFB50, 0xFDFF + 1):  # Arabic Presentation Forms-A
    _ARABIC_CHAR_RANGE.add(chr(_cp))
for _cp in range(0xFE70, 0xFEFF + 1):  # Arabic Presentation Forms-B
    _ARABIC_CHAR_RANGE.add(chr(_cp))


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _count_arabic_chars(text: str) -> int:
    """Count Arabic-script characters in text (letters + diacritics)."""
    count = 0
    for ch in text:
        if ch in _ARABIC_CHAR_RANGE:
            count += 1
    return count


def _arabic_fraction(text: str) -> float:
    """Return the fraction of characters that are Arabic script."""
    if not text:
        return 0.0
    total = sum(1 for ch in text if not ch.isspace())
    if total == 0:
        return 0.0
    arabic = _count_arabic_chars(text)
    return arabic / total


def _strip_arabic_diacritics(text: str) -> str:
    """Remove Arabic tashkeel marks, zero-width characters, and normalize letter variants."""
    diacritics = set(
        "\u064B\u064C\u064D\u064E\u064F\u0650\u0651\u0652"  # standard tashkeel
        "\u0653\u0654\u0655\u0656\u0657\u0658\u0659\u065A\u065B\u065C\u065D\u065E\u065F"  # extended marks
        "\u0670"  # superscript alef
    )
    # Also strip zero-width and directional characters that appear in source text
    # encoding but are invisible: ZWNJ, ZWJ, ZWSP, BOM, LRM, RLM, ALM
    zero_width = set("\u200B\u200C\u200D\u200E\u200F\u061C\uFEFF")
    exclude = diacritics | zero_width
    # Normalize Arabic/Persian letter variants (Quran sources use Persian forms)
    # Also normalize alef/hamza variants so diacritized AI forms match undiacritized source text.
    # e.g. الإمامة (with hamza-below إ) vs الامامة (plain ا) in source — same word.
    letter_map = {
        "\u06A9": "\u0643",  # ک KEHEH (Persian kaf) → ك KAF
        "\u0649": "\u064A",  # ى ALEF MAKSURA → ي YEH
        "\u06CC": "\u064A",  # ی FARSI YEH → ي YEH
        "\u0623": "\u0627",  # أ ALEF WITH HAMZA ABOVE → ا ALEF
        "\u0625": "\u0627",  # إ ALEF WITH HAMZA BELOW → ا ALEF
        "\u0622": "\u0627",  # آ ALEF WITH MADDA → ا ALEF
        "\u0671": "\u0627",  # ٱ ALEF WASLA → ا ALEF
        "\u0624": "\u0648",  # ؤ WAW WITH HAMZA ABOVE → و WAW
    }
    result = []
    for ch in text:
        if ch in exclude:
            continue
        result.append(letter_map.get(ch, ch))
    return "".join(result)


# Common Arabic name case-ending variants (nominative/genitive/accusative).
_ARABIC_NAME_NORMALIZATIONS = {
    "\u0627\u0628\u064a": "\u0627\u0628\u0648",       # ابي → ابو (Abu)
    "\u0627\u0628\u0627": "\u0627\u0628\u0648",       # ابا → ابو (Abu)
    "\u0644\u0627\u0628\u064a": "\u0627\u0628\u0648",  # لابي → ابو (li-Abi → Abu)
}


def _normalize_arabic_name(text: str) -> str:
    """Normalize Arabic name case endings for comparison.

    Handles grammatical case variation (e.g., أبو/أبي/أبا are the same
    name in nominative/genitive/accusative) and common prefixes.
    Input should already have diacritics stripped.
    """
    words = text.split()
    normalized = []
    for w in words:
        # Skip parenthetical markers
        if w in ("(", ")", "[", "]"):
            continue
        normalized.append(_ARABIC_NAME_NORMALIZATIONS.get(w, w))
    return " ".join(normalized)


# ---------------------------------------------------------------------------
# review_result() — quality checks beyond schema validation
# ---------------------------------------------------------------------------

def review_result(result: dict, request: PipelineRequest) -> List[ReviewWarning]:
    """Run quality checks on a pipeline result beyond schema validation.

    Accepts both full and stripped formats. If stripped (diacritized_text
    missing but word_analysis present), fields are reconstructed first.

    Returns a list of ReviewWarning objects. Empty list means all checks passed.

    Checks:
    1. Translation length ratio — detect summaries masquerading as translations
    2. Arabic echo-back in word translations — detect untranslated words
    3. European language diacritics — detect ASCII-only European text
    4. Empty related_quran for Quran verses — Quran verses should self-reference
    5. Chunk translation coherence — chunk translations should sum to ~verse length
    6. Missing isnad chunk — has_chain=True should have isnad chunk
    7. Back-reference without chain — Arabic starts with back-ref but has_chain=False
    9. Word analysis text match — word_analysis words must match original Arabic text
    10. Narrator word_ranges — verify word_ranges point to correct narrator names
    """
    # Auto-reconstruct stripped format before reviewing
    if "diacritized_text" not in result and "word_analysis" in result:
        result = reconstruct_fields(result)

    warnings: List[ReviewWarning] = []

    arabic_text = request.arabic_text
    arabic_len = len(arabic_text.strip())
    chunks = result.get("chunks", [])

    # --- Check 1: Translation length ratio ---
    if arabic_len >= 20 and "translations" in result:
        for lang, lang_data in result["translations"].items():
            if lang not in VALID_LANGUAGE_KEYS:
                continue
            if not isinstance(lang_data, dict):
                continue
            text = lang_data.get("text", "")
            if not text:
                continue
            ratio = len(text) / arabic_len
            bounds = TRANSLATION_RATIO_BOUNDS.get(lang, (0.3, 5.0))
            # Short Arabic texts (<50 chars) naturally have higher ratios because
            # translations include context (isnad chain) the matn alone doesn't.
            # Widen upper bound for short texts.
            if arabic_len < 50:
                bounds = (bounds[0], bounds[1] * 2)
            if ratio < bounds[0] or ratio > bounds[1]:
                severity = "high" if ratio < bounds[0] else "medium"
                warnings.append(ReviewWarning(
                    field=f"translations.{lang}",
                    category="length_ratio",
                    severity=severity,
                    message=(
                        f"Translation length ratio {ratio:.2f} outside bounds "
                        f"{bounds} for {lang} (arabic={arabic_len}, trans={len(text)})"
                    ),
                    suggestion=(
                        f"Check if {lang} translation is a summary instead of "
                        f"a faithful translation. Regenerate if too short."
                    ),
                ))

    # --- Check 2: Arabic echo-back in word translations ---
    if "word_analysis" in result and isinstance(result["word_analysis"], list):
        for i, word_entry in enumerate(result["word_analysis"]):
            if not isinstance(word_entry, dict):
                continue
            word_ar = word_entry.get("word", "")
            trans = word_entry.get("translation", {})
            if not isinstance(trans, dict):
                continue
            for lang, word_trans in trans.items():
                if lang not in VALID_LANGUAGE_KEYS:
                    continue
                if not isinstance(word_trans, str) or not word_trans.strip():
                    continue
                if lang in ("fa", "ur"):
                    # Farsi/Urdu use Arabic script — proper nouns and common
                    # Islamic terms are legitimately identical in Arabic script.
                    # Downgrade to "low" since most echo-backs are correct.
                    stripped_word = _strip_arabic_diacritics(word_ar).strip()
                    stripped_trans = _strip_arabic_diacritics(word_trans).strip()
                    if stripped_word and stripped_trans and stripped_word == stripped_trans:
                        warnings.append(ReviewWarning(
                            field=f"word_analysis[{i}].translation.{lang}",
                            category="arabic_echo",
                            severity="low",
                            message=(
                                f"Word translation in {lang} is identical to Arabic word "
                                f"'{word_ar}' (likely correct for proper nouns/terms)"
                            ),
                            suggestion=(
                                f"Verify '{word_ar}' is a proper noun or shared term in {lang}."
                            ),
                        ))
                else:
                    # Non Perso-Arabic: flag if >50% Arabic characters
                    frac = _arabic_fraction(word_trans)
                    if frac > 0.5:
                        warnings.append(ReviewWarning(
                            field=f"word_analysis[{i}].translation.{lang}",
                            category="arabic_echo",
                            severity="high",
                            message=(
                                f"Word translation in {lang} is {frac:.0%} Arabic characters "
                                f"('{word_trans}' for word '{word_ar}')"
                            ),
                            suggestion=(
                                f"Translate '{word_ar}' into {lang} script — do not echo Arabic."
                            ),
                        ))

    # --- Check 3: European language diacritics ---
    # Check both verse-level text and chunk translations. For stripped files,
    # verse-level text is reconstructed from chunks. As a fallback, also check
    # chunk translations directly in case reconstruction produced empty text.
    if "translations" in result:
        for lang, required_chars in EUROPEAN_DIACRITICS.items():
            lang_data = result["translations"].get(lang)
            if not isinstance(lang_data, dict):
                continue
            text = lang_data.get("text", "")
            # Fallback: concatenate chunk translations if top-level is empty/short
            if not text and isinstance(chunks, list):
                parts = [c.get("translations", {}).get(lang, "") for c in chunks if isinstance(c, dict)]
                text = " ".join(parts)
            min_len = EUROPEAN_DIACRITICS_MIN_LENGTH.get(lang, 100)
            if len(text) < min_len:
                continue
            if not any(ch in text.lower() for ch in required_chars):
                # German text about Islamic topics often legitimately lacks ä/ö/ü/ß
                # (e.g. "Muhammad sagte..." has no umlauts). Downgrade to low.
                sev = "low" if lang == "de" else "medium"
                warnings.append(ReviewWarning(
                    field=f"translations.{lang}",
                    category="missing_diacritics",
                    severity=sev,
                    message=(
                        f"Translation in {lang} ({len(text)} chars) contains none of "
                        f"the expected diacritical characters ({required_chars})"
                    ),
                    suggestion=(
                        f"Check that {lang} text uses proper diacritics, not ASCII-only."
                    ),
                ))

    # --- Check 4: Empty related_quran for Quran verses ---
    if request.verse_path.startswith("/books/quran:"):
        related = result.get("related_quran", [])
        if isinstance(related, list) and len(related) == 0:
            warnings.append(ReviewWarning(
                field="related_quran",
                category="empty_related_quran",
                severity="low",
                message="Quran verse has empty related_quran — should self-reference",
                suggestion="Add at least a thematic self-reference for Quran verses.",
            ))

    # --- Check 5: Chunk translation coherence ---
    chunks = result.get("chunks", [])
    translations = result.get("translations", {})
    if isinstance(chunks, list) and len(chunks) > 1 and isinstance(translations, dict):
        for lang in VALID_LANGUAGE_KEYS:
            lang_data = translations.get(lang)
            if not isinstance(lang_data, dict):
                continue
            verse_text = lang_data.get("text", "")
            if not verse_text:
                continue
            # Concatenate chunk translations for this language
            chunk_parts = []
            for chunk in chunks:
                ct = chunk.get("translations", {})
                if isinstance(ct, dict):
                    chunk_parts.append(ct.get(lang, ""))
            if lang == "zh":
                concatenated = "".join(chunk_parts)
            else:
                concatenated = " ".join(chunk_parts)
            if not concatenated:
                continue
            verse_len = len(verse_text)
            concat_len = len(concatenated)
            if verse_len > 0:
                diff_ratio = abs(concat_len - verse_len) / verse_len
                if diff_ratio > 0.3:
                    warnings.append(ReviewWarning(
                        field=f"chunks.translations.{lang}",
                        category="chunk_translation_mismatch",
                        severity="medium",
                        message=(
                            f"Concatenated chunk translations for {lang} differ from "
                            f"verse-level text by {diff_ratio:.0%} "
                            f"(chunk={concat_len}, verse={verse_len})"
                        ),
                        suggestion=(
                            f"Review chunk boundaries and translations for {lang}."
                        ),
                    ))
                    break  # One warning per result is enough for this check

    # --- Check 6: Missing isnad chunk ---
    isnad_matn = result.get("isnad_matn", {})
    if isinstance(isnad_matn, dict) and isnad_matn.get("has_chain") is True:
        has_isnad_chunk = False
        if isinstance(chunks, list):
            for chunk in chunks:
                if isinstance(chunk, dict) and chunk.get("chunk_type") == "isnad":
                    has_isnad_chunk = True
                    break
        isnad_ar = isnad_matn.get("isnad_ar", "")
        if not has_isnad_chunk and not isnad_ar:
            warnings.append(ReviewWarning(
                field="chunks",
                category="missing_isnad_chunk",
                severity="medium",
                message="has_chain=True but no isnad chunk and empty isnad_ar",
                suggestion="Add an isnad chunk or populate isnad_ar for chained hadith.",
            ))

    # --- Check 7: Back-reference without chain ---
    arabic_stripped = arabic_text.strip()
    for pattern in BACK_REFERENCE_PATTERNS:
        if arabic_stripped.startswith(pattern):
            if isinstance(isnad_matn, dict) and isnad_matn.get("has_chain") is False:
                warnings.append(ReviewWarning(
                    field="isnad_matn.has_chain",
                    category="back_reference_no_chain",
                    severity="low",
                    message=(
                        f"Arabic text starts with back-reference pattern '{pattern}' "
                        f"but has_chain=False"
                    ),
                    suggestion=(
                        "Back-references typically indicate a continued chain from "
                        "a previous hadith. Consider setting has_chain=True."
                    ),
                ))
            break  # Only check the first matching pattern

    # --- Check 9: word_analysis matches original Arabic text ---
    # Note: word_analysis may cover the FULL hadith text (including isnad chain)
    # while request.arabic_text may only contain the matn. The AI also tokenizes
    # differently (attaching particles like وَ, omitting parenthetical markers).
    # We downgrade severity when word_analysis has MORE words (expected chain
    # inclusion) and only flag "high" when it has significantly fewer (lost content).
    if "word_analysis" in result and isinstance(result["word_analysis"], list):
        reconstructed_words = [w.get("word", "") for w in result["word_analysis"] if isinstance(w, dict)]
        reconstructed = " ".join(reconstructed_words)
        # Strip punctuation alongside diacritics for comparison — source text may
        # include trailing periods or other non-Arabic punctuation (including
        # Arabic comma U+060C, Arabic semicolon U+061B, Arabic question mark U+061F)
        _punct = set(".,;:!?()[]{}\"'\u060c\u061b\u061f")
        reconstructed_clean = _strip_arabic_diacritics(reconstructed).split()
        original_clean = _strip_arabic_diacritics(arabic_text).split()
        _punct_str = "".join(_punct)
        reconstructed_clean = [w.strip(_punct_str) for w in reconstructed_clean if w.strip(_punct_str)]
        original_clean = [w.strip(_punct_str) for w in original_clean if w.strip(_punct_str)]

        if len(reconstructed_clean) != len(original_clean):
            more_words = len(reconstructed_clean) > len(original_clean)
            # AI typically includes isnad chain → more words; or uses
            # different tokenization (attached particles) → fewer words.
            # Only flag as high severity when word_analysis has significantly
            # fewer words (>30% loss), suggesting content was dropped.
            fewer_ratio = (len(original_clean) - len(reconstructed_clean)) / max(len(original_clean), 1)
            if more_words:
                severity = "low"
            elif fewer_ratio > 0.3:
                severity = "high"
            else:
                severity = "low"
            warnings.append(ReviewWarning(
                field="word_analysis",
                category="word_count_mismatch",
                severity=severity,
                message=f"word_analysis has {len(reconstructed_clean)} words but original has {len(original_clean)}",
                suggestion=(
                    "word_analysis likely includes isnad chain or uses different tokenization."
                    if more_words else
                    "Regenerate word_analysis to match original text word count."
                ),
            ))
        elif reconstructed_clean != original_clean:
            # Same word count but different content — find first divergence
            for i, (rw, ow) in enumerate(zip(reconstructed_clean, original_clean)):
                if rw != ow:
                    warnings.append(ReviewWarning(
                        field=f"word_analysis[{i}]",
                        category="word_text_mismatch",
                        severity="high",
                        message=f"word_analysis[{i}] is '{rw}' but original has '{ow}'",
                        suggestion="Fix the word to match the original Arabic text.",
                    ))
                    break

    # --- Check 8: key_terms count parity across languages ---
    if isinstance(translations, dict):
        kt_counts = {}
        for lang in VALID_LANGUAGE_KEYS:
            lang_data = translations.get(lang)
            if isinstance(lang_data, dict):
                kt = lang_data.get("key_terms")
                if isinstance(kt, dict):
                    kt_counts[lang] = len(kt)
        if kt_counts:
            min_count = min(kt_counts.values())
            max_count = max(kt_counts.values())
            if min_count > 0 and max_count > 2 * min_count:
                max_lang = max(kt_counts, key=kt_counts.get)
                min_lang = min(kt_counts, key=kt_counts.get)
                warnings.append(ReviewWarning(
                    field="translations.*.key_terms",
                    category="key_terms_count_disparity",
                    severity="low",
                    message=(
                        f"key_terms count disparity: {max_lang}={max_count} vs "
                        f"{min_lang}={min_count} (>2x difference)"
                    ),
                    suggestion=(
                        "Ensure all languages cover the same Arabic terms in key_terms."
                    ),
                ))

    # --- Check 10: narrator word_ranges match ---
    word_analysis = result.get("word_analysis", [])
    if isinstance(isnad_matn, dict) and isinstance(word_analysis, list) and len(word_analysis) > 0:
        narrators = isnad_matn.get("narrators", [])
        has_chain = isnad_matn.get("has_chain", False)
        for ni, narrator in enumerate(narrators):
            if not isinstance(narrator, dict):
                continue
            wr = narrator.get("word_ranges")
            name_ar = narrator.get("name_ar", "")
            if wr is not None and isinstance(wr, list):
                # Verify words at ranges contain the narrator's name.
                # We normalize Arabic case endings (e.g., أبو/أبي/أبا are
                # the same name in different grammatical cases) and strip
                # diacritics before comparing.
                name_clean = _normalize_arabic_name(_strip_arabic_diacritics(name_ar).strip())
                if name_clean:
                    name_parts = name_clean.split()
                    for rng in wr:
                        if not isinstance(rng, dict):
                            continue
                        ws = rng.get("word_start", 0)
                        we = rng.get("word_end", 0)
                        if ws < 0 or we > len(word_analysis) or we <= ws:
                            continue
                        range_words = [
                            _normalize_arabic_name(_strip_arabic_diacritics(w.get("word", "")))
                            for w in word_analysis[ws:we]
                            if isinstance(w, dict)
                        ]
                        range_text = " ".join(range_words)
                        # Check if name or key parts appear in the range text.
                        # Full name match or >50% of name words matching is OK.
                        if name_clean in range_text:
                            continue  # exact match after normalization
                        matching_parts = sum(1 for p in name_parts if p in range_text)
                        if len(name_parts) >= 2 and matching_parts >= len(name_parts) * 0.5:
                            continue  # partial match (>50% of name words found)
                        warnings.append(ReviewWarning(
                            field=f"isnad_matn.narrators[{ni}].word_ranges",
                            category="narrator_word_range_mismatch",
                            severity="low",
                            message=(
                                f"Narrator '{name_ar}' word_ranges [{ws}:{we}] "
                                f"contains '{range_text}' which does not match name"
                            ),
                            suggestion="Adjust word_ranges to cover the narrator's name in word_analysis.",
                        ))
            elif wr is None and has_chain:
                # Missing word_ranges for a chained hadith — low severity suggestion
                warnings.append(ReviewWarning(
                    field=f"isnad_matn.narrators[{ni}]",
                    category="missing_narrator_word_ranges",
                    severity="low",
                    message=(
                        f"Narrator '{narrator.get('name_en', '?')}' has no word_ranges "
                        f"in a chained hadith"
                    ),
                    suggestion="Add word_ranges to enable narrator name highlighting in the UI.",
                ))

    return warnings


# ---------------------------------------------------------------------------
# Chunked processing functions
# ---------------------------------------------------------------------------

def estimate_word_count(request: PipelineRequest) -> int:
    """Estimate the Arabic word count for a pipeline request.

    Uses whitespace splitting on the Arabic text as a rough estimate.
    """
    text = request.arabic_text.strip()
    if not text:
        return 0
    return len(text.split())


def should_use_chunked_processing(request: PipelineRequest,
                                   threshold: int = CHUNKED_PROCESSING_THRESHOLD) -> bool:
    """Determine if a request should use chunked processing.

    Returns True if the estimated word count exceeds the threshold.
    """
    return estimate_word_count(request) > threshold


def build_structure_prompt(request: PipelineRequest,
                           glossary: Optional[dict] = None,
                           few_shot_examples: Optional[dict] = None) -> str:
    """Build the structure pass prompt for chunked processing.

    The structure pass generates all fields EXCEPT word_analysis and
    chunk-level translations. It defines chunk boundaries (types and
    word ranges) plus verse-level translations in all 11 languages.

    Args:
        request: The PipelineRequest to process.
        glossary: Optional pre-loaded glossary.
        few_shot_examples: Optional pre-loaded examples.

    Returns:
        Complete prompt string for the structure pass.
    """
    system_prompt = build_system_prompt(glossary, few_shot_examples)
    user_msg = build_user_message(request)

    structure_instructions = """

SPECIAL INSTRUCTIONS — STRUCTURE PASS (Chunked Processing):

This is a STRUCTURE PASS for a long hadith. Generate ALL fields EXCEPT:
- word_analysis (will be generated per-chunk in detail passes)
- chunk translations (will be generated per-chunk in detail passes)

You MUST generate:
1. diacritized_text (full text with tashkeel)
2. diacritics_status, diacritics_changes
3. tags, content_type
4. related_quran
5. isnad_matn (full narrator analysis)
6. translations (verse-level in all 11 languages — full faithful translations, NOT summaries)
   SUMMARY GUIDANCE: The "summary" should be 2-3 sentences explaining the verse's meaning and significance. Where relevant, note the historical context — who the audience was, what circumstances prompted this teaching, and how the original audience would have understood the key terms.
7. chunks — define boundaries with:
   - chunk_type (isnad/opening/body/quran_quote/closing)
   - arabic_text (the Arabic segment for this chunk)
   - word_start and word_end (estimated — will be finalized in detail passes)
   - translations: set to empty object {} (will be filled in detail passes)
8. topics (1-5 Level 2 topic keys from the TOPIC TAXONOMY in the system prompt)
9. key_phrases (0-5 multi-word Arabic expressions with English translations and categories)
10. similar_content_hints (0-3 thematic hints for finding similar hadiths/verses)

For word_analysis, output an empty array [].

Focus on accurate chunk boundary segmentation and complete verse-level translations."""

    return f"SYSTEM:\n{system_prompt}\n\nUSER:\n{user_msg}{structure_instructions}"


def build_chunk_detail_prompt(request: PipelineRequest,
                              structure_result: dict,
                              chunk_index: int,
                              glossary: Optional[dict] = None,
                              few_shot_examples: Optional[dict] = None) -> str:
    """Build a detail pass prompt for one chunk.

    Generates word_analysis entries and chunk translations in all 11
    languages for a single chunk. Includes the full text for context
    but focuses on the chunk's Arabic segment.

    Args:
        request: Original PipelineRequest.
        structure_result: Result from the structure pass.
        chunk_index: Zero-based index of the chunk to detail.
        glossary: Optional pre-loaded glossary.
        few_shot_examples: Optional pre-loaded examples.

    Returns:
        Complete prompt string for this chunk's detail pass.

    Raises:
        IndexError: If chunk_index is out of bounds.
    """
    chunks = structure_result.get("chunks", [])
    if chunk_index < 0 or chunk_index >= len(chunks):
        raise IndexError(
            f"chunk_index {chunk_index} out of range (0..{len(chunks) - 1})"
        )

    chunk = chunks[chunk_index]
    chunk_arabic = chunk.get("arabic_text", "")
    chunk_type = chunk.get("chunk_type", "body")
    word_start = chunk.get("word_start", 0)
    word_end = chunk.get("word_end", 0)

    system_prompt = build_system_prompt(glossary, few_shot_examples)

    detail_instructions = f"""
SPECIAL INSTRUCTIONS — CHUNK DETAIL PASS:

This is chunk {chunk_index + 1} of {len(chunks)} (type: {chunk_type}).
Word range: [{word_start}, {word_end}) in the full text.

FULL Arabic text (for context):
{request.arabic_text}

THIS CHUNK's Arabic text (analyze ONLY these words):
{chunk_arabic}

Generate a JSON object with exactly two fields:
1. "word_analysis": Array of word-by-word analysis for ONLY the words in this chunk.
   Each entry: {{"word": "diacritized", "translation": {{"en": "...", "ur": "...", ...all 11 langs}}, "pos": "TAG"}}
2. "translations": Object with all 11 language keys, each a plain string translation of THIS CHUNK ONLY.
   {{"en": "...", "ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}}

IMPORTANT:
- Translate ONLY this chunk, not the full hadith
- Each word in word_analysis must have the fully diacritized form
- All 11 language keys are required in both word translations and chunk translations
- Context from surrounding chunks should inform translation but output covers only this chunk"""

    return f"SYSTEM:\n{system_prompt}\n\nUSER:\n{detail_instructions}"


def assemble_chunked_result(structure_result: dict,
                            chunk_details: List[dict]) -> dict:
    """Assemble a complete result from structure + chunk detail results.

    Concatenates word_analysis from all chunks, inserts chunk translations,
    and validates the assembled result.

    Args:
        structure_result: Result from the structure pass.
        chunk_details: List of detail pass results, one per chunk, in order.

    Returns:
        Complete assembled result dict.

    Raises:
        ValueError: If word counts don't match or assembly fails.
    """
    result = dict(structure_result)  # shallow copy
    chunks = result.get("chunks", [])

    if len(chunk_details) != len(chunks):
        raise ValueError(
            f"Expected {len(chunks)} chunk details, got {len(chunk_details)}"
        )

    # Concatenate word_analysis from all chunks
    all_words = []
    for i, detail in enumerate(chunk_details):
        words = detail.get("word_analysis", [])
        all_words.extend(words)

        # Insert chunk translations
        if i < len(chunks) and "translations" in detail:
            chunks[i] = dict(chunks[i])  # shallow copy the chunk
            chunks[i]["translations"] = detail["translations"]

    result["word_analysis"] = all_words
    result["chunks"] = chunks

    # Fix word_start/word_end to match actual word counts
    offset = 0
    for i, detail in enumerate(chunk_details):
        word_count = len(detail.get("word_analysis", []))
        if i < len(result["chunks"]):
            result["chunks"][i]["word_start"] = offset
            result["chunks"][i]["word_end"] = offset + word_count
        offset += word_count

    # Validate that last chunk ends at total word count
    total_words = len(all_words)
    if result["chunks"]:
        last_end = result["chunks"][-1].get("word_end", 0)
        if last_end != total_words:
            raise ValueError(
                f"Last chunk word_end ({last_end}) does not match "
                f"total word_analysis length ({total_words})"
            )

    # Run schema validation
    errors = validate_result(result)
    if errors:
        raise ValueError(
            f"Assembled result has {len(errors)} validation errors: "
            + "; ".join(errors[:5])
        )

    return result


# ---------------------------------------------------------------------------
# Prompt builders for review and fix passes
# ---------------------------------------------------------------------------

def build_review_prompt(result: dict, request: PipelineRequest) -> str:
    """Build a review pass prompt for quality assessment.

    Includes the result JSON and a specific review checklist.

    Args:
        result: The pipeline result to review.
        request: The original request for context.

    Returns:
        Complete prompt string for the review pass.
    """
    result_json = json.dumps(result, ensure_ascii=False, indent=2)

    return f"""You are reviewing an AI-generated analysis of an Islamic scripture verse/hadith.

ORIGINAL ARABIC TEXT:
{request.arabic_text}

{f"ENGLISH REFERENCE: {request.english_text}" if request.english_text else ""}

GENERATED RESULT:
{result_json}

REVIEW CHECKLIST:
1. **Translation faithfulness**: Are the 11-language translations faithful to the Arabic? Watch for:
   - Summaries instead of full translations (especially in long texts)
   - Omitted sections or paraphrasing
   - Incorrect theological interpretations
2. **Script correctness**: Does each translation use the correct script?
   - Turkish/French/German/Spanish should have proper diacritics
   - Farsi/Urdu should be in Arabic script but NOT echo the Arabic source
   - Chinese should be simplified Chinese
3. **Word analysis quality**: Are word translations correct for EACH language?
   - No Arabic echo-back in non-Arabic-script languages
   - Context-appropriate translations (not dictionary entries)
4. **Semantic chunking**: Are chunk boundaries at natural divisions?
   - Isnad/matn separation correct?
   - Quran quotes properly identified?
5. **Narrator accuracy**: Are narrator names, roles, and confidence levels correct?
6. **Diacritization**: Is the diacritized_text complete and accurate?
7. **Tags and content_type**: Appropriate classification?
8. **Topics**: Are the assigned topics from the controlled vocabulary and accurately describe the content?
9. **Key phrases**: Are extracted phrases genuinely multi-word, meaningful, and not generic narrator formulae?
10. **Similar content hints**: Are the thematic hints reasonable and not hallucinated specific references?

OUTPUT FORMAT:
Respond with a JSON object:
{{
  "overall_quality": "pass" | "needs_fix" | "needs_regeneration",
  "score": 1-10,
  "issues": [
    {{
      "field": "field.path",
      "severity": "low" | "medium" | "high",
      "description": "What's wrong",
      "suggestion": "How to fix it"
    }}
  ],
  "notes": "Any additional observations"
}}

Use "pass" if all checks pass with only minor issues. Use "needs_fix" if specific
fields need correction. Use "needs_regeneration" only if the result is fundamentally
flawed (wrong text analyzed, major theological errors, >50% fields problematic)."""


def build_fix_prompt(result: dict, request: PipelineRequest,
                     warnings: List[ReviewWarning]) -> str:
    """Build a fix pass prompt to correct specific flagged fields.

    Includes only the flagged fields and warning details, asking the agent
    to output corrected fields only.

    Args:
        result: The pipeline result with issues.
        request: The original request for context.
        warnings: List of ReviewWarning objects to address.

    Returns:
        Complete prompt string for the fix pass.
    """
    # Extract just the flagged fields for context
    flagged_fields = {}
    for w in warnings:
        # Navigate to the field in the result
        field_path = w.field
        if "." in field_path or "[" in field_path:
            # Top-level field for context
            top_key = field_path.split(".")[0].split("[")[0]
            if top_key in result:
                flagged_fields[top_key] = result[top_key]
        elif field_path in result:
            flagged_fields[field_path] = result[field_path]

    flagged_json = json.dumps(flagged_fields, ensure_ascii=False, indent=2)

    warnings_text = ""
    for i, w in enumerate(warnings, 1):
        warnings_text += (
            f"\n{i}. [{w.severity.upper()}] {w.category} in {w.field}\n"
            f"   Problem: {w.message}\n"
            f"   Suggestion: {w.suggestion}\n"
        )

    return f"""You are fixing specific issues in an AI-generated analysis of an Islamic scripture verse/hadith.

ORIGINAL ARABIC TEXT:
{request.arabic_text}

{f"ENGLISH REFERENCE: {request.english_text}" if request.english_text else ""}

FLAGGED FIELDS (current values):
{flagged_json}

ISSUES TO FIX:
{warnings_text}

INSTRUCTIONS:
- Fix ONLY the flagged fields. Do not modify unflagged content.
- Output a JSON object containing ONLY the corrected fields, using the same structure.
- For nested fields (e.g., "translations.tr"), output the full parent object with the correction.
- For word_analysis corrections, output the full word_analysis array with fixes applied.
- Ensure all fixes maintain theological accuracy and use proper scripts/diacritics.
- Do NOT output fields that don't need fixing."""
