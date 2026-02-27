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
EUROPEAN_DIACRITICS: Dict[str, str] = {
    "tr": "\u00f6\u00fc\u015f\u00e7\u011f\u0131",   # oushcgi with diacritics
    "fr": "\u00e9\u00e8\u00ea\u00e0\u00e7",           # eeeac with diacritics
    "de": "\u00e4\u00f6\u00fc\u00df",                  # aoub with diacritics
    "es": "\u00f1\u00e1\u00e9\u00ed\u00f3\u00fa",     # naeiou with diacritics
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
    """Remove Arabic tashkeel marks from text for comparison."""
    diacritics = set("\u064B\u064C\u064D\u064E\u064F\u0650\u0651\u0652\u0670")
    return "".join(ch for ch in text if ch not in diacritics)


# ---------------------------------------------------------------------------
# review_result() — quality checks beyond schema validation
# ---------------------------------------------------------------------------

def review_result(result: dict, request: PipelineRequest) -> List[ReviewWarning]:
    """Run quality checks on a pipeline result beyond schema validation.

    Returns a list of ReviewWarning objects. Empty list means all checks passed.

    Checks:
    1. Translation length ratio — detect summaries masquerading as translations
    2. Arabic echo-back in word translations — detect untranslated words
    3. European language diacritics — detect ASCII-only European text
    4. Empty related_quran for Quran verses — Quran verses should self-reference
    5. Chunk translation coherence — chunk translations should sum to ~verse length
    6. Missing isnad chunk — has_chain=True should have isnad chunk
    7. Back-reference without chain — Arabic starts with back-ref but has_chain=False
    """
    warnings: List[ReviewWarning] = []

    arabic_text = request.arabic_text
    arabic_len = len(arabic_text.strip())

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
                    # Farsi/Urdu use Arabic script — check for exact echo
                    stripped_word = _strip_arabic_diacritics(word_ar).strip()
                    stripped_trans = _strip_arabic_diacritics(word_trans).strip()
                    if stripped_word and stripped_trans and stripped_word == stripped_trans:
                        warnings.append(ReviewWarning(
                            field=f"word_analysis[{i}].translation.{lang}",
                            category="arabic_echo",
                            severity="high",
                            message=(
                                f"Word translation in {lang} is identical to Arabic word "
                                f"'{word_ar}' (exact echo-back)"
                            ),
                            suggestion=(
                                f"Translate '{word_ar}' into {lang} — do not echo the Arabic."
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
    if "translations" in result:
        for lang, required_chars in EUROPEAN_DIACRITICS.items():
            lang_data = result["translations"].get(lang)
            if not isinstance(lang_data, dict):
                continue
            text = lang_data.get("text", "")
            min_len = EUROPEAN_DIACRITICS_MIN_LENGTH.get(lang, 100)
            if len(text) < min_len:
                continue
            if not any(ch in text.lower() for ch in required_chars):
                warnings.append(ReviewWarning(
                    field=f"translations.{lang}",
                    category="missing_diacritics",
                    severity="medium",
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
7. chunks — define boundaries with:
   - chunk_type (isnad/opening/body/quran_quote/closing)
   - arabic_text (the Arabic segment for this chunk)
   - word_start and word_end (estimated — will be finalized in detail passes)
   - translations: set to empty object {} (will be filled in detail passes)

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
