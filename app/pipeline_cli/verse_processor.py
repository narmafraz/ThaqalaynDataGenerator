"""Verse processor for v3 pipeline — prepare, postprocess, and fix verses.

Core functions called by the orchestrator. Each function is pure Python
(zero Claude tokens) except where explicitly noted.

Functions:
    prepare_verse: Extract verse data, build system + user prompts, write to work_dir
    postprocess_verse: Parse AI response, validate, review, save
    prepare_fix: Build fix prompt from review warnings
    apply_fix: Apply fix response, re-validate, save
"""

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.ai_pipeline import (
    PIPELINE_VERSION,
    VALID_TOPICS,
    PipelineRequest,
    build_system_prompt,
    build_user_message,
    extract_pipeline_request,
    reconstruct_fields,
    strip_redundant_fields,
    validate_result,
)
from app.ai_pipeline_review import CHUNKED_PROCESSING_THRESHOLD, ReviewWarning, build_fix_prompt, review_result
from app.config import (
    AI_CONTENT_DIR,
    AI_CONTENT_SUBDIR,
    AI_PIPELINE_DATA_DIR,
    AI_RESPONSES_DIR,
    DEFAULT_DESTINATION_DIR,
)

logger = logging.getLogger(__name__)

VALID_LANGUAGE_KEYS = {"en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"}
LANGUAGE_ORDER = ["en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]

# Compact array-of-arrays format for word_analysis — MANDATORY to fit 32K output limit
COMPACT_WORD_INSTRUCTIONS = """
CRITICAL OUTPUT FORMAT REQUIREMENTS (mandatory for token budget):

1. word_analysis MUST use compact arrays (NOT objects):
   ["word","POS","en","ur","tr","fa","id","bn","es","fr","de","ru","zh"]
   Language order is FIXED: en, ur, tr, fa, id, bn, es, fr, de, ru, zh.
   Example: ["قَالَ","V","he said","کہا","dedi","گفت","berkata","বলেছেন","dijo","a dit","sagte","сказал","他说"]

2. Be CONCISE in all text fields. Translations should be faithful but not padded.
   Summaries: 1-2 sentences max. Key_terms: 2-4 terms per language.
   SEO questions: 1 sentence.

3. Output the COMPLETE JSON in a single response. Do NOT split across messages.
   Do NOT continue from a previous response or say "Continuing from...".
   Your output must be a single, self-contained JSON object."""

V4_COMPACT_INSTRUCTIONS = """
CRITICAL OUTPUT FORMAT REQUIREMENTS (mandatory for token budget):

1. word_tags MUST use compact arrays: ["word","POS"]
   Example: ["قَالَ","V"]

2. Do NOT include translations.*.text — only summary, key_terms, seo_question.
   Full translation text is reconstructed from chunk translations.

3. Be CONCISE in all text fields. Translations should be faithful but not padded.
   Summaries: 1-2 sentences max. Key_terms: 2-4 terms per language.
   SEO questions: 1 sentence.

4. Output the COMPLETE JSON in a single response. Do NOT split across messages.
   Do NOT continue from a previous response or say "Continuing from...".
   Your output must be a single, self-contained JSON object."""


@dataclass
class VersePlan:
    """Output of prepare_verse — everything needed for an LLM call."""
    verse_path: str
    verse_id: str
    mode: str  # "single" or "chunked"
    request: PipelineRequest
    system_prompt: str
    user_message: str
    work_dir: str
    word_count: int = 0
    backend: str = "claude"  # "claude" or "openai"
    model: str = ""  # Actual model name used for generation


@dataclass
class VerseResult:
    """Output of postprocess_verse."""
    verse_id: str
    status: str  # "pass", "needs_fix", "error", "skipped"
    warnings: List[ReviewWarning] = field(default_factory=list)
    validation_errors: List[str] = field(default_factory=list)
    error: Optional[str] = None
    result_dict: Optional[dict] = None
    raw_response: Optional[str] = None
    token_usage: Optional[dict] = None
    false_positive_accepted: bool = False


def verse_path_to_id(verse_path: str) -> str:
    """Convert /books/al-kafi:1:1:1:1 to al-kafi_1_1_1_1."""
    return verse_path.replace("/books/", "").replace(":", "_")


def id_to_verse_path(verse_id: str) -> str:
    """Convert al-kafi_1_1_1_1 to /books/al-kafi:1:1:1:1."""
    parts = verse_id.split("_")
    book = parts[0]
    rest = ":".join(parts[1:])
    return f"/books/{book}:{rest}"


def is_complete(verse_id: str, responses_dir: Optional[str] = None) -> bool:
    """Check if a verse already has a completed response file."""
    if responses_dir is None:
        responses_dir = AI_RESPONSES_DIR
    return os.path.exists(os.path.join(responses_dir, f"{verse_id}.json"))


def load_word_dictionary(path: Optional[str] = None) -> Optional[dict]:
    """Load word translations cache if it exists."""
    if path is None:
        path = os.path.join(AI_PIPELINE_DATA_DIR, "word_translations_cache.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_narrator_templates(path: Optional[str] = None) -> Optional[dict]:
    """Load narrator templates if they exist."""
    if path is None:
        path = os.path.join(AI_PIPELINE_DATA_DIR, "narrator_templates.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Prepare
# ---------------------------------------------------------------------------

def prepare_verse(
    verse_path: str,
    work_dir: str,
    data_dir: Optional[str] = None,
    include_few_shot: bool = False,
    use_compact_format: bool = True,
    use_v3: bool = False,
) -> Optional[VersePlan]:
    """Extract verse data and build prompts. Zero Claude tokens.

    Args:
        verse_path: e.g. "/books/al-kafi:1:1:1:1"
        work_dir: Directory to write prompt files to
        data_dir: ThaqalaynData directory
        include_few_shot: Whether to include few-shot examples in system prompt
        use_compact_format: Whether to add compact array format instructions
        use_v3: If True, use v3 compact word format instead of v4 word_tags

    Returns:
        VersePlan or None if verse not found.
    """
    request = extract_pipeline_request(verse_path, data_dir)
    if request is None:
        return None

    verse_id = verse_path_to_id(verse_path)
    word_count = len(request.arabic_text.split())

    # Build system prompt (with or without few-shot)
    few_shot = None if not include_few_shot else None  # None = load from file
    if not include_few_shot:
        few_shot = {"examples": []}  # empty = skip examples section

    system_prompt = build_system_prompt(few_shot_examples=few_shot)

    # Add compact format instructions (v3 or v4)
    if use_compact_format:
        if use_v3:
            system_prompt += "\n" + COMPACT_WORD_INSTRUCTIONS
        else:
            system_prompt += "\n" + V4_COMPACT_INSTRUCTIONS

    user_message = build_user_message(request)

    # Write files to work_dir for auditability
    os.makedirs(work_dir, exist_ok=True)
    sp_path = os.path.abspath(os.path.join(work_dir, "system_prompt.txt"))
    with open(sp_path, "w", encoding="utf-8") as f:
        f.write(system_prompt)
    logger.info("WROTE %s", sp_path)
    um_path = os.path.abspath(os.path.join(work_dir, "user_message.txt"))
    with open(um_path, "w", encoding="utf-8") as f:
        f.write(user_message)
    logger.info("WROTE %s", um_path)
    meta_path = os.path.abspath(os.path.join(work_dir, "metadata.json"))
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({
            "verse_path": verse_path,
            "verse_id": verse_id,
            "word_count": word_count,
            "mode": "chunked" if word_count > CHUNKED_PROCESSING_THRESHOLD else "single",
            "include_few_shot": include_few_shot,
            "use_compact_format": use_compact_format,
            "pipeline_format": "v3" if use_v3 else "v4",
            "prepared_at": datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)
    logger.info("WROTE %s", meta_path)

    return VersePlan(
        verse_path=verse_path,
        verse_id=verse_id,
        mode="chunked" if word_count > CHUNKED_PROCESSING_THRESHOLD else "single",
        request=request,
        system_prompt=system_prompt,
        user_message=user_message,
        work_dir=work_dir,
        word_count=word_count,
    )


# ---------------------------------------------------------------------------
# Postprocess
# ---------------------------------------------------------------------------

def expand_compact_words(word_analysis: list) -> list:
    """Convert compact array-of-arrays word format to standard dict format.

    Accepts both formats transparently:
    - If entry is a dict, return as-is
    - If entry is a list, expand to dict with proper keys

    Returns list of standard word dicts.
    """
    expanded = []
    for entry in word_analysis:
        if isinstance(entry, dict):
            expanded.append(entry)
        elif isinstance(entry, list) and len(entry) >= 13:
            word_dict = {
                "word": entry[0],
                "pos": entry[1],
                "translation": {
                    lang: entry[i + 2]
                    for i, lang in enumerate(LANGUAGE_ORDER)
                },
            }
            expanded.append(word_dict)
        else:
            # Malformed entry — keep as-is for error reporting
            expanded.append(entry)
    return expanded


def override_known_words(word_analysis: list, word_dict_data: Optional[dict]) -> Tuple[list, List[dict]]:
    """Override word translations with dictionary values for consistency.

    Returns (updated_word_analysis, list_of_overrides_applied).
    """
    if not word_dict_data or "words" not in word_dict_data:
        return word_analysis, []

    words_cache = word_dict_data["words"]
    overrides = []

    for entry in word_analysis:
        if not isinstance(entry, dict):
            continue
        word = entry.get("word", "")
        pos = entry.get("pos", "")
        key = f"{word}|{pos}"

        if key not in words_cache:
            continue

        cached = words_cache[key]
        cached_translations = cached.get("translations", {})
        entry_translations = entry.get("translation", {})

        for lang, cached_val in cached_translations.items():
            current_val = entry_translations.get(lang, "")
            if current_val != cached_val:
                overrides.append({
                    "word": word,
                    "pos": pos,
                    "lang": lang,
                    "was": current_val,
                    "now": cached_val,
                })
                entry_translations[lang] = cached_val

    return word_analysis, overrides


def override_narrators(
    result: dict,
    narrator_templates: Optional[dict],
    registry: Optional["NarratorRegistry"] = None,
) -> Tuple[dict, List[dict]]:
    """Override narrator transliterations and resolve canonical_id.

    Uses templates for name_en overrides and canonical_id (fast path).
    Falls back to registry.resolve() with chain context for disambiguation.

    Returns (updated_result, list_of_overrides_applied).
    """
    templates = {}
    if narrator_templates and "narrators" in narrator_templates:
        templates = narrator_templates["narrators"]

    if not templates and registry is None:
        return result, []

    overrides = []
    isnad_matn = result.get("isnad_matn", {})
    narrators = isnad_matn.get("narrators", [])

    preceding_names: List[str] = []
    for n in narrators:
        name_ar = n.get("name_ar", "").strip()

        # Override English name from template
        if name_ar in templates:
            tmpl = templates[name_ar]
            if n.get("name_en") != tmpl["name_en"]:
                overrides.append({
                    "name_ar": name_ar,
                    "field": "name_en",
                    "was": n.get("name_en"),
                    "now": tmpl["name_en"],
                })
                n["name_en"] = tmpl["name_en"]

        # Resolve canonical_id
        canonical_id = None

        # Fast path: use canonical_id from template if present
        if name_ar in templates and "canonical_id" in templates[name_ar]:
            canonical_id = templates[name_ar]["canonical_id"]

        # Disambiguation path: use registry with chain context
        # when template has no canonical_id, or name is ambiguous
        if registry and (canonical_id is None or _is_ambiguous_name(name_ar, templates)):
            resolved = registry.resolve(name_ar, preceding_names=preceding_names)
            if resolved is not None:
                canonical_id = resolved

        if canonical_id is not None:
            if n.get("canonical_id") != canonical_id:
                overrides.append({
                    "name_ar": name_ar,
                    "field": "canonical_id",
                    "was": n.get("canonical_id"),
                    "now": canonical_id,
                })
            n["canonical_id"] = canonical_id

        preceding_names.append(name_ar)

    return result, overrides


def _is_ambiguous_name(name_ar: str, templates: dict) -> bool:
    """Check if a narrator name is ambiguous (multiple possible identities).

    A name is ambiguous if the template entry has an explicit
    disambiguation_context or identity_confidence of 'ambiguous'.
    """
    tmpl = templates.get(name_ar)
    if not tmpl:
        return False
    if tmpl.get("disambiguation_context"):
        return True
    if tmpl.get("identity_confidence") == "ambiguous":
        return True
    return False


def repair_json_quotes(text: str) -> str:
    """Escape unescaped ASCII double quotes inside JSON string values.

    The model sometimes outputs unescaped " inside Chinese/Russian text
    (e.g. 说："text"  or «текст»"text") which breaks JSON parsing.

    Strategy: walk char by char tracking JSON structure. When inside a
    string value, if we hit a " that is NOT a valid string terminator
    (next non-whitespace isn't , } ] :) we escape it.
    """
    import re

    # First try parsing as-is — if it works, no repair needed
    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass

    # Repair: find unescaped quotes inside string values by checking
    # if the quote is followed by valid JSON structure tokens
    chars = list(text)
    i = 0
    in_string = False
    result = []

    while i < len(chars):
        ch = chars[i]

        if ch == '\\' and in_string:
            # Escaped char — copy both
            result.append(ch)
            if i + 1 < len(chars):
                i += 1
                result.append(chars[i])
            i += 1
            continue

        if ch == '"':
            if not in_string:
                in_string = True
                result.append(ch)
            else:
                # Is this the real end of the string?
                # Look ahead past whitespace for a valid JSON token after string
                j = i + 1
                while j < len(chars) and chars[j] in ' \t\n\r':
                    j += 1
                if j < len(chars) and chars[j] in ',}]:':
                    # Valid string terminator
                    in_string = False
                    result.append(ch)
                elif j >= len(chars):
                    # End of text — valid terminator
                    in_string = False
                    result.append(ch)
                else:
                    # Unescaped quote inside string — escape it
                    result.append('\\')
                    result.append('"')
            i += 1
            continue

        result.append(ch)
        i += 1

    repaired = ''.join(result)

    # Verify repair worked
    try:
        json.loads(repaired)
        return repaired
    except json.JSONDecodeError:
        # If repair didn't help, return original for better error reporting
        return text


def strip_code_fences(text: str) -> str:
    """Strip markdown code fences and trailing text from AI response.

    Claude sometimes wraps JSON in ```json ... ``` blocks and may add
    commentary before/after the fences. Extract just the JSON portion.
    """
    text = text.strip()
    # Handle narrative text before code fences (common in fix responses)
    if not text.startswith("```") and not text.startswith("{"):
        fence_idx = text.find("```")
        if fence_idx >= 0:
            text = text[fence_idx:].strip()
    if text.startswith("```"):
        # Remove opening fence (```json or ```)
        first_newline = text.index("\n") if "\n" in text else len(text)
        text = text[first_newline + 1:]
        # Find closing fence and discard everything after it
        closing = text.rfind("```")
        if closing >= 0:
            text = text[:closing]
    # If no fences but starts with { and has extra text after }, trim it
    elif text.startswith("{"):
        # Find the matching closing brace by tracking nesting
        depth = 0
        in_string = False
        escape = False
        for i, ch in enumerate(text):
            if escape:
                escape = False
                continue
            if ch == '\\' and in_string:
                escape = True
                continue
            if ch == '"' and not escape:
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    text = text[:i + 1]
                    break
    return text.strip()


def _normalize_narrator_positions(result: dict) -> None:
    """Fix narrator positions to be 1-based sequential.

    Models sometimes output 0-based positions. This renumbers them
    in-place to match the expected 1-based convention.
    """
    narrators = result.get("isnad_matn", {}).get("narrators", [])
    if not narrators:
        return
    for i, narrator in enumerate(narrators):
        if isinstance(narrator, dict):
            narrator["position"] = i + 1


# ---------------------------------------------------------------------------
# Auto-fix trivial validation errors (zero LLM cost)
# ---------------------------------------------------------------------------

# Validation errors that can be auto-fixed without LLM
AUTO_FIXABLE_PATTERNS = {
    "missing ambiguity_note",
    "invalid topic:",
}

# Validation errors that can be sent to fix pass (not terminal)
FIXABLE_PATTERNS = {
    "missing ambiguity_note",
    "invalid topic:",
    "invalid tag:",
    "invalid content_type:",
    "invalid narrator role:",
    "invalid identity_confidence:",
    "invalid chunk_type:",
    "invalid diacritics_status:",
    "invalid quran relationship:",
    "invalid pos:",
    "key_terms key",  # non-Arabic key_terms keys
    "has no diacritics",  # undiacritized multi-letter words — fixable by LLM
}


def fix_chunk_boundaries(result: dict) -> list:
    """Recalculate chunk word_start/word_end from actual text.

    Fixes zero-length chunks (word_start == word_end) using isnad_matn text
    and enforces sequential coverage. Called before validate_result() to
    prevent structural validation failures from OpenAI models.

    Returns list of fixes applied (for logging).
    """
    fixes = []
    word_analysis = result.get("word_analysis") or result.get("word_tags", [])
    total_words = len(word_analysis)
    chunks = result.get("chunks", [])
    if not chunks or total_words == 0:
        return fixes

    for i, chunk in enumerate(chunks):
        ws = chunk.get("word_start", 0)
        we = chunk.get("word_end", 0)

        # Fix zero-length chunks (word_start == word_end)
        if ws == we:
            if chunk.get("chunk_type") == "isnad" and result.get("isnad_matn", {}).get("isnad_ar"):
                isnad_words = len(result["isnad_matn"]["isnad_ar"].split())
                chunk["word_start"] = 0
                chunk["word_end"] = min(isnad_words, total_words)
                fixes.append(
                    f"chunk[{i}] isnad: set word_end={chunk['word_end']} "
                    f"from isnad_ar word count ({isnad_words})"
                )
            elif len(chunks) == 1:
                chunk["word_start"] = 0
                chunk["word_end"] = total_words
                fixes.append(f"chunk[{i}] single: set word_end={total_words}")

        # Fix off-by-one on last chunk (model uses last index instead of length)
        if i == len(chunks) - 1:
            if chunk.get("word_end", 0) == total_words - 1 and total_words > 1:
                chunk["word_end"] = total_words
                fixes.append(f"chunk[{i}] last: word_end {total_words - 1} -> {total_words}")

    # Enforce sequential coverage: chunk[i+1].word_start = chunk[i].word_end
    for i in range(len(chunks) - 1):
        expected = chunks[i]["word_end"]
        actual = chunks[i + 1].get("word_start")
        if actual != expected:
            chunks[i + 1]["word_start"] = expected
            fixes.append(f"chunk[{i + 1}] sequential: word_start {actual} -> {expected}")

    return fixes


def _auto_fix_validation_errors(result: dict) -> list:
    """Attempt to programmatically fix trivial validation errors in-place.

    Returns list of fixes applied (for logging).
    """
    fixes = []

    # Fix 1: Missing ambiguity_note for likely/ambiguous narrators
    narrators = result.get("isnad_matn", {}).get("narrators", [])
    for narrator in narrators:
        if not isinstance(narrator, dict):
            continue
        confidence = narrator.get("identity_confidence")
        if confidence in ("likely", "ambiguous") and not narrator.get("ambiguity_note"):
            narrator["ambiguity_note"] = (
                "Multiple narrators share this name; "
                "identified based on chain context and historical records"
            )
            fixes.append(f"auto-filled ambiguity_note for {narrator.get('name_en', '?')}")

    # Fix 2: Last chunk word_end off-by-one (model uses last index instead of length)
    word_analysis = result.get("word_analysis", [])
    chunks = result.get("chunks", [])
    if word_analysis and chunks:
        last_chunk = chunks[-1]
        wa_len = len(word_analysis)
        if (isinstance(last_chunk.get("word_end"), int)
                and last_chunk["word_end"] == wa_len - 1):
            last_chunk["word_end"] = wa_len
            fixes.append(f"corrected last chunk word_end from {wa_len - 1} to {wa_len}")

    # Fix 3: Narrator word_ranges with word_end == word_start (zero-width range)
    for narrator in narrators:
        if not isinstance(narrator, dict):
            continue
        for wr in narrator.get("word_ranges", []):
            if (isinstance(wr.get("word_start"), int)
                    and isinstance(wr.get("word_end"), int)
                    and wr["word_end"] == wr["word_start"]):
                wr["word_end"] = wr["word_start"] + 1
                fixes.append(
                    f"corrected narrator {narrator.get('name_en', '?')} "
                    f"word_end from {wr['word_start']} to {wr['word_start'] + 1}"
                )

    # Fix 4: Invalid topics — strip invalid values, keep valid ones
    if "topics" in result and isinstance(result["topics"], list) and VALID_TOPICS:
        original = result["topics"]
        valid = [t for t in original if t in VALID_TOPICS]
        invalid = [t for t in original if t not in VALID_TOPICS]
        if invalid:
            if valid:
                result["topics"] = valid
                fixes.append(f"removed invalid topics: {invalid}")
            else:
                # All topics invalid — remove field entirely to avoid 0-item error
                del result["topics"]
                fixes.append(f"removed all invalid topics: {invalid}")

    return fixes


def _is_fixable_error(error_msg: str) -> bool:
    """Check if a validation error can be sent to the fix pass instead of terminal error."""
    return any(pattern in error_msg for pattern in FIXABLE_PATTERNS)


def _validation_error_to_field(error_msg: str) -> str:
    """Map a validation error message to its corresponding result field name.

    This ensures build_fix_prompt() can include the relevant field values
    so the fix model has context to work with. Without this, the fix prompt
    shows an empty 'flagged_fields' object, preventing the model from fixing
    anything.

    Examples:
        "word_tags[5] word 'xxx' has no diacritics" → "word_tags"
        "word_analysis[5] word 'xxx' has no diacritics" → "word_analysis"
        "invalid topic: quran_commentary" → "topics"
        "invalid tag: bad_tag" → "tags"
        "invalid content_type: foo" → "content_type"
        "missing ambiguity_note" → "isnad_matn"
        "invalid narrator role: foo" → "isnad_matn"
        "invalid chunk_type: foo" → "chunks"
        "invalid diacritics_status: foo" → "diacritics_status"
        "invalid quran relationship: foo" → "related_quran"
        "invalid pos: foo" → "word_tags" (v4) or "word_analysis" (v3)
        "key_terms key 'en' is not an Arabic term" → "translations"
    """
    msg_lower = error_msg.lower()

    # Word-level fields — check explicit field name first
    if "word_tags" in msg_lower:
        return "word_tags"
    if "word_analysis" in msg_lower:
        return "word_analysis"

    # Diacritics (fallback for generic "has no diacritics" match)
    if "has no diacritics" in msg_lower:
        return "word_tags"  # v4 default; v3 handled above via "word_analysis"

    # POS errors (generic "invalid pos:" without explicit field name)
    if "invalid pos:" in msg_lower:
        return "word_tags"  # v4 default; v3 rare at this point

    # Narrator / isnad
    if any(p in msg_lower for p in ("ambiguity_note", "narrator role", "identity_confidence")):
        return "isnad_matn"

    # Topics / tags / content_type
    if "invalid topic:" in msg_lower:
        return "topics"
    if "invalid tag:" in msg_lower:
        return "tags"
    if "invalid content_type:" in msg_lower:
        return "content_type"

    # Chunks
    if "invalid chunk_type:" in msg_lower:
        return "chunks"

    # Diacritics status
    if "invalid diacritics_status:" in msg_lower:
        return "diacritics_status"

    # Quran references
    if "invalid quran relationship:" in msg_lower:
        return "related_quran"

    # Translations / key_terms
    if "key_terms key" in msg_lower:
        return "translations"

    # Fallback — no match; return "validation" so it appears in the fix context
    # as a note even if it can't be looked up in the result
    return "validation"


def postprocess_verse(
    plan: VersePlan,
    raw_response: str,
    word_dict_data: Optional[dict] = None,
    narrator_templates: Optional[dict] = None,
    responses_dir: Optional[str] = None,
    parsed_dict: Optional[dict] = None,
    registry: Optional["NarratorRegistry"] = None,
) -> VerseResult:
    """Parse AI response, validate, review, apply overrides, save.

    Args:
        plan: The VersePlan from prepare_verse.
        raw_response: Raw JSON string from Claude.
        word_dict_data: Loaded word dictionary (or None).
        narrator_templates: Loaded narrator templates (or None).
        responses_dir: Where to save the final response file.
        parsed_dict: Pre-parsed dict from --json-schema structured_output.
            If provided, skips JSON parsing/fence stripping/quote repair.
        registry: Optional NarratorRegistry for canonical_id resolution.

    Returns:
        VerseResult with status and details.
    """
    if responses_dir is None:
        responses_dir = AI_RESPONSES_DIR

    verse_result = VerseResult(
        verse_id=plan.verse_id,
        status="error",
        raw_response=raw_response,
    )

    # Use pre-parsed dict if available (from --json-schema structured_output),
    # otherwise parse raw string with defensive processing
    if parsed_dict is not None:
        result = parsed_dict
    else:
        try:
            cleaned = strip_code_fences(raw_response)
            cleaned = repair_json_quotes(cleaned)
            result = json.loads(cleaned)
        except json.JSONDecodeError as e:
            verse_result.error = f"JSON parse error: {e}"
            return verse_result

    # Handle v4 word_tags format — expand to minimal word_analysis for validation
    if "word_tags" in result and "word_analysis" not in result:
        result["word_analysis"] = [
            {"word": wt[0], "pos": wt[1]}
            for wt in result["word_tags"]
            if isinstance(wt, list) and len(wt) >= 2
        ]
        word_overrides = []
    else:
        # Expand compact word format if used (v3)
        if "word_analysis" in result:
            result["word_analysis"] = expand_compact_words(result["word_analysis"])

        # Apply word dictionary overrides (v3 only — v4 has no word translations)
        if "word_analysis" in result:
            first_entry = result["word_analysis"][0] if result["word_analysis"] else {}
            has_translations = isinstance(first_entry, dict) and "translation" in first_entry
            if has_translations:
                result["word_analysis"], word_overrides = override_known_words(
                    result["word_analysis"], word_dict_data
                )
            else:
                word_overrides = []
        else:
            word_overrides = []

    # Apply narrator overrides (including canonical_id resolution)
    result, narrator_overrides = override_narrators(result, narrator_templates, registry=registry)

    # Auto-normalize narrator positions to 1-based
    _normalize_narrator_positions(result)

    # Deterministic chunk boundary fix (zero-cost, before validation)
    chunk_fixes = fix_chunk_boundaries(result)
    if chunk_fixes:
        logger.info("CHUNK-FIX %s: %s", plan.verse_id, "; ".join(chunk_fixes))

    # Validate schema
    validation_errors = validate_result(result)

    # Auto-fix trivial errors (zero LLM cost)
    if validation_errors:
        auto_fixes = _auto_fix_validation_errors(result)
        if auto_fixes:
            logger.info("Auto-fixed %d issues: %s", len(auto_fixes), "; ".join(auto_fixes))
            # Re-validate after auto-fix
            validation_errors = validate_result(result)

    verse_result.validation_errors = validation_errors

    if validation_errors:
        # Check if remaining errors are fixable via LLM fix pass
        fixable = [e for e in validation_errors if _is_fixable_error(e)]
        unfixable = [e for e in validation_errors if not _is_fixable_error(e)]

        if unfixable:
            # Terminal errors that can't be fixed
            verse_result.error = f"{len(validation_errors)} validation errors ({len(unfixable)} unfixable)"
            verse_result.result_dict = result
            return verse_result
        else:
            # All remaining errors are fixable — route to fix pass
            logger.info("Routing %d fixable validation errors to fix pass: %s",
                        len(fixable), "; ".join(fixable))
            verse_result.status = "needs_fix"
            verse_result.result_dict = result
            # Store validation errors as synthetic warnings for fix pass.
            # Use _validation_error_to_field() so build_fix_prompt() can look up
            # the relevant result field — previously field="validation" caused an
            # empty flagged_fields context, preventing the model from fixing anything.
            from app.ai_pipeline_review import ReviewWarning
            for err in fixable:
                verse_result.warnings.append(ReviewWarning(
                    field=_validation_error_to_field(err),
                    category="validation_error",
                    severity="high",
                    message=err,
                    suggestion=f"Fix this validation error: {err}",
                ))
            # Save full result for fix pass (no stripping — kept in ThaqalaynDataSources)
            verse_result.result_dict = result
            _save_audit(plan, verse_result, word_overrides, narrator_overrides)
            return verse_result

    # Run quality review
    warnings = review_result(result, plan.request)
    verse_result.warnings = warnings

    high_medium = [w for w in warnings if w.severity in ("high", "medium")]

    if high_medium:
        verse_result.status = "needs_fix"
    else:
        verse_result.status = "pass"

    # Save full result (no stripping — kept in ThaqalaynDataSources;
    # stripping happens in the merger when writing to ThaqalaynData)
    verse_result.result_dict = result

    # Save response file
    if verse_result.status == "pass":
        _save_response(plan, stripped, responses_dir)

    # Save audit log
    _save_audit(plan, verse_result, word_overrides, narrator_overrides)

    return verse_result


def _save_response(plan: VersePlan, stripped_result: dict, responses_dir: str) -> None:
    """Save the final response wrapper to the responses directory."""
    os.makedirs(responses_dir, exist_ok=True)
    generation_method = "openai_api" if plan.backend == "openai" else "claude_cli_p"
    model_label = plan.model or "pipeline_v4"
    wrapper = {
        "verse_path": plan.verse_path,
        "ai_attribution": {
            "model": model_label,
            "generated_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "pipeline_version": PIPELINE_VERSION,
            "generation_method": generation_method,
        },
        "generation_attempts": 1,
        "result": stripped_result,
    }
    path = os.path.abspath(os.path.join(responses_dir, f"{plan.verse_id}.json"))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(wrapper, f, ensure_ascii=False, indent=2)
    logger.info("WROTE %s", path)


def _save_audit(plan: VersePlan, verse_result: VerseResult,
                word_overrides: list, narrator_overrides: list) -> None:
    """Save audit log to work_dir."""
    audit = {
        "verse_id": plan.verse_id,
        "status": verse_result.status,
        "validation_errors": verse_result.validation_errors,
        "warnings": [
            {"category": w.category, "severity": w.severity, "field": w.field, "message": w.message}
            for w in verse_result.warnings
        ],
        "word_overrides_count": len(word_overrides),
        "narrator_overrides_count": len(narrator_overrides),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    if word_overrides:
        audit["word_overrides"] = word_overrides[:20]  # cap for readability
    if narrator_overrides:
        audit["narrator_overrides"] = narrator_overrides

    audit_path = os.path.abspath(os.path.join(plan.work_dir, "audit.json"))
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)
    logger.info("WROTE %s", audit_path)


# ---------------------------------------------------------------------------
# Fix
# ---------------------------------------------------------------------------

def prepare_fix(plan: VersePlan, verse_result: VerseResult) -> Tuple[str, str]:
    """Build fix prompt from review warnings. Zero Claude tokens.

    Returns (system_prompt, user_message) for the fix call.
    """
    # Reconstruct full result for fix prompt
    result = verse_result.result_dict
    if result and "diacritized_text" not in result:
        result = reconstruct_fields(result)

    fix_prompt = build_fix_prompt(
        result=result,
        request=plan.request,
        warnings=verse_result.warnings,
    )

    system = "You are a specialist editor fixing specific issues in Islamic text analysis. Fix ONLY the flagged issues. Output a JSON object containing ONLY the corrected fields — do NOT output the full document or any unflagged fields."
    return system, fix_prompt


def _deep_merge(base: dict, patch: dict) -> dict:
    """Merge patch into base, recursively for nested dicts."""
    merged = dict(base)
    for k, v in patch.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def apply_fix(plan: VersePlan, fix_response: str,
              word_dict_data: Optional[dict] = None,
              narrator_templates: Optional[dict] = None,
              responses_dir: Optional[str] = None,
              original_result: Optional[dict] = None,
              registry: Optional["NarratorRegistry"] = None) -> VerseResult:
    """Apply fix response, re-validate, and save if clean.

    The fix response may be a complete result or just the corrected fields.
    If original_result is provided and the fix is partial, the corrections
    are merged into the original.

    Returns updated VerseResult.
    """
    if responses_dir is None:
        responses_dir = AI_RESPONSES_DIR

    verse_result = VerseResult(verse_id=plan.verse_id, status="error")

    try:
        cleaned = strip_code_fences(fix_response)
        cleaned = repair_json_quotes(cleaned)
        fix_data = json.loads(cleaned)
    except json.JSONDecodeError as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning("FIX %s: JSON parse error: %s | raw starts: %r",
                        plan.verse_id, e, fix_response[:100] if fix_response else "<empty>")
        verse_result.error = f"Fix JSON parse error: {e}"
        return verse_result

    # Determine if fix is partial or complete.
    # A complete result must contain ALL major required fields. Checking only for
    # content_type was fragile — if the fix model outputs content_type alongside
    # the corrected field (e.g. {"content_type": "narrative", "word_tags": [...]}),
    # the result was wrongly treated as complete and validation would fail on all
    # other missing required fields.
    _COMPLETE_RESULT_FIELDS = {"content_type", "translations", "chunks", "isnad_matn", "tags"}
    is_partial = (
        not all(f in fix_data for f in _COMPLETE_RESULT_FIELDS)
        and original_result is not None
    )
    if is_partial:
        # Merge partial corrections into original result
        result = _deep_merge(original_result, fix_data)
    else:
        result = fix_data

    # Handle v4 word_tags format in fix result
    if "word_tags" in result and "word_analysis" not in result:
        result["word_analysis"] = [
            {"word": wt[0], "pos": wt[1]}
            for wt in result["word_tags"]
            if isinstance(wt, list) and len(wt) >= 2
        ]
    elif "word_analysis" in result:
        # Expand compact format (v3)
        result["word_analysis"] = expand_compact_words(result["word_analysis"])
        # Apply word dictionary overrides (v3 only — v4 has no per-word translations)
        first_entry = result["word_analysis"][0] if result["word_analysis"] else {}
        has_translations = isinstance(first_entry, dict) and "translation" in first_entry
        if has_translations:
            result["word_analysis"], _ = override_known_words(result["word_analysis"], word_dict_data)
    result, _ = override_narrators(result, narrator_templates, registry=registry)

    # Auto-normalize narrator positions to 1-based
    _normalize_narrator_positions(result)

    # Deterministic chunk boundary fix (zero-cost, before validation)
    fix_chunk_boundaries(result)

    # Validate
    errors = validate_result(result)
    verse_result.validation_errors = errors
    if errors:
        verse_result.error = f"Fix still has {len(errors)} validation errors"
        verse_result.result_dict = result
        return verse_result

    # Review again
    warnings = review_result(result, plan.request)
    verse_result.warnings = warnings
    high_medium = [w for w in warnings if w.severity in ("high", "medium")]

    # Save full result (no stripping)
    verse_result.result_dict = result

    if high_medium:
        # If fix produced no actual changes, the fix model confirmed
        # the warnings are false positives — accept as pass
        if is_partial and not fix_data:
            verse_result.status = "pass"
            verse_result.false_positive_accepted = True
            _save_response(plan, result, responses_dir)
        else:
            verse_result.status = "needs_fix"  # still broken after fix
    else:
        verse_result.status = "pass"
        _save_response(plan, stripped, responses_dir)

    return verse_result
