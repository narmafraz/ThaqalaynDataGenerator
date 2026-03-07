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
    PipelineRequest,
    build_system_prompt,
    build_user_message,
    extract_pipeline_request,
    reconstruct_fields,
    strip_redundant_fields,
    validate_result,
)
from app.ai_pipeline_review import ReviewWarning, build_fix_prompt, review_result
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

3. Output the COMPLETE JSON in a single response. Do NOT split across messages."""


@dataclass
class VersePlan:
    """Output of prepare_verse — everything needed for a Claude call."""
    verse_path: str
    verse_id: str
    mode: str  # "single" or "chunked"
    request: PipelineRequest
    system_prompt: str
    user_message: str
    work_dir: str
    word_count: int = 0


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
) -> Optional[VersePlan]:
    """Extract verse data and build prompts. Zero Claude tokens.

    Args:
        verse_path: e.g. "/books/al-kafi:1:1:1:1"
        work_dir: Directory to write prompt files to
        data_dir: ThaqalaynData directory
        include_few_shot: Whether to include few-shot examples in system prompt
        use_compact_format: Whether to add compact array format instructions

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

    # Add compact word format instructions
    if use_compact_format:
        system_prompt += "\n" + COMPACT_WORD_INSTRUCTIONS

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
            "mode": "chunked" if word_count > 200 else "single",
            "include_few_shot": include_few_shot,
            "use_compact_format": use_compact_format,
            "prepared_at": datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)
    logger.info("WROTE %s", meta_path)

    return VersePlan(
        verse_path=verse_path,
        verse_id=verse_id,
        mode="chunked" if word_count > 200 else "single",
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


def override_narrators(result: dict, narrator_templates: Optional[dict]) -> Tuple[dict, List[dict]]:
    """Override narrator transliterations with template values for consistency.

    Returns (updated_result, list_of_overrides_applied).
    """
    if not narrator_templates or "narrators" not in narrator_templates:
        return result, []

    templates = narrator_templates["narrators"]
    overrides = []
    isnad_matn = result.get("isnad_matn", {})
    narrators = isnad_matn.get("narrators", [])

    for n in narrators:
        name_ar = n.get("name_ar", "").strip()
        if name_ar not in templates:
            continue
        tmpl = templates[name_ar]
        # Override English name if different
        if n.get("name_en") != tmpl["name_en"]:
            overrides.append({
                "name_ar": name_ar,
                "field": "name_en",
                "was": n.get("name_en"),
                "now": tmpl["name_en"],
            })
            n["name_en"] = tmpl["name_en"]

    return result, overrides


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


def postprocess_verse(
    plan: VersePlan,
    raw_response: str,
    word_dict_data: Optional[dict] = None,
    narrator_templates: Optional[dict] = None,
    responses_dir: Optional[str] = None,
    parsed_dict: Optional[dict] = None,
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

    # Expand compact word format if used
    if "word_analysis" in result:
        result["word_analysis"] = expand_compact_words(result["word_analysis"])

    # Apply word dictionary overrides
    if "word_analysis" in result:
        result["word_analysis"], word_overrides = override_known_words(
            result["word_analysis"], word_dict_data
        )
    else:
        word_overrides = []

    # Apply narrator overrides
    result, narrator_overrides = override_narrators(result, narrator_templates)

    # Auto-normalize narrator positions to 1-based
    _normalize_narrator_positions(result)

    # Validate schema
    validation_errors = validate_result(result)
    verse_result.validation_errors = validation_errors

    if validation_errors:
        verse_result.error = f"{len(validation_errors)} validation errors"
        verse_result.result_dict = result
        return verse_result

    # Run quality review
    warnings = review_result(result, plan.request)
    verse_result.warnings = warnings

    high_medium = [w for w in warnings if w.severity in ("high", "medium")]

    if high_medium:
        verse_result.status = "needs_fix"
    else:
        verse_result.status = "pass"

    # Strip redundant fields for storage
    stripped = strip_redundant_fields(result)
    verse_result.result_dict = stripped

    # Save response file
    if verse_result.status == "pass":
        _save_response(plan, stripped, responses_dir)

    # Save audit log
    _save_audit(plan, verse_result, word_overrides, narrator_overrides)

    return verse_result


def _save_response(plan: VersePlan, stripped_result: dict, responses_dir: str) -> None:
    """Save the final response wrapper to the responses directory."""
    os.makedirs(responses_dir, exist_ok=True)
    wrapper = {
        "verse_path": plan.verse_path,
        "ai_attribution": {
            "model": "pipeline_v3",
            "generated_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "pipeline_version": PIPELINE_VERSION,
            "generation_method": "claude_cli_p",
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

    system = "You are a specialist editor fixing specific issues in Islamic text analysis. Fix ONLY the flagged issues. Output the complete corrected JSON."
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
              original_result: Optional[dict] = None) -> VerseResult:
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

    # Determine if fix is partial or complete by checking for required fields
    is_partial = "content_type" not in fix_data and original_result is not None
    if is_partial:
        # Merge partial corrections into original result
        result = _deep_merge(original_result, fix_data)
    else:
        result = fix_data

    # Expand compact format
    if "word_analysis" in result:
        result["word_analysis"] = expand_compact_words(result["word_analysis"])

    # Apply overrides
    if "word_analysis" in result:
        result["word_analysis"], _ = override_known_words(result["word_analysis"], word_dict_data)
    result, _ = override_narrators(result, narrator_templates)

    # Auto-normalize narrator positions to 1-based
    _normalize_narrator_positions(result)

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

    stripped = strip_redundant_fields(result)
    verse_result.result_dict = stripped

    if high_medium:
        verse_result.status = "needs_fix"  # still broken after fix
    else:
        verse_result.status = "pass"
        _save_response(plan, stripped, responses_dir)

    return verse_result
