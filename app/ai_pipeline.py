"""AI content pipeline for comprehensive hadith/verse analysis and translation.

This module builds on the design in docs/AI_CONTENT_PIPELINE.md to generate
structured AI content for every verse/hadith in the Thaqalayn corpus:
- Translations in 10 languages
- Word-by-word Arabic analysis
- Diacritization (tashkeel)
- Thematic tags and classification
- Summaries, key terms, SEO questions
- Narrator extraction (isnad/matn separation)
- Related Quran references

The pipeline supports two modes:
1. "manual" mode: validates pre-generated JSON files (e.g., from Claude Code)
2. "api" mode: submits requests to the Anthropic Batch API (requires API key)

Usage:
    # Generate request JSONL files for sample verses
    python -m app.ai_pipeline generate-requests

    # Validate response files
    python -m app.ai_pipeline validate --dir ai-content/samples/responses/

    # Estimate cost for full corpus
    python -m app.ai_pipeline estimate
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from app.config import DEFAULT_DESTINATION_DIR, JSON_ENCODING, JSON_ENSURE_ASCII, JSON_INDENT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants — valid enum values (from AI_CONTENT_PIPELINE.md Section 3)
# ---------------------------------------------------------------------------

VALID_DIACRITICS_STATUS = {"added", "completed", "validated", "corrected"}

VALID_POS_TAGS = {
    "N", "V", "ADJ", "ADV", "PREP", "CONJ", "PRON", "DET",
    "PART", "INTJ", "REL", "DEM", "NEG", "COND", "INTERR",
}

VALID_TAGS = {
    "theology", "ethics", "jurisprudence", "worship", "quran_commentary",
    "prophetic_tradition", "family", "social_relations", "knowledge",
    "dua", "afterlife", "history", "economy", "governance",
}

VALID_HADITH_TYPES = {
    "legal_ruling", "ethical_teaching", "dua", "narrative",
    "prophetic_tradition", "quranic_commentary", "supplication",
    "creedal", "eschatological", "biographical",
}

VALID_LANGUAGE_KEYS = {"ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"}

VALID_QURAN_RELATIONSHIPS = {"explicit", "thematic"}

VALID_NARRATOR_ROLES = {"narrator", "companion", "imam", "author"}

VALID_IDENTITY_CONFIDENCE = {"definite", "likely", "ambiguous"}

# Pipeline defaults
DEFAULT_MODEL = "claude-opus-4-6-20260205"
DEFAULT_TEMPERATURE = 0.5
DEFAULT_MAX_TOKENS = 16000
PIPELINE_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PipelineRequest:
    """A single verse/hadith to be processed by the AI pipeline."""
    verse_path: str
    arabic_text: str
    english_text: str = ""
    book_name: str = ""
    chapter_title: str = ""
    hadith_number: Optional[int] = None
    existing_narrator_chain: Optional[str] = None


@dataclass
class PipelineConfig:
    """Configuration for the AI pipeline."""
    model: str = DEFAULT_MODEL
    temperature: float = DEFAULT_TEMPERATURE
    max_tokens: int = DEFAULT_MAX_TOKENS
    output_dir: str = "ai-content/samples"
    data_dir: str = DEFAULT_DESTINATION_DIR


# ---------------------------------------------------------------------------
# Data file loaders
# ---------------------------------------------------------------------------

def _data_dir() -> str:
    """Return the path to the ai_pipeline_data directory."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "ai_pipeline_data")


def load_glossary() -> dict:
    """Load the Islamic term glossary from ai_pipeline_data/glossary.json."""
    path = os.path.join(_data_dir(), "glossary.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_few_shot_examples() -> dict:
    """Load few-shot examples from ai_pipeline_data/few_shot_examples.json."""
    path = os.path.join(_data_dir(), "few_shot_examples.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_sample_verses() -> dict:
    """Load sample verse paths from ai_pipeline_data/sample_verses.json."""
    path = os.path.join(_data_dir(), "sample_verses.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

def _format_glossary_table(glossary: dict) -> str:
    """Format glossary terms as a compact table for the system prompt."""
    lines = ["Arabic | English | Urdu | Turkish | Farsi"]
    for term in glossary.get("terms", []):
        lines.append(
            f"{term['ar']} | {term['en']} | {term.get('ur', '')} | "
            f"{term.get('tr', '')} | {term.get('fa', '')}"
        )
    return "\n".join(lines)


def _format_few_shot_examples(examples_data: dict) -> str:
    """Format few-shot examples as input/output pairs for the system prompt."""
    parts = []
    for i, example in enumerate(examples_data.get("examples", []), 1):
        inp = example["input"]
        out = example["output"]
        parts.append(f"--- Example {i} ---")
        parts.append(f"Arabic text: {inp['arabic_text']}")
        if inp.get("english_text"):
            parts.append(f"English reference translation: {inp['english_text']}")
        parts.append(f"Book: {inp.get('book_name', '')}")
        parts.append(f"Chapter: {inp.get('chapter_title', '')}")
        if inp.get("hadith_number") is not None:
            parts.append(f"Hadith number: {inp['hadith_number']}")
        chain = inp.get("existing_narrator_chain")
        parts.append(f"Existing narrator chain: {chain if chain else 'null'}")
        parts.append("")
        parts.append("Expected output:")
        parts.append(json.dumps(out, ensure_ascii=False, indent=2))
        parts.append("")
    return "\n".join(parts)


def build_system_prompt(glossary: Optional[dict] = None,
                        few_shot_examples: Optional[dict] = None) -> str:
    """Build the full system prompt per AI_CONTENT_PIPELINE.md Sections 3 + 14.

    Args:
        glossary: Loaded glossary dict. If None, loads from file.
        few_shot_examples: Loaded examples dict. If None, loads from file.

    Returns:
        Complete system prompt string.
    """
    if glossary is None:
        glossary = load_glossary()
    if few_shot_examples is None:
        few_shot_examples = load_few_shot_examples()

    glossary_table = _format_glossary_table(glossary)
    examples_text = _format_few_shot_examples(few_shot_examples)
    num_examples = len(few_shot_examples.get("examples", []))

    prompt = f"""You are a specialist in Shia Islamic scholarly texts. You are translating and analyzing hadith from the Four Books and other primary Shia sources.

IMPORTANT RULES:
- Preserve all honorifics: عليه السلام (peace be upon him), صلى الله عليه وآله وسلم, etc.
- Use established Islamic terminology (do not translate terms like "wudu", "salat", "zakat" unless the target language has established equivalents — see GLOSSARY below)
- Be faithful to Shia scholarly tradition in interpretation
- Do not add commentary or interpretation — translate faithfully
- When quoting Quran, reproduce the exact text — never paraphrase scripture
- In the narrator chain (isnad), treat narrator names as proper nouns — transliterate consistently, do not translate names into the target language
- The body text (matn) is the substantive content — translate this faithfully
- For ALL enum fields, use ONLY the exact values listed. Do not invent new values.
- This text is in classical Arabic (fusha qadima). Note that vocabulary and syntax differ from Modern Standard Arabic.
- Output valid JSON only

GLOSSARY OF ISLAMIC TERMS:
{glossary_table}

EXAMPLES:
Below are {num_examples} examples showing the expected input and output format.
{examples_text}"""

    return prompt


def build_user_message(request: PipelineRequest) -> str:
    """Build the user message for a single verse/hadith.

    Args:
        request: PipelineRequest with verse data.

    Returns:
        User message string containing the verse data and output schema instructions.
    """
    parts = [f"Arabic text: {request.arabic_text}"]

    if request.english_text:
        parts.append(f"English reference translation: {request.english_text}")

    parts.append(f"Book: {request.book_name}")
    parts.append(f"Chapter: {request.chapter_title}")

    if request.hadith_number is not None:
        parts.append(f"Hadith number: {request.hadith_number}")

    chain = request.existing_narrator_chain
    parts.append(f"Existing narrator chain (if available): {chain if chain else 'null'}")

    parts.append("")
    parts.append("""Generate a single JSON object with these fields:

1. "diacritized_text": (string) The full Arabic text with complete tashkeel.
2. "diacritics_status": (enum) "added" | "completed" | "validated" | "corrected"
3. "diacritics_changes": (array) Corrections made. Empty [] if status is "added" or "validated".
4. "word_analysis": (array) One entry per Arabic word:
   {"word": "...", "translation_en": "...", "root": "...", "pos": (enum N|V|ADJ|ADV|PREP|CONJ|PRON|DET|PART|INTJ|REL|DEM|NEG|COND|INTERR), "is_proper_noun": boolean}
5. "tags": (array of 2-5 enums) theology|ethics|jurisprudence|worship|quran_commentary|prophetic_tradition|family|social_relations|knowledge|dua|afterlife|history|economy|governance
6. "hadith_type": (enum) legal_ruling|ethical_teaching|dua|narrative|prophetic_tradition|quranic_commentary|supplication|creedal|eschatological|biographical
7. "related_quran": (array) [{"ref": "surah:ayah", "relationship": "explicit"|"thematic"}] or []
8. "isnad_matn": {"isnad_ar": "...", "matn_ar": "...", "has_chain": boolean, "narrators": [...]}
   Each narrator: {"name_ar": "...", "name_en": "...", "role": "narrator"|"companion"|"imam"|"author", "position": int, "identity_confidence": "definite"|"likely"|"ambiguous", "ambiguity_note": string|null, "known_identity": string|null}
9. "translations": Object with keys ur, tr, fa, id, bn, es, fr, de, ru, zh. Each:
   {"text": "...", "summary": "...", "key_terms": {"arabic_term": "explanation"}, "seo_question": "..."}""")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Verse data extraction
# ---------------------------------------------------------------------------

def extract_pipeline_request(verse_path: str,
                             data_dir: Optional[str] = None) -> Optional[PipelineRequest]:
    """Load a verse from ThaqalaynData and build a PipelineRequest.

    Handles both chapter-level paths (loads chapter, finds verse) and
    direct verse paths.

    Args:
        verse_path: Path like "/books/al-kafi:1:1:1:1" or "/books/quran:1:1"
        data_dir: Base data directory. Defaults to DEFAULT_DESTINATION_DIR.

    Returns:
        PipelineRequest or None if verse not found.
    """
    if data_dir is None:
        data_dir = DEFAULT_DESTINATION_DIR

    # Determine if this is a verse path or chapter path
    # Verse paths have more segments than chapter paths
    path_parts = verse_path.replace("/books/", "").split(":")
    book_name = path_parts[0]

    # Try loading as a verse within a chapter
    # For hadith: /books/al-kafi:1:1:1:1 -> chapter is /books/al-kafi:1:1:1
    # For quran:  /books/quran:1:1 -> chapter is /books/quran:1
    chapter_path = ":".join(verse_path.rsplit(":", 1)[:-1]) if ":" in verse_path else verse_path
    verse_index_str = verse_path.rsplit(":", 1)[-1] if ":" in verse_path else None

    # Convert path to filesystem path
    sanitised = chapter_path.replace(":", "/")
    if sanitised.startswith("/"):
        sanitised = sanitised[1:]
    chapter_file = os.path.join(data_dir, sanitised + ".json")

    if not os.path.exists(chapter_file):
        logger.warning("Chapter file not found: %s", chapter_file)
        return None

    with open(chapter_file, "r", encoding="utf-8") as f:
        chapter_data = json.load(f)

    data = chapter_data.get("data", chapter_data)
    verses = data.get("verses", [])
    titles = data.get("titles", {})
    chapter_title = titles.get("en", titles.get("ar", ""))

    # Find the specific verse
    target_verse = None
    if verse_index_str and verse_index_str.isdigit():
        verse_index = int(verse_index_str)
        for v in verses:
            if v.get("path") == verse_path or v.get("local_index") == verse_index:
                target_verse = v
                break

    if target_verse is None and verses:
        # If no specific verse index, use first verse (for chapter-level paths)
        target_verse = verses[0]

    if target_verse is None:
        logger.warning("No verse found at path: %s", verse_path)
        return None

    # Extract text
    arabic_lines = target_verse.get("text", [])
    arabic_text = "\n".join(arabic_lines) if arabic_lines else ""

    translations = target_verse.get("translations", {})
    english_text = ""
    for tid, lines in translations.items():
        if tid.startswith("en."):
            english_text = "\n".join(lines) if isinstance(lines, list) else str(lines)
            break

    # Extract narrator chain if present
    narrator_chain = None
    nc = target_verse.get("narrator_chain")
    if nc:
        chain_parts = nc.get("parts", [])
        if chain_parts:
            narrator_chain = "".join(p.get("text", "") for p in chain_parts)
        elif nc.get("text"):
            narrator_chain = nc["text"]

    hadith_number = target_verse.get("local_index")

    return PipelineRequest(
        verse_path=target_verse.get("path", verse_path),
        arabic_text=arabic_text,
        english_text=english_text,
        book_name=book_name,
        chapter_title=chapter_title,
        hadith_number=hadith_number,
        existing_narrator_chain=narrator_chain,
    )


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_response(response_text: str) -> dict:
    """Extract JSON from an AI response, handling markdown code blocks.

    Args:
        response_text: Raw response text from the AI model.

    Returns:
        Parsed dict.

    Raises:
        ValueError: If no valid JSON can be extracted.
    """
    text = response_text.strip()

    # Try direct JSON parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try extracting from markdown code block
    if "```" in text:
        # Find the JSON block
        start = text.find("```json")
        if start != -1:
            start = text.find("\n", start) + 1
        else:
            start = text.find("```")
            start = text.find("\n", start) + 1

        end = text.find("```", start)
        if end != -1:
            json_str = text[start:end].strip()
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass

    # Try finding first { and last }
    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        json_str = text[first_brace:last_brace + 1]
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not extract valid JSON from response: {text[:200]}...")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_result(result: dict) -> List[str]:
    """Validate a pipeline output against the schema and enum constraints.

    Args:
        result: Parsed JSON dict from the AI pipeline.

    Returns:
        List of error strings. Empty list means validation passed.
    """
    errors = []

    # --- Required top-level fields ---
    required_fields = [
        "diacritized_text", "diacritics_status", "diacritics_changes",
        "word_analysis", "tags", "hadith_type", "related_quran",
        "isnad_matn", "translations",
    ]
    for field_name in required_fields:
        if field_name not in result:
            errors.append(f"missing required field: {field_name}")

    # --- diacritized_text ---
    if "diacritized_text" in result:
        if not isinstance(result["diacritized_text"], str):
            errors.append(f"diacritized_text must be string, got {type(result['diacritized_text']).__name__}")

    # --- diacritics_status ---
    if "diacritics_status" in result:
        if result["diacritics_status"] not in VALID_DIACRITICS_STATUS:
            errors.append(f"invalid diacritics_status: {result['diacritics_status']}")

    # --- diacritics_changes ---
    if "diacritics_changes" in result:
        if not isinstance(result["diacritics_changes"], list):
            errors.append(f"diacritics_changes must be array, got {type(result['diacritics_changes']).__name__}")

    # --- word_analysis ---
    if "word_analysis" in result:
        if not isinstance(result["word_analysis"], list):
            errors.append(f"word_analysis must be array, got {type(result['word_analysis']).__name__}")
        else:
            for i, word in enumerate(result["word_analysis"]):
                if not isinstance(word, dict):
                    errors.append(f"word_analysis[{i}] must be object")
                    continue
                for wf in ("word", "translation_en", "root", "pos"):
                    if wf not in word:
                        errors.append(f"word_analysis[{i}] missing field: {wf}")
                if word.get("pos") not in VALID_POS_TAGS:
                    errors.append(f"invalid pos: {word.get('pos')} for word {word.get('word', '?')}")
                if "is_proper_noun" in word and not isinstance(word["is_proper_noun"], bool):
                    errors.append(f"is_proper_noun must be boolean for word {word.get('word', '?')}")

    # --- tags ---
    if "tags" in result:
        if not isinstance(result["tags"], list):
            errors.append(f"tags must be array, got {type(result['tags']).__name__}")
        else:
            if len(result["tags"]) < 2 or len(result["tags"]) > 5:
                errors.append(f"tags must have 2-5 items, got {len(result['tags'])}")
            for tag in result["tags"]:
                if tag not in VALID_TAGS:
                    errors.append(f"invalid tag: {tag}")

    # --- hadith_type ---
    if "hadith_type" in result:
        if result["hadith_type"] not in VALID_HADITH_TYPES:
            errors.append(f"invalid hadith_type: {result['hadith_type']}")

    # --- related_quran ---
    if "related_quran" in result:
        if not isinstance(result["related_quran"], list):
            errors.append(f"related_quran must be array, got {type(result['related_quran']).__name__}")
        else:
            for ref_obj in result["related_quran"]:
                if not isinstance(ref_obj, dict):
                    errors.append("related_quran entry must be object")
                    continue
                if ref_obj.get("relationship") not in VALID_QURAN_RELATIONSHIPS:
                    errors.append(f"invalid quran relationship: {ref_obj.get('relationship')} for ref {ref_obj.get('ref')}")
                ref = ref_obj.get("ref", "")
                parts = ref.split(":")
                if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                    errors.append(f"invalid quran ref format: {ref}")
                elif not (1 <= int(parts[0]) <= 114):
                    errors.append(f"invalid surah number: {parts[0]} in ref {ref}")

    # --- isnad_matn ---
    if "isnad_matn" in result:
        isnad = result["isnad_matn"]
        if not isinstance(isnad, dict):
            errors.append(f"isnad_matn must be object, got {type(isnad).__name__}")
        else:
            if not isinstance(isnad.get("has_chain"), bool):
                errors.append(f"invalid has_chain: {isnad.get('has_chain')}")
            if "isnad_ar" not in isnad:
                errors.append("isnad_matn missing isnad_ar")
            if "matn_ar" not in isnad:
                errors.append("isnad_matn missing matn_ar")
            if isnad.get("has_chain"):
                if not isnad.get("isnad_ar"):
                    errors.append("has_chain is true but isnad_ar is empty")
                if not isnad.get("narrators"):
                    errors.append("has_chain is true but narrators is empty")
            for i, narrator in enumerate(isnad.get("narrators", [])):
                if not isinstance(narrator, dict):
                    errors.append(f"narrator[{i}] must be object")
                    continue
                if narrator.get("role") not in VALID_NARRATOR_ROLES:
                    errors.append(f"invalid narrator role: {narrator.get('role')} at position {i+1}")
                if narrator.get("identity_confidence") not in VALID_IDENTITY_CONFIDENCE:
                    errors.append(
                        f"invalid identity_confidence: {narrator.get('identity_confidence')} "
                        f"for {narrator.get('name_en', '?')}"
                    )
                if narrator.get("identity_confidence") in ("likely", "ambiguous") and not narrator.get("ambiguity_note"):
                    errors.append(
                        f"missing ambiguity_note for {narrator.get('name_en', '?')} "
                        f"with confidence {narrator.get('identity_confidence')}"
                    )
                if narrator.get("position") != i + 1:
                    errors.append(f"narrator position mismatch: expected {i+1}, got {narrator.get('position')}")

    # --- translations ---
    if "translations" in result:
        if not isinstance(result["translations"], dict):
            errors.append(f"translations must be object, got {type(result['translations']).__name__}")
        else:
            for key in result["translations"]:
                if key not in VALID_LANGUAGE_KEYS:
                    errors.append(f"invalid language key: {key}")
            missing_langs = VALID_LANGUAGE_KEYS - set(result["translations"].keys())
            if missing_langs:
                errors.append(f"missing languages: {sorted(missing_langs)}")
            for lang_key, lang_data in result["translations"].items():
                if lang_key not in VALID_LANGUAGE_KEYS:
                    continue
                if not isinstance(lang_data, dict):
                    errors.append(f"translations.{lang_key} must be object")
                    continue
                for tf in ("text", "summary", "key_terms", "seo_question"):
                    if tf not in lang_data:
                        errors.append(f"translations.{lang_key} missing field: {tf}")

    return errors


# ---------------------------------------------------------------------------
# Request generation
# ---------------------------------------------------------------------------

def generate_sample_requests(data_dir: Optional[str] = None) -> List[PipelineRequest]:
    """Create PipelineRequest objects for all sample verses.

    Args:
        data_dir: Base data directory for loading verse JSON files.

    Returns:
        List of PipelineRequest objects (only for verses that exist on disk).
    """
    sample_data = load_sample_verses()
    requests = []

    for entry in sample_data.get("verses", []):
        path = entry["path"]
        req = extract_pipeline_request(path, data_dir=data_dir)
        if req:
            requests.append(req)
        else:
            logger.warning("Skipping %s: verse not found in data", path)

    logger.info("Generated %d pipeline requests from %d sample paths",
                len(requests), len(sample_data.get("verses", [])))
    return requests


def write_request_jsonl(requests: List[PipelineRequest],
                        output_path: str,
                        config: Optional[PipelineConfig] = None) -> None:
    """Write pipeline requests as JSONL for the Anthropic Batch API.

    Each line is a valid JSON object matching the Batch API format with
    the full system prompt and user message.

    Args:
        requests: List of PipelineRequest objects.
        output_path: Path to write the JSONL file.
        config: Pipeline configuration. Uses defaults if None.
    """
    if config is None:
        config = PipelineConfig()

    glossary = load_glossary()
    few_shot = load_few_shot_examples()
    system_prompt = build_system_prompt(glossary, few_shot)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", encoding=JSON_ENCODING) as f:
        for req in requests:
            user_msg = build_user_message(req)
            custom_id = req.verse_path.replace("/books/", "")
            batch_entry = {
                "custom_id": custom_id,
                "params": {
                    "model": config.model,
                    "max_tokens": config.max_tokens,
                    "temperature": config.temperature,
                    "system": system_prompt,
                    "messages": [
                        {"role": "user", "content": user_msg}
                    ],
                },
            }
            json.dump(batch_entry, f, ensure_ascii=JSON_ENSURE_ASCII)
            f.write("\n")

    logger.info("Wrote %d requests to %s", len(requests), output_path)


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------

def estimate_cost(num_verses: int = 46857) -> dict:
    """Estimate the total pipeline cost for a given number of verses.

    Uses the multi-language approach from AI_CONTENT_PIPELINE.md Section 10.

    Args:
        num_verses: Total verses to process.

    Returns:
        Cost breakdown dict.
    """
    # Generation (Opus 4.6 Batch)
    gen_input_per_req = 3150
    gen_output_per_req = 4400
    gen_total_input = num_verses * gen_input_per_req
    gen_total_output = num_verses * gen_output_per_req
    gen_input_cost = (gen_total_input / 1_000_000) * 2.50
    gen_output_cost = (gen_total_output / 1_000_000) * 12.50
    gen_total = gen_input_cost + gen_output_cost

    # Validation (Sonnet 4.6 Batch, per-language)
    val_requests = num_verses * 10
    val_input_per_req = 600
    val_output_per_req = 100
    val_total_input = val_requests * val_input_per_req
    val_total_output = val_requests * val_output_per_req
    val_input_cost = (val_total_input / 1_000_000) * 1.50
    val_output_cost = (val_total_output / 1_000_000) * 7.50
    val_total = val_input_cost + val_output_cost

    # Regeneration (~5% failure rate)
    regen_verses = int(num_verses * 0.05)
    regen_total = regen_verses * ((gen_input_per_req / 1_000_000) * 2.50 +
                                   (gen_output_per_req / 1_000_000) * 12.50)

    # Back-translation (1% sample)
    bt_cost = 50.0

    total = gen_total + val_total + regen_total + bt_cost

    return {
        "num_verses": num_verses,
        "generation": {
            "requests": num_verses,
            "input_tokens": gen_total_input,
            "output_tokens": gen_total_output,
            "cost_usd": round(gen_total, 2),
        },
        "validation": {
            "requests": val_requests,
            "input_tokens": val_total_input,
            "output_tokens": val_total_output,
            "cost_usd": round(val_total, 2),
        },
        "regeneration": {
            "verses": regen_verses,
            "cost_usd": round(regen_total, 2),
        },
        "back_translation": {
            "cost_usd": bt_cost,
        },
        "total_cost_usd": round(total, 2),
    }


# ---------------------------------------------------------------------------
# Validation runner
# ---------------------------------------------------------------------------

def validate_directory(dir_path: str) -> dict:
    """Validate all response JSON files in a directory.

    Args:
        dir_path: Directory containing response JSON files.

    Returns:
        Validation report dict.
    """
    report = {
        "total": 0,
        "passed": 0,
        "failed": 0,
        "errors_by_file": {},
    }

    if not os.path.isdir(dir_path):
        logger.error("Directory not found: %s", dir_path)
        return report

    for filename in sorted(os.listdir(dir_path)):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(dir_path, filename)
        report["total"] += 1

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            report["failed"] += 1
            report["errors_by_file"][filename] = [f"file read error: {e}"]
            continue

        # If the file has a wrapper with verse_path + result, extract result
        result = data.get("result", data)

        errors = validate_result(result)
        if errors:
            report["failed"] += 1
            report["errors_by_file"][filename] = errors
        else:
            report["passed"] += 1

    return report


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    """CLI entry point for the AI content pipeline."""
    parser = argparse.ArgumentParser(
        prog="ai_pipeline",
        description="AI content pipeline for Thaqalayn scripture analysis"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # generate-requests
    gen_parser = subparsers.add_parser(
        "generate-requests",
        help="Generate batch request JSONL for sample verses"
    )
    gen_parser.add_argument(
        "--output", default="ai-content/samples/requests/sample_requests.jsonl",
        help="Output JSONL file path"
    )
    gen_parser.add_argument(
        "--data-dir", default=DEFAULT_DESTINATION_DIR,
        help="ThaqalaynData directory"
    )

    # validate
    val_parser = subparsers.add_parser(
        "validate",
        help="Validate AI response files"
    )
    val_parser.add_argument(
        "--dir", default="ai-content/samples/responses/",
        help="Directory containing response JSON files"
    )

    # estimate
    est_parser = subparsers.add_parser(
        "estimate",
        help="Estimate pipeline cost for full corpus"
    )
    est_parser.add_argument(
        "--verses", type=int, default=46857,
        help="Number of verses to estimate for"
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "generate-requests":
        requests = generate_sample_requests(data_dir=args.data_dir)
        if not requests:
            logger.error("No requests generated. Check that ThaqalaynData exists.")
            sys.exit(1)
        write_request_jsonl(requests, args.output)
        print(f"Generated {len(requests)} requests -> {args.output}")

    elif args.command == "validate":
        report = validate_directory(args.dir)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        if report["failed"] > 0:
            sys.exit(1)

    elif args.command == "estimate":
        cost = estimate_cost(args.verses)
        print(json.dumps(cost, indent=2))


if __name__ == "__main__":
    main()
