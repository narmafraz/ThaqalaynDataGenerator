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
import xml.etree.ElementTree
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from app.config import AI_CONTENT_DIR, AI_CONTENT_SUBDIR, AI_PIPELINE_DATA_DIR, AI_RESPONSES_DIR, DEFAULT_DESTINATION_DIR, JSON_ENCODING, JSON_ENSURE_ASCII, JSON_INDENT, SOURCE_DATA_DIR

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

VALID_CONTENT_TYPES = {
    "legal_ruling", "ethical_teaching", "narrative",
    "prophetic_tradition", "quranic_commentary", "supplication",
    "creedal", "eschatological", "biographical", "theological",
    "exhortation", "cosmological",
}

VALID_LANGUAGE_KEYS = {"en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"}

VALID_QURAN_RELATIONSHIPS = {"explicit", "thematic"}

VALID_NARRATOR_ROLES = {"narrator", "companion", "imam", "author"}

VALID_IDENTITY_CONFIDENCE = {"definite", "likely", "ambiguous"}

VALID_CHUNK_TYPES = {"isnad", "opening", "body", "quran_quote", "closing"}

VALID_PHRASE_CATEGORIES = {
    "theological_concept", "well_known_saying", "jurisprudential_term",
    "quranic_echo", "prophetic_formula",
}

# Pipeline defaults
DEFAULT_MODEL = "claude-opus-4-6-20260205"
DEFAULT_TEMPERATURE = 0.5
DEFAULT_MAX_TOKENS = 16000
PIPELINE_VERSION = "2.0.0"

# VALID_TOPICS is loaded dynamically from topic_taxonomy.json at module init.
# It is a set of all Level 2 topic keys across all Level 1 categories.
VALID_TOPICS: set = set()

# QURAN_SURAH_AYAH_COUNTS maps surah number -> max ayah count.
# Loaded lazily from quran-data.xml at module init. If the XML is missing,
# ayah-level validation is silently skipped (surah range 1-114 still enforced).
QURAN_SURAH_AYAH_COUNTS: Dict[int, int] = {}

# Maximum number of generation attempts before quarantining a verse.
MAX_GENERATION_ATTEMPTS = 3


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
    output_dir: str = os.path.join(AI_CONTENT_DIR, "samples")
    data_dir: str = DEFAULT_DESTINATION_DIR


# ---------------------------------------------------------------------------
# Data file loaders
# ---------------------------------------------------------------------------

def _data_dir() -> str:
    """Return the path to the ai-pipeline-data directory."""
    return AI_PIPELINE_DATA_DIR


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


def load_word_dictionary() -> Optional[dict]:
    """Load the word-level translation dictionary from ai_pipeline_data/word_dictionary.json.

    Returns None if the file does not exist (optional resource).
    """
    path = os.path.join(_data_dir(), "word_dictionary.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_topic_taxonomy() -> Optional[dict]:
    """Load the two-level topic taxonomy from ai_pipeline_data/topic_taxonomy.json.

    Returns None if the file does not exist (optional resource).
    """
    path = os.path.join(_data_dir(), "topic_taxonomy.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_key_phrases_dictionary() -> Optional[dict]:
    """Load the key phrases seed dictionary from ai_pipeline_data/key_phrases_dictionary.json.

    Returns None if the file does not exist (optional resource).
    """
    path = os.path.join(_data_dir(), "key_phrases_dictionary.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _extract_valid_topics(taxonomy: Optional[dict] = None) -> set:
    """Extract all valid Level 2 topic keys from the taxonomy.

    Args:
        taxonomy: Loaded taxonomy dict. If None, loads from file.

    Returns:
        Set of valid topic key strings.
    """
    if taxonomy is None:
        taxonomy = load_topic_taxonomy()
    if taxonomy is None:
        return set()
    topics = set()
    for _category_key, category_data in taxonomy.get("taxonomy", {}).items():
        for topic_key in category_data.get("topics", {}):
            topics.add(topic_key)
    return topics


def _load_quran_ayah_counts() -> Dict[int, int]:
    """Parse quran-data.xml and return {surah_number: ayah_count}.

    Returns empty dict if the XML file is missing (validation silently skips
    ayah-level checks in that case).
    """
    xml_path = os.path.join(SOURCE_DATA_DIR, "scraped", "tanzil_net", "quran-data.xml")
    if not os.path.exists(xml_path):
        return {}
    try:
        tree = xml.etree.ElementTree.parse(xml_path)
        root = tree.getroot()
        counts: Dict[int, int] = {}
        for sura in root.iter("sura"):
            idx = sura.get("index")
            ayas = sura.get("ayas")
            if idx and ayas:
                counts[int(idx)] = int(ayas)
        return counts
    except Exception:
        logger.warning("Failed to parse quran-data.xml for ayah counts")
        return {}


def _init_module_data() -> None:
    """Initialize module-level data at import time."""
    global VALID_TOPICS, QURAN_SURAH_AYAH_COUNTS
    VALID_TOPICS = _extract_valid_topics()
    QURAN_SURAH_AYAH_COUNTS = _load_quran_ayah_counts()


# Initialize module data at load time
_init_module_data()


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


def _format_word_dictionary(word_dict: dict) -> str:
    """Format the word dictionary as a compact table for the system prompt."""
    lines = ["Diacritized | POS | EN | UR | TR | FA | Notes"]
    for entry in word_dict.get("words", []):
        lines.append(
            "{} | {} | {} | {} | {} | {} | {}".format(
                entry.get("diacritized", ""),
                entry.get("pos", ""),
                entry.get("en", ""),
                entry.get("ur", ""),
                entry.get("tr", ""),
                entry.get("fa", ""),
                entry.get("notes", ""),
            )
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


def _format_topic_taxonomy(taxonomy: dict) -> str:
    """Format the topic taxonomy as a compact reference for the system prompt."""
    lines = ["Level 1 Tag | Level 2 Topic Key | English Label"]
    for category_key, category_data in taxonomy.get("taxonomy", {}).items():
        for topic_key, topic_data in category_data.get("topics", {}).items():
            lines.append(f"{category_key} | {topic_key} | {topic_data.get('en', '')}")
    return "\n".join(lines)


def _format_key_phrases_sample(phrases_dict: dict, max_entries: int = 30) -> str:
    """Format a sample of key phrases for the system prompt (not the full dictionary)."""
    phrases = phrases_dict.get("phrases", [])
    lines = ["Arabic Phrase | English | Category"]
    for entry in phrases[:max_entries]:
        lines.append(
            "{} | {} | {}".format(
                entry.get("phrase_ar", ""),
                entry.get("phrase_en", ""),
                entry.get("category", ""),
            )
        )
    if len(phrases) > max_entries:
        lines.append(f"... and {len(phrases) - max_entries} more entries")
    return "\n".join(lines)


def build_system_prompt(glossary: Optional[dict] = None,
                        few_shot_examples: Optional[dict] = None,
                        word_dictionary: Optional[dict] = None,
                        topic_taxonomy: Optional[dict] = None,
                        key_phrases_dict: Optional[dict] = None) -> str:
    """Build the full system prompt per AI_CONTENT_PIPELINE.md Sections 3 + 14.

    Args:
        glossary: Loaded glossary dict. If None, loads from file.
        few_shot_examples: Loaded examples dict. If None, loads from file.
        word_dictionary: Loaded word dictionary dict. If None, loads from file
            (returns None if file doesn't exist, in which case section is omitted).
        topic_taxonomy: Loaded taxonomy dict. If None, loads from file.
        key_phrases_dict: Loaded key phrases dict. If None, loads from file.

    Returns:
        Complete system prompt string.
    """
    if glossary is None:
        glossary = load_glossary()
    if few_shot_examples is None:
        few_shot_examples = load_few_shot_examples()
    if word_dictionary is None:
        word_dictionary = load_word_dictionary()
    if topic_taxonomy is None:
        topic_taxonomy = load_topic_taxonomy()
    if key_phrases_dict is None:
        key_phrases_dict = load_key_phrases_dictionary()

    glossary_table = _format_glossary_table(glossary)
    examples_text = _format_few_shot_examples(few_shot_examples)
    num_examples = len(few_shot_examples.get("examples", []))

    word_dict_section = ""
    if word_dictionary and word_dictionary.get("words"):
        word_dict_table = _format_word_dictionary(word_dictionary)
        word_dict_section = f"""

COMMON WORD TRANSLATIONS:
Use these canonical translations for high-frequency grammatical words in word_analysis.
Only deviate when context clearly requires a different meaning (see Notes column).
{word_dict_table}"""

    taxonomy_section = ""
    if topic_taxonomy and topic_taxonomy.get("taxonomy"):
        taxonomy_table = _format_topic_taxonomy(topic_taxonomy)
        taxonomy_section = f"""

TOPIC TAXONOMY (for field #11 "topics"):
Assign 1-3 Level 2 topic keys from this controlled vocabulary. Use ONLY the topic keys listed below.
{taxonomy_table}"""

    phrases_section = ""
    if key_phrases_dict and key_phrases_dict.get("phrases"):
        phrases_table = _format_key_phrases_sample(key_phrases_dict)
        phrases_section = f"""

KEY PHRASES REFERENCE (for field #12 "key_phrases"):
Below are common Islamic multi-word expressions. When these phrases appear in the text, include them in key_phrases. You may also extract NEW phrases not in this list — the list is a seed, not exhaustive.
{phrases_table}"""

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
{glossary_table}{word_dict_section}{taxonomy_section}{phrases_section}

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
   {"word": "...", "translation": {"en": "...", "ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}, "pos": (enum N|V|ADJ|ADV|PREP|CONJ|PRON|DET|PART|INTJ|REL|DEM|NEG|COND|INTERR)}
   IMPORTANT: The "word" field must contain the fully diacritized form of the word (with complete tashkeel), matching the corresponding word in "diacritized_text". This is critical because the same consonantal skeleton can represent different words with different meanings (e.g. عَلِمَ "he knew" vs عَلَّمَ "he taught"), and diacritics are needed to distinguish them.
   The "translation" object must have all 11 language keys with context-appropriate translations for the word as used in this specific verse/hadith.
5. "tags": (array of 2-5 enums) theology|ethics|jurisprudence|worship|quran_commentary|prophetic_tradition|family|social_relations|knowledge|dua|afterlife|history|economy|governance
6. "content_type": (enum) legal_ruling|ethical_teaching|narrative|prophetic_tradition|quranic_commentary|supplication|creedal|eschatological|biographical|theological|exhortation|cosmological
   - "theological": Verses/hadith about God's attributes, names, or nature (e.g. Quran 112:1, Ayat al-Kursi)
   - "creedal": Core doctrinal statements of faith (shahada, pillars of belief, imamate)
   - "ethical_teaching": Moral guidance, virtues, vices
   - "legal_ruling": Jurisprudential rulings (fiqh, halal/haram)
   - "narrative": Historical accounts, stories of prophets, events
   - "prophetic_tradition": Sayings/actions attributed to the Prophet
   - "quranic_commentary": Explanations or commentary on Quran verses
   - "supplication": Du'a, prayers, invocations
   - "eschatological": Day of Judgment, afterlife, signs of the end
   - "biographical": About specific people, their qualities, or lineage
   - "exhortation": Advice, warnings, encouragement to action (e.g. letters, sermons)
   - "cosmological": Creation narratives, nature of the universe, jinn, angels
7. "related_quran": (array) [{"ref": "surah:ayah", "relationship": "explicit"|"thematic", "word_start": int (optional), "word_end": int (optional)}] or []
   For "explicit" references where the Quran verse is mentioned or cited in the text, include optional "word_start" and "word_end" (half-open indexing into word_analysis) marking where the reference/citation appears. This enables UI highlighting of Quran references. Omit for "thematic" references.
8. "isnad_matn": {"isnad_ar": "...", "matn_ar": "...", "has_chain": boolean, "narrators": [...]}
   Each narrator: {"name_ar": "...", "name_en": "...", "role": "narrator"|"companion"|"imam"|"author", "position": int, "identity_confidence": "definite"|"likely"|"ambiguous", "ambiguity_note": string|null, "known_identity": string|null, "word_ranges": [{"word_start": int, "word_end": int}]}
   "word_ranges" is optional but recommended — array of {word_start, word_end} marking where this narrator's name appears in word_analysis (half-open indexing, same as chunk word ranges). This enables clickable narrator highlighting in the UI.
9. "translations": Object with keys en, ur, tr, fa, id, bn, es, fr, de, ru, zh. Each:
   {"text": "...", "summary": "...", "key_terms": {"arabic_term": "explanation"}, "seo_question": "..."}
   SUMMARY GUIDANCE: The "summary" should be 2-3 sentences explaining the verse's meaning and significance. Where relevant, note the historical context — who the audience was, what circumstances prompted this teaching, and how the original audience would have understood the key terms.
10. "chunks": (array) Paragraph-level segmentation of the text with aligned translations.
   Each chunk: {"chunk_type": (enum) "isnad"|"opening"|"body"|"quran_quote"|"closing", "arabic_text": "...", "word_start": int, "word_end": int, "translations": {"en": "...", "ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}}
   CHUNKING RULES:
   - Every text MUST have at least 1 chunk. Even a single short sentence is 1 "body" chunk.
   - Segment at natural boundaries: topic shifts, speaker changes, Quran quotes within hadith, isnad→matn transition, opening formulae (بسم الله, أما بعد), closing supplications.
   - word_start/word_end use Python half-open indexing: word_analysis[start:end] gives the chunk's words.
   - Sequential, non-overlapping, complete: first chunk starts at 0, each chunk starts where the prior ended, last chunk ends at len(word_analysis).
   - Chunk translations are plain text strings (NOT objects with summary/key_terms/seo_question — those stay at verse level in field #9).
   - All 11 language keys are required in each chunk's translations object.
   - CJK CONVENTION: For Chinese (zh), Japanese, and Korean text, chunk translations must NOT assume space-joining. When concatenating chunk translations to reconstruct full text, Chinese text should be joined with empty string (""), not space (" "). Write Chinese chunk translations so they form coherent text when concatenated directly without spaces.
11. "topics": (array of 1-5 strings) Level 2 topic keys from the TOPIC TAXONOMY above.
   Select the most specific and relevant topics that describe this verse/hadith's subject matter.
   Use ONLY keys listed in the taxonomy. Do not invent new topic keys.
12. "key_phrases": (array of 0-5 objects) Multi-word Arabic expressions found in this text.
   Each: {"phrase_ar": "...", "phrase_en": "...", "category": "theological_concept"|"well_known_saying"|"jurisprudential_term"|"quranic_echo"|"prophetic_formula"}
   EXTRACTION RULES:
   - Must be multi-word (2+ words). Single words go in key_terms, not here.
   - NOT generic narrator formulae ("he said", "from him") — those are isnad.
   - Must be specific enough to be meaningful but common enough to appear in multiple hadiths.
   - Include phrases from the KEY PHRASES REFERENCE when they appear in the text.
   - You may extract NEW phrases not in the reference — the reference is a seed, not exhaustive.
   - Empty array [] is valid if no significant phrases are present.
13. "similar_content_hints": (array of 0-3 objects) Thematic hints for finding similar hadiths/verses.
   Each: {"description": "...", "theme": "..."}
   - "description": Brief description of a similar narration or verse (1-2 sentences).
   - "theme": Short thematic tag (lowercase_with_underscores) grouping this hint with similar ones across the corpus.
   - These are UNVERIFIED suggestions based on your knowledge of hadith literature. The post-processing pipeline will verify against the actual corpus.
   - Do NOT guess specific paths or hadith numbers — just describe the content and assign a theme.
   - Empty array [] is valid for texts with no obvious parallels.""")

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
# JSON repair — fix common LLM output issues
# ---------------------------------------------------------------------------


def repair_json_text(raw: str) -> str:
    """Attempt to repair common JSON issues in LLM-generated output.

    Handles:
    - Unescaped double quotes inside string values (Chinese dialogue markers,
      German quotation, etc.)
    - Unescaped newlines/tabs inside strings
    - Trailing commas before closing brackets

    Args:
        raw: Raw text that should be JSON but may have syntax errors.

    Returns:
        Repaired JSON string. Raises ValueError if unrepairable.
    """
    # First try parsing as-is
    try:
        json.loads(raw)
        return raw
    except json.JSONDecodeError:
        pass

    # Strip markdown fences if present
    text = raw.strip()
    if text.startswith("```"):
        first_nl = text.index("\n") if "\n" in text else 3
        text = text[first_nl + 1:]
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3].rstrip()

    # Strategy: scan through the text character by character,
    # tracking whether we're inside a JSON string. When we encounter
    # a quote inside a string that doesn't look like a JSON delimiter,
    # escape it.
    result = []
    i = 0
    in_string = False
    string_start = -1

    while i < len(text):
        c = text[i]

        if not in_string:
            result.append(c)
            if c == '"':
                in_string = True
                string_start = i
        else:
            if c == '\\' and i + 1 < len(text):
                # Escape sequence — pass through
                result.append(c)
                result.append(text[i + 1])
                i += 2
                continue
            elif c == '"':
                # Is this the closing JSON quote or an unescaped interior quote?
                # Look ahead: after a closing JSON quote we expect , : ] } or whitespace
                j = i + 1
                while j < len(text) and text[j] in ' \t\r\n':
                    j += 1
                next_significant = text[j] if j < len(text) else ''

                if next_significant in ',:]}\n' or j >= len(text):
                    # Looks like a proper JSON closing quote
                    result.append(c)
                    in_string = False
                else:
                    # Interior quote — escape it
                    result.append('\\"')
            elif c == '\n':
                result.append('\\n')
            elif c == '\r':
                result.append('\\r')
            elif c == '\t':
                result.append('\\t')
            elif ord(c) < 0x20 and c not in '\n\r\t':
                # Control character — escape it
                result.append(f'\\u{ord(c):04x}')
            else:
                result.append(c)
        i += 1

    repaired = ''.join(result)

    # Remove trailing commas before ] or }
    import re
    repaired = re.sub(r',(\s*[}\]])', r'\1', repaired)

    # Validate
    try:
        json.loads(repaired)
        return repaired
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON repair failed: {e}")


def repair_response_file(filepath: str) -> bool:
    """Attempt to repair a response JSON file in-place.

    Args:
        filepath: Path to the JSON file to repair.

    Returns:
        True if the file was repaired (or was already valid), False if unrepairable.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        raw = f.read()

    try:
        # Already valid?
        data = json.loads(raw)
        return True
    except json.JSONDecodeError:
        pass

    try:
        repaired = repair_json_text(raw)
        data = json.loads(repaired)
        # Re-serialize with proper encoding
        with open(filepath, "w", encoding=JSON_ENCODING) as f:
            json.dump(data, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT)
        logger.info("Repaired JSON: %s", filepath)
        return True
    except (ValueError, json.JSONDecodeError) as e:
        logger.warning("Could not repair %s: %s", filepath, e)
        return False


# ---------------------------------------------------------------------------
# Schema optimization — strip/reconstruct redundant fields
# ---------------------------------------------------------------------------

def strip_redundant_fields(result: dict) -> dict:
    """Remove fields that can be reconstructed from other data.

    Strips 3 categories of duplicate fields to reduce stored file size:
    - diacritized_text (= ' '.join(word_analysis[].word))
    - chunks[].arabic_text (= ' '.join(word_analysis[start:end]))
    - translations[lang].text (= concatenation of chunks[].translations[lang])

    Note: isnad_matn.isnad_ar and matn_ar are NOT stripped because they
    encode independent information that doesn't always map to chunk types
    (e.g., chunks may all be "body" type even when has_chain=True).

    Args:
        result: A validated pipeline result dict (full format).

    Returns:
        A new dict with redundant fields removed.
    """
    result = json.loads(json.dumps(result))  # deep copy
    result.pop("diacritized_text", None)
    for chunk in result.get("chunks", []):
        chunk.pop("arabic_text", None)
    for lang_obj in result.get("translations", {}).values():
        if isinstance(lang_obj, dict):
            lang_obj.pop("text", None)
    return result


def reconstruct_fields(result: dict) -> dict:
    """Reconstruct stripped fields from canonical data sources.

    Restores fields removed by strip_redundant_fields(). Only adds
    fields that are missing — if a field already exists, it is left as-is.

    Reconstructs:
    - diacritized_text from word_analysis[].word
    - chunks[].arabic_text from word_analysis[word_start:word_end]
    - translations[lang].text from chunks[].translations[lang]
    - isnad_matn.isnad_ar/matn_ar from chunk words (fallback only)

    Args:
        result: A pipeline result dict (stripped or full format).

    Returns:
        A new dict with all fields present.
    """
    result = json.loads(json.dumps(result))  # deep copy
    words = result.get("word_analysis", [])
    chunks = result.get("chunks", [])

    # Reconstruct diacritized_text from word_analysis
    if "diacritized_text" not in result and words:
        result["diacritized_text"] = " ".join(w["word"] for w in words)

    # Reconstruct chunks[].arabic_text from word_analysis ranges
    for chunk in chunks:
        if "arabic_text" not in chunk:
            ws = chunk.get("word_start", 0)
            we = chunk.get("word_end", 0)
            chunk["arabic_text"] = " ".join(w["word"] for w in words[ws:we])

    # Reconstruct translations[lang].text from chunk translations
    for lang, obj in result.get("translations", {}).items():
        if isinstance(obj, dict) and "text" not in obj:
            parts = [c.get("translations", {}).get(lang, "") for c in chunks]
            joiner = "" if lang == "zh" else " "
            obj["text"] = joiner.join(parts)

    # Reconstruct isnad_matn.isnad_ar and matn_ar from chunks
    im = result.get("isnad_matn", {})
    if isinstance(im, dict):
        if "isnad_ar" not in im:
            isnad_chunks = [c for c in chunks if c.get("chunk_type") == "isnad"]
            im["isnad_ar"] = " ".join(
                " ".join(w["word"] for w in words[c.get("word_start", 0):c.get("word_end", 0)])
                for c in isnad_chunks
            )
        if "matn_ar" not in im:
            matn_chunks = [c for c in chunks if c.get("chunk_type") != "isnad"]
            im["matn_ar"] = " ".join(
                " ".join(w["word"] for w in words[c.get("word_start", 0):c.get("word_end", 0)])
                for c in matn_chunks
            )

    return result


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_result(result: dict) -> List[str]:
    """Validate a pipeline output against the schema and enum constraints.

    Accepts both full and stripped formats. If stripped (diacritized_text
    missing but word_analysis present), fields are reconstructed first.

    Args:
        result: Parsed JSON dict from the AI pipeline.

    Returns:
        List of error strings. Empty list means validation passed.
    """
    # Auto-reconstruct stripped format before validating
    if "diacritized_text" not in result and "word_analysis" in result:
        result = reconstruct_fields(result)

    errors = []

    # --- Required top-level fields ---
    required_fields = [
        "diacritized_text", "diacritics_status", "diacritics_changes",
        "word_analysis", "tags", "content_type", "related_quran",
        "isnad_matn", "translations", "chunks",
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

    # --- diacritics cross-check: "added"/"validated" should have empty changes ---
    status = result.get("diacritics_status")
    changes = result.get("diacritics_changes", [])
    if status in ("added", "validated") and isinstance(changes, list) and len(changes) > 0:
        errors.append(
            f"diacritics_status is '{status}' but diacritics_changes is non-empty "
            f"({len(changes)} entries) — use 'corrected' or 'completed' instead"
        )

    # --- word_analysis ---
    if "word_analysis" in result:
        if not isinstance(result["word_analysis"], list):
            errors.append(f"word_analysis must be array, got {type(result['word_analysis']).__name__}")
        else:
            for i, word in enumerate(result["word_analysis"]):
                if not isinstance(word, dict):
                    errors.append(f"word_analysis[{i}] must be object")
                    continue
                for wf in ("word", "translation", "pos"):
                    if wf not in word:
                        errors.append(f"word_analysis[{i}] missing field: {wf}")
                # Validate translation object (multilingual word translations)
                if "translation" in word:
                    if not isinstance(word["translation"], dict):
                        errors.append(f"word_analysis[{i}] translation must be object, got {type(word['translation']).__name__}")
                    else:
                        missing_word_langs = VALID_LANGUAGE_KEYS - set(word["translation"].keys())
                        if missing_word_langs:
                            errors.append(f"word_analysis[{i}] translation missing languages: {sorted(missing_word_langs)}")
                if word.get("pos") not in VALID_POS_TAGS:
                    errors.append(f"invalid pos: {word.get('pos')} for word {word.get('word', '?')}")
                # Validate word is fully diacritized (contains tashkeel marks)
                # Skip check for punctuation-only words (no Arabic letters)
                if "word" in word and isinstance(word["word"], str):
                    _DIACRITIC_MARKS = set("\u064B\u064C\u064D\u064E\u064F\u0650\u0651\u0652\u0670")  # tanwin, fatha, damma, kasra, shadda, sukun, superscript alef
                    _ARABIC_LETTER_RANGE = range(0x0621, 0x064B)  # Arabic letters (alef..ya)
                    has_arabic_letter = any(ord(ch) in _ARABIC_LETTER_RANGE for ch in word["word"])
                    if has_arabic_letter and not any(ch in _DIACRITIC_MARKS for ch in word["word"]):
                        errors.append(f"word_analysis[{i}] word '{word['word']}' has no diacritics (must be fully diacritized)")

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

    # --- content_type ---
    if "content_type" in result:
        if result["content_type"] not in VALID_CONTENT_TYPES:
            errors.append(f"invalid content_type: {result['content_type']}")

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
                elif QURAN_SURAH_AYAH_COUNTS:
                    surah_num = int(parts[0])
                    ayah_num = int(parts[1])
                    max_ayas = QURAN_SURAH_AYAH_COUNTS.get(surah_num)
                    if max_ayas and not (1 <= ayah_num <= max_ayas):
                        errors.append(
                            f"invalid ayah number: {ayah_num} exceeds max {max_ayas} "
                            f"for surah {surah_num} in ref {ref}"
                        )
                # Validate optional word_start/word_end for explicit refs
                if "word_start" in ref_obj or "word_end" in ref_obj:
                    ws = ref_obj.get("word_start")
                    we = ref_obj.get("word_end")
                    if ws is not None and not isinstance(ws, int):
                        errors.append(f"related_quran word_start must be int for ref {ref}")
                    if we is not None and not isinstance(we, int):
                        errors.append(f"related_quran word_end must be int for ref {ref}")
                    if isinstance(ws, int) and isinstance(we, int):
                        if ws < 0:
                            errors.append(f"related_quran word_start < 0 for ref {ref}")
                        if we <= ws:
                            errors.append(f"related_quran word_end <= word_start for ref {ref}")
                        word_count = len(result.get("word_analysis", []))
                        if word_count and we > word_count:
                            errors.append(f"related_quran word_end {we} exceeds word_analysis length {word_count} for ref {ref}")

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
                # Validate optional word_ranges
                if "word_ranges" in narrator:
                    wr = narrator["word_ranges"]
                    if not isinstance(wr, list):
                        errors.append(f"narrator[{i}] word_ranges must be array")
                    else:
                        word_count = len(result.get("word_analysis", []))
                        for j, rng in enumerate(wr):
                            if not isinstance(rng, dict):
                                errors.append(f"narrator[{i}] word_ranges[{j}] must be object")
                                continue
                            ws = rng.get("word_start")
                            we = rng.get("word_end")
                            if not isinstance(ws, int) or not isinstance(we, int):
                                errors.append(f"narrator[{i}] word_ranges[{j}] word_start/word_end must be int")
                            elif we <= ws:
                                errors.append(f"narrator[{i}] word_ranges[{j}] word_end ({we}) must be > word_start ({ws})")
                            elif word_count > 0 and we > word_count:
                                errors.append(f"narrator[{i}] word_ranges[{j}] word_end ({we}) exceeds word_analysis length ({word_count})")

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
                # Validate key_terms is a dict with Arabic keys
                kt = lang_data.get("key_terms")
                if kt is not None:
                    if not isinstance(kt, dict):
                        errors.append(
                            f"translations.{lang_key}.key_terms must be dict, "
                            f"got {type(kt).__name__}"
                        )
                    else:
                        for kt_key in kt:
                            if not any(
                                "\u0600" <= ch <= "\u06FF" for ch in str(kt_key)
                            ):
                                errors.append(
                                    f"translations.{lang_key}.key_terms key "
                                    f"'{kt_key}' has no Arabic characters"
                                )

    # --- chunks ---
    if "chunks" in result:
        if not isinstance(result["chunks"], list):
            errors.append(f"chunks must be array, got {type(result['chunks']).__name__}")
        elif len(result["chunks"]) == 0:
            errors.append("chunks must have at least 1 entry")
        else:
            word_count = len(result.get("word_analysis", []))
            chunk_required_fields = ("chunk_type", "arabic_text", "word_start", "word_end", "translations")
            for i, chunk in enumerate(result["chunks"]):
                if not isinstance(chunk, dict):
                    errors.append(f"chunks[{i}] must be object")
                    continue
                for cf in chunk_required_fields:
                    if cf not in chunk:
                        errors.append(f"chunks[{i}] missing field: {cf}")
                if chunk.get("chunk_type") not in VALID_CHUNK_TYPES:
                    errors.append(f"chunks[{i}] invalid chunk_type: {chunk.get('chunk_type')}")
                ws = chunk.get("word_start")
                we = chunk.get("word_end")
                if isinstance(ws, int) and isinstance(we, int):
                    if we <= ws:
                        errors.append(f"chunks[{i}] word_end ({we}) must be greater than word_start ({ws})")
                    if we > word_count:
                        errors.append(f"chunks[{i}] word_end ({we}) exceeds word_analysis length ({word_count})")
                # Validate chunk translations
                if "translations" in chunk:
                    if not isinstance(chunk["translations"], dict):
                        errors.append(f"chunks[{i}] translations must be object")
                    else:
                        missing_chunk_langs = VALID_LANGUAGE_KEYS - set(chunk["translations"].keys())
                        if missing_chunk_langs:
                            errors.append(f"chunks[{i}] translations missing languages: {sorted(missing_chunk_langs)}")
                        for lang_key, lang_val in chunk["translations"].items():
                            if lang_key in VALID_LANGUAGE_KEYS and not isinstance(lang_val, str):
                                errors.append(f"chunks[{i}] translations.{lang_key} must be string, got {type(lang_val).__name__}")
            # Sequential coverage checks
            first_chunk = result["chunks"][0]
            if isinstance(first_chunk.get("word_start"), int) and first_chunk["word_start"] != 0:
                errors.append(f"chunks[0] word_start must be 0, got {first_chunk['word_start']}")
            for i in range(1, len(result["chunks"])):
                prev_end = result["chunks"][i - 1].get("word_end")
                curr_start = result["chunks"][i].get("word_start")
                if isinstance(prev_end, int) and isinstance(curr_start, int) and curr_start != prev_end:
                    errors.append(
                        f"chunks[{i}] word_start ({curr_start}) must equal "
                        f"chunks[{i-1}] word_end ({prev_end})"
                    )
            last_chunk = result["chunks"][-1]
            if isinstance(last_chunk.get("word_end"), int) and last_chunk["word_end"] != word_count:
                errors.append(
                    f"last chunk word_end ({last_chunk['word_end']}) must equal "
                    f"word_analysis length ({word_count})"
                )

    # --- topics (optional field, validated if present) ---
    if "topics" in result:
        if not isinstance(result["topics"], list):
            errors.append(f"topics must be array, got {type(result['topics']).__name__}")
        else:
            if len(result["topics"]) < 1 or len(result["topics"]) > 5:
                errors.append(f"topics must have 1-5 items, got {len(result['topics'])}")
            if VALID_TOPICS:
                for topic in result["topics"]:
                    if topic not in VALID_TOPICS:
                        errors.append(f"invalid topic: {topic}")

    # --- key_phrases (optional field, validated if present) ---
    if "key_phrases" in result:
        if not isinstance(result["key_phrases"], list):
            errors.append(f"key_phrases must be array, got {type(result['key_phrases']).__name__}")
        else:
            if len(result["key_phrases"]) > 5:
                errors.append(f"key_phrases must have 0-5 items, got {len(result['key_phrases'])}")
            for i, phrase in enumerate(result["key_phrases"]):
                if not isinstance(phrase, dict):
                    errors.append(f"key_phrases[{i}] must be object")
                    continue
                for pf in ("phrase_ar", "phrase_en", "category"):
                    if pf not in phrase:
                        errors.append(f"key_phrases[{i}] missing field: {pf}")
                if phrase.get("category") not in VALID_PHRASE_CATEGORIES:
                    errors.append(f"key_phrases[{i}] invalid category: {phrase.get('category')}")
                # Validate phrase_ar is multi-word (2+ words)
                phrase_ar = phrase.get("phrase_ar", "")
                if isinstance(phrase_ar, str) and phrase_ar.strip():
                    word_count_phrase = len(phrase_ar.strip().split())
                    if word_count_phrase < 2:
                        errors.append(f"key_phrases[{i}] phrase_ar must be multi-word (2+ words), got {word_count_phrase}")

    # --- similar_content_hints (optional field, validated if present) ---
    if "similar_content_hints" in result:
        if not isinstance(result["similar_content_hints"], list):
            errors.append(f"similar_content_hints must be array, got {type(result['similar_content_hints']).__name__}")
        else:
            if len(result["similar_content_hints"]) > 3:
                errors.append(f"similar_content_hints must have 0-3 items, got {len(result['similar_content_hints'])}")
            for i, hint in enumerate(result["similar_content_hints"]):
                if not isinstance(hint, dict):
                    errors.append(f"similar_content_hints[{i}] must be object")
                    continue
                for hf in ("description", "theme"):
                    if hf not in hint:
                        errors.append(f"similar_content_hints[{i}] missing field: {hf}")

    return errors


def validate_wrapper(wrapper: dict) -> List[str]:
    """Validate the outer wrapper format of a pipeline response file.

    Checks:
    - Required fields: verse_path, ai_attribution, result
    - ai_attribution has required sub-fields
    - generation_attempts does not exceed MAX_GENERATION_ATTEMPTS

    Args:
        wrapper: The full response file dict.

    Returns:
        List of error strings. Empty means valid.
    """
    errors: List[str] = []

    if not isinstance(wrapper, dict):
        return [f"wrapper must be object, got {type(wrapper).__name__}"]

    for field in ("verse_path", "ai_attribution", "result"):
        if field not in wrapper:
            errors.append(f"wrapper missing field: {field}")

    attr = wrapper.get("ai_attribution")
    if isinstance(attr, dict):
        for af in ("model", "generated_date", "pipeline_version", "generation_method"):
            if af not in attr:
                errors.append(f"ai_attribution missing field: {af}")

    attempts = wrapper.get("generation_attempts")
    if attempts is not None:
        if not isinstance(attempts, int) or attempts < 1:
            errors.append(f"generation_attempts must be a positive integer, got {attempts}")
        elif attempts > MAX_GENERATION_ATTEMPTS:
            errors.append(
                f"generation_attempts ({attempts}) exceeds max ({MAX_GENERATION_ATTEMPTS}) — "
                f"verse should be quarantined"
            )

    return errors


# ---------------------------------------------------------------------------
# Corpus manifest generation
# ---------------------------------------------------------------------------

def generate_corpus_manifest(
    data_dir: Optional[str] = None,
    book_filter: Optional[str] = None,
    volume_filter: Optional[int] = None,
    range_start: Optional[int] = None,
    range_end: Optional[int] = None,
) -> dict:
    """Walk ThaqalaynData/books/ and build a manifest of all verse paths.

    Args:
        data_dir: Base data directory. Defaults to DEFAULT_DESTINATION_DIR.
        book_filter: Only include this book (e.g., "al-kafi", "quran").
        volume_filter: Only include this volume (requires book_filter).
        range_start: Start index (1-based) for slicing the verse list.
        range_end: End index (1-based, inclusive) for slicing the verse list.

    Returns:
        Dict with "total", "verses" (list of {"path", "book"}).
    """
    if data_dir is None:
        data_dir = DEFAULT_DESTINATION_DIR

    books_dir = os.path.join(data_dir, "books")
    if not os.path.isdir(books_dir):
        logger.warning("Books directory not found: %s", books_dir)
        return {"total": 0, "verses": []}

    verses: List[Dict[str, str]] = []

    for root, _dirs, files in os.walk(books_dir):
        # Skip 'complete' directory and 'index' directory
        rel = os.path.relpath(root, data_dir).replace("\\", "/")
        if "/complete" in rel or rel.startswith("books/complete"):
            continue

        for fname in sorted(files):
            if not fname.endswith(".json"):
                continue

            filepath = os.path.join(root, fname)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            content = data.get("data", data)
            file_verses = content.get("verses", [])
            if not file_verses:
                continue

            for v in file_verses:
                path = v.get("path", "")
                if not path:
                    continue
                book = path.replace("/books/", "").split(":")[0] if path.startswith("/books/") else ""

                # Apply filters
                if book_filter and book != book_filter:
                    continue
                if volume_filter and book_filter:
                    parts = path.replace("/books/", "").split(":")
                    if len(parts) >= 2 and parts[1].isdigit():
                        if int(parts[1]) != volume_filter:
                            continue

                verses.append({"path": path, "book": book})

    # Apply range filter
    if range_start is not None or range_end is not None:
        start = (range_start or 1) - 1  # convert to 0-based
        end = range_end or len(verses)
        verses = verses[start:end]

    return {"total": len(verses), "verses": verses}


# ---------------------------------------------------------------------------
# Per-verse stats & corpus progress
# ---------------------------------------------------------------------------


def write_verse_stats(
    verse_id: str,
    stats_dict: dict,
    stats_dir: Optional[str] = None,
) -> str:
    """Write stats for a single verse to its own file (race-condition-free).

    Each agent writes to ``stats_dir/{verse_id}.stats.json`` so there is no
    contention on a shared file when running 10+ parallel agents.

    Args:
        verse_id: E.g. ``al-kafi_1_1_1_1``.
        stats_dict: Dict of metrics for this verse (see ai-generate step 8).
        stats_dir: Directory for per-verse stats files.
            Defaults to ``ai-content/{subdir}/stats/``.

    Returns:
        Absolute path of the written stats file.
    """
    import time

    if stats_dir is None:
        stats_dir = os.path.join(AI_CONTENT_DIR, AI_CONTENT_SUBDIR, "stats")
    os.makedirs(stats_dir, exist_ok=True)

    stats_dict.setdefault("verse_id", verse_id)
    stats_dict.setdefault("stats_recorded_at", time.strftime("%Y-%m-%dT%H:%M:%S"))

    out_path = os.path.join(stats_dir, f"{verse_id}.stats.json")
    with open(out_path, "w", encoding=JSON_ENCODING) as f:
        json.dump(stats_dict, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT)

    return out_path


def merge_stats(
    stats_dir: Optional[str] = None,
    output_path: Optional[str] = None,
) -> dict:
    """Merge all per-verse stats files into a single ``generation_stats.json``.

    This is called by the orchestrator periodically — never by individual
    generation agents — so there is no write contention.

    Args:
        stats_dir: Directory containing ``*.stats.json`` files.
            Defaults to ``ai-content/{subdir}/stats/``.
        output_path: Where to write the merged file.
            Defaults to ``ai-content/{subdir}/generation_stats.json``.

    Returns:
        Merged stats dict with ``total_hadiths``, ``stats``, etc.
    """
    import time

    if stats_dir is None:
        stats_dir = os.path.join(AI_CONTENT_DIR, AI_CONTENT_SUBDIR, "stats")
    if output_path is None:
        output_path = os.path.join(
            AI_CONTENT_DIR, AI_CONTENT_SUBDIR, "generation_stats.json"
        )

    merged: Dict[str, Any] = {}

    if os.path.isdir(stats_dir):
        for fname in sorted(os.listdir(stats_dir)):
            if not fname.endswith(".stats.json"):
                continue
            fpath = os.path.join(stats_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    entry = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
            vid = entry.get("verse_id", fname.replace(".stats.json", ""))
            merged[vid] = entry

    result = {
        "generated_at": time.strftime("%Y-%m-%d"),
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "total_hadiths": len(merged),
        "stats": merged,
    }

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding=JSON_ENCODING) as f:
        json.dump(result, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT)

    return result


def compute_remaining(
    manifest_path: Optional[str] = None,
    responses_dir: Optional[str] = None,
    sort_by_word_count: bool = False,
) -> List[Dict[str, Any]]:
    """Compute verse paths that still need generation (resume-safe).

    Diffs the corpus manifest against existing response files on disk.

    Args:
        manifest_path: Path to ``corpus_manifest.json``.
            Defaults to ``ai-pipeline-data/corpus_manifest.json``.
        responses_dir: Directory containing response JSON files.
            Defaults to ``AI_RESPONSES_DIR`` (``ai-content/{subdir}/responses/``).
        sort_by_word_count: If True, sort by estimated word count (short first).

    Returns:
        List of manifest entry dicts (``{"path": ..., "book": ...}``)
        for verses that do not yet have a response file.
    """
    if manifest_path is None:
        manifest_path = os.path.join(AI_PIPELINE_DATA_DIR, "corpus_manifest.json")
    if responses_dir is None:
        responses_dir = AI_RESPONSES_DIR

    if not os.path.isfile(manifest_path):
        logger.warning("Manifest not found: %s", manifest_path)
        return []

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    # Build set of existing verse IDs from response filenames
    existing: set = set()
    if os.path.isdir(responses_dir):
        for fname in os.listdir(responses_dir):
            if fname.endswith(".json"):
                existing.add(fname[:-5])  # strip .json

    remaining = []
    for entry in manifest.get("verses", []):
        path = entry.get("path", "")
        verse_id = path.replace("/books/", "").replace(":", "_")
        if verse_id not in existing:
            remaining.append(entry)

    if sort_by_word_count:
        # Estimate word count from the path depth as a proxy
        # (actual word count requires loading verse data, too slow for 40k)
        # Instead sort by book then path length (shorter paths = shorter texts)
        remaining.sort(key=lambda e: (e.get("book", ""), len(e.get("path", ""))))

    return remaining


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


# ===========================================================================
# BATCH API FUNCTIONS (not currently used — requires Anthropic API key)
# For current workflow, use Claude Code agents. See .claude/agents/.
# ===========================================================================


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
    word_dict = load_word_dictionary()
    taxonomy = load_topic_taxonomy()
    phrases = load_key_phrases_dictionary()
    system_prompt = build_system_prompt(glossary, few_shot, word_dict, taxonomy, phrases)

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

# (Also Batch API only — see comment above write_request_jsonl)
def estimate_cost(num_verses: int = 46857) -> dict:
    """Estimate the total pipeline cost for a given number of verses.

    Uses the multi-language approach from AI_CONTENT_PIPELINE.md Section 10.

    Args:
        num_verses: Total verses to process.

    Returns:
        Cost breakdown dict.
    """
    # Generation (Opus 4.6 Batch)
    # Updated for 11 languages (en added) + multilingual word translations
    gen_input_per_req = 3150
    gen_output_per_req = 5325  # was 5200; +125 for chunks field (field #10)
    gen_total_input = num_verses * gen_input_per_req
    gen_total_output = num_verses * gen_output_per_req
    gen_input_cost = (gen_total_input / 1_000_000) * 2.50
    gen_output_cost = (gen_total_output / 1_000_000) * 12.50
    gen_total = gen_input_cost + gen_output_cost

    # Validation (Sonnet 4.6 Batch, per-language)
    val_requests = num_verses * 11  # 11 languages including English
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
        "--output", default=os.path.join(AI_CONTENT_DIR, "samples", "requests", "sample_requests.jsonl"),
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
        "--dir", default=os.path.join(AI_CONTENT_DIR, "samples", "responses"),
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

    # manifest
    manifest_parser = subparsers.add_parser(
        "manifest",
        help="Generate corpus manifest (list of all verse paths)"
    )
    manifest_parser.add_argument(
        "--data-dir", default=DEFAULT_DESTINATION_DIR,
        help="ThaqalaynData directory"
    )
    manifest_parser.add_argument(
        "--book", default=None,
        help="Filter by book (e.g., al-kafi, quran)"
    )
    manifest_parser.add_argument(
        "--volume", type=int, default=None,
        help="Filter by volume (requires --book)"
    )
    manifest_parser.add_argument(
        "--range", default=None, dest="range_str",
        help="Slice range (e.g., 1-100)"
    )
    manifest_parser.add_argument(
        "--output", default=os.path.join(AI_PIPELINE_DATA_DIR, "corpus_manifest.json"),
        help="Output manifest file path"
    )

    # repair
    repair_parser = subparsers.add_parser(
        "repair",
        help="Repair malformed JSON in response files"
    )
    repair_parser.add_argument(
        "--dir", default=AI_RESPONSES_DIR,
        help="Directory containing response JSON files"
    )

    # merge-stats
    subparsers.add_parser(
        "merge-stats",
        help="Merge per-verse stats files into generation_stats.json"
    )

    # remaining
    rem_parser = subparsers.add_parser(
        "remaining",
        help="Show remaining verses that need generation"
    )
    rem_parser.add_argument(
        "--sort", action="store_true",
        help="Sort remaining by estimated complexity (short first)"
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "repair":
        repair_dir = args.dir
        if not os.path.isdir(repair_dir):
            print(f"Directory not found: {repair_dir}")
            sys.exit(1)
        repaired = 0
        failed = 0
        already_valid = 0
        for fname in sorted(os.listdir(repair_dir)):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(repair_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    json.load(f)
                already_valid += 1
            except json.JSONDecodeError:
                if repair_response_file(fpath):
                    repaired += 1
                    print(f"  REPAIRED: {fname}")
                else:
                    failed += 1
                    print(f"  FAILED: {fname}")
        print(f"Already valid: {already_valid}, Repaired: {repaired}, Failed: {failed}")

    elif args.command == "generate-requests":
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

    elif args.command == "manifest":
        range_start = None
        range_end = None
        if args.range_str:
            parts = args.range_str.split("-")
            range_start = int(parts[0])
            range_end = int(parts[1]) if len(parts) > 1 else range_start

        manifest = generate_corpus_manifest(
            data_dir=args.data_dir,
            book_filter=args.book,
            volume_filter=args.volume,
            range_start=range_start,
            range_end=range_end,
        )

        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", encoding=JSON_ENCODING) as f:
            json.dump(manifest, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT)
        print(f"Manifest: {manifest['total']} verses -> {args.output}")

    elif args.command == "merge-stats":
        result = merge_stats()
        print(f"Merged {result['total_hadiths']} verse stats -> generation_stats.json")

    elif args.command == "remaining":
        remaining = compute_remaining(sort_by_word_count=getattr(args, "sort", False))
        total_manifest = 0
        manifest_path = os.path.join(AI_PIPELINE_DATA_DIR, "corpus_manifest.json")
        if os.path.isfile(manifest_path):
            with open(manifest_path, "r", encoding="utf-8") as f:
                total_manifest = json.load(f).get("total", 0)
        done = total_manifest - len(remaining)
        pct = (done / total_manifest * 100) if total_manifest else 0
        print(f"{done}/{total_manifest} done ({pct:.1f}%), {len(remaining)} remaining")
        # Show breakdown by book
        book_counts: Dict[str, int] = {}
        for entry in remaining:
            b = entry.get("book", "unknown")
            book_counts[b] = book_counts.get(b, 0) + 1
        for book, count in sorted(book_counts.items(), key=lambda x: -x[1]):
            print(f"  {book}: {count}")


if __name__ == "__main__":
    main()
